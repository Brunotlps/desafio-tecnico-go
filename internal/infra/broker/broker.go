// Package broker provisiona o ambiente de mensageria (filas SQS FIFO, tópico SNS FIFO, identidades IAM e
// políticas) em um emulador compatível com a API da AWS (MiniStack com AUTH=true) e distribui as
// credenciais por responsabilidade.
//
// O provisionamento é idempotente: recursos existentes são convergidos para a configuração desejada,
// a assinatura do tópico não é duplicada, usuários e políticas são reaplicados e uma chave de acesso
// já distribuída (e ainda válida no emulador) é reaproveitada, de modo que reexecutar não quebra
// credenciais em uso.
//
// Limite do emulador: AUTH=true avalia as políticas do principal identificado pela access key, mas
// NÃO valida assinaturas SigV4 (o segredo não é conferido). Isto demonstra AUTORIZAÇÃO emulada por
// políticas, não autenticação AWS completa.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
)

// Role é a responsabilidade de um principal.
type Role string

const (
	// Producer é o produtor interno confiável: só envia à fila de entrada.
	Producer Role = "producer"
	// Runtime é o consumidor: recebe, altera a visibilidade e remove da entrada; envia à DLQ.
	Runtime Role = "runtime"
	// Publisher publica no tópico de saída (e o descreve); não consome a entrada.
	Publisher Role = "publisher"
	// Inspector observa/prepara cenários nas filas de DLQ e de auditoria (testes e diagnóstico).
	Inspector Role = "inspector"
	// Nobody não tem política alguma: serve para comprovar AccessDenied.
	Nobody Role = "nobody"
)

// Roles lista as responsabilidades na ordem de provisionamento.
var Roles = []Role{Producer, Runtime, Publisher, Inspector, Nobody}

// Config parametriza o provisionamento.
type Config struct {
	Endpoint string // URL do emulador
	Region   string
	// Credenciais do principal de PROVISIONAMENTO (raiz do emulador). Só esta etapa as usa.
	AdminAccessKeyID, AdminSecretAccessKey string
	// Prefix isola um conjunto de recursos e identidades (vazio no ambiente normal; a prova usa um
	// prefixo único por execução).
	Prefix string
	// InputVisibility e InputMaxReceive substituem os padrões aprovados (60 s e 5 recebimentos) em
	// recursos isolados de prova; zero mantém os padrões.
	InputVisibility int
	InputMaxReceive int
	// CredentialsFile recebe as credenciais por responsabilidade (um perfil por usuário).
	CredentialsFile string
}

// Padrões aprovados da fila de entrada.
const (
	DefaultVisibility = 60
	DefaultMaxReceive = 5
)

// Names são os nomes dos recursos e dos usuários.
type Names struct {
	Input, DLQ, Topic, Audit string
	Users                    map[Role]string
}

// ResourceNames devolve os nomes para o prefixo.
func ResourceNames(prefix string) Names {
	n := Names{
		Input: prefix + "wager-transactions.fifo", DLQ: prefix + "wager-transactions-dlq.fifo",
		Topic: prefix + "wager-events.fifo", Audit: prefix + "wager-events-audit.fifo",
		Users: map[Role]string{},
	}
	for _, r := range Roles {
		n.Users[r] = prefix + "wager-" + string(r)
	}
	return n
}

// Resources descreve o que foi provisionado.
type Resources struct {
	Names                                Names
	Account                              string
	InputURL, DLQURL, AuditURL           string
	InputARN, DLQARN, AuditARN, TopicARN string
	SubscriptionARN                      string
	// Profiles mapeia a responsabilidade ao perfil (= nome do usuário) no arquivo de credenciais.
	Profiles map[Role]string
	// NewCredentials indica os perfis cuja chave foi (re)emitida nesta execução.
	NewCredentials []Role
}

type clients struct {
	sqs *sqs.Client
	sns *sns.Client
	iam *iam.Client
}

func adminClients(c Config) clients {
	cfg := aws.Config{
		Region:       c.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(c.AdminAccessKeyID, c.AdminSecretAccessKey, ""),
		BaseEndpoint: aws.String(c.Endpoint),
	}
	return clients{sqs: sqs.NewFromConfig(cfg), sns: sns.NewFromConfig(cfg), iam: iam.NewFromConfig(cfg)}
}

func hasCode(err error, codes ...string) bool {
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		return false
	}
	for _, c := range codes {
		if strings.Contains(ae.ErrorCode(), c) {
			return true
		}
	}
	return false
}

// Provision cria ou converge todo o ambiente e (re)escreve o arquivo de credenciais.
func Provision(ctx context.Context, c Config) (Resources, error) {
	if c.Endpoint == "" || c.Region == "" || c.CredentialsFile == "" {
		return Resources{}, errors.New("broker: endpoint, region and credentials file are required")
	}
	if c.AdminAccessKeyID == "" {
		return Resources{}, errors.New("broker: administrator credentials are required")
	}
	vis, maxRecv := c.InputVisibility, c.InputMaxReceive
	if vis == 0 {
		vis = DefaultVisibility
	}
	if maxRecv == 0 {
		maxRecv = DefaultMaxReceive
	}
	cl := adminClients(c)
	names := ResourceNames(c.Prefix)
	r := Resources{Names: names, Profiles: map[Role]string{}}

	// --- Filas: DLQ primeiro (o redrive precisa do seu ARN) -------------------------------------
	var err error
	if r.DLQURL, r.DLQARN, err = cl.ensureQueue(ctx, names.DLQ, map[string]string{
		"FifoQueue": "true", "ContentBasedDeduplication": "false", "MessageRetentionPeriod": "1209600", // 14 dias
	}); err != nil {
		return r, err
	}
	redrive, _ := json.Marshal(map[string]string{"deadLetterTargetArn": r.DLQARN, "maxReceiveCount": fmt.Sprint(maxRecv)})
	if r.InputURL, r.InputARN, err = cl.ensureQueue(ctx, names.Input, map[string]string{
		"FifoQueue": "true", "ContentBasedDeduplication": "false", "VisibilityTimeout": fmt.Sprint(vis),
		"MessageRetentionPeriod": "345600", "RedrivePolicy": string(redrive),
	}); err != nil {
		return r, err
	}
	r.Account = accountOf(r.InputARN)

	// --- Saída: tópico FIFO e fila de auditoria assinante ------------------------------------------
	topic, err := cl.sns.CreateTopic(ctx, &sns.CreateTopicInput{
		Name:       aws.String(names.Topic),
		Attributes: map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "false"},
	})
	if err != nil {
		return r, fmt.Errorf("create topic %s: %w", names.Topic, err)
	}
	r.TopicARN = aws.ToString(topic.TopicArn)
	auditARN := fmt.Sprintf("arn:aws:sqs:%s:%s:%s", c.Region, r.Account, names.Audit)
	// Só o serviço SNS, e só a partir deste tópico (aws:SourceArn), entrega na fila de auditoria.
	deliveryPolicy := mustJSON(policyDoc{Version: "2012-10-17", Statement: []statement{{
		Sid: "AllowDeliveryFromTheEventTopicOnly", Effect: "Allow",
		Principal: map[string]string{"Service": "sns.amazonaws.com"},
		Action:    []string{"sqs:SendMessage"}, Resource: []string{auditARN},
		Condition: map[string]map[string]string{"ArnEquals": {"aws:SourceArn": r.TopicARN}},
	}}})
	if r.AuditURL, r.AuditARN, err = cl.ensureQueue(ctx, names.Audit, map[string]string{
		"FifoQueue": "true", "ContentBasedDeduplication": "false", "Policy": deliveryPolicy,
	}); err != nil {
		return r, err
	}
	if r.SubscriptionARN, err = cl.ensureSubscription(ctx, r.TopicARN, r.AuditARN); err != nil {
		return r, err
	}

	// --- Identidades, políticas e credenciais -------------------------------------------------------
	existing, _ := ReadCredentials(c.CredentialsFile)
	issued := map[string]Credentials{}
	for _, role := range Roles {
		user := names.Users[role]
		r.Profiles[role] = user
		if err := cl.ensureUser(ctx, user); err != nil {
			return r, err
		}
		if doc := policyFor(role, r); doc != "" {
			if _, err := cl.iam.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
				UserName: aws.String(user), PolicyName: aws.String("wager-" + string(role)), PolicyDocument: aws.String(doc),
			}); err != nil {
				return r, fmt.Errorf("put policy of %s: %w", user, err)
			}
		}
		cred, fresh, err := cl.ensureAccessKey(ctx, user, existing[user])
		if err != nil {
			return r, err
		}
		issued[user] = cred
		if fresh {
			r.NewCredentials = append(r.NewCredentials, role)
		}
	}
	if err := WriteCredentials(c.CredentialsFile, issued); err != nil {
		return r, err
	}
	return r, nil
}

// Destroy remove (melhor esforço) os recursos e as identidades de um conjunto isolado de prova.
func Destroy(ctx context.Context, c Config) {
	cl := adminClients(c)
	names := ResourceNames(c.Prefix)
	for _, q := range []string{names.Input, names.DLQ, names.Audit} {
		if out, err := cl.sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(q)}); err == nil {
			_, _ = cl.sqs.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: out.QueueUrl})
		}
	}
	if topics, err := cl.sns.ListTopics(ctx, &sns.ListTopicsInput{}); err == nil {
		for _, t := range topics.Topics {
			if strings.HasSuffix(aws.ToString(t.TopicArn), ":"+names.Topic) {
				_, _ = cl.sns.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: t.TopicArn})
			}
		}
	}
	for _, user := range names.Users {
		if keys, err := cl.iam.ListAccessKeys(ctx, &iam.ListAccessKeysInput{UserName: aws.String(user)}); err == nil {
			for _, k := range keys.AccessKeyMetadata {
				_, _ = cl.iam.DeleteAccessKey(ctx, &iam.DeleteAccessKeyInput{UserName: aws.String(user), AccessKeyId: k.AccessKeyId})
			}
		}
		if pols, err := cl.iam.ListUserPolicies(ctx, &iam.ListUserPoliciesInput{UserName: aws.String(user)}); err == nil {
			for _, p := range pols.PolicyNames {
				_, _ = cl.iam.DeleteUserPolicy(ctx, &iam.DeleteUserPolicyInput{UserName: aws.String(user), PolicyName: aws.String(p)})
			}
		}
		_, _ = cl.iam.DeleteUser(ctx, &iam.DeleteUserInput{UserName: aws.String(user)})
	}
}

func accountOf(arn string) string {
	if parts := strings.Split(arn, ":"); len(parts) > 4 {
		return parts[4]
	}
	return ""
}

func (cl clients) ensureQueue(ctx context.Context, name string, attrs map[string]string) (url, arn string, err error) {
	out, err := cl.sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	switch {
	case err == nil:
		url = aws.ToString(out.QueueUrl)
		mutable := map[string]string{}
		for k, v := range attrs {
			if k != "FifoQueue" { // imutável
				mutable[k] = v
			}
		}
		if _, err := cl.sqs.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: aws.String(url), Attributes: mutable}); err != nil {
			return "", "", fmt.Errorf("converge queue %s: %w", name, err)
		}
	case hasCode(err, "NonExistentQueue", "QueueDoesNotExist"):
		created, err := cl.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
		if err != nil {
			return "", "", fmt.Errorf("create queue %s: %w", name, err)
		}
		url = aws.ToString(created.QueueUrl)
	default:
		return "", "", fmt.Errorf("look up queue %s: %w", name, err)
	}
	got, err := cl.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(url), AttributeNames: []sqsAttr{"QueueArn"}})
	if err != nil {
		return "", "", fmt.Errorf("read queue arn %s: %w", name, err)
	}
	return url, got.Attributes["QueueArn"], nil
}

func (cl clients) ensureSubscription(ctx context.Context, topicARN, queueARN string) (string, error) {
	var subARN string
	var next *string
	for {
		page, err := cl.sns.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(topicARN), NextToken: next})
		if err != nil {
			return "", fmt.Errorf("list subscriptions of %s: %w", topicARN, err)
		}
		for _, s := range page.Subscriptions {
			if aws.ToString(s.Endpoint) == queueARN && aws.ToString(s.Protocol) == "sqs" {
				subARN = aws.ToString(s.SubscriptionArn)
			}
		}
		if page.NextToken == nil {
			break
		}
		next = page.NextToken
	}
	if subARN == "" {
		out, err := cl.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(topicARN), Protocol: aws.String("sqs"), Endpoint: aws.String(queueARN)})
		if err != nil {
			return "", fmt.Errorf("subscribe %s to %s: %w", queueARN, topicARN, err)
		}
		subARN = aws.ToString(out.SubscriptionArn)
	}
	if _, err := cl.sns.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(subARN), AttributeName: aws.String("RawMessageDelivery"), AttributeValue: aws.String("true"),
	}); err != nil {
		return "", fmt.Errorf("enable raw delivery: %w", err)
	}
	return subARN, nil
}

func (cl clients) ensureUser(ctx context.Context, user string) error {
	if _, err := cl.iam.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String(user)}); err != nil && !hasCode(err, "EntityAlreadyExists") {
		return fmt.Errorf("create user %s: %w", user, err)
	}
	return nil
}

// ensureAccessKey reaproveita a chave já distribuída se ela ainda existe no emulador; caso contrário
// emite uma nova e remove as chaves desconhecidas do usuário (cujo segredo não temos).
func (cl clients) ensureAccessKey(ctx context.Context, user string, have Credentials) (cred Credentials, fresh bool, err error) {
	keys, err := cl.iam.ListAccessKeys(ctx, &iam.ListAccessKeysInput{UserName: aws.String(user)})
	if err != nil {
		return Credentials{}, false, fmt.Errorf("list access keys of %s: %w", user, err)
	}
	for _, k := range keys.AccessKeyMetadata {
		if have.AccessKeyID != "" && aws.ToString(k.AccessKeyId) == have.AccessKeyID && have.SecretAccessKey != "" {
			return have, false, nil
		}
	}
	for _, k := range keys.AccessKeyMetadata {
		if _, err := cl.iam.DeleteAccessKey(ctx, &iam.DeleteAccessKeyInput{UserName: aws.String(user), AccessKeyId: k.AccessKeyId}); err != nil {
			return Credentials{}, false, fmt.Errorf("delete stale key of %s: %w", user, err)
		}
	}
	out, err := cl.iam.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String(user)})
	if err != nil {
		return Credentials{}, false, fmt.Errorf("create access key of %s: %w", user, err)
	}
	return Credentials{AccessKeyID: aws.ToString(out.AccessKey.AccessKeyId), SecretAccessKey: aws.ToString(out.AccessKey.SecretAccessKey)}, true, nil
}

// WaitReady espera o emulador responder a uma chamada autenticada (tentativas curtas e limitadas).
func WaitReady(ctx context.Context, c Config, within time.Duration) error {
	cl := adminClients(c)
	deadline := time.Now().Add(within)
	for {
		_, err := cl.sqs.ListQueues(ctx, &sqs.ListQueuesInput{})
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("broker at %s not ready: %w", c.Endpoint, err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
	}
}
