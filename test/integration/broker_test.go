//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"

	"desafio-tecnico-go/internal/infra/broker"
)

// Prova de autorização do broker (MiniStack com AUTH=true) com o SDK Go v2 e as credenciais REAIS de
// cada principal — não basta conferir o JSON das políticas. Cada teste provisiona o seu próprio conjunto
// de recursos e identidades, com prefixo único (isolado do ambiente de desenvolvimento e dos demais
// testes), usando o mesmo código do provisionamento do Compose.
//
// Limite do emulador: AUTH=true avalia políticas, mas NÃO valida assinaturas SigV4 (o segredo não é
// conferido). Isto prova autorização emulada por políticas, não autenticação AWS completa; por isso não
// há testes de "segredo inválido".

type proofEnv struct {
	cfg broker.Config
	res broker.Resources
	t   *testing.T
}

// provisionProof provisiona um conjunto isolado (e o remove ao final).
func provisionProof(t *testing.T, mods ...func(*broker.Config)) *proofEnv {
	t.Helper()
	cfg := broker.Config{
		Endpoint: env.awsEndpoint, Region: env.region, AdminAccessKeyID: env.adminAccessKey, AdminSecretAccessKey: env.adminSecret,
		Prefix: "proof-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8] + "-", CredentialsFile: t.TempDir() + "/credentials",
	}
	for _, m := range mods {
		m(&cfg)
	}
	ctx := context.Background()
	res, err := broker.Provision(ctx, cfg)
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	t.Cleanup(func() { broker.Destroy(context.Background(), cfg) })
	return &proofEnv{cfg: cfg, res: res, t: t}
}

func (p *proofEnv) awsConfig(role broker.Role) aws.Config {
	p.t.Helper()
	c, err := broker.ProfileCredentials(p.cfg.CredentialsFile, p.res.Profiles[role])
	if err != nil {
		p.t.Fatal(err)
	}
	return aws.Config{Region: p.cfg.Region, Credentials: c.Provider(), BaseEndpoint: aws.String(p.cfg.Endpoint)}
}

func (p *proofEnv) sqs(role broker.Role) *sqs.Client { return sqs.NewFromConfig(p.awsConfig(role)) }
func (p *proofEnv) sns(role broker.Role) *sns.Client { return sns.NewFromConfig(p.awsConfig(role)) }

func (p *proofEnv) admin() aws.Config {
	return aws.Config{Region: p.cfg.Region, BaseEndpoint: aws.String(p.cfg.Endpoint),
		Credentials: broker.Credentials{AccessKeyID: p.cfg.AdminAccessKeyID, SecretAccessKey: p.cfg.AdminSecretAccessKey}.Provider()}
}

// requireDenied exige que o BROKER tenha negado a chamada por política (AccessDenied tipado), e não que
// ela tenha falhado por endpoint, credencial inexistente, fila ausente ou timeout.
func requireDenied(t *testing.T, name string, err error) {
	t.Helper()
	var ae smithy.APIError
	if err == nil {
		t.Errorf("%s: the call succeeded, want AccessDenied", name)
		return
	}
	if !errors.As(err, &ae) || !strings.Contains(ae.ErrorCode(), "AccessDenied") {
		t.Errorf("%s: %v, want an AccessDenied from the broker", name, err)
		return
	}
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) && re.HTTPStatusCode() != 403 {
		t.Errorf("%s: AccessDenied with HTTP %d, want 403", name, re.HTTPStatusCode())
	}
}

func send(c *sqs.Client, url, group, dedup, body string) error {
	_, err := c.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(url), MessageBody: aws.String(body), MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup)})
	return err
}

// receive devolve até max mensagens, esperando (long polling limitado) até wait segundos.
func receive(t *testing.T, c *sqs.Client, url string, max int32, wait int32, visibility int32) []sqstypes.Message {
	t.Helper()
	in := &sqs.ReceiveMessageInput{QueueUrl: aws.String(url), MaxNumberOfMessages: max, WaitTimeSeconds: wait,
		MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameAll}}
	if visibility > 0 {
		in.VisibilityTimeout = visibility
	}
	out, err := c.ReceiveMessage(context.Background(), in)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	return out.Messages
}

// depth devolve (visíveis, em voo) pelos atributos da fila, lidos pelo inspetor.
func depth(t *testing.T, c *sqs.Client, url string) (visible, inFlight int) {
	t.Helper()
	out, err := c.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: aws.String(url),
		AttributeNames: []sqstypes.QueueAttributeName{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"}})
	if err != nil {
		t.Fatalf("queue attributes: %v", err)
	}
	visible, _ = strconv.Atoi(out.Attributes["ApproximateNumberOfMessages"])
	inFlight, _ = strconv.Atoi(out.Attributes["ApproximateNumberOfMessagesNotVisible"])
	return
}

func requireEmpty(t *testing.T, what string, c *sqs.Client, url string) {
	t.Helper()
	if v, f := depth(t, c, url); v != 0 || f != 0 {
		t.Errorf("%s: %d visible and %d in flight, want none (a denied call must have no effect)", what, v, f)
	}
}

func publish(c *sns.Client, topic, group, dedup, body string) error {
	_, err := c.Publish(context.Background(), &sns.PublishInput{
		TopicArn: aws.String(topic), Message: aws.String(body), MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup)})
	return err
}

func TestBrokerAuthorizedFlowEndToEnd(t *testing.T) {
	p := provisionProof(t)
	ctx := context.Background()
	producer, runtime, publisher, inspector := p.sqs(broker.Producer), p.sqs(broker.Runtime), p.sns(broker.Publisher), p.sqs(broker.Inspector)

	// Entrada: o produtor envia; o runtime recebe, altera a visibilidade e remove.
	msgID, wallet := "msg-"+uuid.NewString(), uuid.NewString()
	body := fmt.Sprintf(`{"messageId":%q,"type":"WagerTransactionRequested","data":{"walletId":%q}}`, msgID, wallet)
	if out, err := producer.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(p.res.Names.Input)}); err != nil || aws.ToString(out.QueueUrl) != p.res.InputURL {
		t.Fatalf("producer GetQueueUrl: %v", err)
	}
	if err := send(producer, p.res.InputURL, wallet, msgID, body); err != nil {
		t.Fatalf("producer send: %v", err)
	}
	if out, err := runtime.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(p.res.Names.Input)}); err != nil || aws.ToString(out.QueueUrl) != p.res.InputURL {
		t.Fatalf("runtime GetQueueUrl: %v", err)
	}
	got := receive(t, runtime, p.res.InputURL, 10, 2, 0)
	if len(got) != 1 || aws.ToString(got[0].Body) != body || got[0].Attributes["MessageGroupId"] != wallet || got[0].Attributes["ApproximateReceiveCount"] != "1" {
		t.Fatalf("runtime received %+v", got)
	}
	if _, err := runtime.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(p.res.InputURL), ReceiptHandle: got[0].ReceiptHandle, VisibilityTimeout: 30}); err != nil {
		t.Fatalf("runtime ChangeMessageVisibility: %v", err)
	}
	if v, f := depth(t, inspector, p.res.InputURL); v != 0 || f != 1 {
		t.Fatalf("while in flight: %d visible, %d in flight", v, f)
	}
	if _, err := runtime.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(p.res.InputURL), ReceiptHandle: got[0].ReceiptHandle}); err != nil {
		t.Fatalf("runtime DeleteMessage: %v", err)
	}
	requireEmpty(t, "input queue after the removal", inspector, p.res.InputURL)

	// Saída: o publisher publica; o assinante (fila de auditoria) recebe o conteúdo raw correspondente.
	eventID := uuid.NewString()
	event := fmt.Sprintf(`{"eventId":%q,"eventType":"WalletBalanceChanged","data":{"walletId":%q}}`, eventID, wallet)
	if _, err := publisher.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(p.res.TopicARN)}); err != nil {
		t.Fatalf("publisher GetTopicAttributes: %v", err)
	}
	if err := publish(publisher, p.res.TopicARN, wallet, eventID, event); err != nil {
		t.Fatalf("publisher Publish: %v", err)
	}
	// Publish bem-sucedido sozinho não prova a entrega: o inspetor lê a fila assinante.
	delivered := receive(t, inspector, p.res.AuditURL, 10, 5, 0)
	if len(delivered) != 1 || aws.ToString(delivered[0].Body) != event {
		t.Fatalf("the subscriber received %+v, want exactly the raw event", delivered)
	}
	if delivered[0].Attributes["MessageGroupId"] != wallet || delivered[0].Attributes["MessageDeduplicationId"] != eventID {
		t.Fatalf("group/dedup of the delivered event: %v", delivered[0].Attributes)
	}
}

func TestBrokerDeniesEverythingOutsideEachResponsibility(t *testing.T) {
	p := provisionProof(t)
	ctx := context.Background()
	producer, runtime, inspector := p.sqs(broker.Producer), p.sqs(broker.Runtime), p.sqs(broker.Inspector)
	publisher, nobody := p.sqs(broker.Publisher), p.sqs(broker.Nobody)
	snsProducer, snsRuntime, snsNobody, snsPublisher := p.sns(broker.Producer), p.sns(broker.Runtime), p.sns(broker.Nobody), p.sns(broker.Publisher)

	// Uma mensagem legítima em voo (o runtime a recebeu): as negativas de remoção/visibilidade precisam
	// deixá-la intacta.
	if err := send(producer, p.res.InputURL, "w", "legit", "legit"); err != nil {
		t.Fatal(err)
	}
	held := receive(t, runtime, p.res.InputURL, 1, 2, 0)
	if len(held) != 1 {
		t.Fatalf("setup: %d messages", len(held))
	}
	handle := held[0].ReceiptHandle

	// Pares (ação permitida × recurso não autorizado): mesma ação, outro recurso, ou ação fora da responsabilidade.
	other := "arn:aws:sns:" + p.cfg.Region + ":" + p.res.Account + ":" + p.cfg.Prefix + "wager-other.fifo"
	if _, err := sns.NewFromConfig(p.admin()).CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(p.cfg.Prefix + "wager-other.fifo"),
		Attributes: map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "false"}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = sns.NewFromConfig(p.admin()).DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: aws.String(other)})
	})

	cases := []struct {
		name string
		call func() error
	}{
		// Produtor interno: só envia à entrada.
		{"producer receives from the input", func() error {
			_, err := producer.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(p.res.InputURL)})
			return err
		}},
		{"producer deletes from the input", func() error {
			_, err := producer.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(p.res.InputURL), ReceiptHandle: handle})
			return err
		}},
		{"producer changes visibility", func() error {
			_, err := producer.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(p.res.InputURL), ReceiptHandle: handle, VisibilityTimeout: 0})
			return err
		}},
		{"producer sends to the DLQ (same action, another resource)", func() error { return send(producer, p.res.DLQURL, "w", "d1", "x") }},
		{"producer sends to the audit queue", func() error { return send(producer, p.res.AuditURL, "w", "d2", "x") }},
		{"producer publishes to SNS", func() error { return publish(snsProducer, p.res.TopicARN, "w", "e1", "x") }},
		// Runtime: consome a entrada e envia à DLQ; não produz na entrada.
		{"runtime sends to the input (same action, another resource)", func() error { return send(runtime, p.res.InputURL, "w", "r1", "x") }},
		{"runtime sends to the audit queue", func() error { return send(runtime, p.res.AuditURL, "w", "r2", "x") }},
		{"runtime receives from the DLQ", func() error {
			_, err := runtime.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(p.res.DLQURL)})
			return err
		}},
		{"runtime publishes to SNS", func() error { return publish(snsRuntime, p.res.TopicARN, "w", "e2", "x") }},
		// Publisher: só publica; não consome nem produz.
		{"publisher receives from the input", func() error {
			_, err := publisher.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(p.res.InputURL)})
			return err
		}},
		{"publisher sends to the input", func() error { return send(publisher, p.res.InputURL, "w", "p1", "x") }},
		{"publisher publishes to another topic (same action, another resource)", func() error { return publish(snsPublisher, other, "w", "e3", "x") }},
		// Sem políticas.
		{"nobody sends to the input", func() error { return send(nobody, p.res.InputURL, "w", "n1", "x") }},
		{"nobody receives from the input", func() error {
			_, err := nobody.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(p.res.InputURL)})
			return err
		}},
		{"nobody deletes from the input", func() error {
			_, err := nobody.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(p.res.InputURL), ReceiptHandle: handle})
			return err
		}},
		{"nobody resolves the queue url", func() error {
			_, err := nobody.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(p.res.Names.Input)})
			return err
		}},
		{"nobody publishes to SNS", func() error { return publish(snsNobody, p.res.TopicARN, "w", "e4", "x") }},
		// Inspeção: não envia à entrada nem a consome.
		{"inspector sends to the input", func() error { return send(inspector, p.res.InputURL, "w", "i1", "x") }},
		{"inspector receives from the input", func() error {
			_, err := inspector.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(p.res.InputURL)})
			return err
		}},
	}
	for _, c := range cases {
		requireDenied(t, c.name, c.call())
	}

	// Ausência de efeito: a mensagem legítima segue em voo (nada foi removido nem liberado), e nenhuma
	// mensagem negada chegou a qualquer fila.
	if v, f := depth(t, inspector, p.res.InputURL); v != 0 || f != 1 {
		t.Errorf("input queue after the denials: %d visible, %d in flight, want only the legitimate message in flight", v, f)
	}
	requireEmpty(t, "DLQ", inspector, p.res.DLQURL)
	requireEmpty(t, "audit queue", inspector, p.res.AuditURL)

	// Os pares permitidos continuam funcionando (a negativa não é falha geral do emulador).
	if err := send(runtime, p.res.DLQURL, "w", "ok-dlq", "dlq"); err != nil {
		t.Errorf("runtime must forward to the DLQ: %v", err)
	}
	if err := send(producer, p.res.InputURL, "w2", "ok-input", "in"); err != nil {
		t.Errorf("producer must send to the input: %v", err)
	}
	if err := publish(snsPublisher, p.res.TopicARN, "w", "ok-topic", "ev"); err != nil {
		t.Errorf("publisher must publish to the event topic: %v", err)
	}
	if _, err := runtime.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(p.res.InputURL), ReceiptHandle: handle}); err != nil {
		t.Errorf("runtime must still be able to remove the held message: %v", err)
	}
}

func TestBrokerFIFOTransportDedupAndParallelGroups(t *testing.T) {
	p := provisionProof(t)
	producer, runtime := p.sqs(broker.Producer), p.sqs(broker.Runtime)

	// Deduplicação de TRANSPORTE do FIFO (janela temporária do broker): o mesmo MessageDeduplicationId vale uma vez.
	// Não é a garantia financeira da aplicação (inbox e idempotência no banco), que não depende dela.
	for i := 0; i < 2; i++ {
		if err := send(producer, p.res.InputURL, "A", "dup-1", "A1"); err != nil {
			t.Fatal(err)
		}
	}
	first := receive(t, runtime, p.res.InputURL, 10, 2, 0)
	if len(first) != 1 || aws.ToString(first[0].Body) != "A1" {
		t.Fatalf("after sending the same dedup id twice: %+v", first)
	}
	if _, err := runtime.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(p.res.InputURL), ReceiptHandle: first[0].ReceiptHandle}); err != nil {
		t.Fatal(err)
	}

	// Grupos distintos são compatíveis com paralelismo: com uma mensagem do grupo A em voo, a outra mensagem
	// do grupo A espera (ordem por grupo), mas o grupo B é entregue.
	for _, m := range []struct{ group, id string }{{"A", "A2"}, {"A", "A3"}, {"B", "B1"}} {
		if err := send(producer, p.res.InputURL, m.group, m.id, m.id); err != nil {
			t.Fatal(err)
		}
	}
	a := receive(t, runtime, p.res.InputURL, 1, 2, 0)
	b := receive(t, runtime, p.res.InputURL, 1, 2, 0)
	if len(a) != 1 || aws.ToString(a[0].Body) != "A2" || len(b) != 1 || aws.ToString(b[0].Body) != "B1" {
		t.Fatalf("group A in flight must not block group B: a=%+v b=%+v", a, b)
	}
	if again := receive(t, runtime, p.res.InputURL, 1, 1, 0); len(again) != 0 {
		t.Fatalf("a second message of the in-flight group was delivered: %+v", again)
	}
	for _, m := range []sqstypes.Message{a[0], b[0]} {
		if _, err := runtime.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(p.res.InputURL), ReceiptHandle: m.ReceiptHandle}); err != nil {
			t.Fatal(err)
		}
	}
	if next := receive(t, runtime, p.res.InputURL, 1, 2, 0); len(next) != 1 || aws.ToString(next[0].Body) != "A3" {
		t.Fatalf("group A order after removal: %+v", next)
	}
}

// Visibilidade e redrive com recursos ISOLADOS e valores reduzidos só neste teste (1 s e 2 recebimentos);
// os padrões do ambiente (60 s e 5) são conferidos em TestBrokerProvisioningUsesTheApprovedDefaults e por
// scripts/check-infra.sh.
func TestBrokerVisibilityTimeoutAndRedrivePreserveTheMessage(t *testing.T) {
	p := provisionProof(t, func(c *broker.Config) { c.InputVisibility, c.InputMaxReceive = 1, 2 })
	producer, runtime, inspector := p.sqs(broker.Producer), p.sqs(broker.Runtime), p.sqs(broker.Inspector)

	body := fmt.Sprintf(`{"messageId":%q,"payload":"preserved exactly"}`, uuid.NewString())
	if err := send(producer, p.res.InputURL, "wallet-1", "redrive-1", body); err != nil {
		t.Fatal(err)
	}
	// Recebimento 1; sem remover, a visibilidade expira e a mesma mensagem volta (recebimento 2).
	deadline := time.Now().Add(15 * time.Second)
	counts := map[string]bool{}
	for len(counts) < 2 && time.Now().Before(deadline) {
		for _, m := range receive(t, runtime, p.res.InputURL, 1, 2, 0) {
			if aws.ToString(m.Body) != body {
				t.Fatalf("body changed on redelivery: %q", aws.ToString(m.Body))
			}
			counts[m.Attributes["ApproximateReceiveCount"]] = true
		}
	}
	if !counts["1"] || !counts["2"] {
		t.Fatalf("receive counts observed: %v, want 1 and 2", counts)
	}
	// Esgotados os recebimentos, a mensagem vai à DLQ com o conteúdo e o grupo preservados.
	// (O MiniStack aplica o redrive quando o consumidor tenta receber de novo depois do último recebimento
	// permitido; a AWS o faz em segundo plano. Por isso o consumidor continua tentando, e nunca recebe a mensagem.)
	var dead []sqstypes.Message
	for deadline := time.Now().Add(15 * time.Second); len(dead) == 0 && time.Now().Before(deadline); {
		if again := receive(t, runtime, p.res.InputURL, 1, 1, 0); len(again) != 0 {
			t.Fatalf("a message was delivered beyond maxReceiveCount: %+v", again)
		}
		dead = receive(t, inspector, p.res.DLQURL, 1, 1, 0)
	}
	if len(dead) != 1 || aws.ToString(dead[0].Body) != body || dead[0].Attributes["MessageGroupId"] != "wallet-1" {
		t.Fatalf("DLQ content: %+v", dead)
	}
	if v, f := depth(t, inspector, p.res.InputURL); v != 0 || f != 0 {
		t.Fatalf("input queue after redrive: %d visible, %d in flight", v, f)
	}
}

func TestBrokerProvisioningUsesTheApprovedDefaults(t *testing.T) {
	p := provisionProof(t)
	ctx := context.Background()
	inspector := p.sqs(broker.Inspector)
	in, err := inspector.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(p.res.InputURL), AttributeNames: []sqstypes.QueueAttributeName{"All"}})
	if err != nil {
		t.Fatal(err)
	}
	a := in.Attributes
	if a["FifoQueue"] != "true" || a["ContentBasedDeduplication"] != "false" || a["VisibilityTimeout"] != "60" ||
		!strings.Contains(a["RedrivePolicy"], `"maxReceiveCount":"5"`) && !strings.Contains(a["RedrivePolicy"], `"maxReceiveCount":5`) ||
		!strings.Contains(a["RedrivePolicy"], p.res.DLQARN) {
		t.Fatalf("input queue attributes: %v", a)
	}
	dlq, err := inspector.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(p.res.DLQURL), AttributeNames: []sqstypes.QueueAttributeName{"All"}})
	if err != nil || dlq.Attributes["MessageRetentionPeriod"] != "1209600" || dlq.Attributes["FifoQueue"] != "true" {
		t.Fatalf("DLQ attributes: %v %v", dlq.Attributes, err)
	}
	subs, err := sns.NewFromConfig(p.admin()).ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(p.res.TopicARN)})
	if err != nil || len(subs.Subscriptions) != 1 || aws.ToString(subs.Subscriptions[0].Endpoint) != p.res.AuditARN {
		t.Fatalf("subscriptions: %+v %v", subs, err)
	}
	attrs, err := sns.NewFromConfig(p.admin()).GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{SubscriptionArn: subs.Subscriptions[0].SubscriptionArn})
	if err != nil || attrs.Attributes["RawMessageDelivery"] != "true" {
		t.Fatalf("subscription attributes: %v %v", attrs, err)
	}
	topic, err := sns.NewFromConfig(p.admin()).GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(p.res.TopicARN)})
	if err != nil || topic.Attributes["FifoTopic"] != "true" {
		t.Fatalf("topic attributes: %v %v", topic, err)
	}
}

// A política da fila de auditoria é o controle de entrega entre serviços: só o tópico autorizado entrega
// (aws:SourceArn). Um Publish bem-sucedido, ou apenas a existência de uma assinatura, não prova a entrega.
func TestBrokerSNSDeliveryDependsOnTheAuditQueuePolicy(t *testing.T) {
	p := provisionProof(t)
	ctx := context.Background()
	admin, inspector, publisher := sns.NewFromConfig(p.admin()), p.sqs(broker.Inspector), p.sns(broker.Publisher)
	adminSQS := sqs.NewFromConfig(p.admin())
	group := "wallet-" + uuid.NewString()[:8]

	// 1. Outro tópico, assinado na mesma fila de auditoria, não entrega: a política só admite o tópico autorizado.
	otherName := p.cfg.Prefix + "wager-other.fifo"
	other, err := admin.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(otherName), Attributes: map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "false"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: other.TopicArn})
	})
	sub, err := admin.Subscribe(ctx, &sns.SubscribeInput{TopicArn: other.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(p.res.AuditARN)})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = admin.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{SubscriptionArn: sub.SubscriptionArn, AttributeName: aws.String("RawMessageDelivery"), AttributeValue: aws.String("true")})
	if err := publish(admin, aws.ToString(other.TopicArn), group, "from-other", "from-other-topic"); err != nil {
		t.Fatalf("publish to the other topic (accepted by SNS): %v", err)
	}
	// Observação limitada (long polling de 3 s): a ausência de entrega não tem um sinal de estado a aguardar.
	if got := receive(t, inspector, p.res.AuditURL, 10, 3, 0); len(got) != 0 {
		t.Fatalf("an unauthorized topic delivered to the audit queue: %+v", got)
	}

	// 2. Sem a política, nem o tópico autorizado entrega (o Publish, porém, é aceito).
	if _, err := adminSQS.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: aws.String(p.res.AuditURL), Attributes: map[string]string{"Policy": ""}}); err != nil {
		t.Fatal(err)
	}
	if err := publish(publisher, p.res.TopicARN, group, "no-policy", "published-without-policy"); err != nil {
		t.Fatalf("publish is accepted even when the delivery will be refused: %v", err)
	}
	if got := receive(t, inspector, p.res.AuditURL, 10, 3, 0); len(got) != 0 {
		t.Fatalf("delivery succeeded without the audit queue policy: %+v", got)
	}

	// 3. Reprovisionar restaura a política; o tópico autorizado entrega (e só ele).
	if _, err := broker.Provision(ctx, p.cfg); err != nil {
		t.Fatal(err)
	}
	if err := publish(publisher, p.res.TopicARN, group, "with-policy", "delivered"); err != nil {
		t.Fatal(err)
	}
	got := receive(t, inspector, p.res.AuditURL, 10, 5, 0)
	if len(got) != 1 || aws.ToString(got[0].Body) != "delivered" {
		t.Fatalf("after restoring the policy: %+v", got)
	}
}

func TestBrokerProvisioningIsIdempotentAndKeepsCredentialsInUse(t *testing.T) {
	p := provisionProof(t)
	ctx := context.Background()
	before, err := os.ReadFile(p.cfg.CredentialsFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.res.NewCredentials) != len(broker.Roles) {
		t.Fatalf("first run issued credentials for %v", p.res.NewCredentials)
	}
	again, err := broker.Provision(ctx, p.cfg)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(p.cfg.CredentialsFile)
	if string(before) != string(after) || len(again.NewCredentials) != 0 {
		t.Fatalf("re-provisioning changed credentials in use (new: %v)", again.NewCredentials)
	}
	if again.InputURL != p.res.InputURL || again.InputARN != p.res.InputARN || again.TopicARN != p.res.TopicARN || again.SubscriptionARN != p.res.SubscriptionARN {
		t.Fatalf("resources changed: %+v vs %+v", again, p.res)
	}
	subs, err := sns.NewFromConfig(p.admin()).ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(p.res.TopicARN)})
	if err != nil || len(subs.Subscriptions) != 1 {
		t.Fatalf("subscriptions after the second run: %d %v", len(subs.Subscriptions), err)
	}
	// A chave distribuída antes continua valendo, com as mesmas permissões.
	if err := send(p.sqs(broker.Producer), p.res.InputURL, "w", "still-works", "x"); err != nil {
		t.Fatalf("the producer key stopped working after re-provisioning: %v", err)
	}
	requireDenied(t, "nobody still has no access", send(p.sqs(broker.Nobody), p.res.InputURL, "w", "n", "x"))

	// Chave perdida no emulador: só o runtime é reemitido; as demais seguem iguais.
	runtimeUser := p.res.Profiles[broker.Runtime]
	oldRuntime, _ := broker.ProfileCredentials(p.cfg.CredentialsFile, runtimeUser)
	adminIAM := iam.NewFromConfig(p.admin())
	if _, err := adminIAM.DeleteAccessKey(ctx, &iam.DeleteAccessKeyInput{UserName: aws.String(runtimeUser), AccessKeyId: aws.String(oldRuntime.AccessKeyID)}); err != nil {
		t.Fatal(err)
	}
	reissued, err := broker.Provision(ctx, p.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(reissued.NewCredentials) != 1 || reissued.NewCredentials[0] != broker.Runtime {
		t.Fatalf("reissued: %v", reissued.NewCredentials)
	}
	newRuntime, _ := broker.ProfileCredentials(p.cfg.CredentialsFile, runtimeUser)
	if newRuntime.AccessKeyID == oldRuntime.AccessKeyID {
		t.Fatal("the lost key was not replaced")
	}
	if _, err := p.sqs(broker.Runtime).GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(p.res.Names.Input)}); err != nil {
		t.Fatalf("the reissued runtime key does not work: %v", err)
	}
	if err := send(p.sqs(broker.Producer), p.res.InputURL, "w", "producer-unchanged", "x"); err != nil {
		t.Fatalf("an unrelated key changed: %v", err)
	}
}
