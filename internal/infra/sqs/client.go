// Package sqs concentra o acesso ao AWS SQS: readiness, o transporte do consumidor (receber uma mensagem, remover, ajustar a
// visibilidade e encaminhar à DLQ, sempre com o principal de RUNTIME) e o produtor interno (principal PRODUCER).
package sqs

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"desafio-tecnico-go/internal/inbound"
	"desafio-tecnico-go/internal/infra/broker"
)

// Config parametriza o cliente. Sem CredentialsFile, usa a cadeia padrão do SDK (AWS_PROFILE/AWS_SHARED_CREDENTIALS_FILE do
// processo: o perfil de runtime). Com CredentialsFile, lê SOMENTE o perfil indicado desse arquivo, sem depender do ambiente.
type Config struct {
	Region      string
	EndpointURL string // vazio na AWS; MiniStack localmente
	QueueName   string // fila de entrada
	DLQName     string // DLQ (encaminhamento manual de mensagens permanentes); vazio = sem encaminhamento

	CredentialsFile string
	Profile         string
}

// Client verifica a disponibilidade do SQS e implementa o transporte do consumidor.
type Client struct {
	api       *awssqs.Client
	queueName string
	dlqName   string

	mu     sync.Mutex
	inURL  string
	dlqURL string
}

func awsConfig(ctx context.Context, region, credentialsFile, profile string) (aws.Config, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if credentialsFile != "" {
		creds, err := broker.ProfileCredentials(credentialsFile, profile)
		if err != nil {
			return aws.Config{}, err
		}
		opts = append(opts, awsconfig.WithCredentialsProvider(creds.Provider()))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("load aws config: %w", err)
	}
	return cfg, nil
}

// NewClient cria o cliente. Nenhuma chamada de rede acontece aqui.
func NewClient(ctx context.Context, c Config) (*Client, error) {
	cfg, err := awsConfig(ctx, c.Region, c.CredentialsFile, c.Profile)
	if err != nil {
		return nil, err
	}
	api := awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		if c.EndpointURL != "" {
			o.BaseEndpoint = aws.String(c.EndpointURL)
		}
	})
	return &Client{api: api, queueName: c.QueueName, dlqName: c.DLQName}, nil
}

// Ready confirma que o SQS responde e que a fila de entrada (e a DLQ, se configurada) está provisionada.
func (c *Client) Ready(ctx context.Context) error {
	if _, err := c.queueURL(ctx, false); err != nil {
		return err
	}
	if c.dlqName != "" {
		if _, err := c.queueURL(ctx, true); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) queueURL(ctx context.Context, dlq bool) (string, error) {
	c.mu.Lock()
	cached, name := c.inURL, c.queueName
	if dlq {
		cached, name = c.dlqURL, c.dlqName
	}
	c.mu.Unlock()
	if cached != "" {
		return cached, nil
	}
	out, err := c.api.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return "", fmt.Errorf("sqs queue %q: %w", name, err)
	}
	u := aws.ToString(out.QueueUrl)
	if u == "" {
		return "", fmt.Errorf("sqs queue %q: empty url", name)
	}
	c.mu.Lock()
	if dlq {
		c.dlqURL = u
	} else {
		c.inURL = u
	}
	c.mu.Unlock()
	return u, nil
}

// Receive busca no máximo UMA mensagem (long polling cancelável por ctx). Devolve nil, nil quando não há mensagem.
func (c *Client) Receive(ctx context.Context, wait time.Duration) (*inbound.Delivery, error) {
	u, err := c.queueURL(ctx, false)
	if err != nil {
		return nil, err
	}
	out, err := c.api.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl: aws.String(u), MaxNumberOfMessages: 1, WaitTimeSeconds: int32(wait / time.Second),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
	})
	if err != nil {
		return nil, err
	}
	if len(out.Messages) == 0 {
		return nil, nil
	}
	m := out.Messages[0]
	count, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	return &inbound.Delivery{BrokerID: aws.ToString(m.MessageId), ReceiptHandle: aws.ToString(m.ReceiptHandle), Body: []byte(aws.ToString(m.Body)), ReceiveCount: max(count, 1)}, nil
}

// Delete remove a mensagem da entrada com o receipt handle da entrega atual.
func (c *Client) Delete(ctx context.Context, d inbound.Delivery) error {
	u, err := c.queueURL(ctx, false)
	if err != nil {
		return err
	}
	_, err = c.api.DeleteMessage(ctx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(u), ReceiptHandle: aws.String(d.ReceiptHandle)})
	return err
}

// ChangeVisibility define quando a mensagem volta a ficar visível (segundos inteiros, nunca negativos).
func (c *Client) ChangeVisibility(ctx context.Context, d inbound.Delivery, delay time.Duration) error {
	u, err := c.queueURL(ctx, false)
	if err != nil {
		return err
	}
	secs := int32(0)
	if delay > 0 {
		secs = int32((delay + time.Second - 1) / time.Second) // arredonda para cima: nunca um atraso zero involuntário
	}
	_, err = c.api.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(u), ReceiptHandle: aws.String(d.ReceiptHandle), VisibilityTimeout: secs})
	return err
}

// ForwardToDLQ envia o corpo ORIGINAL à DLQ (a confirmação do broker é o retorno sem erro). Metadados operacionais vão como atributos
// de mensagem, sem alterar o corpo.
func (c *Client) ForwardToDLQ(ctx context.Context, body []byte, groupID, dedupID string, attrs map[string]string) error {
	u, err := c.queueURL(ctx, true)
	if err != nil {
		return err
	}
	ma := map[string]types.MessageAttributeValue{}
	for k, v := range attrs {
		ma[k] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
	}
	_, err = c.api.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl: aws.String(u), MessageBody: aws.String(string(body)), MessageGroupId: aws.String(groupID),
		MessageDeduplicationId: aws.String(dedupID), MessageAttributes: ma})
	return err
}

var _ inbound.Transport = (*Client)(nil)
