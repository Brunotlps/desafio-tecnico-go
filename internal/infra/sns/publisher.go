// Package sns publica os eventos da outbox no tópico FIFO de saída, com as credenciais do principal wager-publisher.
package sns

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"desafio-tecnico-go/internal/infra/broker"
	"desafio-tecnico-go/internal/outbox"
)

// Config parametriza o cliente. As credenciais vêm do PERFIL do publisher no arquivo de credenciais compartilhado, lido por
// este construtor: não dependem de variáveis de ambiente do processo (o cliente SQS de readiness continua com o perfil de runtime).
type Config struct {
	Region          string
	EndpointURL     string // vazio na AWS; MiniStack localmente
	TopicARN        string
	CredentialsFile string
	Profile         string // padrão do chamador: wager-publisher
}

// Publisher implementa outbox.Sink sobre o SNS.
type Publisher struct {
	api      *awssns.Client
	topicARN string
}

var _ outbox.Sink = (*Publisher)(nil)

// New cria o cliente. Nenhuma chamada de rede acontece aqui (ver Ready).
func New(ctx context.Context, c Config) (*Publisher, error) {
	if c.TopicARN == "" {
		return nil, errors.New("sns topic arn is required")
	}
	creds, err := broker.ProfileCredentials(c.CredentialsFile, c.Profile)
	if err != nil {
		return nil, err
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(c.Region),
		awsconfig.WithCredentialsProvider(creds.Provider()),
		awsconfig.WithRetryMaxAttempts(3),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	api := awssns.NewFromConfig(cfg, func(o *awssns.Options) {
		if c.EndpointURL != "" {
			o.BaseEndpoint = aws.String(c.EndpointURL)
		}
	})
	return &Publisher{api: api, topicARN: c.TopicARN}, nil
}

// Ready confirma que o tópico existe, é FIFO e que o principal pode descrevê-lo (falha de permissão aparece na partida).
func (p *Publisher) Ready(ctx context.Context) error {
	out, err := p.api.GetTopicAttributes(ctx, &awssns.GetTopicAttributesInput{TopicArn: aws.String(p.topicARN)})
	if err != nil {
		return fmt.Errorf("sns topic %q: %w", p.topicARN, err)
	}
	if out.Attributes["FifoTopic"] != "true" {
		return fmt.Errorf("sns topic %q is not a FIFO topic", p.topicARN)
	}
	return nil
}

// Publish envia o envelope armazenado, sem reescrevê-lo: MessageGroupId = walletId e MessageDeduplicationId = eventId.
func (p *Publisher) Publish(ctx context.Context, m outbox.Message) error {
	_, err := p.api.Publish(ctx, &awssns.PublishInput{
		TopicArn:               aws.String(p.topicARN),
		Message:                aws.String(m.Body),
		MessageGroupId:         aws.String(m.GroupID),
		MessageDeduplicationId: aws.String(m.DedupID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType": {DataType: aws.String("String"), StringValue: aws.String(m.Type)},
		},
	})
	return err
}
