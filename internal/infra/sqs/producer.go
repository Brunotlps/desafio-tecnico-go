package sqs

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Producer publica envelopes na fila de entrada com o principal PRODUCER (só SendMessage na entrada). É uma ferramenta de produtor
// INTERNO confiável: a vinculação da origem ao providerId é feita por inbound.Publisher, antes de qualquer envio.
type Producer struct {
	api       *awssqs.Client
	queueName string
}

// ProducerConfig: credenciais SEMPRE de um arquivo e perfil explícitos (nunca o administrador, nunca o runtime).
type ProducerConfig struct {
	Region, EndpointURL, QueueName string
	CredentialsFile, Profile       string
}

func NewProducer(ctx context.Context, c ProducerConfig) (*Producer, error) {
	if c.CredentialsFile == "" || c.Profile == "" || c.QueueName == "" {
		return nil, errors.New("producer requires a queue name, a credentials file and a profile")
	}
	cfg, err := awsConfig(ctx, c.Region, c.CredentialsFile, c.Profile)
	if err != nil {
		return nil, err
	}
	api := awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		if c.EndpointURL != "" {
			o.BaseEndpoint = aws.String(c.EndpointURL)
		}
	})
	return &Producer{api: api, queueName: c.QueueName}, nil
}

// Send envia o corpo EXATO. MessageGroupId = walletId e MessageDeduplicationId = messageId do envelope; reenviar o mesmo envelope
// repete exatamente corpo e identidade (a deduplicação FIFO do broker é temporária).
func (p *Producer) Send(ctx context.Context, body []byte, walletID, messageID string) error {
	out, err := p.api.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(p.queueName)})
	if err != nil {
		return fmt.Errorf("sqs queue %q: %w", p.queueName, err)
	}
	_, err = p.api.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl: out.QueueUrl, MessageBody: aws.String(string(body)), MessageGroupId: aws.String(walletID), MessageDeduplicationId: aws.String(messageID)})
	return err
}
