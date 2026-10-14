// Package sqs concentra o acesso ao AWS SQS. Neste incremento oferece apenas a
// verificação de prontidão do broker; o consumidor é um incremento posterior.
package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Config parametriza o cliente.
type Config struct {
	Region      string
	EndpointURL string // vazio na AWS; LocalStack localmente
	QueueName   string
}

// Client verifica a disponibilidade do SQS e da fila de entrada.
type Client struct {
	api       *awssqs.Client
	queueName string
}

// NewClient cria o cliente. Credenciais seguem a cadeia padrão do SDK
// (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, perfil ou papel).
func NewClient(ctx context.Context, c Config) (*Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(c.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	api := awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		if c.EndpointURL != "" {
			o.BaseEndpoint = aws.String(c.EndpointURL)
		}
	})
	return &Client{api: api, queueName: c.QueueName}, nil
}

// Ready confirma que o SQS responde e que a fila de entrada está provisionada.
func (c *Client) Ready(ctx context.Context) error {
	out, err := c.api.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(c.queueName)})
	if err != nil {
		return fmt.Errorf("sqs queue %q: %w", c.queueName, err)
	}
	if aws.ToString(out.QueueUrl) == "" {
		return fmt.Errorf("sqs queue %q: empty url", c.queueName)
	}
	return nil
}
