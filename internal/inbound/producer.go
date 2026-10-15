package inbound

import (
	"context"
	"errors"
	"fmt"

	"desafio-tecnico-go/internal/app"
)

// Produtor interno: publica envelopes WagerTransactionRequested na fila de entrada. A fila é de PRODUTORES INTERNOS confiáveis; o
// corpo NÃO autentica ninguém. Antes de qualquer envio, a origem (um token verificado pelo IdP/verificador existentes, ou a
// identidade do serviço interno) é vinculada ao providerId: uma origem provider-a não publica como provider-b.

// ErrOriginMismatch: a origem autenticada não pode publicar para o providerId do envelope. Nada foi enviado.
var ErrOriginMismatch = errors.New("the authenticated origin may not publish for this provider")

// Origin é a identidade autenticada da origem. Internal (serviço interno) pode publicar para qualquer provedor; um provedor, só
// para o seu providerId.
type Origin struct {
	ProviderID string
	Internal   bool
}

// Sender envia o corpo EXATO (MessageGroupId = walletId, MessageDeduplicationId = messageId).
type Sender interface {
	Send(ctx context.Context, body []byte, walletID, messageID string) error
}

// Publisher valida a origem e envia. A autenticação do token é injetada (origin), para reaproveitar o verificador existente.
type Publisher struct {
	send   Sender
	origin func(token string) (Origin, error)
}

func NewPublisher(send Sender, origin func(token string) (Origin, error)) *Publisher {
	return &Publisher{send: send, origin: origin}
}

// Publish autentica a origem, confere o vínculo com o providerId e só então envia o corpo exato. Devolve o messageId do envelope.
func (p *Publisher) Publish(ctx context.Context, token string, body []byte) (string, error) {
	o, err := p.origin(token)
	if err != nil {
		return "", fmt.Errorf("origin not authenticated: %w", err)
	}
	env, err := app.ParseEnvelope(body)
	if err != nil {
		return "", err
	}
	if !o.Internal && (o.ProviderID == "" || o.ProviderID != env.Data.ProviderID) {
		return "", ErrOriginMismatch
	}
	if err := p.send.Send(ctx, body, env.Data.WalletID, env.MessageID); err != nil {
		return "", err
	}
	return env.MessageID, nil
}
