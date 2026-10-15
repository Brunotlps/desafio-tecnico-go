// Comando sqs-producer: produtor INTERNO da fila de entrada. Autentica a origem (token de client credentials verificado contra o IdP),
// vincula-a ao providerId do envelope e envia o corpo exato com o principal wager-producer: MessageGroupId = walletId e
// MessageDeduplicationId = messageId. Provedores externos não recebem SendMessage direto na fila compartilhada.
//
//	sqs-producer -body envelope.json            (ou o corpo na entrada padrão)
//
// Ambiente: KEYCLOAK_TOKEN_URL, PRODUCER_CLIENT_ID, PRODUCER_CLIENT_SECRET, OIDC_ISSUER, OIDC_JWKS_URL, OIDC_AUDIENCE,
// BROKER_CREDENTIALS_FILE (perfil wager-producer), AWS_ENDPOINT_URL, AWS_REGION, SQS_WAGER_QUEUE_NAME.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"desafio-tecnico-go/internal/inbound"
	"desafio-tecnico-go/internal/infra/auth"
	"desafio-tecnico-go/internal/infra/sqs"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "sqs-producer:", err)
	os.Exit(1)
}

func clientToken(ctx context.Context, tokenURL, id, secret string) (string, error) {
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {id}, "client_secret": {secret}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&out) != nil || out.AccessToken == "" {
		return "", fmt.Errorf("token endpoint answered %d", resp.StatusCode)
	}
	return out.AccessToken, nil
}

func main() {
	bodyFile := flag.String("body", "", "arquivo com o envelope (padrão: entrada padrão)")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var body []byte
	var err error
	if *bodyFile != "" {
		body, err = os.ReadFile(*bodyFile)
	} else {
		body, err = io.ReadAll(os.Stdin)
	}
	if err != nil {
		fail(err)
	}

	tok, err := clientToken(ctx, getenv("KEYCLOAK_TOKEN_URL", "http://localhost:8081/realms/wager/protocol/openid-connect/token"),
		getenv("PRODUCER_CLIENT_ID", ""), getenv("PRODUCER_CLIENT_SECRET", ""))
	if err != nil {
		fail(fmt.Errorf("authenticate the origin: %w", err))
	}
	v := auth.NewVerifier(auth.Config{
		Issuer: getenv("OIDC_ISSUER", "http://localhost:8081/realms/wager"), JWKSURL: getenv("OIDC_JWKS_URL", "http://localhost:8081/realms/wager/protocol/openid-connect/certs"),
		Audience: getenv("OIDC_AUDIENCE", "wager-api"), ClockSkew: 5 * time.Second,
	})
	if err := v.Start(ctx); err != nil {
		fail(err)
	}
	defer v.Stop()

	prod, err := sqs.NewProducer(ctx, sqs.ProducerConfig{
		Region: getenv("AWS_REGION", "us-east-1"), EndpointURL: getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
		QueueName:       getenv("SQS_WAGER_QUEUE_NAME", "wager-transactions.fifo"),
		CredentialsFile: getenv("BROKER_CREDENTIALS_FILE", ".local/broker/credentials"), Profile: getenv("PRODUCER_AWS_PROFILE", "wager-producer"),
	})
	if err != nil {
		fail(err)
	}
	pub := inbound.NewPublisher(prod, func(token string) (inbound.Origin, error) {
		p, err := v.Verify(token)
		if err != nil {
			return inbound.Origin{}, err
		}
		return inbound.Origin{ProviderID: p.ProviderID, Internal: p.IsInternal()}, nil
	})
	id, err := pub.Publish(ctx, tok, body)
	if err != nil {
		fail(err)
	}
	fmt.Println("published", id)
}
