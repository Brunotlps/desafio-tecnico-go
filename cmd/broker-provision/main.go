// Comando broker-provision: provisiona, de forma idempotente, filas, tópico, identidades, políticas e
// credenciais do broker (MiniStack com AUTH=true). Roda uma vez por subida do ambiente (serviço
// one-shot do Compose) e pode ser reexecutado sem efeitos colaterais.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"desafio-tecnico-go/internal/infra/broker"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	cfg := broker.Config{
		Endpoint:             getenv("BROKER_ENDPOINT_URL", getenv("AWS_ENDPOINT_URL", "http://localhost:4566")),
		Region:               getenv("AWS_REGION", "us-east-1"),
		AdminAccessKeyID:     getenv("BROKER_ADMIN_ACCESS_KEY_ID", "test"),
		AdminSecretAccessKey: getenv("BROKER_ADMIN_SECRET_ACCESS_KEY", "test"),
		CredentialsFile:      getenv("BROKER_CREDENTIALS_FILE", ".local/broker/credentials"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := broker.WaitReady(ctx, cfg, 60*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	res, err := broker.Provision(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "broker provisioning failed:", err)
		os.Exit(1)
	}
	fmt.Printf("provisioned: %s (+%s, redrive after %d receives), %s -> %s (raw), principals %d, new credentials for %v, file %s\n",
		res.Names.Input, res.Names.DLQ, broker.DefaultMaxReceive, res.Names.Topic, res.Names.Audit, len(res.Profiles), res.NewCredentials, cfg.CredentialsFile)
}
