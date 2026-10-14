//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/fx"

	"desafio-tecnico-go/internal/bootstrap"
)

// A aplicação usa o principal de RUNTIME do broker pela cadeia padrão do SDK (AWS_PROFILE): com ele a
// partida e a readiness funcionam; com um principal sem permissão a partida falha com AccessDenied do
// broker (não com erro de endpoint), sem vazar conexões.
func TestApplicationReadinessUsesTheRuntimePrincipalAndFailsWithoutPermission(t *testing.T) {
	a := startApp(t, testConfig())
	if r := call(t, a.URL, "GET", "/health/ready", "", nil, nil); r.Status != 200 || r.str("checks", "sqs") != "ok" {
		t.Fatalf("readiness with the runtime principal: %d %s", r.Status, r.Raw)
	}

	for profile, want := range map[string]string{
		"wager-nobody":    "AccessDenied", // existe, mas sem política
		"wager-publisher": "AccessDenied", // publica no SNS; não resolve a fila de entrada
	} {
		t.Run(profile, func(t *testing.T) {
			t.Setenv("AWS_PROFILE", profile)
			appName := "readiness-" + uuid.NewString()[:8]
			app := fx.New(bootstrap.Options(shutdownConfig(appName)), fx.NopLogger)
			if err := app.Err(); err != nil {
				t.Fatalf("compose: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			err := app.Start(ctx)
			if err == nil {
				_ = app.Stop(ctx)
				t.Fatalf("startup succeeded with the %s principal", profile)
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("startup error = %v, want the broker's %s", err, want)
			}
			eventually(t, "database connections to be released", 5*time.Second, func() bool { return connectionsOf(t, appName) == 0 })
		})
	}
}
