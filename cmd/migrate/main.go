// Comando migrate: aplica e reverte as migrations versionadas.
//
//	migrate up                 aplica todas as pendentes
//	migrate down               reverte a última aplicada
//	migrate down-to <versão>   reverte até a versão (0 reverte todas)
//	migrate status             lista o estado das migrations
//	migrate grants             (re)aplica os privilégios mínimos do papel de runtime
//
// Conecta como o papel dono do schema: MIGRATE_DATABASE_URL (ou, na falta,
// DATABASE_URL). Se RUNTIME_DB_ROLE estiver definido, `up` também cria o papel
// de runtime (RUNTIME_DB_PASSWORD) e aplica seus privilégios mínimos.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/pressly/goose/v3"

	"desafio-tecnico-go/internal/infra/postgres"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: migrate up | down | down-to <version> | grants | status   (down/down-to N>0 preserve the pending-reference schedule; down-to 0 is the destructive reset)")
	}
	url := os.Getenv("MIGRATE_DATABASE_URL")
	if url == "" {
		url = os.Getenv("DATABASE_URL")
	}
	if url == "" {
		return fmt.Errorf("MIGRATE_DATABASE_URL (or DATABASE_URL) is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m, err := postgres.NewMigrator(url)
	if err != nil {
		return err
	}
	defer m.Close()

	switch args[0] {
	case "up":
		var res []*goose.MigrationResult
		var err error
		if role, ok := runtimeRoleFromEnv(); ok {
			// O Up e a provisão dos privilégios formam uma única operação coordenada do migrador.
			res, err = m.UpAndProvision(ctx, role)
		} else {
			res, err = m.Up(ctx)
		}
		for _, r := range res {
			fmt.Printf("applied %s (%s)\n", r.Source.Path, r.Duration)
		}
		if err == nil && len(res) == 0 {
			fmt.Println("already up to date")
		}
		if err != nil {
			return err
		}
		if role, ok := runtimeRoleFromEnv(); ok {
			fmt.Printf("runtime role %q provisioned with minimal privileges\n", role.Name)
		}
		return nil
	case "down":
		r, err := m.Down(ctx)
		if r != nil {
			fmt.Printf("reverted %s (%s)\n", r.Source.Path, r.Duration)
		}
		return err
	case "down-to":
		if len(args) != 2 {
			return fmt.Errorf("usage: migrate down-to <version>")
		}
		v, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || v < 0 {
			return fmt.Errorf("invalid version %q", args[1])
		}
		res, err := m.DownTo(ctx, v)
		for _, r := range res {
			fmt.Printf("reverted %s (%s)\n", r.Source.Path, r.Duration)
		}
		return err
	case "grants":
		role, ok := runtimeRoleFromEnv()
		if !ok {
			return fmt.Errorf("RUNTIME_DB_ROLE is required")
		}
		// Coordenado com as demais operações do migrador: espera um Down em andamento e respeita o cancelamento.
		if err := m.ProvisionRuntime(ctx, role); err != nil {
			return err
		}
		fmt.Printf("runtime role %q provisioned with minimal privileges\n", role.Name)
		return nil
	case "status":
		st, err := m.Status(ctx)
		if err != nil {
			return err
		}
		for _, s := range st {
			applied := "pending"
			if s.State == "applied" {
				applied = "applied " + s.AppliedAt.Format("2006-01-02 15:04:05")
			}
			fmt.Printf("%05d %-30s %s\n", s.Source.Version, s.Source.Path, applied)
		}
		return nil
	}
	return fmt.Errorf("unknown command %q", args[0])
}

// runtimeRoleFromEnv lê o papel de runtime de RUNTIME_DB_ROLE/RUNTIME_DB_PASSWORD (ok=false se não definido).
func runtimeRoleFromEnv() (postgres.RuntimeRole, bool) {
	name := os.Getenv("RUNTIME_DB_ROLE")
	if name == "" {
		return postgres.RuntimeRole{}, false
	}
	return postgres.RuntimeRole{Name: name, Password: os.Getenv("RUNTIME_DB_PASSWORD")}, true
}
