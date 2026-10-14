//go:build integration

// Testes de integração: exigem PostgreSQL, Keycloak e o broker (MiniStack com AUTH=true) em execução e provisionado
// (docker compose up -d --wait postgres keycloak ministack && docker compose run --rm broker-provision). Veja README.md.
package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-tecnico-go/internal/infra/postgres"
)

// env reúne a configuração dos testes.
//
// A suíte opera APENAS em bancos de teste: o banco de TEST_DATABASE_URL (qualquer
// nome) e bancos auxiliares com sufixo aleatório derivados dele. Antes de
// recriar o schema, TestMain exige que o banco seja vazio ou já marcado como de
// teste (ver prepareTestDatabase) e recusa o banco da aplicação.
var env struct {
	dbURL       string // papel dono do schema (migrations e SQL direto dos testes)
	runtimeURL  string // papel restrito usado pela aplicação sob teste
	runtimeRole string
	runtimePass string
	tokenURL    string
	issuer      string
	jwksURL     string
	awsEndpoint string
	region      string
	// brokerCreds é o arquivo de credenciais por responsabilidade escrito pelo provisionamento do broker;
	// os testes selecionam o perfil de cada principal. Administrador: só o provisionamento da prova.
	brokerCreds                 string
	adminAccessKey, adminSecret string
}

var testPool *pgxpool.Pool

// wallet identifica uma carteira criada por um teste.
type wallet struct {
	ID, PlayerID string
}

// testDatabaseMarker identifica, por COMMENT ON DATABASE, um banco criado ou
// adotado pela suíte; só bancos marcados (ou vazios) são recriados.
const testDatabaseMarker = "wager-integration-test-database"

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func TestMain(m *testing.M) {
	env.dbURL = getenv("TEST_DATABASE_URL", "postgres://wager:wager@localhost:5433/wager_test?sslmode=disable")
	env.runtimeRole = getenv("TEST_RUNTIME_DB_ROLE", "wager_test_app")
	env.runtimePass = getenv("TEST_RUNTIME_DB_PASSWORD", "wager_test_app")
	env.tokenURL = getenv("KEYCLOAK_TOKEN_URL", "http://localhost:8081/realms/wager/protocol/openid-connect/token")
	env.issuer = getenv("OIDC_ISSUER", "http://localhost:8081/realms/wager")
	env.jwksURL = getenv("OIDC_JWKS_URL", "http://localhost:8081/realms/wager/protocol/openid-connect/certs")
	env.awsEndpoint = getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
	env.region = getenv("AWS_REGION", "us-east-1")
	os.Setenv("AWS_REGION", env.region)
	env.adminAccessKey, env.adminSecret = getenv("BROKER_ADMIN_ACCESS_KEY_ID", "test"), getenv("BROKER_ADMIN_SECRET_ACCESS_KEY", "test")
	creds, err := filepath.Abs(getenv("BROKER_CREDENTIALS_FILE", "../../.local/broker/credentials"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	env.brokerCreds = creds
	if _, err := os.Stat(creds); err != nil {
		fmt.Fprintf(os.Stderr, "broker credentials not found (%v): run `docker compose run --rm broker-provision` first\n", err)
		os.Exit(1)
	}
	// Guarda: a prova cria e remove recursos no broker; só roda contra um MiniStack (nunca contra outro
	// emulador ou ambiente que por acaso escute na porta padrão).
	if resp, err := http.Get(env.awsEndpoint + "/_ministack/health"); err != nil || resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "no MiniStack answers at %s (%v): start it with `docker compose up -d --wait ministack` and set AWS_ENDPOINT_URL if the port differs\n", env.awsEndpoint, err)
		os.Exit(1)
	} else {
		resp.Body.Close()
	}
	// A aplicação sob teste (e os processos filhos) usa o principal de RUNTIME do broker pela cadeia padrão
	// do SDK; chaves estáticas no ambiente prevaleceriam sobre o perfil e são removidas.
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
		os.Unsetenv(k)
	}
	os.Setenv("AWS_SHARED_CREDENTIALS_FILE", creds)
	os.Setenv("AWS_PROFILE", "wager-runtime")

	ctx := context.Background()
	fail := func(what string, err error) {
		fmt.Fprintf(os.Stderr, "%s: %v\n", what, err)
		os.Exit(1)
	}
	if err := prepareTestDatabase(ctx, env.dbURL); err != nil {
		fail("prepare test database", err)
	}
	// Reaplica o schema do zero: exercita a reversão e a aplicação das migrations.
	if err := resetSchema(ctx, env.dbURL); err != nil {
		fail("reset schema", err)
	}
	if err := postgres.ProvisionRuntimeRole(ctx, env.dbURL, postgres.RuntimeRole{Name: env.runtimeRole, Password: env.runtimePass}); err != nil {
		fail("provision runtime role", err)
	}
	if env.runtimeURL, err = withCredentials(env.dbURL, env.runtimeRole, env.runtimePass); err != nil {
		fail("runtime url", err)
	}
	if testPool, err = pgxpool.New(ctx, env.dbURL); err != nil {
		fail("pool", err)
	}
	code := m.Run()
	testPool.Close()
	os.Exit(code)
}

func resetSchema(ctx context.Context, ownerURL string) error {
	mig, err := postgres.NewMigrator(ownerURL)
	if err != nil {
		return err
	}
	defer mig.Close()
	if _, err := mig.DownTo(ctx, 0); err != nil {
		return fmt.Errorf("migrate down: %w", err)
	}
	if _, err := mig.Up(ctx); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

// withDBName devolve a URL com outro banco, alterando apenas o componente de caminho.
func withDBName(dbURL, name string) (string, error) {
	u, err := url.Parse(dbURL)
	if err != nil {
		return "", err
	}
	u.Path = "/" + name
	return u.String(), nil
}

func dbName(dbURL string) (string, error) {
	u, err := url.Parse(dbURL)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(u.Path, "/"), nil
}

// withCredentials devolve a URL com outro usuário e senha.
func withCredentials(dbURL, user, pass string) (string, error) {
	u, err := url.Parse(dbURL)
	if err != nil {
		return "", err
	}
	u.User = url.UserPassword(user, pass)
	return u.String(), nil
}

func withAppName(dbURL, name string) string {
	sep := "?"
	if strings.Contains(dbURL, "?") {
		sep = "&"
	}
	return dbURL + sep + "application_name=" + name
}

// prepareTestDatabase cria o banco de teste se não existir e garante que ele é
// seguro de recriar: não pode ser o banco da aplicação (DATABASE_URL /
// MIGRATE_DATABASE_URL), um banco de sistema, nem um banco com tabelas que não
// tenha sido marcado como de teste por esta suíte.
func prepareTestDatabase(ctx context.Context, dbURL string) error {
	name, err := dbName(dbURL)
	if err != nil || name == "" {
		return fmt.Errorf("TEST_DATABASE_URL must name a database: %v", err)
	}
	switch name {
	case "postgres", "template0", "template1":
		return fmt.Errorf("refusing to use system database %q", name)
	}
	for _, key := range []string{"DATABASE_URL", "MIGRATE_DATABASE_URL"} {
		if app := os.Getenv(key); app != "" {
			if appName, err := dbName(app); err == nil && appName == name {
				return fmt.Errorf("refusing to reset %q: it is the application database (%s)", name, key)
			}
		}
	}
	adminURL, err := withDBName(dbURL, "postgres")
	if err != nil {
		return err
	}
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return fmt.Errorf("connect to postgres (is `docker compose up -d postgres` running?): %w", err)
	}
	defer admin.Close(ctx)

	var exists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		if _, err := admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()); err != nil {
			return err
		}
	}
	var comment *string
	if err := admin.QueryRow(ctx, `SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = $1`, name).Scan(&comment); err != nil {
		return err
	}
	if comment == nil || *comment != testDatabaseMarker {
		// Banco sem marca: só é adotado se estiver vazio.
		conn, err := pgx.Connect(ctx, dbURL)
		if err != nil {
			return err
		}
		var tables int
		err = conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')`).Scan(&tables)
		conn.Close(ctx)
		if err != nil {
			return err
		}
		if tables > 0 {
			return fmt.Errorf("refusing to reset %q: it has %d table(s) and is not marked as a test database", name, tables)
		}
		if _, err := admin.Exec(ctx, `COMMENT ON DATABASE `+pgx.Identifier{name}.Sanitize()+` IS '`+testDatabaseMarker+`'`); err != nil {
			return err
		}
	}
	return nil
}

// createScratchDatabase cria um banco de teste exclusivo (sufixo aleatório), vazio
// e marcado, removido ao fim do teste. Devolve a URL (papel dono).
func createScratchDatabase(t testing.TB, purpose string) string {
	t.Helper()
	ctx := context.Background()
	base, err := dbName(env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("%s_%s_%s", base, purpose, strings.ReplaceAll(uuid.NewString(), "-", "")[:10])
	adminURL, _ := withDBName(env.dbURL, "postgres")
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `COMMENT ON DATABASE `+pgx.Identifier{name}.Sanitize()+` IS '`+testDatabaseMarker+`'`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), adminURL)
		if err != nil {
			return
		}
		defer c.Close(context.Background())
		_, _ = c.Exec(context.Background(), `DROP DATABASE IF EXISTS `+pgx.Identifier{name}.Sanitize()+` WITH (FORCE)`)
	})
	u, err := withDBName(env.dbURL, name)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// --- consultas diretas ao banco ------------------------------------------------------------

// --- consultas diretas ao banco ------------------------------------------------------------

func queryInt(t testing.TB, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := testPool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return n
}

func walletState(t testing.TB, id string) (balance, version int64) {
	t.Helper()
	if err := testPool.QueryRow(context.Background(), `SELECT balance, version FROM wallets WHERE id = $1`, id).Scan(&balance, &version); err != nil {
		t.Fatal(err)
	}
	return
}

func countRows(t testing.TB, table, where string, args ...any) int64 {
	t.Helper()
	return queryInt(t, `SELECT count(*) FROM `+table+` WHERE `+where, args...)
}

// assertLedgerMatchesBalance confere o saldo armazenado contra créditos - débitos do ledger.
func assertLedgerMatchesBalance(t testing.TB, walletID string) {
	t.Helper()
	stored, _ := walletState(t, walletID)
	calc := queryInt(t, `SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0)
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
	if stored != calc {
		t.Fatalf("wallet %s: stored balance %d != ledger-derived %d", walletID, stored, calc)
	}
}

// scoped devolve uma função que torna únicos, por teste, chaves e ids externos:
// o banco é compartilhado entre testes e execuções.
func scoped() func(string) string {
	suffix := uuid.NewString()[:8]
	return func(s string) string { return s + "-" + suffix }
}

// parallel executa n chamadas simultaneamente (liberadas juntas por uma barreira)
// e devolve seus resultados na ordem dos índices.
func parallel[T any](n int, fn func(i int) T) []T {
	out := make([]T, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			out[i] = fn(i)
		}()
	}
	close(start)
	wg.Wait()
	return out
}

func connectionsOf(t testing.TB, appName string) int64 {
	return queryInt(t, `SELECT count(*) FROM pg_stat_activity WHERE application_name = $1`, appName)
}
