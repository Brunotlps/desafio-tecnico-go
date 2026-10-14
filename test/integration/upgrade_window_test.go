//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"desafio-tecnico-go/internal/bootstrap"
	"desafio-tecnico-go/internal/config"
	"desafio-tecnico-go/internal/infra/postgres"
)

// Aplicação das migrations com escritores ativos.
//
// A atomicidade da transação SQL de uma migration não inclui o commit de outras conexões,
// e o lock de sessão do migrador só coordena migrations concorrentes. Um escritor do schema
// anterior que já inseriu linhas e mantém o commit aberto não é visto por uma validação do
// histórico; se a migration só validasse e depois instalasse as garantias, esse commit
// atravessaria a janela. Estes testes reproduzem a sondagem da auditoria e fixam o
// comportamento: o schema só chega à versão final se o histórico foi validado SOB exclusão
// dos escritores, e a aplicação recusa iniciar enquanto a versão final não for alcançada.

type upgradeDB struct {
	t       *testing.T
	url     string
	name    string
	mig     *postgres.Migrator
	owner   *pgx.Conn
	runtime *pgxpool.Pool
}

// newUpgradeDB cria um banco exclusivo na versão indicada, com o papel de runtime próprio da suíte.
func newUpgradeDB(t *testing.T, purpose string, version int64) *upgradeDB {
	t.Helper()
	ctx := context.Background()
	url, mig, conn := scratchAt(t, purpose, version)
	if err := postgres.ProvisionRuntimeRole(ctx, url, postgres.RuntimeRole{Name: env.runtimeRole, Password: env.runtimePass}); err != nil {
		t.Fatal(err)
	}
	rtURL, err := withCredentials(url, env.runtimeRole, env.runtimePass)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, rtURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close) // registrado depois do banco: fecha antes de ele ser removido
	name, _ := dbName(url)
	return &upgradeDB{t: t, url: url, name: name, mig: mig, owner: conn, runtime: pool}
}

func (d *upgradeDB) runtimeURL() string {
	u, err := withCredentials(d.url, env.runtimeRole, env.runtimePass)
	if err != nil {
		d.t.Fatal(err)
	}
	return u
}

// seedWallet grava (como dono) uma carteira com abertura de 100,00 e a confirma.
func (d *upgradeDB) seedWallet() wallet {
	d.t.Helper()
	ctx := context.Background()
	tx, err := d.owner.Begin(ctx)
	if err != nil {
		d.t.Fatal(err)
	}
	sw := seedOpened(d.t, ctx, tx, 10000)
	if err := tx.Commit(ctx); err != nil {
		d.t.Fatal(err)
	}
	return wallet{ID: sw.id.String(), PlayerID: sw.player.String()}
}

func (d *upgradeDB) version() int64 { return schemaVersion(d.t, d.owner) }

func (d *upgradeDB) count(sql string, args ...any) int64 {
	d.t.Helper()
	var n int64
	if err := d.owner.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		d.t.Fatal(err)
	}
	return n
}

// orphans conta BET/WIN/... PROCESSED sem lançamento da própria carteira.
func (d *upgradeDB) orphans() int64 {
	return d.count(`SELECT count(*) FROM wager_transactions t WHERE t.status = 'PROCESSED' AND t.kind <> 'LOSS'
		AND NOT EXISTS (SELECT 1 FROM wallet_ledger_entries l WHERE l.transaction_id = t.id AND l.wallet_id = t.wallet_id)`)
}

// waitMigrationBlocked espera a sessão do migrador (qualquer papel que não seja o de runtime)
// ficar bloqueada num lock, ou seja, esperando um escritor confirmar.
func (d *upgradeDB) waitMigrationBlocked() string {
	d.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var q string
		err := testPool.QueryRow(context.Background(), `SELECT query FROM pg_stat_activity
			WHERE datname = $1 AND wait_event_type = 'Lock' AND usename <> $2 LIMIT 1`, d.name, env.runtimeRole).Scan(&q)
		if err == nil {
			return q
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			d.t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	d.t.Fatal("the migration never blocked waiting for the open writer")
	return ""
}

// waitRuntimeBlocked espera uma sessão do papel de runtime ficar bloqueada num lock.
func (d *upgradeDB) waitRuntimeBlocked() {
	d.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := testPool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity
			WHERE datname = $1 AND wait_event_type = 'Lock' AND usename = $2`, d.name, env.runtimeRole).Scan(&n); err != nil {
			d.t.Fatal(err)
		}
		if n > 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	d.t.Fatal("the late writer was not held back while the upgrade was waiting")
}

type upResult struct {
	err error
}

// startUp inicia mig.Up em segundo plano.
func (d *upgradeDB) startUp() <-chan upResult {
	ch := make(chan upResult, 1)
	go func() {
		_, err := d.mig.Up(context.Background())
		ch <- upResult{err}
	}()
	return ch
}

func awaitUp(t *testing.T, ch <-chan upResult) error {
	t.Helper()
	select {
	case r := <-ch:
		return r.err
	case <-time.After(60 * time.Second):
		t.Fatal("the upgrade did not finish")
		return nil
	}
}

// writeValidDebit grava, na transação dada, uma BET PROCESSED completa (transação, lançamento,
// saldo e resultado coerentes), na mesma ordem de locks do caso de uso: carteira, transação, lançamento.
func writeValidDebit(t *testing.T, ctx context.Context, tx pgx.Tx, w wallet, bet directTx, before, after int64) error {
	t.Helper()
	if err := setBalance(ctx, tx, w, after); err != nil {
		return err
	}
	if err := bet.insert(t, ctx, tx, w, "PROCESSED", nil, after); err != nil {
		return err
	}
	return entry(ctx, tx, w, bet.ID, "DEBIT", bet.Minor, before, after)
}

// tryStartApp tenta iniciar a aplicação (Fx completo) contra o banco, com o papel de runtime.
func tryStartApp(t *testing.T, d *upgradeDB) (*testApp, error) {
	t.Helper()
	cfg := testConfig(func(c *config.Config) { c.DatabaseURL = d.runtimeURL() })
	a := &testApp{}
	a.app = fx.New(bootstrap.Options(cfg), fx.Populate(&a.srv), fx.NopLogger)
	if err := a.app.Err(); err != nil {
		t.Fatalf("compose app: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.app.Start(ctx); err != nil {
		return nil, err
	}
	a.URL = "http://" + a.srv.Addr()
	t.Cleanup(func() { a.Stop(t) })
	return a, nil
}

func finalVersion(t *testing.T) int64 {
	t.Helper()
	v, err := postgres.ExpectedVersion()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func requireStartupRefused(t *testing.T, d *upgradeDB) {
	t.Helper()
	_, err := tryStartApp(t, d)
	if err == nil {
		t.Fatalf("the application started on a database at version %d, below the expected %d", d.version(), finalVersion(t))
	}
	if !strings.Contains(err.Error(), "schema is at version") {
		t.Fatalf("startup refused for another reason: %v", err)
	}
}

// A sondagem da auditoria: escritor do schema anterior com uma BET PROCESSED sem lançamento,
// commit ainda aberto, enquanto o upgrade começa. O upgrade NUNCA pode terminar na versão final
// com a operação órfã confirmada; o histórico incompatível é relatado, nada é apagado e a
// aplicação recusa iniciar.
func TestUpgradeWithAnOpenOldWriterNeverEndsAtTheFinalVersionWithAnOrphan(t *testing.T) {
	ctx := context.Background()
	d := newUpgradeDB(t, "upg_orphan", 2)
	w := d.seedWallet()
	other := d.seedWallet() // carteira de um escritor que chega durante o upgrade

	// Escritor antigo (papel restrito, esquema 2): a falha aceita pelas garantias antigas.
	old, err := d.runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Rollback(context.Background())
	orphan := newDirectBet("provider-a", uniqueID("upg-k"), uniqueID("upg-t"), 1000)
	if err := orphan.insert(t, ctx, old, w, "PROCESSED", nil, int64(9000)); err != nil {
		t.Fatal(err)
	}

	up := d.startUp()
	t.Logf("upgrade blocked on: %.80s", d.waitMigrationBlocked())

	// Um segundo escritor chega durante o upgrade e tenta confirmar outra operação órfã: é segurado
	// pelo upgrade e, quando prossegue, já encontra as garantias instaladas.
	late := newDirectBet("provider-a", uniqueID("late-k"), uniqueID("late-t"), 1000)
	var lateErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		lateErr = rtInTx(t, d.runtime, func(ctx context.Context, tx pgx.Tx) error {
			return late.insert(t, ctx, tx, other, "PROCESSED", nil, int64(9000))
		})
	}()
	d.waitRuntimeBlocked()

	if err := old.Commit(ctx); err != nil { // aceito pelo schema 2
		t.Fatalf("the old writer must be able to commit under the old guarantees: %v", err)
	}
	upErr := awaitUp(t, up)
	wg.Wait()

	if upErr == nil {
		t.Fatalf("the upgrade succeeded (schema %d) although a PROCESSED movement without ledger entry committed during it", d.version())
	}
	if !strings.Contains(upErr.Error(), "without a ledger entry") || !strings.Contains(upErr.Error(), orphan.ID.String()) {
		t.Fatalf("the refusal must report the offending transaction %s: %v", orphan.ID, upErr)
	}
	if v := d.version(); v == finalVersion(t) {
		t.Fatalf("the schema reached the final version %d despite the incompatible history", v)
	}
	expectPgMessage(t, "late writer under the new guarantees", lateErr, "23514", "no ledger entry")

	// Nada foi apagado nem reescrito: a operação órfã segue lá para correção manual.
	if n := d.count(`SELECT count(*) FROM wager_transactions WHERE id = $1 AND status = 'PROCESSED'`, orphan.ID); n != 1 {
		t.Fatalf("the offending transaction was altered or removed (%d)", n)
	}
	if n := d.count(`SELECT count(*) FROM wager_transactions WHERE id = $1`, late.ID); n != 0 {
		t.Fatal("the late orphan was persisted")
	}
	requireStartupRefused(t, d)

	// Depois da correção manual do registro (aqui, completando o movimento), o upgrade conclui.
	fix := d.owner
	tx, err := fix.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := entry(ctx, tx, w, orphan.ID, "DEBIT", 1000, 10000, 9000); err != nil {
		t.Fatal(err)
	}
	if err := setBalance(ctx, tx, w, 9000); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("manual correction: %v", err)
	}
	if _, err := d.mig.Up(ctx); err != nil {
		t.Fatalf("upgrade after the manual correction: %v", err)
	}
	if d.version() != finalVersion(t) || d.orphans() != 0 {
		t.Fatalf("version %d, orphans %d", d.version(), d.orphans())
	}
	a, err := tryStartApp(t, d)
	if err != nil {
		t.Fatalf("startup after the final version: %v", err)
	}
	r := submit(t, a, "provider-a", orphan.Key, betBodyFor(w, orphan))
	if r.Status != 200 || r.Body["idempotentReplay"] != true || r.str("balance", "amount") != "90.00" {
		t.Fatalf("replay of the corrected operation: %d %s", r.Status, r.Raw)
	}
}

// Escritores válidos, abertos ou chegando durante o upgrade, não são prejudicados: o upgrade
// espera os que já escreveram, segura os novos por instantes e termina na versão final,
// preservando todas as operações.
func TestUpgradeWithConcurrentValidWritersCompletes(t *testing.T) {
	ctx := context.Background()
	d := newUpgradeDB(t, "upg_valid", 2)
	w1, w2 := d.seedWallet(), d.seedWallet()

	old, err := d.runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Rollback(context.Background())
	first := newDirectBet("provider-a", uniqueID("v1-k"), uniqueID("v1-t"), 1000)
	if err := writeValidDebit(t, ctx, old, w1, first, 10000, 9000); err != nil {
		t.Fatal(err)
	}

	up := d.startUp()
	d.waitMigrationBlocked()

	second := newDirectBet("provider-a", uniqueID("v2-k"), uniqueID("v2-t"), 2500)
	lateDone := make(chan error, 1)
	go func() { // chega durante o upgrade, em outra carteira
		lateDone <- rtInTx(t, d.runtime, func(ctx context.Context, tx pgx.Tx) error {
			return writeValidDebit(t, ctx, tx, w2, second, 10000, 7500)
		})
	}()
	d.waitRuntimeBlocked()

	if err := old.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := awaitUp(t, up); err != nil {
		t.Fatalf("upgrade with valid concurrent writers: %v", err)
	}
	if err := <-lateDone; err != nil {
		t.Fatalf("the late valid writer must commit under the new guarantees: %v", err)
	}
	if d.version() != finalVersion(t) {
		t.Fatalf("version %d, want %d", d.version(), finalVersion(t))
	}
	if d.orphans() != 0 {
		t.Fatal("orphans after the upgrade")
	}
	for _, c := range []struct {
		w     wallet
		debit int64
	}{{w1, 9000}, {w2, 7500}} {
		var bal int64
		if err := d.owner.QueryRow(ctx, `SELECT balance FROM wallets WHERE id = $1`, c.w.ID).Scan(&bal); err != nil || bal != c.debit {
			t.Fatalf("wallet %s balance %d (%v), want %d", c.w.ID, bal, err, c.debit)
		}
	}
	a, err := tryStartApp(t, d)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		w    wallet
		bet  directTx
		want string
	}{{w1, first, "90.00"}, {w2, second, "75.00"}} {
		r := submit(t, a, "provider-a", c.bet.Key, betBodyFor(c.w, c.bet))
		if r.Status != 200 || r.Body["idempotentReplay"] != true || r.str("balance", "amount") != c.want {
			t.Fatalf("replay of %s: %d %s", c.bet.Ex, r.Status, r.Raw)
		}
	}
}

// Banco em que a janela já ocorreu (a 00003 foi aplicada com um escritor ativo e deixou registros
// incompatíveis, apesar do trigger instalado): a migration nova relê TODO o histórico e recusa,
// relatando contagem e ids, sem alterar nada e sem alcançar a versão final.
func TestUpgradeRefusesHistoryLeftByTheWindowWithoutTouchingIt(t *testing.T) {
	ctx := context.Background()

	// Cada caso fabrica, como dono, o estado que a janela deixaria (desabilitando um trigger só
	// pelo tempo de gravar, como o escritor antigo fez sem ele) e depois reabilita tudo.
	type fabricate func(t *testing.T, d *upgradeDB, w wallet) (offenderID string)
	cases := map[string]struct {
		triggers  []struct{ table, name string }
		fabricate fabricate
		want      string
	}{
		"completed BET without a ledger entry": {
			triggers: []struct{ table, name string }{{"wager_transactions", "ctrg_wager_tx_requires_movement"}},
			fabricate: func(t *testing.T, d *upgradeDB, w wallet) string {
				bet := newDirectBet("provider-a", uniqueID("h-k"), uniqueID("h-t"), 1000)
				mustExec(t, d, func(ctx context.Context, tx pgx.Tx) error {
					return bet.insert(t, ctx, tx, w, "PROCESSED", nil, int64(9000))
				})
				return bet.ID.String()
			},
			want: "without a ledger entry",
		},
		"result differs from the ledger entry": {
			triggers: []struct{ table, name string }{
				{"wager_transactions", "ctrg_wager_tx_requires_movement"}, {"wallet_ledger_entries", "ctrg_ledger_matches_transaction"}},
			fabricate: func(t *testing.T, d *upgradeDB, w wallet) string {
				bet := newDirectBet("provider-a", uniqueID("h-k"), uniqueID("h-t"), 1000)
				mustExec(t, d, func(ctx context.Context, tx pgx.Tx) error {
					if err := bet.insert(t, ctx, tx, w, "PROCESSED", nil, int64(77700)); err != nil {
						return err
					}
					if err := entry(ctx, tx, w, bet.ID, "DEBIT", 1000, 10000, 9000); err != nil {
						return err
					}
					return setBalance(ctx, tx, w, 9000)
				})
				return bet.ID.String()
			},
			want: "result_balance differs from the balance_after",
		},
		"broken ledger chain": {
			triggers: []struct{ table, name string }{{"wallet_ledger_entries", "trg_ledger_enforce_chain"}},
			fabricate: func(t *testing.T, d *upgradeDB, w wallet) string {
				bet := newDirectBet("provider-a", uniqueID("h-k"), uniqueID("h-t"), 1000)
				mustExec(t, d, func(ctx context.Context, tx pgx.Tx) error {
					// o lançamento declara antes = 90,00 em vez de 100,00 (depois = 80,00)
					if err := bet.insert(t, ctx, tx, w, "PROCESSED", nil, int64(8000)); err != nil {
						return err
					}
					if err := entry(ctx, tx, w, bet.ID, "DEBIT", 1000, 9000, 8000); err != nil {
						return err
					}
					return setBalance(ctx, tx, w, 8000)
				})
				return bet.ID.String()
			},
			want: "break the balance chain",
		},
		"wallet balance differs from the ledger": {
			triggers: []struct{ table, name string }{
				{"wallets", "ctrg_wallets_balance_matches_ledger"}, {"wallet_ledger_entries", "ctrg_ledger_balance_matches_wallet"}},
			fabricate: func(t *testing.T, d *upgradeDB, w wallet) string {
				mustExec(t, d, func(ctx context.Context, tx pgx.Tx) error { return setBalance(ctx, tx, w, 12345) })
				return w.ID
			},
			want: "balance differs from the ledger",
		},
		"ledger entry of a rejected transaction": {
			triggers: []struct{ table, name string }{{"wallet_ledger_entries", "ctrg_ledger_matches_transaction"}},
			fabricate: func(t *testing.T, d *upgradeDB, w wallet) string {
				bet := newDirectBet("provider-a", uniqueID("h-k"), uniqueID("h-t"), 1000)
				mustExec(t, d, func(ctx context.Context, tx pgx.Tx) error {
					if err := bet.insert(t, ctx, tx, w, "REJECTED", "INSUFFICIENT_FUNDS", int64(9000)); err != nil {
						return err
					}
					if err := entry(ctx, tx, w, bet.ID, "DEBIT", 1000, 10000, 9000); err != nil {
						return err
					}
					return setBalance(ctx, tx, w, 9000)
				})
				return bet.ID.String()
			},
			want: "inconsistent with their financial transaction",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			d := newUpgradeDB(t, "upg_history", 3)
			w := d.seedWallet()
			for _, tg := range tc.triggers {
				if _, err := d.owner.Exec(ctx, `ALTER TABLE `+tg.table+` DISABLE TRIGGER `+tg.name); err != nil {
					t.Fatal(err)
				}
			}
			offender := tc.fabricate(t, d, w)
			for _, tg := range tc.triggers {
				if _, err := d.owner.Exec(ctx, `ALTER TABLE `+tg.table+` ENABLE TRIGGER `+tg.name); err != nil {
					t.Fatal(err)
				}
			}
			before := [3]int64{
				d.count(`SELECT count(*) FROM wager_transactions`), d.count(`SELECT count(*) FROM wallet_ledger_entries`),
				d.count(`SELECT balance FROM wallets WHERE id = $1`, w.ID)}

			_, err := d.mig.Up(ctx)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "preflight") {
				t.Fatalf("expected a preflight refusal containing %q, got: %v", tc.want, err)
			}
			if !strings.Contains(err.Error(), offender) {
				t.Fatalf("the report must identify the offender %s: %v", offender, err)
			}
			if d.version() != 3 {
				t.Fatalf("the schema moved to %d despite the refusal", d.version())
			}
			after := [3]int64{
				d.count(`SELECT count(*) FROM wager_transactions`), d.count(`SELECT count(*) FROM wallet_ledger_entries`),
				d.count(`SELECT balance FROM wallets WHERE id = $1`, w.ID)}
			if after != before {
				t.Fatalf("historical records were altered: %v -> %v", before, after)
			}
			requireStartupRefused(t, d)
		})
	}
}

func mustExec(t *testing.T, d *upgradeDB, fn func(ctx context.Context, tx pgx.Tx) error) {
	t.Helper()
	if err := inTxOn(d.owner, fn); err != nil {
		t.Fatal(err)
	}
}

// Uma garantia ausente ou desabilitada no banco impede a versão final: a migration verifica,
// já com os escritores excluídos, que os triggers existem e estão habilitados.
func TestUpgradeRefusesWhenAGuaranteeIsMissingOrDisabled(t *testing.T) {
	ctx := context.Background()
	for _, tg := range []struct{ table, name string }{
		{"wager_transactions", "ctrg_wager_tx_requires_movement"},
		{"wallet_ledger_entries", "trg_ledger_enforce_chain"},
		{"wallet_ledger_entries", "trg_ledger_no_update_delete"},
		{"wallets", "ctrg_wallets_balance_matches_ledger"},
	} {
		t.Run(tg.name, func(t *testing.T) {
			d := newUpgradeDB(t, "upg_trigger", 3)
			d.seedWallet()
			if _, err := d.owner.Exec(ctx, `ALTER TABLE `+tg.table+` DISABLE TRIGGER `+tg.name); err != nil {
				t.Fatal(err)
			}
			_, err := d.mig.Up(ctx)
			if err == nil || !strings.Contains(err.Error(), tg.name) {
				t.Fatalf("expected a refusal naming %s, got: %v", tg.name, err)
			}
			if d.version() != 3 {
				t.Fatalf("version %d", d.version())
			}
			requireStartupRefused(t, d)
		})
	}
}

// Histórico válido é preservado em qualquer caminho (2→final e 3→final), e a reversão e a
// reaplicação funcionam sem tocar nos dados.
func TestUpgradePreservesValidHistoryAndIsReversible(t *testing.T) {
	ctx := context.Background()
	for _, from := range []int64{2, 3} {
		d := newUpgradeDB(t, "upg_preserve", from)
		data := populateV2(t, d.owner, false, false)
		before := counts(t, d.owner, data.wallet)

		if _, err := d.mig.Up(ctx); err != nil {
			t.Fatalf("from %d: %v", from, err)
		}
		if d.version() != finalVersion(t) {
			t.Fatalf("from %d: version %d", from, d.version())
		}
		if after := counts(t, d.owner, data.wallet); after != before {
			t.Fatalf("from %d: data changed %v -> %v", from, before, after)
		}
		if !movementGuaranteeInstalled(t, d.owner) {
			t.Fatal("guarantee missing after the upgrade")
		}
		// reverte até o ponto de partida e reaplica
		for d.version() > from {
			if _, err := d.mig.Down(ctx); err != nil {
				t.Fatalf("down: %v", err)
			}
		}
		if after := counts(t, d.owner, data.wallet); after != before {
			t.Fatalf("from %d: data changed by the reversal %v -> %v", from, before, after)
		}
		if _, err := d.mig.Up(ctx); err != nil {
			t.Fatalf("re-apply from %d: %v", from, err)
		}
		if d.version() != finalVersion(t) {
			t.Fatalf("re-apply: version %d", d.version())
		}
		a, err := tryStartApp(t, d)
		if err != nil {
			t.Fatalf("startup at the final version: %v", err)
		}
		a.Stop(t)
	}
}

// A migration final (00004) é a que lê o histórico sob exclusão: ela ESPERA o escritor que já
// escreveu e ainda não confirmou, retém os que chegam e só então valida e conclui.
func TestFinalMigrationWaitsForOpenWritersAndHoldsNewOnes(t *testing.T) {
	ctx := context.Background()
	d := newUpgradeDB(t, "upg_final_lock", 3)
	w1, w2 := d.seedWallet(), d.seedWallet()

	open, err := d.runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer open.Rollback(context.Background())
	first := newDirectBet("provider-a", uniqueID("f1-k"), uniqueID("f1-t"), 1000)
	if err := writeValidDebit(t, ctx, open, w1, first, 10000, 9000); err != nil {
		t.Fatal(err)
	}

	up := d.startUp()
	if q := d.waitMigrationBlocked(); !strings.Contains(q, "LOCK TABLE") {
		t.Fatalf("the final migration must wait on the writer exclusion lock, it is waiting on: %.100s", q)
	}
	second := newDirectBet("provider-a", uniqueID("f2-k"), uniqueID("f2-t"), 2500)
	lateDone := make(chan error, 1)
	go func() {
		lateDone <- rtInTx(t, d.runtime, func(ctx context.Context, tx pgx.Tx) error {
			return writeValidDebit(t, ctx, tx, w2, second, 10000, 7500)
		})
	}()
	d.waitRuntimeBlocked()

	if err := open.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := awaitUp(t, up); err != nil {
		t.Fatalf("final migration: %v", err)
	}
	if err := <-lateDone; err != nil {
		t.Fatalf("late valid writer: %v", err)
	}
	if d.version() != finalVersion(t) || d.orphans() != 0 {
		t.Fatalf("version %d orphans %d", d.version(), d.orphans())
	}
}

// Escritores que NÃO passam pelos triggers (sessão com session_replication_role = replica, restauração com
// triggers desabilitados, dono do schema com trigger desabilitado) só são detectados pela validação — por
// isso ela precisa ocorrer sob exclusão. Um commit assim, aberto durante o upgrade, é visto e recusado.
func TestFinalMigrationSeesCommitsOfWritersThatBypassTriggers(t *testing.T) {
	ctx := context.Background()
	d := newUpgradeDB(t, "upg_bypass", 3)
	w := d.seedWallet()

	bypass, err := pgx.Connect(ctx, d.url)
	if err != nil {
		t.Fatal(err)
	}
	defer bypass.Close(ctx)
	tx, err := bypass.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Skipf("the owner role cannot bypass triggers (superuser required): %v", err)
	}
	orphan := newDirectBet("provider-a", uniqueID("bp-k"), uniqueID("bp-t"), 1000)
	if err := orphan.insert(t, ctx, tx, w, "PROCESSED", nil, int64(9000)); err != nil {
		t.Fatal(err)
	}

	up := d.startUp()
	d.waitMigrationBlocked()
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("a writer that bypasses the triggers commits without checks: %v", err)
	}
	upErr := awaitUp(t, up)
	if upErr == nil || !strings.Contains(upErr.Error(), "without a ledger entry") || !strings.Contains(upErr.Error(), orphan.ID.String()) {
		t.Fatalf("the final migration must see the commit and refuse, got: %v", upErr)
	}
	if d.version() != 3 {
		t.Fatalf("version %d", d.version())
	}
	if n := d.count(`SELECT count(*) FROM wager_transactions WHERE id = $1`, orphan.ID); n != 1 {
		t.Fatal("the offending record was altered")
	}
	requireStartupRefused(t, d)
}
