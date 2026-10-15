//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/postgres"
)

// Entrada por SQS no nível do caso de uso e do PostgreSQL real: inbox e finanças no mesmo commit, identidades e hash dos bytes exatos,
// duplicatas, conflitos, replays financeiros, classificação permanente × transitória e atomicidade.

const consumerName = "test-consumer"

type inboxEnv struct {
	*pubEnv
	svc *app.Service
}

func newInboxEnv(t *testing.T) *inboxEnv {
	e := newStoreEnv(t)
	return &inboxEnv{pubEnv: e, svc: newInboxService(e, nil)}
}

func newInboxService(e *pubEnv, wrap func(app.UnitOfWork) app.UnitOfWork) *app.Service {
	var uow app.UnitOfWork = postgres.NewStore(postgres.StaticPool(e.db.runtime))
	if wrap != nil {
		uow = wrap(uow)
	}
	return app.NewService(uow, app.Config{AllowedCurrencies: []domain.Currency{"BRL"}, Reference: testRef}, quietLog())
}

// body monta o envelope normativo com o texto EXATO (o hash da mensagem é o dos bytes).
func body(msgID, provider, key, ext, wallet, player, kind, amount, ref string) string {
	refField := ""
	if ref != "" {
		refField = fmt.Sprintf(`,"referenceExternalTransactionId":%q`, ref)
	}
	return fmt.Sprintf(`{"messageId":%q,"type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","data":{"providerId":%q,"externalTransactionId":%q,`+
		`"idempotencyKey":%q,"playerId":%q,"walletId":%q,"roundId":"round-1","gameId":"fortune-chimp","kind":%q,"money":{"amount":%q,"currency":"BRL"}%s}}`,
		msgID, provider, ext, key, player, wallet, kind, amount, refField)
}

type walletView struct{ balance, version, ledger, outbox, txs, inbox int64 }

func (e *pubEnv) view(walletID string) walletView {
	e.t.Helper()
	var v walletView
	ctx := context.Background()
	if err := e.owner.QueryRow(ctx, `SELECT balance, version FROM wallets WHERE id = $1`, walletID).Scan(&v.balance, &v.version); err != nil {
		e.t.Fatal(err)
	}
	_ = e.owner.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&v.ledger)
	_ = e.owner.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE payload #>> '{data,walletId}' = $1`, walletID).Scan(&v.outbox)
	_ = e.owner.QueryRow(ctx, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`, walletID).Scan(&v.txs)
	_ = e.owner.QueryRow(ctx, `SELECT count(*) FROM inbox_messages i JOIN wager_transactions t ON t.id = i.transaction_id WHERE t.wallet_id = $1`, walletID).Scan(&v.inbox)
	return v
}

func (e *pubEnv) count(sql string, args ...any) int64 {
	e.t.Helper()
	var n int64
	if err := e.owner.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

// assertLedger confere o saldo armazenado contra a soma do ledger NO BANCO EXCLUSIVO do teste.
func (e *pubEnv) assertLedger(walletID string) {
	e.t.Helper()
	stored := e.count(`SELECT balance FROM wallets WHERE id = $1`, walletID)
	calc := e.count(`SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
	if stored != calc {
		e.t.Fatalf("wallet %s: stored balance %d != ledger-derived %d", walletID, stored, calc)
	}
}

func (e *inboxEnv) open(amount string) (string, string) { return svcOpen(e.t, e.svc, amount) }

func (e *inboxEnv) process(b string) app.InboundResult {
	return e.svc.ProcessMessage(context.Background(), consumerName, []byte(b))
}

func requireDone(t *testing.T, r app.InboundResult, status string) {
	t.Helper()
	if r.Disposition != app.InboundDone || r.Status != status {
		t.Fatalf("result = %+v (cause %v), want Done/%s", r, r.Cause, status)
	}
}

func TestAMessageCommitsInboxFinancesLedgerAndOutboxTogether(t *testing.T) {
	e := newInboxEnv(t)
	w, p := e.open("100.00")
	if v := e.view(w); v.outbox != 2 || v.ledger != 1 || v.version != 1 {
		t.Fatalf("after opening: %+v", v)
	}
	b := body("msg-1", "provider-a", "k-1", "tx-1", w, p, "BET", "25.00", "")
	r := e.process(b)
	requireDone(t, r, "PROCESSED")
	if r.Duplicate || r.FinancialReplay {
		t.Fatalf("a first delivery is neither: %+v", r)
	}
	if v := e.view(w); v.balance != 7500 || v.version != 2 || v.ledger != 2 || v.outbox != 4 || v.inbox != 1 {
		t.Fatalf("after the BET: %+v", v)
	}
	var hash []byte
	var disp string
	var replay bool
	var txID uuid.UUID
	if err := e.owner.QueryRow(context.Background(), `SELECT body_hash, disposition, financial_replay, transaction_id FROM inbox_messages
		WHERE consumer_name = $1 AND message_id = 'msg-1'`, consumerName).Scan(&hash, &disp, &replay, &txID); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(b))
	if string(hash) != string(sum[:]) || disp != "PROCESSED" || replay || txID != r.TransactionID {
		t.Fatalf("inbox row: hash ok=%v disp=%s replay=%v tx=%s vs %s", string(hash) == string(sum[:]), disp, replay, txID, r.TransactionID)
	}
	e.assertLedger(w)
}

func TestFiftyRedeliveriesOfTheSameEnvelopeKeepOneInboxAndNoExtraEffect(t *testing.T) {
	e := newInboxEnv(t)
	w, p := e.open("100.00")
	b := body("msg-dup", "provider-a", "k-dup", "tx-dup", w, p, "BET", "25.00", "")
	first := e.process(b)
	requireDone(t, first, "PROCESSED")
	before := e.view(w)
	for i := 0; i < 50; i++ {
		r := e.process(b)
		requireDone(t, r, "PROCESSED")
		if !r.Duplicate || r.TransactionID != first.TransactionID {
			t.Fatalf("redelivery %d: %+v", i, r)
		}
	}
	if after := e.view(w); after != before {
		t.Fatalf("redeliveries changed the state: %+v -> %+v", before, after)
	}
}

func TestTheSameMessageIdWithADifferentBodyIsAPermanentConflictAndKeepsTheOriginal(t *testing.T) {
	e := newInboxEnv(t)
	w, p := e.open("100.00")
	requireDone(t, e.process(body("msg-c", "provider-a", "k-c", "tx-c", w, p, "BET", "25.00", "")), "PROCESSED")
	before := e.view(w)
	var origHash []byte
	_ = e.owner.QueryRow(context.Background(), `SELECT body_hash FROM inbox_messages WHERE message_id = 'msg-c'`).Scan(&origHash)

	// Mesmo messageId, corpo diferente (outro valor) — e outro com o mesmo conteúdo financeiro mas bytes diferentes (espaços).
	for name, other := range map[string]string{
		"different amount":       body("msg-c", "provider-a", "k-c2", "tx-c2", w, p, "BET", "10.00", ""),
		"same fields, new bytes": " " + body("msg-c", "provider-a", "k-c", "tx-c", w, p, "BET", "25.00", ""),
	} {
		r := e.process(other)
		if r.Disposition != app.InboundPermanent || r.Code != "MESSAGE_ID_CONFLICT" {
			t.Fatalf("%s: %+v", name, r)
		}
	}
	if after := e.view(w); after != before {
		t.Fatalf("a conflicting body moved money or wrote records: %+v -> %+v", before, after)
	}
	var h []byte
	_ = e.owner.QueryRow(context.Background(), `SELECT body_hash FROM inbox_messages WHERE message_id = 'msg-c'`).Scan(&h)
	if string(h) != string(origHash) {
		t.Fatal("the original inbox hash must be intact")
	}
}

func TestANewMessageForTheSameOperationIsAFinancialReplay(t *testing.T) {
	e := newInboxEnv(t)
	w, p := e.open("100.00")
	first := e.process(body("msg-r1", "provider-a", "k-r", "tx-r", w, p, "BET", "25.00", ""))
	requireDone(t, first, "PROCESSED")
	before := e.view(w)
	r := e.process(body("msg-r2", "provider-a", "k-r", "tx-r", w, p, "BET", "25.00", ""))
	requireDone(t, r, "PROCESSED")
	if r.Duplicate || !r.FinancialReplay || r.TransactionID != first.TransactionID {
		t.Fatalf("a new envelope for a registered operation is a financial replay of the original: %+v", r)
	}
	after := e.view(w)
	if after.balance != before.balance || after.version != before.version || after.ledger != before.ledger || after.outbox != before.outbox || after.txs != before.txs || after.inbox != 2 {
		t.Fatalf("replay: %+v -> %+v", before, after)
	}
}

func TestARejectionIsDurableAuditableAndHasNoLedger(t *testing.T) {
	e := newInboxEnv(t)
	w, p := e.open("50.00")
	r := e.process(body("msg-rej", "provider-a", "k-rej", "tx-rej", w, p, "BET", "80.00", ""))
	requireDone(t, r, "REJECTED")
	if v := e.view(w); v.balance != 5000 || v.version != 1 || v.ledger != 1 || v.outbox != 3 || v.inbox != 1 {
		t.Fatalf("rejection: %+v", v)
	}
	var disp string
	_ = e.owner.QueryRow(context.Background(), `SELECT disposition FROM inbox_messages WHERE message_id = 'msg-rej'`).Scan(&disp)
	if disp != "REJECTED" {
		t.Fatal(disp)
	}
}

func TestAPendingReferenceIsDurableWithItsScheduleBeforeTheMessageIsDone(t *testing.T) {
	e := newInboxEnv(t)
	w, p := e.open("100.00")
	r := e.process(body("msg-pend", "provider-a", "k-pend", "tx-pend", w, p, "REFUND", "10.00", "bet-that-arrives-later"))
	requireDone(t, r, "PENDING_REFERENCE")
	if n := e.count(`SELECT count(*) FROM pending_references WHERE transaction_id = $1 AND completed_at IS NULL`, r.TransactionID); n != 1 {
		t.Fatalf("the open schedule must exist with the inbox: %d", n)
	}
	if v := e.view(w); v.balance != 10000 || v.ledger != 1 || v.inbox != 1 {
		t.Fatalf("pending: %+v", v)
	}
}

func TestConcurrentDeliveriesOfTheSameMessageAreResolvedByTheDatabase(t *testing.T) {
	e := newInboxEnv(t)
	w, p := e.open("100.00")
	same := body("msg-race", "provider-a", "k-race", "tx-race", w, p, "BET", "25.00", "")
	results := parallel(12, func(int) app.InboundResult { return e.process(same) })
	firsts := 0
	for _, r := range results {
		requireDone(t, r, "PROCESSED")
		if !r.Duplicate {
			firsts++
		}
	}
	if firsts != 1 {
		t.Fatalf("exactly one delivery applies the operation, got %d", firsts)
	}
	if v := e.view(w); v.balance != 7500 || v.version != 2 || v.ledger != 2 || v.inbox != 1 {
		t.Fatalf("one effect expected: %+v", v)
	}

	// Mesma identidade, corpos diferentes, em corrida: um vence, os demais são duplicata ou conflito permanente; um só efeito.
	w2, p2 := e.open("100.00")
	results = parallel(12, func(i int) app.InboundResult {
		return e.process(body("msg-race2", "provider-a", fmt.Sprintf("k-%d", i), fmt.Sprintf("tx-%d", i), w2, p2, "BET", fmt.Sprintf("%d.00", 10+i), ""))
	})
	won := 0
	for _, r := range results {
		switch {
		case r.Disposition == app.InboundDone && !r.Duplicate:
			won++
		case r.Disposition == app.InboundPermanent && r.Code == "MESSAGE_ID_CONFLICT":
		default:
			t.Fatalf("unexpected result in the race of different bodies: %+v (%v)", r, r.Cause)
		}
	}
	if won != 1 {
		t.Fatalf("exactly one body wins the identity, got %d", won)
	}
	if v := e.view(w2); v.version != 2 || v.ledger != 2 || v.txs != 2 || v.inbox != 1 {
		t.Fatalf("the losers' effects must be rolled back: %+v", v)
	}
}

func TestPermanentInputsCreateNoFinancialRecordsAndTheDiagnosticIsIdempotent(t *testing.T) {
	e := newInboxEnv(t)
	w, p := e.open("100.00")
	base := e.view(w)
	cases := map[string]struct{ body, code string }{
		"not json":            {`not json`, "INVALID_ENVELOPE"},
		"unknown field":       {`{"messageId":"m","extra":1}`, "INVALID_ENVELOPE"},
		"opening":             {body("m-op", "provider-a", "k", "t", w, p, "OPENING", "10.00", ""), "KIND_NOT_ALLOWED"},
		"unknown kind":        {body("m-k", "provider-a", "k", "t", w, p, "GIFT", "10.00", ""), "INVALID_KIND"},
		"bad amount":          {body("m-a", "provider-a", "k", "t", w, p, "BET", "10.999", ""), "INVALID_AMOUNT"},
		"wallet not found":    {body("m-w", "provider-a", "k", "t", uuid.NewString(), p, "BET", "10.00", ""), "WALLET_NOT_FOUND"},
		"wallet mismatch":     {body("m-m", "provider-a", "k", "t", w, uuid.NewString(), "BET", "10.00", ""), "WALLET_MISMATCH"},
		"currency refused":    {`{"messageId":"m-c","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00Z","data":{"providerId":"provider-a","externalTransactionId":"t","idempotencyKey":"k","playerId":"` + p + `","walletId":"` + w + `","roundId":"r","gameId":"g","kind":"BET","money":{"amount":"10.00","currency":"USD"}}}`, "CURRENCY_NOT_SUPPORTED"},
		"refund no reference": {body("m-r", "provider-a", "k", "t", w, p, "REFUND", "10.00", ""), "INVALID_REQUEST"},
	}
	var txsBefore, inboxBefore int64
	_ = e.owner.QueryRow(context.Background(), `SELECT count(*) FROM wager_transactions`).Scan(&txsBefore)
	_ = e.owner.QueryRow(context.Background(), `SELECT count(*) FROM inbox_messages`).Scan(&inboxBefore)
	for name, c := range cases {
		r := e.process(c.body)
		if r.Disposition != app.InboundPermanent || r.Code != c.code {
			t.Errorf("%s: %+v (cause %v), want Permanent/%s", name, r, r.Cause, c.code)
		}
	}
	var txsAfter, inboxAfter int64
	_ = e.owner.QueryRow(context.Background(), `SELECT count(*) FROM wager_transactions`).Scan(&txsAfter)
	_ = e.owner.QueryRow(context.Background(), `SELECT count(*) FROM inbox_messages`).Scan(&inboxAfter)
	if after := e.view(w); after != base || txsAfter != txsBefore || inboxAfter != inboxBefore {
		t.Fatalf("a permanent refusal wrote financial records: %+v -> %+v (txs %d->%d, inbox %d->%d)", base, after, txsBefore, txsAfter, inboxBefore, inboxAfter)
	}

	// Diagnóstico: idempotente, sem transação/ledger, e a consulta distingue ausência de existência.
	ctx := context.Background()
	h := app.MessageHash([]byte(`not json`))
	if ok, err := e.svc.HasDiagnostic(ctx, consumerName, "q", "broker-1", h); err != nil || ok {
		t.Fatalf("absent diagnostic: %v %v", ok, err)
	}
	d := app.Diagnostic{ConsumerName: consumerName, SourceQueue: "q", BrokerMessageID: "broker-1", BodyHash: h, Code: "INVALID_ENVELOPE"}
	for i := 0; i < 3; i++ {
		if err := e.svc.RecordDiagnostic(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	if n := e.count(`SELECT count(*) FROM message_diagnostics WHERE broker_message_id = 'broker-1'`); n != 1 {
		t.Fatalf("diagnostic rows = %d, want 1", n)
	}
	if ok, err := e.svc.HasDiagnostic(ctx, consumerName, "q", "broker-1", h); err != nil || !ok {
		t.Fatalf("present diagnostic: %v %v", ok, err)
	}
	if ok, _ := e.svc.HasDiagnostic(ctx, consumerName, "q", "broker-1", app.MessageHash([]byte("other"))); ok {
		t.Fatal("a different body is a different diagnostic")
	}
	var txsEnd int64
	_ = e.owner.QueryRow(ctx, `SELECT count(*) FROM wager_transactions`).Scan(&txsEnd)
	if txsEnd != txsBefore {
		t.Fatal("a diagnostic must not create a financial transaction")
	}
}

// faultyUOW injeta falha: de toda a UOW, ou só da gravação da inbox DEPOIS de o núcleo financeiro ter gravado tudo (atomicidade).
type faultyUOW struct {
	app.UnitOfWork
	doErr       error
	inboxInsert error
}

func (f faultyUOW) Do(ctx context.Context, fn func(ctx context.Context, r app.Repositories) error) error {
	if f.doErr != nil {
		return f.doErr
	}
	return f.UnitOfWork.Do(ctx, func(ctx context.Context, r app.Repositories) error {
		if f.inboxInsert != nil {
			r.Inbox = failingInbox{r.Inbox, f.inboxInsert}
		}
		return fn(ctx, r)
	})
}

type failingInbox struct {
	app.InboxRepository
	err error
}

func (f failingInbox) Insert(context.Context, app.InboxRecord) error { return f.err }

func TestTransientFailuresAreRetriesAndAnAbortedInboxLeavesNoFinancialEffect(t *testing.T) {
	e := newInboxEnv(t)
	w, p := e.open("100.00")
	before := e.view(w)
	b := body("msg-f", "provider-a", "k-f", "tx-f", w, p, "BET", "25.00", "")
	for name, err := range map[string]error{
		"transient":      fmt.Errorf("%w: %w", app.ErrTransient, errors.New("connection reset")),
		"commit unknown": fmt.Errorf("%w: %w", app.ErrCommitUnknown, errors.New("no answer")),
		"unexpected":     errors.New("something nobody classified"),
		"cancelled":      context.Canceled,
	} {
		svc := newInboxService(e.pubEnv, func(u app.UnitOfWork) app.UnitOfWork { return faultyUOW{UnitOfWork: u, doErr: err} })
		r := svc.ProcessMessage(context.Background(), consumerName, []byte(b))
		if r.Disposition != app.InboundRetry {
			t.Errorf("%s: %+v, want Retry (never a permanent refusal for an infrastructure failure)", name, r)
		}
	}
	// A inbox falha DEPOIS de o núcleo financeiro gravar saldo, transação, ledger e outbox: nada disso pode sobrar.
	svc := newInboxService(e.pubEnv, func(u app.UnitOfWork) app.UnitOfWork {
		return faultyUOW{UnitOfWork: u, inboxInsert: errors.New("inbox write failed")}
	})
	if r := svc.ProcessMessage(context.Background(), consumerName, []byte(b)); r.Disposition != app.InboundRetry {
		t.Fatalf("aborted inbox: %+v", r)
	}
	if after := e.view(w); after != before {
		t.Fatalf("an aborted attempt left financial effects: %+v -> %+v", before, after)
	}
	// Recuperação pela identidade: a entrega seguinte executa a operação uma vez.
	requireDone(t, e.process(b), "PROCESSED")
	if v := e.view(w); v.balance != 7500 || v.version != 2 || v.inbox != 1 {
		t.Fatalf("recovery: %+v", v)
	}
}

func TestInboxAndDiagnosticsAreAppendOnlyWithMinimalPrivileges(t *testing.T) {
	e := newInboxEnv(t)
	w, p := e.open("100.00")
	requireDone(t, e.process(body("msg-g", "provider-a", "k-g", "tx-g", w, p, "BET", "10.00", "")), "PROCESSED")
	if err := e.svc.RecordDiagnostic(context.Background(), app.Diagnostic{ConsumerName: consumerName, SourceQueue: "q", BrokerMessageID: "b1",
		BodyHash: app.MessageHash([]byte("x")), Code: "INVALID_ENVELOPE"}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	code := func(err error) string {
		var pg *pgconn.PgError
		if errors.As(err, &pg) {
			return pg.Code
		}
		return fmt.Sprint(err)
	}
	for name, stmt := range map[string]string{
		"runtime UPDATE inbox":         `UPDATE inbox_messages SET disposition = 'REJECTED'`,
		"runtime DELETE inbox":         `DELETE FROM inbox_messages`,
		"runtime TRUNCATE inbox":       `TRUNCATE inbox_messages`,
		"runtime UPDATE diagnostics":   `UPDATE message_diagnostics SET code = 'X'`,
		"runtime DELETE diagnostics":   `DELETE FROM message_diagnostics`,
		"runtime TRUNCATE diagnostics": `TRUNCATE message_diagnostics`,
	} {
		if _, err := e.db.runtime.Exec(ctx, stmt); code(err) != "42501" {
			t.Errorf("%s: %v, want permission denied", name, err)
		}
	}
	for name, stmt := range map[string]string{
		"owner UPDATE inbox":       `UPDATE inbox_messages SET message_id = 'other'`,
		"owner DELETE inbox":       `DELETE FROM inbox_messages`,
		"owner UPDATE diagnostics": `UPDATE message_diagnostics SET code = 'X'`,
		"owner TRUNCATE inbox":     `TRUNCATE inbox_messages`,
	} {
		if _, err := e.owner.Exec(ctx, stmt); code(err) != "23000" {
			t.Errorf("%s: %v, want the append-only trigger (23000)", name, err)
		}
	}
	// Forma imposta pelo banco: hash de 32 bytes, disposição válida, FK da transação, identidade única.
	tx := uuid.New()
	for name, stmt := range map[string]string{
		"short hash":        `INSERT INTO inbox_messages VALUES ('c','m1','\x00',now(),now(),'PROCESSED',false,'` + tx.String() + `')`,
		"orphan completion": `INSERT INTO inbox_messages VALUES ('c','m2',decode(repeat('00',32),'hex'),now(),now(),'PROCESSED',false,'` + tx.String() + `')`,
		"bad disposition":   `INSERT INTO inbox_messages VALUES ('c','m3',decode(repeat('00',32),'hex'),now(),now(),'DONE',false,gen_random_uuid())`,
		"empty message id":  `INSERT INTO inbox_messages VALUES ('c','',decode(repeat('00',32),'hex'),now(),now(),'PROCESSED',false,gen_random_uuid())`,
	} {
		if _, err := e.owner.Exec(ctx, stmt); err == nil {
			t.Errorf("%s: the database accepted an invalid inbox row", name)
		}
	}
}

// A migration 00008 aplica, reverte e reaplica sem erro; a concessão ao runtime só inclui as tabelas novas depois dela e só SELECT/INSERT.
func TestTheInboxMigrationAppliesRollsBackAndGrantsOnlySelectAndInsert(t *testing.T) {
	ctx := context.Background()
	url, mig, conn := scratchAt(t, "inboxmig", 7)
	exists := func() bool {
		var ok bool
		if err := conn.QueryRow(ctx, `SELECT to_regclass('public.inbox_messages') IS NOT NULL AND to_regclass('public.message_diagnostics') IS NOT NULL`).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if exists() {
		t.Fatal("the tables must not exist before 00008")
	}
	if err := postgres.ProvisionRuntimeRole(ctx, url, postgres.RuntimeRole{Name: env.runtimeRole, Password: env.runtimePass}); err != nil {
		t.Fatalf("provisioning before 00008 must omit the new tables: %v", err)
	}
	if _, err := mig.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if !exists() || schemaVersion(t, conn) != 8 {
		t.Fatal("00008 must create both tables")
	}
	if err := postgres.ProvisionRuntimeRole(ctx, url, postgres.RuntimeRole{Name: env.runtimeRole, Password: env.runtimePass}); err != nil {
		t.Fatal(err)
	}
	priv := func(table, p string) bool {
		var ok bool
		if err := conn.QueryRow(ctx, `SELECT has_table_privilege($1, $2, $3)`, env.runtimeRole, table, p).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	for _, table := range []string{"inbox_messages", "message_diagnostics"} {
		if !priv(table, "SELECT") || !priv(table, "INSERT") || priv(table, "UPDATE") || priv(table, "DELETE") || priv(table, "TRUNCATE") {
			t.Fatalf("%s: the runtime must have exactly SELECT and INSERT", table)
		}
	}
	if _, err := mig.Down(ctx); err != nil {
		t.Fatalf("down: %v", err)
	}
	if exists() || schemaVersion(t, conn) != 7 {
		t.Fatal("the rollback must drop the tables and return to schema 7")
	}
	if _, err := mig.Up(ctx); err != nil || !exists() {
		t.Fatalf("reapply: %v", err)
	}
}
