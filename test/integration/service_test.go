//go:build integration

package integration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/postgres"
)

// Casos de uso sobre o PostgreSQL real, sem HTTP: abertura de carteira e BET com
// idempotência, rejeição e concorrência, usando o papel de runtime restrito.

func newService(t *testing.T) *app.Service {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), env.runtimeURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return app.NewService(postgres.NewStore(postgres.StaticPool(pool)),
		app.Config{AllowedCurrencies: []domain.Currency{"BRL", "USD"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func svcOpen(t *testing.T, svc *app.Service, amount string) (walletID, playerID string) {
	t.Helper()
	player := uuid.NewString()
	w, err := svc.OpenWallet(context.Background(), app.OpenWalletCommand{PlayerID: player, Amount: amount, Currency: "BRL", CorrelationID: "svc-test"})
	if err != nil {
		t.Fatal(err)
	}
	return w.ID().String(), player
}

func rawBet(walletID, playerID, key, ext, amount string) domain.RawExternalRequest {
	return domain.RawExternalRequest{
		ProviderID: "svc-provider", ExternalTransactionID: ext, IdempotencyKey: key, PlayerID: playerID, WalletID: walletID,
		RoundID: "round", GameID: "game", Kind: "BET", Amount: amount, Currency: "BRL",
	}
}

func TestServiceOpenWallet(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()

	id, player := svcOpen(t, svc, "1000.00")
	if bal, ver := walletState(t, id); bal != 100000 || ver != 1 {
		t.Fatalf("balance=%d version=%d", bal, ver)
	}
	if n := countRows(t, "wager_transactions", "wallet_id = $1 AND kind = 'OPENING' AND status = 'PROCESSED'", id); n != 1 {
		t.Fatalf("openings = %d", n)
	}
	if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1", id); n != 1 {
		t.Fatalf("ledger entries = %d", n)
	}
	if n := countRows(t, "outbox_events", "aggregate_id = $1 OR payload->'data'->>'walletId' = $2", uuid.MustParse(id), id); n != 2 {
		t.Fatalf("events = %d", n)
	}

	_, err := svc.OpenWallet(ctx, app.OpenWalletCommand{PlayerID: player, Amount: "5.00", Currency: "BRL", CorrelationID: "c"})
	var exists *app.WalletExistsError
	if !errors.Is(err, app.ErrWalletAlreadyExists) || !errors.As(err, &exists) || exists.WalletID.String() != id {
		t.Fatalf("duplicate: %v", err)
	}

	zero, _ := svcOpen(t, svc, "0.00")
	if countRows(t, "wager_transactions", "wallet_id = $1", zero) != 0 || countRows(t, "wallet_ledger_entries", "wallet_id = $1", zero) != 0 {
		t.Fatal("zero opening must create only the wallet")
	}
	for name, cmd := range map[string]app.OpenWalletCommand{
		"negative":          {PlayerID: uuid.NewString(), Amount: "-1.00", Currency: "BRL"},
		"bad scale":         {PlayerID: uuid.NewString(), Amount: "1.001", Currency: "BRL"},
		"bad player":        {PlayerID: "x", Amount: "1.00", Currency: "BRL"},
		"unlisted currency": {PlayerID: uuid.NewString(), Amount: "1.00", Currency: "EUR"},
	} {
		if _, err := svc.OpenWallet(ctx, cmd); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestServiceSubmitBet(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")
	key, ext := id("k"), id("t")

	first, err := svc.Submit(ctx, rawBet(wallet, player, key, ext, "80.00"), "c1")
	if err != nil || first.Replay || first.Transaction.Status() != domain.StatusProcessed || first.Transaction.ResultBalance().Amount() != "20.00" {
		t.Fatalf("first: %+v %v", first, err)
	}
	rej, err := svc.Submit(ctx, rawBet(wallet, player, id("k2"), id("t2"), "80.00"), "c2")
	if err != nil || rej.Transaction.Status() != domain.StatusRejected || rej.Transaction.FailureCode() != domain.FailureInsufficientFunds {
		t.Fatalf("rejection: %+v %v", rej, err)
	}
	replay, err := svc.Submit(ctx, rawBet(wallet, player, key, ext, "80.00"), "c3")
	if err != nil || !replay.Replay || replay.Transaction.ID() != first.Transaction.ID() || replay.Transaction.ResultBalance().Amount() != "20.00" {
		t.Fatalf("replay: %+v %v", replay, err)
	}

	// A identidade registrada vence as validações dependentes do estado atual.
	for name, raw := range map[string]domain.RawExternalRequest{
		"nonexistent wallet": rawBet(uuid.NewString(), player, key, ext, "80.00"),
		"another player":     rawBet(wallet, uuid.NewString(), key, ext, "80.00"),
		"another amount":     rawBet(wallet, player, key, ext, "81.00"),
	} {
		if _, err := svc.Submit(ctx, raw, "c"); !errors.Is(err, app.ErrIdempotencyKeyReused) {
			t.Errorf("%s: %v, want ErrIdempotencyKeyReused", name, err)
		}
	}
	if _, err := svc.Submit(ctx, rawBet(uuid.NewString(), player, id("k3"), ext, "80.00"), "c"); !errors.Is(err, app.ErrExternalTransactionReused) {
		t.Errorf("external id under another key: %v", err)
	}
	// Entradas novas continuam validadas.
	if _, err := svc.Submit(ctx, rawBet(uuid.NewString(), player, id("k4"), id("t4"), "1.00"), "c"); !errors.Is(err, app.ErrWalletNotFound) {
		t.Errorf("new operation, unknown wallet: %v", err)
	}
	if _, err := svc.Submit(ctx, rawBet(wallet, uuid.NewString(), id("k5"), id("t5"), "1.00"), "c"); !errors.Is(err, app.ErrWalletMismatch) {
		t.Errorf("new operation, wrong player: %v", err)
	}
	// Tipos sem processamento e entradas inválidas não gravam nada.
	win := rawBet(wallet, player, id("k6"), id("t6"), "1.00")
	win.Kind = "WIN"
	if _, err := svc.Submit(ctx, win, "c"); !errors.Is(err, app.ErrKindNotSupported) {
		t.Errorf("WIN: %v", err)
	}
	opening := rawBet(wallet, player, id("k7"), id("t7"), "1.00")
	opening.Kind = "OPENING"
	if _, err := svc.Submit(ctx, opening, "c"); !errors.Is(err, domain.ErrKindNotAllowed) {
		t.Errorf("OPENING: %v", err)
	}
	if n := countRows(t, "wager_transactions", "wallet_id = $1", wallet); n != 3 { // OPENING + 2 BET
		t.Fatalf("transactions = %d", n)
	}
	assertLedgerMatchesBalance(t, wallet)
}

func TestServiceConcurrency(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()
	id := scoped()

	t.Run("50 resends produce one debit", func(t *testing.T) {
		wallet, player := svcOpen(t, svc, "100.00")
		key, ext := id("resend-k"), id("resend-t")
		results := make([]app.SubmitResult, 50)
		errs := make([]error, 50)
		parallel(50, func(i int) struct{} {
			results[i], errs[i] = svc.Submit(ctx, rawBet(wallet, player, key, ext, "25.00"), "c")
			return struct{}{}
		})
		var originals int
		for i, r := range results {
			if errs[i] != nil || r.Transaction.ResultBalance().Amount() != "75.00" {
				t.Fatalf("request %d: %v", i, errs[i])
			}
			if !r.Replay {
				originals++
			}
		}
		if originals != 1 || countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'DEBIT'", wallet) != 1 {
			t.Fatalf("originals=%d", originals)
		}
		assertLedgerMatchesBalance(t, wallet)
	})

	t.Run("two 80.00 bets on 100.00", func(t *testing.T) {
		wallet, player := svcOpen(t, svc, "100.00")
		results := make([]app.SubmitResult, 8)
		errs := make([]error, 8)
		parallel(8, func(i int) struct{} {
			bet := i % 2 // duas apostas distintas, cada uma enviada 4 vezes
			name := id("race-" + string(rune('a'+bet)))
			results[i], errs[i] = svc.Submit(ctx, rawBet(wallet, player, name, name, "80.00"), "c")
			return struct{}{}
		})
		processed, rejected := map[uuid.UUID]bool{}, map[uuid.UUID]bool{}
		for i, r := range results {
			if errs[i] != nil {
				t.Fatalf("request %d: %v", i, errs[i])
			}
			switch r.Transaction.Status() {
			case domain.StatusProcessed:
				processed[r.Transaction.ID()] = true
			case domain.StatusRejected:
				rejected[r.Transaction.ID()] = true
			}
		}
		if len(processed) != 1 || len(rejected) != 1 {
			t.Fatalf("processed=%d rejected=%d", len(processed), len(rejected))
		}
		if bal, _ := walletState(t, wallet); bal != 2000 {
			t.Fatalf("balance %d", bal)
		}
		if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'DEBIT'", wallet); n != 1 {
			t.Fatalf("debits = %d", n)
		}
		assertLedgerMatchesBalance(t, wallet)
	})
}
