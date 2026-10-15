package postgres

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
)

var _ app.LedgerReader = (*Store)(nil)

// LedgerPage lê uma página do ledger por posição (seq > afterSeq, sem OFFSET), fora de transação: cada chamada tem a sua
// visão. A ordem por seq é a ordem de confirmação dentro de uma carteira porque todo lançamento é gravado sob o lock de
// linha da carteira, que só é solto no commit: uma posição menor nunca é confirmada depois de uma maior da mesma carteira.
func (s *Store) LedgerPage(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]app.LedgerRow, error) {
	pool, err := s.pool()
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `SELECT seq, id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at
		FROM wallet_ledger_entries WHERE wallet_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`, walletID, afterSeq, limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []app.LedgerRow
	for rows.Next() {
		var (
			seq                   int64
			id, wID, txID         uuid.UUID
			direction, currency   string
			amount, before, after int64
			createdAt             time.Time
		)
		if err := rows.Scan(&seq, &id, &wID, &txID, &direction, &amount, &currency, &before, &after, &createdAt); err != nil {
			return nil, classify(err)
		}
		cur := domain.Currency(currency)
		m, err1 := domain.FromMinor(amount, cur)
		b, err2 := domain.FromMinor(before, cur)
		a, err3 := domain.FromMinor(after, cur)
		if err1 != nil || err2 != nil || err3 != nil {
			return nil, fmt.Errorf("ledger entry %s has an invalid amount or currency", id)
		}
		e, err := domain.RehydrateLedgerEntry(domain.LedgerEntryState{
			ID: id, WalletID: wID, TransactionID: txID, Direction: domain.Direction(direction),
			Money: m, BalanceBefore: b, BalanceAfter: a, CreatedAt: createdAt,
		})
		if err != nil {
			return nil, fmt.Errorf("ledger entry %s cannot be rehydrated: %w", id, err)
		}
		out = append(out, app.LedgerRow{Seq: seq, Entry: e})
	}
	return out, classify(rows.Err())
}

// SetReconciliationHook instala (ou, com nil, remove) um observador chamado entre as consultas do snapshot de
// reconciliação deste Store. Existe apenas para testes que movimentam a carteira entre as duas leituras.
func (s *Store) SetReconciliationHook(fn func(stage string)) {
	if fn == nil {
		s.reconcileHook.Store(nil)
		return
	}
	s.reconcileHook.Store(&fn)
}

// ReconciliationSnapshot lê o saldo e reconstrói o ledger na MESMA transação READ ONLY, REPEATABLE READ: as duas consultas
// enxergam o mesmo instante, sem lock financeiro (um escritor independente não espera, nem é esperado). O isolamento é só
// desta leitura; a UnitOfWork financeira continua READ COMMITTED com lock da carteira.
//
// A soma é feita pelo PostgreSQL em NUMERIC (exata, sem o limite de int64) e volta como texto para ser convertida com
// verificação de faixa; nenhum valor monetário passa por ponto flutuante.
func (s *Store) ReconciliationSnapshot(ctx context.Context, walletID uuid.UUID) (app.ReconciliationSnapshot, error) {
	pool, err := s.pool()
	if err != nil {
		return app.ReconciliationSnapshot{}, err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return app.ReconciliationSnapshot{}, classify(err)
	}
	defer s.rollback(ctx, tx) // leitura: nada a confirmar
	wallet, err := walletRepo{tx}.GetByID(ctx, walletID)
	if err != nil {
		return app.ReconciliationSnapshot{}, err
	}
	if h := s.reconcileHook.Load(); h != nil {
		(*h)("after-wallet-read")
	}
	var (
		entries, foreign int64
		net              string
	)
	err = tx.QueryRow(ctx, `SELECT count(*),
			COALESCE(sum(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0)::text,
			count(*) FILTER (WHERE currency <> $2)
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID, string(wallet.Currency())).Scan(&entries, &net, &foreign)
	if err != nil {
		return app.ReconciliationSnapshot{}, classify(err)
	}
	n, ok := new(big.Int).SetString(net, 10)
	if !ok {
		return app.ReconciliationSnapshot{}, fmt.Errorf("%w: unreadable ledger sum %q", app.ErrReconciliationFailed, net)
	}
	return app.ReconciliationSnapshot{Wallet: wallet, Entries: entries, ForeignCurrencyEntries: foreign, NetMinor: n}, nil
}
