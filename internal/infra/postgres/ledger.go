package postgres

import (
	"context"

	"desafio-tecnico-go/internal/domain"
)

type ledgerRepo struct{ q querier }

// Append insere um lançamento. O ledger é append-only: não há operação de
// atualização ou exclusão, e o banco as rejeita por trigger.
func (r ledgerRepo) Append(ctx context.Context, e domain.LedgerEntry) error {
	_, err := r.q.Exec(ctx, `INSERT INTO wallet_ledger_entries
		(id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Money().Minor(),
		string(e.Money().Currency()), e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), e.CreatedAt())
	return classify(err)
}
