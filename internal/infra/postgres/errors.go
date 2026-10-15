package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgconn"

	"desafio-tecnico-go/internal/app"
)

// Nomes das constraints consultados para classificar violações de unicidade.
const (
	constraintWalletPlayerCurrency = "uq_wallets_player_currency"
	constraintIdempotencyKey       = "uq_wager_tx_provider_idempotency_key"
	constraintExternalID           = "uq_wager_tx_provider_external_id"
	constraintOneReversalPerTarget = "uq_wager_tx_one_reversal_per_target"
	constraintInboxIdentity        = "inbox_messages_pkey"
)

// classify traduz erros do driver. Violações de unicidade conhecidas viram
// erros específicos (recuperáveis pelo caso de uso); falhas temporárias de
// infraestrutura viram app.ErrTransient; o restante segue como erro interno.
func classify(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "23505" {
			switch pgErr.ConstraintName {
			case constraintWalletPlayerCurrency:
				return app.ErrWalletAlreadyExists
			case constraintIdempotencyKey:
				return app.ErrDuplicateIdempotencyKey
			case constraintExternalID:
				return app.ErrDuplicateExternalTransaction
			case constraintOneReversalPerTarget:
				return app.ErrReversalTargetTaken
			case constraintInboxIdentity:
				return app.ErrDuplicateInbox
			}
		}
		if isTransientCode(pgErr.Code) {
			return fmt.Errorf("%w: %w", app.ErrTransient, err)
		}
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		pgconn.Timeout(err) || pgconn.SafeToRetry(err) || isConnectError(err) {
		return fmt.Errorf("%w: %w", app.ErrTransient, err)
	}
	return err
}

func isConnectError(err error) bool {
	var ce *pgconn.ConnectError
	if errors.As(err, &ce) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne)
}

// isTransientCode reconhece SQLSTATEs que indicam indisponibilidade ou
// conflito temporário: conexão (08), recursos (53), desligamento do servidor
// (57P01-57P03), serialização (40001), deadlock (40P01) e lock (55P03).
func isTransientCode(code string) bool {
	if len(code) < 2 {
		return false
	}
	switch code[:2] {
	case "08", "53":
		return true
	}
	switch code {
	case "57P01", "57P02", "57P03", "40001", "40P01", "55P03", "25006":
		return true
	}
	return false
}
