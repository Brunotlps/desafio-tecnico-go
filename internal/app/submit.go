package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"desafio-tecnico-go/internal/domain"
)

// SubmitResult é o resultado de uma operação externa.
type SubmitResult struct {
	Transaction *domain.WagerTransaction
	// Replay indica que o resultado persistido de uma operação anterior foi
	// devolvido, sem reaplicar a operação.
	Replay bool
}

// Submit processa uma operação externa de forma síncrona e atômica: em uma
// única transação trava a carteira, resolve idempotência, aplica a regra,
// e confirma transação, saldo, lançamento e eventos da outbox. O PENDING não
// é confirmado isoladamente; sem dependências, a operação nasce já concluída.
//
// Ordem das decisões: (1) a entrada é analisada e normalizada, e entradas
// estruturalmente inválidas são recusadas; (2) a identidade já registrada
// (chave de idempotência ou id externo, no escopo do provedor) é resolvida como
// replay ou conflito; (3) só então valem as políticas de uma operação NOVA —
// moeda habilitada e tipo processado — e as validações que dependem da carteira.
// Assim uma chave registrada reenviada com outra moeda ou outro tipo é conflito
// (409), não 422/501.
//
// Somente BET está implementado. Os demais tipos válidos, quando a operação é
// nova, devolvem ErrKindNotSupported sem persistir nada.
func (s *Service) Submit(ctx context.Context, raw domain.RawExternalRequest, correlationID string) (SubmitResult, error) {
	req, err := domain.NewExternalRequest(raw)
	if err != nil {
		return SubmitResult{}, err
	}
	for attempt := 0; ; attempt++ {
		res, err := s.submitOnce(ctx, req, correlationID)
		if err == nil {
			return res, nil
		}
		// A unicidade do banco revelou uma operação concorrente confirmada
		// (p.ex. mesma chave aplicada a outra carteira). Uma nova transação a
		// enxerga e a trata como replay ou conflito.
		if (errors.Is(err, ErrDuplicateIdempotencyKey) || errors.Is(err, ErrDuplicateExternalTransaction)) && attempt < maxConflictRetries {
			continue
		}
		return SubmitResult{}, err
	}
}

func (s *Service) submitOnce(ctx context.Context, req domain.ExternalRequest, correlationID string) (SubmitResult, error) {
	var result SubmitResult
	txID := s.newID() // fixado antes da transação: identifica a tentativa em logs de commit incerto
	err := s.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
		result = SubmitResult{}

		// 1. Identidade já registrada tem precedência sobre qualquer validação que
		// dependa do estado atual (p.ex. carteira inexistente): reutilizar a chave com
		// outro conteúdo é conflito, e um replay legítimo não depende da carteira. Esta
		// leitura não trava nada; só enxerga operações já confirmadas.
		if existing, err := s.resolveExisting(ctx, r, req); err != nil || existing != nil {
			result = SubmitResult{Transaction: existing, Replay: existing != nil}
			return err
		}

		// 2. Operação nova: políticas da aplicação (moeda habilitada, tipo processado).
		// Nada foi gravado ainda; a transação é desfeita sem efeito.
		if err := s.checkNewOperation(req); err != nil {
			return err
		}

		// 3. Trava a carteira e valida a entrada contra ela.
		wallet, err := r.Wallets.GetForUpdate(ctx, req.WalletID())
		if err != nil {
			return err
		}
		if wallet.PlayerID() != req.PlayerID() || wallet.Currency() != req.Money().Currency() {
			return ErrWalletMismatch
		}

		// 4. Com a carteira travada, repete a resolução: uma operação concorrente da
		// mesma carteira pode ter confirmado entre os passos 1 e 2. Operações de
		// carteiras diferentes com a mesma identidade são resolvidas pelas
		// unicidades do banco (ver Submit).
		if existing, err := s.resolveExisting(ctx, r, req); err != nil || existing != nil {
			result = SubmitResult{Transaction: existing, Replay: existing != nil}
			return err
		}

		now := s.now()
		tx, err := domain.NewExternalTransaction(txID, req, now)
		if err != nil {
			return err
		}
		versionBefore := wallet.Version()
		events, entry, err := s.applyBet(wallet, tx, correlationID, now)
		if err != nil {
			return err
		}
		if entry != nil {
			if err := r.Wallets.UpdateBalance(ctx, wallet, versionBefore); err != nil {
				return err
			}
		}
		if err := r.Transactions.Insert(ctx, tx); err != nil {
			return err
		}
		if entry != nil {
			if err := r.Ledger.Append(ctx, *entry); err != nil {
				return err
			}
		}
		if err := r.Outbox.Append(ctx, events); err != nil {
			return err
		}
		result = SubmitResult{Transaction: tx}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrCommitUnknown) {
			s.log.WarnContext(ctx, "transaction commit outcome unknown; retry with the same idempotency key",
				"transactionId", txID, "walletId", req.WalletID(), "providerId", req.ProviderID(), "correlationId", correlationID)
		}
		return SubmitResult{}, err
	}
	s.logResult(ctx, result, correlationID)
	return result, nil
}

// checkNewOperation aplica as políticas de uma operação nova, nesta ordem: moeda
// habilitada (ErrCurrencyNotSupported) e tipo com processamento implementado
// (ErrKindNotSupported). Só é chamada depois de resolver a identidade existente.
func (s *Service) checkNewOperation(req domain.ExternalRequest) error {
	if err := s.checkCurrency(req.Money().Currency()); err != nil {
		return err
	}
	if req.Kind() != domain.KindBet {
		return fmt.Errorf("%w: %s", ErrKindNotSupported, req.Kind())
	}
	return nil
}

// resolveExisting consulta as identidades da operação, sempre no escopo do
// provedor, numa única consulta: duas consultas separadas, sem o lock da carteira,
// poderiam enxergar estados diferentes de um commit concorrente (chave ausente e,
// logo depois, id externo presente) e reportar conflito para um reenvio legítimo.
// Devolve a transação a reproduzir (mesma chave e mesmo conteúdo);
// ErrIdempotencyKeyReused (mesma chave, conteúdo diferente);
// ErrExternalTransactionReused (mesmo id externo sob outra chave); ou (nil, nil)
// quando a operação é nova. A chave tem precedência sobre o id externo.
func (s *Service) resolveExisting(ctx context.Context, r Repositories, req domain.ExternalRequest) (*domain.WagerTransaction, error) {
	found, err := r.Transactions.FindByIdentity(ctx, req.ProviderID(), req.IdempotencyKey(), req.ExternalTransactionID())
	if err != nil {
		return nil, err
	}
	for _, t := range found {
		if t.External().IdempotencyKey == req.IdempotencyKey() {
			if t.External().PayloadHash != req.PayloadHash() {
				return nil, ErrIdempotencyKeyReused
			}
			return t, nil
		}
	}
	if len(found) > 0 {
		return nil, ErrExternalTransactionReused
	}
	return nil, nil
}

// applyBet aplica a regra de BET em memória: débito se houver saldo; caso
// contrário, rejeição auditável sem movimentação.
func (s *Service) applyBet(w *domain.Wallet, tx *domain.WagerTransaction, correlationID string, now time.Time) ([]domain.Event, *domain.LedgerEntry, error) {
	entry, err := w.Debit(s.newID(), tx.ID(), tx.Money(), now)
	switch {
	case errors.Is(err, domain.ErrInsufficientFunds):
		if err := tx.Reject(domain.FailureInsufficientFunds, w.Balance(), now); err != nil {
			return nil, nil, err
		}
		ev, err := domain.NewWagerTransactionRejected(s.newID(), correlationID, nil, tx, now)
		if err != nil {
			return nil, nil, err
		}
		return []domain.Event{ev}, nil, nil
	case err != nil:
		return nil, nil, err
	}
	if err := tx.Process(w.Balance(), now); err != nil {
		return nil, nil, err
	}
	processed, err := domain.NewWagerTransactionProcessed(s.newID(), correlationID, nil, tx, now)
	if err != nil {
		return nil, nil, err
	}
	causation := processed.ID()
	changed, err := domain.NewWalletBalanceChanged(s.newID(), correlationID, &causation, entry, w.Version(), now)
	if err != nil {
		return nil, nil, err
	}
	return []domain.Event{processed, changed}, &entry, nil
}

func (s *Service) logResult(ctx context.Context, res SubmitResult, correlationID string) {
	t := res.Transaction
	s.log.InfoContext(ctx, "wager transaction handled",
		"transactionId", t.ID(), "walletId", t.WalletID(), "providerId", t.External().ProviderID,
		"kind", t.Kind(), "status", t.Status(), "failureCode", t.FailureCode(),
		"replay", res.Replay, "correlationId", correlationID)
}
