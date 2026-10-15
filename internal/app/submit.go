package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

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
// Todos os tipos externos estão implementados: BET, WIN (com ou sem referência),
// LOSS, REFUND e ROLLBACK. Uma referência ainda indisponível deixa a operação em
// PENDING_REFERENCE, confirmada junto com a agenda de retomada (ver references.go).
// O corpo transacional é processExternal.
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
		if reason := conflictReason(err); reason != "other" {
			s.obs.Conflict(reason)
		}
		// A unicidade do banco revelou uma operação concorrente confirmada
		// (p.ex. mesma chave aplicada a outra carteira). Uma nova transação a
		// enxerga e a trata como replay ou conflito.
		if (errors.Is(err, ErrDuplicateIdempotencyKey) || errors.Is(err, ErrDuplicateExternalTransaction) || errors.Is(err, ErrReversalTargetTaken)) && attempt < maxConflictRetries {
			continue
		}
		return SubmitResult{}, err
	}
}

func (s *Service) submitOnce(ctx context.Context, req domain.ExternalRequest, correlationID string) (SubmitResult, error) {
	var result SubmitResult
	txID := s.newID() // fixado antes da transação: identifica a tentativa em logs de commit incerto
	err := s.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
		var err error
		result, err = s.processExternal(ctx, r, req, txID, correlationID)
		return err
	})
	if err != nil {
		if errors.Is(err, ErrCommitUnknown) {
			s.log.WarnContext(ctx, "transaction commit outcome unknown; retry with the same idempotency key",
				"transactionId", txID, "walletId", req.WalletID(), "providerId", req.ProviderID(), "correlationId", correlationID)
		}
		return SubmitResult{}, err
	}
	s.logResult(ctx, result, correlationID)
	s.obs.Result("http", string(result.Transaction.Kind()), string(result.Transaction.Status()), result.Replay)
	return result, nil
}

// processExternal é o núcleo financeiro compartilhado por todas as entradas
// (HTTP agora; SQS com inbox depois). Opera exclusivamente sobre os repositórios
// recebidos — todos vinculados à transação SQL da tentativa — e não abre nem
// confirma transação: quem chama controla o commit, de modo que outros efeitos
// (p.ex. a inbox) possam compartilhar a mesma transação. Em caso de erro nada
// foi confirmado e o chamador deve desfazer a tentativa.
//
// Ordem das decisões (a entrada já passou pela validação estrutural em
// domain.NewExternalRequest): identidade registrada → políticas de operação nova
// → lock da carteira e validação contra ela → rechecagem da identidade → regra do
// tipo (decide) → gravação de carteira, transação, agenda de pendência, ledger e
// outbox. Ordem única de locks de todo o sistema: carteira → agenda/transação.
func (s *Service) processExternal(ctx context.Context, r Repositories, req domain.ExternalRequest, txID uuid.UUID, correlationID string) (SubmitResult, error) {
	// 1. Identidade já registrada tem precedência sobre qualquer validação que
	// dependa do estado atual (p.ex. carteira inexistente): reutilizar a chave com
	// outro conteúdo é conflito, e um replay legítimo não depende da carteira. Esta
	// leitura não trava nada; só enxerga operações já confirmadas. O replay de uma
	// pendência devolve o estado atual dela (PENDING_REFERENCE): não cria outra
	// agenda, não renova o prazo e não emite de novo o evento inicial.
	if existing, err := s.resolveExisting(ctx, r, req); err != nil || existing != nil {
		return SubmitResult{Transaction: existing, Replay: existing != nil}, err
	}

	// 2. Operação nova: políticas da aplicação (moeda habilitada). Nada foi gravado
	// ainda; a tentativa é desfeita sem efeito.
	if err := s.checkNewOperation(req); err != nil {
		return SubmitResult{}, err
	}

	// 3. Trava a carteira e valida a entrada contra ela.
	wallet, err := r.Wallets.GetForUpdate(ctx, req.WalletID())
	if err != nil {
		return SubmitResult{}, err
	}
	if wallet.PlayerID() != req.PlayerID() || wallet.Currency() != req.Money().Currency() {
		return SubmitResult{}, ErrWalletMismatch
	}

	// 4. Com a carteira travada, repete a resolução: uma operação concorrente da
	// mesma carteira pode ter confirmado entre os passos 1 e 2. Operações de
	// carteiras diferentes com a mesma identidade são resolvidas pelas
	// unicidades do banco (ver Submit).
	if existing, err := s.resolveExisting(ctx, r, req); err != nil || existing != nil {
		return SubmitResult{Transaction: existing, Replay: existing != nil}, err
	}

	now := s.now()
	tx, err := domain.NewExternalTransaction(txID, req, now)
	if err != nil {
		return SubmitResult{}, err
	}
	versionBefore := wallet.Version()
	out, err := s.decide(ctx, r, wallet, tx, correlationID, now, false)
	if err != nil {
		return SubmitResult{}, err
	}
	if out.entry != nil {
		if err := r.Wallets.UpdateBalance(ctx, wallet, versionBefore); err != nil {
			return SubmitResult{}, err
		}
	}
	if err := r.Transactions.Insert(ctx, tx); err != nil {
		return SubmitResult{}, err
	}
	if out.wait {
		// A pendência só é confirmada junto com a sua agenda: o prazo começa aqui.
		if err := r.Pending.Insert(ctx, PendingReference{
			TransactionID: tx.ID(), WalletID: tx.WalletID(), FirstPersistedAt: now,
			ExpiresAt: now.Add(s.ref.TTL), NextAttemptAt: now.Add(s.backoff(0)),
		}); err != nil {
			return SubmitResult{}, err
		}
	}
	if out.entry != nil {
		if err := r.Ledger.Append(ctx, *out.entry); err != nil {
			return SubmitResult{}, err
		}
	}
	if err := r.Outbox.Append(ctx, out.events); err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{Transaction: tx}, nil
}

// checkNewOperation aplica as políticas de uma operação nova: moeda habilitada
// (ErrCurrencyNotSupported). Só é chamada depois de resolver a identidade existente.
func (s *Service) checkNewOperation(req domain.ExternalRequest) error {
	return s.checkCurrency(req.Money().Currency())
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

// outcome é o resultado de decidir uma operação: eventos da outbox, lançamento do
// ledger (nil sem movimentação) e se a operação deve (continuar a) esperar a referência.
type outcome struct {
	events []domain.Event
	entry  *domain.LedgerEntry
	wait   bool
}

// decide aplica em memória a regra do tipo da operação sobre a carteira já
// travada, tanto para uma operação nova (tx em PENDING) quanto para a retomada de
// uma pendência (tx em PENDING_REFERENCE): as regras financeiras são as mesmas. O
// chamador persiste o resultado. expired informa que o prazo da referência esgotou.
func (s *Service) decide(ctx context.Context, r Repositories, w *domain.Wallet, tx *domain.WagerTransaction, correlationID string, now time.Time, expired bool) (outcome, error) {
	switch tx.Kind() {
	case domain.KindBet:
		return s.applyMovement(w, tx, w.Debit, domain.FailureInsufficientFunds, correlationID, now)
	case domain.KindLoss:
		return s.applyLoss(w, tx, correlationID, now)
	case domain.KindWin:
		if tx.External().ReferenceExternalTransactionID == "" {
			return s.applyMovement(w, tx, w.Credit, "", correlationID, now)
		}
		return s.decideWithReference(ctx, r, w, tx, correlationID, now, expired)
	case domain.KindRefund, domain.KindRollback:
		return s.decideWithReference(ctx, r, w, tx, correlationID, now, expired)
	}
	return outcome{}, fmt.Errorf("%w: %s", ErrKindNotSupported, tx.Kind())
}

type moveFunc func(entryID, transactionID uuid.UUID, amount domain.Money, now time.Time) (domain.LedgerEntry, error)

// applyMovement aplica um débito ou crédito: conclui a transação com o saldo
// resultante e gera WagerTransactionProcessed e WalletBalanceChanged. Saldo
// insuficiente é rejeição de negócio auditável (insufficient), sem movimentação;
// qualquer outro erro (p.ex. overflow do saldo) interrompe a tentativa sem
// efeitos, pois a carteira só muda depois de a movimentação ser validada por inteiro.
func (s *Service) applyMovement(w *domain.Wallet, tx *domain.WagerTransaction, move moveFunc, insufficient domain.FailureCode, correlationID string, now time.Time) (outcome, error) {
	entry, err := move(s.newID(), tx.ID(), tx.Money(), now)
	switch {
	case errors.Is(err, domain.ErrInsufficientFunds):
		return s.reject(w, tx, insufficient, correlationID, now)
	case err != nil:
		return outcome{}, err
	}
	if err := tx.Process(w.Balance(), now); err != nil {
		return outcome{}, err
	}
	processed, err := domain.NewWagerTransactionProcessed(s.newID(), correlationID, nil, tx, now)
	if err != nil {
		return outcome{}, err
	}
	causation := processed.ID()
	changed, err := domain.NewWalletBalanceChanged(s.newID(), correlationID, &causation, entry, w.Version(), now)
	if err != nil {
		return outcome{}, err
	}
	return outcome{events: []domain.Event{processed, changed}, entry: &entry}, nil
}

// reject conclui a transação como rejeição de negócio, sem movimentação, com o
// saldo observado sob o lock, e gera WagerTransactionRejected.
func (s *Service) reject(w *domain.Wallet, tx *domain.WagerTransaction, code domain.FailureCode, correlationID string, now time.Time) (outcome, error) {
	if err := tx.Reject(code, w.Balance(), now); err != nil {
		return outcome{}, err
	}
	ev, err := domain.NewWagerTransactionRejected(s.newID(), correlationID, nil, tx, now)
	if err != nil {
		return outcome{}, err
	}
	return outcome{events: []domain.Event{ev}}, nil
}

// applyLoss registra o resultado de uma LOSS: não há movimentação, ledger,
// alteração de saldo ou de versão, nem WalletBalanceChanged. O saldo observado
// sob o lock da carteira é o resultado devolvido (e reproduzido em replays).
func (s *Service) applyLoss(w *domain.Wallet, tx *domain.WagerTransaction, correlationID string, now time.Time) (outcome, error) {
	if err := tx.Process(w.Balance(), now); err != nil {
		return outcome{}, err
	}
	processed, err := domain.NewWagerTransactionProcessed(s.newID(), correlationID, nil, tx, now)
	if err != nil {
		return outcome{}, err
	}
	return outcome{events: []domain.Event{processed}}, nil
}

// decideWithReference resolve a referência da operação (WIN com referência, REFUND
// ou ROLLBACK) no escopo do provedor da própria operação — nunca em outro — e
// aplica a política aprovada:
//
//	ausente            antes do prazo: espera · depois: REJECTED/REFERENCE_NOT_FOUND
//	PENDING*           antes do prazo: espera · depois: REJECTED/REFERENCE_PENDING_TIMEOUT
//	REJECTED/FAILED    REJECTED/REFERENCE_NOT_PROCESSED
//	incompatível       REJECTED/REFERENCE_MISMATCH (escopo, tipo ou valor)
//	PROCESSED          regras financeiras, mesmo depois do prazo
//
// Uma falha de consulta devolve erro (a tentativa é abortada): nunca vale como
// referência ausente. A carteira já está travada, então o estado do alvo (da mesma
// carteira) não muda durante a decisão e a unicidade da reversão é decidida sem corrida.
func (s *Service) decideWithReference(ctx context.Context, r Repositories, w *domain.Wallet, tx *domain.WagerTransaction, correlationID string, now time.Time, expired bool) (outcome, error) {
	ext := tx.External()
	target, err := r.Transactions.FindByExternalID(ctx, ext.ProviderID, ext.ReferenceExternalTransactionID)
	switch {
	case errors.Is(err, ErrTransactionNotFound):
		if expired {
			return s.reject(w, tx, domain.FailureReferenceNotFound, correlationID, now)
		}
		return s.wait(tx, correlationID, now)
	case err != nil:
		return outcome{}, err
	}
	assessment, err := tx.AssessReference(target)
	if err != nil {
		return outcome{}, err
	}
	switch assessment.Outcome {
	case domain.ReferenceRejected:
		return s.reject(w, tx, assessment.Code, correlationID, now)
	case domain.ReferenceUnavailable:
		if expired {
			return s.reject(w, tx, domain.FailureReferencePendingTimeout, correlationID, now)
		}
		return s.wait(tx, correlationID, now)
	}
	if err := tx.ResolveReference(target, now); err != nil {
		return outcome{}, err
	}
	if tx.Kind() == domain.KindWin {
		return s.applyMovement(w, tx, w.Credit, "", correlationID, now)
	}
	// Reversão: uma única reversão bem-sucedida por alvo, em todo o histórico.
	reversed, err := r.Transactions.HasProcessedReversal(ctx, target.ID())
	if err != nil {
		return outcome{}, err
	}
	if reversed {
		return s.reject(w, tx, domain.FailureAlreadyReversed, correlationID, now)
	}
	dir, err := domain.ReversalDirection(tx.Kind(), target.Kind())
	if err != nil {
		return outcome{}, err
	}
	move := moveFunc(w.Credit)
	if dir == domain.Debit {
		move = w.Debit
	}
	return s.applyMovement(w, tx, move, domain.FailureReversalInsufficientFunds, correlationID, now)
}

// wait mantém (ou inicia) a espera pela referência. Na primeira vez a transação vai
// a PENDING_REFERENCE e o evento WagerTransactionPendingReference é emitido; numa
// retomada que continua esperando nada muda e nenhum evento é repetido.
func (s *Service) wait(tx *domain.WagerTransaction, correlationID string, now time.Time) (outcome, error) {
	if tx.Status() == domain.StatusPendingReference {
		return outcome{wait: true}, nil
	}
	if err := tx.AwaitReference(now); err != nil {
		return outcome{}, err
	}
	ev, err := domain.NewWagerTransactionPendingReference(s.newID(), correlationID, nil, tx, now)
	if err != nil {
		return outcome{}, err
	}
	return outcome{events: []domain.Event{ev}, wait: true}, nil
}

func (s *Service) logResult(ctx context.Context, res SubmitResult, correlationID string) {
	t := res.Transaction
	s.log.InfoContext(ctx, "wager transaction handled",
		"transactionId", t.ID(), "walletId", t.WalletID(), "providerId", t.External().ProviderID,
		"kind", t.Kind(), "status", t.Status(), "failureCode", t.FailureCode(),
		"replay", res.Replay, "correlationId", correlationID)
}
