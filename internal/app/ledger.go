package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"time"

	"github.com/google/uuid"

	"desafio-tecnico-go/internal/domain"
)

// Ledger paginado e reconciliação (leituras). Não participam da UnitOfWork financeira: usam a porta LedgerReader.

const (
	DefaultLedgerLimit = 50
	MaxLedgerLimit     = 100

	cursorVersion   = 1
	maxCursorLength = 200
)

// WithLedgerReader liga as leituras do ledger e da reconciliação.
func WithLedgerReader(r LedgerReader) Option { return func(s *Service) { s.ledger = r } }

// WithReconciliationTimeout limita a duração da transação de reconciliação (padrão 10 s).
func WithReconciliationTimeout(d time.Duration) Option {
	return func(s *Service) {
		if d > 0 {
			s.reconTimeout = d
		}
	}
}

// WithReconciliationObserver registra quem é avisado de cada divergência detectada (ponto de integração da métrica).
func WithReconciliationObserver(fn func(ReconciliationResult)) Option {
	return func(s *Service) { s.onDivergence = fn }
}

// ledgerCursor é o conteúdo do cursor opaco: versão, carteira e a última posição (seq) devolvida. O cliente o trata como
// opaco; ele não autoriza nada (a autorização e o escopo continuam no servidor): só delimita a continuação.
type ledgerCursor struct {
	V int    `json:"v"`
	W string `json:"w"`
	S int64  `json:"s"`
}

func encodeCursor(walletID uuid.UUID, seq int64) string {
	b, _ := json.Marshal(ledgerCursor{V: cursorVersion, W: walletID.String(), S: seq})
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeCursor valida formato, tamanho, versão, posição e a correspondência com a carteira da rota. A forma é canônica: um
// cursor que não seja exatamente a codificação que o servidor produziria é rejeitado.
func decodeCursor(raw string, walletID uuid.UUID) (int64, error) {
	bad := func(why string) (int64, error) { return 0, fmt.Errorf("%w: %s", ErrInvalidCursor, why) }
	if len(raw) > maxCursorLength {
		return bad("too long")
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(raw)
	if err != nil {
		return bad("not base64url")
	}
	var c ledgerCursor
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return bad("malformed")
	}
	if _, err := dec.Token(); err != io.EOF {
		return bad("trailing data")
	}
	if c.V != cursorVersion {
		return bad("unsupported version")
	}
	if c.S < 1 {
		return bad("invalid position")
	}
	if c.W != walletID.String() {
		return bad("cursor belongs to another wallet")
	}
	if encodeCursor(walletID, c.S) != raw {
		return bad("not canonical")
	}
	return c.S, nil
}

// LedgerPage é uma página do ledger: lançamentos em ordem crescente de seq e, se houver mais, o cursor da próxima.
type LedgerPage struct {
	Items      []domain.LedgerEntry
	NextCursor string // vazio na última página
}

// ListLedger devolve uma página do ledger da carteira. limit fora de 1..MaxLedgerLimit e cursor inválido (formato,
// versão, posição, outra carteira) são entrada inválida. Carteira inexistente: ErrWalletNotFound; existente sem
// lançamentos: página vazia. Cada página tem a sua visão: lançamentos confirmados depois podem aparecer nas seguintes.
func (s *Service) ListLedger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	if limit < 1 || limit > MaxLedgerLimit {
		return LedgerPage{}, fmt.Errorf("%w: must be between 1 and %d", ErrInvalidLimit, MaxLedgerLimit)
	}
	var after int64
	if cursor != "" {
		var err error
		if after, err = decodeCursor(cursor, walletID); err != nil {
			return LedgerPage{}, err
		}
	}
	if s.ledger == nil {
		return LedgerPage{}, ErrLedgerNotConfigured
	}
	if _, err := s.uow.Reader().Wallets.GetByID(ctx, walletID); err != nil {
		return LedgerPage{}, err
	}
	rows, err := s.ledger.LedgerPage(ctx, walletID, after, limit+1) // o item extra só indica que há próxima página
	if err != nil {
		return LedgerPage{}, err
	}
	page := LedgerPage{Items: make([]domain.LedgerEntry, 0, min(len(rows), limit))}
	for i, r := range rows {
		if i == limit {
			page.NextCursor = encodeCursor(walletID, rows[limit-1].Seq)
			break
		}
		page.Items = append(page.Items, r.Entry)
	}
	return page, nil
}

// ReconciliationResult é o resultado de uma reconciliação, contra uma única visão dos dados.
type ReconciliationResult struct {
	WalletID       uuid.UUID
	Stored         domain.Money
	Calculated     domain.Money
	Difference     domain.Money // armazenado − calculado; pode ser negativo
	Consistent     bool
	CheckedEntries int64
}

// Reconcile compara o saldo armazenado com créditos − débitos do ledger (inclusive a abertura) numa visão consistente
// (READ ONLY, REPEATABLE READ), sem lock financeiro e sem reparo. A soma é exata (feita pelo banco, sem limite de int64) e a
// conversão para Money é verificada: se não couber, ou a moeda for inconsistente, a falha é ErrReconciliationFailed — nunca
// consistent=true nem uma divergência inventada. Falha de leitura é erro de infraestrutura.
func (s *Service) Reconcile(ctx context.Context, walletID uuid.UUID) (ReconciliationResult, error) {
	if s.ledger == nil {
		return ReconciliationResult{}, ErrLedgerNotConfigured
	}
	ctx, cancel := context.WithTimeout(ctx, s.reconTimeout)
	defer cancel()
	snap, err := s.ledger.ReconciliationSnapshot(ctx, walletID)
	if err != nil {
		if !errors.Is(err, ErrWalletNotFound) {
			s.obs.Reconciliation("error")
		}
		return ReconciliationResult{}, err
	}
	fail := func(format string, args ...any) (ReconciliationResult, error) {
		err := fmt.Errorf("%w: "+format, append([]any{ErrReconciliationFailed}, args...)...)
		s.log.ErrorContext(ctx, "reconciliation failed", "walletId", walletID, "error", err.Error())
		s.obs.Reconciliation("error")
		return ReconciliationResult{}, err
	}
	if snap.ForeignCurrencyEntries != 0 || snap.NetMinor == nil || snap.Wallet == nil {
		return fail("%d ledger entrie(s) in another currency than the wallet", snap.ForeignCurrencyEntries)
	}
	cur := snap.Wallet.Currency()
	calculated, err := moneyFromBig(snap.NetMinor, cur)
	if err != nil {
		return fail("calculated balance %s is outside the supported range", snap.NetMinor)
	}
	stored := snap.Wallet.Balance()
	diff := new(big.Int).Sub(big.NewInt(stored.Minor()), snap.NetMinor)
	difference, err := moneyFromBig(diff, cur)
	if err != nil {
		return fail("difference %s is outside the supported range", diff)
	}
	res := ReconciliationResult{
		WalletID: walletID, Stored: stored, Calculated: calculated, Difference: difference,
		Consistent: diff.Sign() == 0, CheckedEntries: snap.Entries,
	}
	if !res.Consistent {
		s.log.ErrorContext(ctx, "reconciliation divergence detected",
			"walletId", walletID, "storedBalance", stored.String(), "calculatedBalance", calculated.String(),
			"difference", difference.String(), "checkedEntries", snap.Entries, "walletVersion", snap.Wallet.Version())
		if s.onDivergence != nil {
			s.onDivergence(res)
		}
	}
	return res, nil
}

// moneyFromBig converte com verificação de faixa (±MaxInt64; MinInt64 é recusado por Money).
func moneyFromBig(v *big.Int, cur domain.Currency) (domain.Money, error) {
	if !v.IsInt64() || v.Int64() == math.MinInt64 {
		return domain.Money{}, domain.ErrAmountOverflow
	}
	return domain.FromMinor(v.Int64(), cur)
}
