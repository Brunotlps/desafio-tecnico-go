package domain

import "errors"

// Erros de domínio classificáveis com errors.Is. Nenhum representa falha de
// infraestrutura; todos descrevem entradas inválidas ou regras de negócio.
var (
	// Money.
	ErrInvalidAmount      = errors.New("invalid amount")
	ErrAmountScale        = errors.New("amount has more than two decimal places")
	ErrAmountNegative     = errors.New("amount must not be negative")
	ErrAmountOverflow     = errors.New("amount out of range")
	ErrInvalidCurrency    = errors.New("invalid currency")
	ErrCurrencyMismatch   = errors.New("currency mismatch")
	ErrUninitializedMoney = errors.New("money is not initialized")

	// Wallet.
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrInvalidWallet     = errors.New("invalid wallet")

	// Ledger.
	ErrInvalidLedgerEntry = errors.New("invalid ledger entry")

	// WagerTransaction.
	ErrInvalidKind         = errors.New("invalid transaction kind")
	ErrKindNotAllowed      = errors.New("transaction kind not allowed on this entry point")
	ErrInvalidRequest      = errors.New("invalid request")
	ErrAmountNotAllowed    = errors.New("amount not allowed for transaction kind")
	ErrInvalidTransition   = errors.New("invalid state transition")
	ErrTerminalTransaction = errors.New("transaction is in a terminal state")
	ErrInvalidTransaction  = errors.New("invalid transaction")
	// ErrInvalidTimestamp: instante zero ou que faria o histórico da transação
	// regredir. É sempre devolvido junto com ErrInvalidTransaction.
	ErrInvalidTimestamp = errors.New("invalid timestamp")

	// Eventos.
	ErrInvalidEvent = errors.New("invalid event")
)
