package httpapi

import (
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/domain/events"
	"github.com/gamsanches06/jungle-test/internal/domain/money"
	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
	"github.com/gamsanches06/jungle-test/internal/domain/wallet"
)

// MoneyDTO is the external money representation. amount is a JSON string.
type MoneyDTO struct {
	Amount   *string `json:"amount"`
	Currency *string `json:"currency"`
}

type openWalletRequest struct {
	PlayerID       string    `json:"playerId"`
	InitialBalance *MoneyDTO `json:"initialBalance"`
}

type walletResponse struct {
	ID        string      `json:"id"`
	PlayerID  string      `json:"playerId"`
	Balance   money.Money `json:"balance"`
	Version   int64       `json:"version"`
	CreatedAt string      `json:"createdAt"`
	UpdatedAt string      `json:"updatedAt"`
}

func toWalletResponse(w *wallet.Wallet) walletResponse {
	return walletResponse{
		ID: w.ID().String(), PlayerID: w.PlayerID().String(), Balance: w.Balance(), Version: w.Version(),
		CreatedAt: ts(w.CreatedAt()), UpdatedAt: ts(w.UpdatedAt()),
	}
}

type ledgerEntryResponse struct {
	ID            string      `json:"id"`
	WalletID      string      `json:"walletId"`
	TransactionID string      `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
	CreatedAt     string      `json:"createdAt"`
}

type ledgerResponse struct {
	WalletID   string                `json:"walletId"`
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor *string               `json:"nextCursor"`
}

// transactionRequest is the body of POST /wagering/transactions.
type transactionRequest struct {
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	PlayerID                       string    `json:"playerId"`
	WalletID                       string    `json:"walletId"`
	RoundID                        string    `json:"roundId"`
	GameID                         string    `json:"gameId"`
	Kind                           string    `json:"kind"`
	Money                          *MoneyDTO `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
}

func (r transactionRequest) raw() wagering.RawRequest {
	raw := wagering.RawRequest{
		ProviderID: r.ProviderID, ExternalTransactionID: r.ExternalTransactionID,
		PlayerID: r.PlayerID, WalletID: r.WalletID, RoundID: r.RoundID, GameID: r.GameID, Kind: r.Kind,
		ReferenceExternalTransactionID: r.ReferenceExternalTransactionID,
	}
	if r.Money != nil && r.Money.Amount != nil && r.Money.Currency != nil {
		raw.HasMoney = true
		raw.Amount = *r.Money.Amount
		raw.Currency = *r.Money.Currency
	}
	return raw
}

// operationResponse is returned by POST /wagering/transactions.
type operationResponse struct {
	TransactionID    string       `json:"transactionId"`
	Status           string       `json:"status"`
	Balance          *money.Money `json:"balance,omitempty"`
	FailureCode      string       `json:"failureCode,omitempty"`
	IdempotentReplay bool         `json:"idempotentReplay"`
}

func toOperationResponse(t *wagering.Transaction, replay bool) operationResponse {
	r := operationResponse{
		TransactionID: t.ID().String(), Status: string(t.Status()),
		FailureCode: string(t.FailureCode()), IdempotentReplay: replay,
	}
	if res, ok := t.Result(); ok {
		b := res.Balance
		r.Balance = &b
	}
	return r
}

// transactionView is returned by the GET transaction endpoints.
type transactionView struct {
	TransactionID                  string       `json:"transactionId"`
	Origin                         string       `json:"origin"`
	Kind                           string       `json:"kind"`
	Status                         string       `json:"status"`
	ProviderID                     string       `json:"providerId,omitempty"`
	ExternalTransactionID          string       `json:"externalTransactionId,omitempty"`
	IdempotencyKey                 string       `json:"idempotencyKey,omitempty"`
	PayloadHash                    string       `json:"payloadHash,omitempty"`
	PlayerID                       string       `json:"playerId"`
	WalletID                       string       `json:"walletId"`
	RoundID                        string       `json:"roundId,omitempty"`
	GameID                         string       `json:"gameId,omitempty"`
	Money                          money.Money  `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         string       `json:"referenceTransactionId,omitempty"`
	FailureCode                    string       `json:"failureCode,omitempty"`
	FailureDetail                  string       `json:"failureDetail,omitempty"`
	Balance                        *money.Money `json:"balance,omitempty"`
	WalletVersion                  *int64       `json:"walletVersion,omitempty"`
	Attempts                       int          `json:"attempts"`
	NextAttemptAt                  *string      `json:"nextAttemptAt,omitempty"`
	ExpiresAt                      *string      `json:"expiresAt,omitempty"`
	CreatedAt                      string       `json:"createdAt"`
	UpdatedAt                      string       `json:"updatedAt"`
	CompletedAt                    *string      `json:"completedAt,omitempty"`
}

func toTransactionView(t *wagering.Transaction) transactionView {
	v := transactionView{
		TransactionID: t.ID().String(), Origin: string(t.Origin()), Kind: string(t.Kind()), Status: string(t.Status()),
		PlayerID: t.PlayerID().String(), WalletID: t.WalletID().String(), Money: t.Money(),
		FailureCode: string(t.FailureCode()), FailureDetail: t.FailureDetail(), Attempts: t.Attempts(),
		CreatedAt: ts(t.CreatedAt()), UpdatedAt: ts(t.UpdatedAt()),
		NextAttemptAt: optTS(t.NextAttemptAt()), ExpiresAt: optTS(t.ExpiresAt()), CompletedAt: optTS(t.CompletedAt()),
	}
	if ext, ok := t.External(); ok {
		v.ProviderID, v.ExternalTransactionID, v.IdempotencyKey = ext.ProviderID, ext.ExternalTransactionID, ext.IdempotencyKey
		v.PayloadHash, v.RoundID, v.GameID = ext.PayloadHash, ext.RoundID, ext.GameID
		v.ReferenceExternalTransactionID = ext.ReferenceExternalTransactionID
	}
	if id := t.ReferenceTxID(); id != uuid.Nil {
		v.ReferenceTransactionID = id.String()
	}
	if res, ok := t.Result(); ok {
		b, ver := res.Balance, res.WalletVersion
		v.Balance, v.WalletVersion = &b, &ver
	}
	return v
}

type reconciliationResponse struct {
	WalletID          string      `json:"walletId"`
	StoredBalance     money.Money `json:"storedBalance"`
	CalculatedBalance money.Money `json:"calculatedBalance"`
	Difference        money.Money `json:"difference"`
	Consistent        bool        `json:"consistent"`
	CheckedEntries    int64       `json:"checkedEntries"`
}

type errorDetail struct {
	Field  string `json:"field,omitempty"`
	Reason string `json:"reason"`
}

type errorBody struct {
	Code    string        `json:"code"`
	Message string        `json:"message"`
	Details []errorDetail `json:"details,omitempty"`
}

type errorResponse struct {
	Error         errorBody `json:"error"`
	CorrelationID string    `json:"correlationId,omitempty"`
}

func ts(t time.Time) string { return events.FormatTime(t) }

func optTS(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := ts(t)
	return &s
}
