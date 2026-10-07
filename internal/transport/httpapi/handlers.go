// Package httpapi exposes the HTTP contract of the service.
package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/application"
	"github.com/gamsanches06/jungle-test/internal/auth"
	"github.com/gamsanches06/jungle-test/internal/domain/money"
	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
	"github.com/gamsanches06/jungle-test/internal/observability"
)

const maxBodyBytes = 64 << 10

// Handlers implements the endpoints.
type Handlers struct {
	process *application.ProcessService
	wallets *application.WalletService
	queries *application.QueryService
	metrics *observability.Metrics
	log     *slog.Logger
}

// NewHandlers builds the handlers.
func NewHandlers(p *application.ProcessService, w *application.WalletService, q *application.QueryService, m *observability.Metrics, log *slog.Logger) *Handlers {
	return &Handlers{process: p, wallets: w, queries: q, metrics: m, log: log}
}

// ------------------------------------------------------------------ wallets

func (h *Handlers) openWallet(w http.ResponseWriter, r *http.Request) {
	var req openWalletRequest
	if err := decodeJSON(r, &req); err != nil {
		h.writeError(w, r, err)
		return
	}
	playerID, err := wagering.ParseID("playerId", req.PlayerID)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	if req.InitialBalance == nil || req.InitialBalance.Amount == nil || req.InitialBalance.Currency == nil {
		h.writeError(w, r, &wagering.ValidationError{Field: "initialBalance", Reason: "amount and currency are required"})
		return
	}
	initial, err := money.ParseNonNegative(*req.InitialBalance.Amount, *req.InitialBalance.Currency)
	if err != nil {
		h.writeError(w, r, &wagering.ValidationError{Field: "initialBalance", Reason: err.Error()})
		return
	}
	wal, err := h.wallets.OpenWallet(r.Context(), playerID, initial, observability.CorrelationID(r.Context()))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toWalletResponse(wal))
}

func (h *Handlers) getWallet(w http.ResponseWriter, r *http.Request) {
	id, err := wagering.ParseID("walletId", r.PathValue("walletId"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	wal, err := h.wallets.GetWallet(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWalletResponse(wal))
}

const (
	defaultLedgerLimit = 50
	maxLedgerLimit     = 200
	cursorPrefix       = "v1:"
)

// encodeCursor makes an opaque cursor from the last returned wallet version.
func encodeCursor(version int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(cursorPrefix + strconv.FormatInt(version, 10)))
}

func decodeCursor(c string) (int64, error) {
	if c == "" {
		return 0, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil || !strings.HasPrefix(string(b), cursorPrefix) {
		return 0, &wagering.ValidationError{Field: "cursor", Reason: "invalid cursor"}
	}
	v, err := strconv.ParseInt(strings.TrimPrefix(string(b), cursorPrefix), 10, 64)
	if err != nil || v < 0 {
		return 0, &wagering.ValidationError{Field: "cursor", Reason: "invalid cursor"}
	}
	return v, nil
}

func (h *Handlers) getLedger(w http.ResponseWriter, r *http.Request) {
	id, err := wagering.ParseID("walletId", r.PathValue("walletId"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	after, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	limit := defaultLedgerLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > maxLedgerLimit {
			h.writeError(w, r, &wagering.ValidationError{Field: "limit", Reason: fmt.Sprintf("must be between 1 and %d", maxLedgerLimit)})
			return
		}
		limit = n
	}
	page, err := h.wallets.ListLedger(r.Context(), id, after, limit)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	resp := ledgerResponse{WalletID: id.String(), Entries: make([]ledgerEntryResponse, 0, len(page.Entries))}
	for _, e := range page.Entries {
		resp.Entries = append(resp.Entries, ledgerEntryResponse{
			ID: e.ID().String(), WalletID: e.WalletID().String(), TransactionID: e.TransactionID().String(),
			Direction: string(e.Direction()), Money: e.Amount(), BalanceBefore: e.BalanceBefore(),
			BalanceAfter: e.BalanceAfter(), WalletVersion: e.WalletVersion(), CreatedAt: ts(e.CreatedAt()),
		})
	}
	if page.NextVersion > 0 {
		c := encodeCursor(page.NextVersion)
		resp.NextCursor = &c
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handlers) reconcile(w http.ResponseWriter, r *http.Request) {
	id, err := wagering.ParseID("walletId", r.PathValue("walletId"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	rec, err := h.wallets.Reconcile(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	h.metrics.ReconciliationRun()
	writeJSON(w, http.StatusOK, reconciliationResponse{
		WalletID: rec.WalletID.String(), StoredBalance: rec.StoredBalance, CalculatedBalance: rec.CalculatedBalance,
		Difference: rec.Difference, Consistent: rec.Consistent, CheckedEntries: rec.CheckedEntries,
	})
}

// ----------------------------------------------------------------- wagering

func (h *Handlers) submitTransaction(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	key := r.Header.Get("Idempotency-Key")
	if err := wagering.ValidateIdempotencyKey(key); err != nil {
		h.writeError(w, r, err)
		return
	}
	var body transactionRequest
	if err := decodeJSON(r, &body); err != nil {
		h.writeError(w, r, err)
		return
	}
	// The provider identity comes from the token; the body must agree with
	// it before anything is read or written.
	if body.ProviderID != p.ProviderID {
		h.metrics.AuthFailures.WithLabelValues("provider_mismatch").Inc()
		h.writeError(w, r, fmt.Errorf("%w: token is bound to another provider", auth.ErrForbidden))
		return
	}
	req, err := wagering.NewRequest(body.raw())
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	ctx := observability.WithFields(r.Context(),
		slog.String(observability.FieldProviderID, req.ProviderID()),
		slog.String(observability.FieldWalletID, req.WalletID().String()))
	res, err := h.process.Process(ctx, application.ProcessCommand{
		Request: req, IdempotencyKey: key, CorrelationID: observability.CorrelationID(ctx), Source: application.SourceHTTP,
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, statusFor(res.Transaction.Status()), toOperationResponse(res.Transaction, res.Replay))
}

func statusFor(s wagering.Status) int {
	switch s {
	case wagering.StatusProcessed:
		return http.StatusOK
	case wagering.StatusPending, wagering.StatusPendingReference:
		return http.StatusAccepted
	case wagering.StatusRejected:
		return http.StatusUnprocessableEntity
	default: // FAILED
		return http.StatusInternalServerError
	}
}

func (h *Handlers) getTransaction(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	id, err := wagering.ParseID("transactionId", r.PathValue("transactionId"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	t, err := h.queries.GetTransaction(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	// Providers only see their own operations; others look nonexistent.
	if !p.IsInternal() && (t.ProviderID() == "" || !p.CanAccessProvider(t.ProviderID())) {
		h.writeError(w, r, application.ErrTransactionNotFound)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionView(t))
}

func (h *Handlers) getProviderTransaction(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	providerID := r.PathValue("providerId")
	if !p.CanAccessProvider(providerID) {
		h.metrics.AuthFailures.WithLabelValues("provider_mismatch").Inc()
		h.writeError(w, r, fmt.Errorf("%w: token is bound to another provider", auth.ErrForbidden))
		return
	}
	t, err := h.queries.GetByExternalID(r.Context(), providerID, r.PathValue("externalTransactionId"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionView(t))
}

// ------------------------------------------------------------------ helpers

func decodeJSON(r *http.Request, v any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		return &wagering.ValidationError{Field: "Content-Type", Reason: "must be application/json"}
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return &wagering.ValidationError{Field: "body", Reason: "malformed JSON: " + err.Error()}
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return &wagering.ValidationError{Field: "body", Reason: "trailing data after JSON object"}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func validationDetails(err error) []errorDetail {
	var out []errorDetail
	var walk func(error)
	walk = func(e error) {
		if j, ok := e.(interface{ Unwrap() []error }); ok {
			for _, c := range j.Unwrap() {
				walk(c)
			}
			return
		}
		var ve *wagering.ValidationError
		if errors.As(e, &ve) {
			out = append(out, errorDetail{Field: ve.Field, Reason: ve.Reason})
		}
	}
	walk(err)
	return out
}

func (h *Handlers) writeError(w http.ResponseWriter, r *http.Request, err error) {
	writeError(w, r, h.log, err)
}

func writeError(w http.ResponseWriter, r *http.Request, log *slog.Logger, err error) {
	ctx := r.Context()
	status, code, msg := http.StatusInternalServerError, "INTERNAL_ERROR", "internal error"
	var details []errorDetail
	switch {
	case errors.Is(err, auth.ErrUnauthenticated):
		status, code, msg = http.StatusUnauthorized, "UNAUTHENTICATED", "missing, invalid or expired credentials"
		w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
	case errors.Is(err, auth.ErrForbidden):
		status, code, msg = http.StatusForbidden, "FORBIDDEN", "operation not allowed for this identity"
	case errors.Is(err, wagering.ErrValidation):
		status, code, msg = http.StatusBadRequest, string(application.CodeOf(err)), "invalid request"
		details = validationDetails(err)
		for _, d := range details {
			if d.Field == "kind" && strings.Contains(d.Reason, "OPENING") {
				code = string(wagering.CodeInternalKindNotAllowed)
			}
		}
	case errors.Is(err, application.ErrWalletNotFound):
		status, code, msg = http.StatusNotFound, string(wagering.CodeWalletNotFound), "wallet not found"
	case errors.Is(err, application.ErrTransactionNotFound):
		status, code, msg = http.StatusNotFound, "TRANSACTION_NOT_FOUND", "transaction not found"
	case errors.Is(err, application.ErrWalletAlreadyExists):
		status, code, msg = http.StatusConflict, "WALLET_ALREADY_EXISTS", err.Error()
	case errors.Is(err, application.ErrIdempotencyConflict):
		status, code, msg = http.StatusConflict, string(wagering.CodeIdempotencyKeyReused), err.Error()
	case errors.Is(err, application.ErrExternalIDReused):
		status, code, msg = http.StatusConflict, string(wagering.CodeExternalIDReused), err.Error()
	case errors.Is(err, application.ErrTransient), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		status, code, msg = http.StatusServiceUnavailable, string(wagering.CodeTransientFailure), "temporarily unavailable, retry with the same Idempotency-Key"
		w.Header().Set("Retry-After", "1")
	}
	attrs := []any{slog.Int("status", status), slog.String("code", code)}
	if status >= 500 {
		log.ErrorContext(ctx, "request failed", append(attrs, slog.String("error", err.Error()))...)
	} else {
		log.InfoContext(ctx, "request rejected", append(attrs, slog.String("error", err.Error()))...)
	}
	writeJSON(w, status, errorResponse{
		Error:         errorBody{Code: code, Message: msg, Details: details},
		CorrelationID: observability.CorrelationID(ctx),
	})
}

func newCorrelationID() string { return uuid.Must(uuid.NewV7()).String() }
