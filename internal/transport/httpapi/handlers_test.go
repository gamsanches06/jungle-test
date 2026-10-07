package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/application"
	"github.com/gamsanches06/jungle-test/internal/application/apptest"
	"github.com/gamsanches06/jungle-test/internal/auth"
	"github.com/gamsanches06/jungle-test/internal/config"
	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
	"github.com/gamsanches06/jungle-test/internal/observability"
)

// fakeVerifier maps opaque test tokens to principals. The real OIDC
// validation is covered by the Keycloak integration tests.
type fakeVerifier map[string]auth.Principal

func (f fakeVerifier) Verify(_ context.Context, raw string) (auth.Principal, error) {
	p, ok := f[raw]
	if !ok {
		return auth.Principal{}, fmt.Errorf("%w: unknown token", auth.ErrUnauthenticated)
	}
	return p, nil
}

var tokens = fakeVerifier{
	"internal":   {ClientID: "wallet-service", Roles: map[string]bool{auth.RoleWalletOperator: true}},
	"provider-a": {ClientID: "provider-a", ProviderID: "provider-a", Roles: map[string]bool{auth.RoleProvider: true}},
	"provider-b": {ClientID: "provider-b", ProviderID: "provider-b", Roles: map[string]bool{auth.RoleProvider: true}},
	"no-role":    {ClientID: "no-role", Roles: map[string]bool{}},
}

type testAPI struct {
	t     *testing.T
	h     http.Handler
	store *apptest.MemStore
}

func newTestAPI(t *testing.T) *testAPI {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := apptest.NewMemStore()
	m := observability.NewMetrics()
	policy := wagering.PendingPolicy{BaseDelay: time.Second, MaxDelay: time.Second, MaxAttempts: 3, TTL: time.Minute}
	proc, err := application.NewProcessService(store, application.UUIDv7{}, application.SystemClock{}, policy, m, log)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandlers(proc, application.NewWalletService(store, application.UUIDv7{}, application.SystemClock{}, m, log),
		application.NewQueryService(store), m, log)
	cfg := config.Config{HTTPHandlerTimeout: 5 * time.Second}
	return &testAPI{t: t, store: store, h: NewRouter(h, tokens, NewReadiness(nil), m, cfg, log)}
}

func (a *testAPI) do(method, path, token string, body any, headers ...string) (int, map[string]any) {
	a.t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, r)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	a.h.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func (a *testAPI) openWallet(balance string) (walletID, playerID string) {
	playerID = uuid.NewString()
	code, w := a.do("POST", "/wallets", "internal", map[string]any{
		"playerId": playerID, "initialBalance": map[string]string{"amount": balance, "currency": "BRL"},
	})
	if code != http.StatusCreated {
		a.t.Fatalf("open wallet = %d %v", code, w)
	}
	return w["id"].(string), playerID
}

func op(provider, ext, player, wallet, kind, amount, ref string) map[string]any {
	m := map[string]any{
		"providerId": provider, "externalTransactionId": ext, "playerId": player, "walletId": wallet,
		"roundId": "round-1", "gameId": "game-1", "kind": kind,
		"money": map[string]string{"amount": amount, "currency": "BRL"},
	}
	if ref != "" {
		m["referenceExternalTransactionId"] = ref
	}
	return m
}

func TestAuthentication(t *testing.T) {
	a := newTestAPI(t)
	if code, body := a.do("POST", "/wallets", "", map[string]any{}); code != 401 || body["error"].(map[string]any)["code"] != "UNAUTHENTICATED" {
		t.Fatalf("missing token = %d %v", code, body)
	}
	if code, _ := a.do("POST", "/wallets", "forged", map[string]any{}); code != 401 {
		t.Fatalf("invalid token = %d", code)
	}
	if code, _ := a.do("GET", "/health/live", "", nil); code != 200 {
		t.Fatalf("live = %d", code)
	}
}

func TestWalletOperationsAreInternalOnly(t *testing.T) {
	a := newTestAPI(t)
	wid, _ := a.openWallet("100.00")
	for _, tok := range []string{"provider-a", "no-role"} {
		for _, c := range []struct{ method, path string }{
			{"POST", "/wallets"}, {"GET", "/wallets/" + wid}, {"GET", "/wallets/" + wid + "/ledger"},
			{"POST", "/wallets/" + wid + "/reconciliation"},
		} {
			if code, _ := a.do(c.method, c.path, tok, map[string]any{}); code != 403 {
				t.Errorf("%s %s with %s = %d", c.method, c.path, tok, code)
			}
		}
	}
	if code, _ := a.do("POST", "/wagering/transactions", "internal", map[string]any{}, "Idempotency-Key", "k"); code != 403 {
		t.Errorf("internal submitting provider operation = %d", code)
	}
}

func TestSubmitContract(t *testing.T) {
	a := newTestAPI(t)
	wid, pid := a.openWallet("100.00")

	code, body := a.do("POST", "/wagering/transactions", "provider-a", op("provider-a", "b1", pid, wid, "BET", "25.00", ""))
	if code != 400 {
		t.Fatalf("missing Idempotency-Key = %d %v", code, body)
	}
	code, body = a.do("POST", "/wagering/transactions", "provider-a", op("provider-a", "b1", pid, wid, "BET", "25.00", ""), "Idempotency-Key", "provider-a:b1")
	if code != 200 || body["status"] != "PROCESSED" || body["idempotentReplay"] != false || body["balance"].(map[string]any)["amount"] != "75.00" {
		t.Fatalf("bet = %d %v", code, body)
	}
	code, body = a.do("POST", "/wagering/transactions", "provider-a", op("provider-a", "b1", pid, wid, "BET", "25.00", ""), "Idempotency-Key", "provider-a:b1")
	if code != 200 || body["idempotentReplay"] != true {
		t.Fatalf("replay = %d %v", code, body)
	}
	code, body = a.do("POST", "/wagering/transactions", "provider-a", op("provider-a", "b1", pid, wid, "BET", "30.00", ""), "Idempotency-Key", "provider-a:b1")
	if code != 409 || body["error"].(map[string]any)["code"] != "IDEMPOTENCY_KEY_REUSED" {
		t.Fatalf("conflict = %d %v", code, body)
	}
	code, body = a.do("POST", "/wagering/transactions", "provider-a", op("provider-a", "b1", pid, wid, "BET", "25.00", ""), "Idempotency-Key", "another-key")
	if code != 409 || body["error"].(map[string]any)["code"] != "EXTERNAL_TRANSACTION_ID_REUSED" {
		t.Fatalf("external id reuse = %d %v", code, body)
	}
	code, body = a.do("POST", "/wagering/transactions", "provider-a", op("provider-a", "b2", pid, wid, "BET", "500.00", ""), "Idempotency-Key", "provider-a:b2")
	if code != 422 || body["status"] != "REJECTED" || body["failureCode"] != "INSUFFICIENT_FUNDS" {
		t.Fatalf("rejection = %d %v", code, body)
	}
	code, body = a.do("POST", "/wagering/transactions", "provider-a", op("provider-a", "r1", pid, wid, "REFUND", "10.00", "missing"), "Idempotency-Key", "provider-a:r1")
	if code != 202 || body["status"] != "PENDING_REFERENCE" {
		t.Fatalf("pending = %d %v", code, body)
	}
	code, body = a.do("POST", "/wagering/transactions", "provider-a", op("provider-a", "o1", pid, wid, "OPENING", "10.00", ""), "Idempotency-Key", "provider-a:o1")
	if code != 400 || body["error"].(map[string]any)["code"] != "INTERNAL_KIND_NOT_ALLOWED" {
		t.Fatalf("opening = %d %v", code, body)
	}
	bad := op("provider-a", "x1", pid, wid, "BET", "25.00", "")
	bad["money"] = map[string]any{"amount": 25.00, "currency": "BRL"}
	if code, _ = a.do("POST", "/wagering/transactions", "provider-a", bad, "Idempotency-Key", "x1"); code != 400 {
		t.Fatalf("numeric amount = %d", code)
	}
	code, body = a.do("POST", "/wagering/transactions", "provider-a", op("provider-a", "x2", pid, uuid.NewString(), "BET", "1.00", ""), "Idempotency-Key", "x2")
	if code != 404 || body["error"].(map[string]any)["code"] != "WALLET_NOT_FOUND" {
		t.Fatalf("unknown wallet = %d %v", code, body)
	}
}

func TestProviderIsolation(t *testing.T) {
	a := newTestAPI(t)
	wid, pid := a.openWallet("100.00")
	_, body := a.do("POST", "/wagering/transactions", "provider-a", op("provider-a", "b1", pid, wid, "BET", "25.00", ""), "Idempotency-Key", "provider-a:b1")
	txID := body["transactionId"].(string)
	before := len(a.store.Txs)

	if code, _ := a.do("POST", "/wagering/transactions", "provider-b", op("provider-a", "b1", pid, wid, "BET", "25.00", ""), "Idempotency-Key", "provider-a:b1"); code != 403 {
		t.Fatalf("impersonation = %d", code)
	}
	if code, _ := a.do("GET", "/wagering/transactions/"+txID, "provider-b", nil); code != 404 {
		t.Fatalf("foreign transaction by id = %d", code)
	}
	if code, _ := a.do("GET", "/providers/provider-a/wagering/transactions/b1", "provider-b", nil); code != 403 {
		t.Fatalf("foreign provider path = %d", code)
	}
	if code, _ := a.do("GET", "/providers/provider-a/wagering/transactions/b1", "provider-a", nil); code != 200 {
		t.Fatalf("own provider path = %d", code)
	}
	if code, _ := a.do("GET", "/wagering/transactions/"+txID, "internal", nil); code != 200 {
		t.Fatalf("internal read = %d", code)
	}
	if len(a.store.Txs) != before {
		t.Fatal("unauthorized requests created transactions")
	}
}

func TestLedgerPaginationAndReconciliation(t *testing.T) {
	a := newTestAPI(t)
	wid, pid := a.openWallet("100.00")
	for i := 1; i <= 4; i++ {
		ext := fmt.Sprintf("b%d", i)
		if code, b := a.do("POST", "/wagering/transactions", "provider-a", op("provider-a", ext, pid, wid, "BET", "1.00", ""), "Idempotency-Key", ext); code != 200 {
			t.Fatalf("bet %d = %d %v", i, code, b)
		}
	}
	var versions []float64
	cursor := ""
	for page := 0; page < 10; page++ {
		path := "/wallets/" + wid + "/ledger?limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		code, body := a.do("GET", path, "internal", nil)
		if code != 200 {
			t.Fatalf("ledger = %d %v", code, body)
		}
		for _, e := range body["entries"].([]any) {
			versions = append(versions, e.(map[string]any)["walletVersion"].(float64))
		}
		next, ok := body["nextCursor"].(string)
		if !ok {
			break
		}
		cursor = next
	}
	if fmt.Sprint(versions) != "[1 2 3 4 5]" {
		t.Fatalf("versions = %v", versions)
	}
	if code, _ := a.do("GET", "/wallets/"+wid+"/ledger?cursor=garbage", "internal", nil); code != 400 {
		t.Fatalf("bad cursor = %d", code)
	}
	code, rec := a.do("POST", "/wallets/"+wid+"/reconciliation", "internal", nil)
	if code != 200 || rec["consistent"] != true || rec["checkedEntries"].(float64) != 5 ||
		rec["difference"].(map[string]any)["amount"] != "0.00" || rec["storedBalance"].(map[string]any)["amount"] != "96.00" {
		t.Fatalf("reconciliation = %d %v", code, rec)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	v, err := decodeCursor(encodeCursor(42))
	if err != nil || v != 42 {
		t.Fatalf("cursor = %d %v", v, err)
	}
	if _, err := decodeCursor("djE6LTE"); err == nil { // "v1:-1"
		t.Fatal("negative cursor accepted")
	}
}
