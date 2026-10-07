//go:build integration

// Package integration exercises the service against real PostgreSQL,
// Keycloak and LocalStack containers (docker compose up -d postgres keycloak
// localstack). Each test gets its own database and its own SQS queues.
package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/gamsanches06/jungle-test/internal/app"
	"github.com/gamsanches06/jungle-test/internal/config"
	"github.com/gamsanches06/jungle-test/internal/infra/sqsx"
	"github.com/gamsanches06/jungle-test/internal/observability"
	"github.com/gamsanches06/jungle-test/internal/transport/httpapi"
	"github.com/gamsanches06/jungle-test/internal/worker"
	"github.com/gamsanches06/jungle-test/migrations"
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var (
	adminDBURL  = getenv("TEST_ADMIN_DATABASE_URL", "postgres://jungle:jungle-1234@localhost:5432/postgres?sslmode=disable")
	appDBPass   = getenv("TEST_APP_DB_PASSWORD", "jungle-app-1234")
	keycloakURL = getenv("TEST_KEYCLOAK_URL", "http://localhost:8081")
	awsEndpoint = getenv("TEST_AWS_ENDPOINT_URL", "http://localhost:4566")
	issuer      = keycloakURL + "/realms/jungle"
	jwksURL     = issuer + "/protocol/openid-connect/certs"
	binaryPath  string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "wagering-it")
	if err != nil {
		panic(err)
	}
	binaryPath = filepath.Join(dir, "wagering")
	build := exec.Command("go", "build", "-o", binaryPath, "../../cmd/wagering")
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build failed:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func randSuffix() string {
	b := make([]byte, 5)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ------------------------------------------------------------------ database

// Database is an isolated, migrated database for one test.
type Database struct {
	Name     string
	OwnerURL string
	AppURL   string
	Owner    *pgxpool.Pool // schema owner, used for assertions and tampering
}

func withDB(base, db string) string {
	u, _ := url.Parse(base)
	u.Path = "/" + db
	return u.String()
}

func withUser(base, user, pass string) string {
	u, _ := url.Parse(base)
	u.User = url.UserPassword(user, pass)
	return u.String()
}

func NewDatabase(t *testing.T) *Database {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, adminDBURL)
	if err != nil {
		t.Fatalf("connect admin (is docker compose up?): %v", err)
	}
	defer admin.Close(ctx)
	name := "it_" + randSuffix()
	if _, err := admin.Exec(ctx, `DO $$ BEGIN
		IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'jungle-app') THEN
			CREATE ROLE "jungle-app" LOGIN PASSWORD '`+appDBPass+`';
		END IF; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "GRANT CONNECT ON DATABASE "+name+" TO \"jungle-app\""); err != nil {
		t.Fatal(err)
	}
	d := &Database{Name: name, OwnerURL: withDB(adminDBURL, name)}
	d.AppURL = withUser(d.OwnerURL, "jungle-app", appDBPass)
	mig, err := migrations.New(d.OwnerURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := mig.Up(); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	_, _ = mig.Close()
	d.Owner, err = pgxpool.New(ctx, d.OwnerURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		d.Owner.Close()
		c, err := pgx.Connect(context.Background(), adminDBURL)
		if err == nil {
			_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
			_ = c.Close(context.Background())
		}
	})
	return d
}

func (d *Database) Int(t *testing.T, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := d.Owner.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func (d *Database) Str(t *testing.T, sql string, args ...any) string {
	t.Helper()
	var s string
	if err := d.Owner.QueryRow(context.Background(), sql, args...).Scan(&s); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return s
}

// AssertConsistent checks stored balance == credits - debits of the ledger
// and that versions form a gap-free chain.
func (d *Database) AssertConsistent(t *testing.T, walletID string) {
	t.Helper()
	stored := d.Int(t, `SELECT balance_minor FROM wallets WHERE id = $1`, walletID)
	sum := d.Int(t, `SELECT COALESCE(SUM(CASE WHEN direction='CREDIT' THEN amount_minor ELSE -amount_minor END),0)::bigint
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
	if stored != sum {
		t.Fatalf("wallet %s: stored %d != ledger %d", walletID, stored, sum)
	}
	version := d.Int(t, `SELECT version FROM wallets WHERE id = $1`, walletID)
	entries := d.Int(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
	opening := d.Int(t, `SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING'`, walletID)
	if version != entries+1-opening {
		t.Fatalf("wallet %s: version %d with %d entries (opening %d)", walletID, version, entries, opening)
	}
}

// ---------------------------------------------------------------------- SQS

// Queues are isolated SQS queues for one test.
type Queues struct {
	Client                      *sqs.Client
	Input, DLQ, Events          string
	InputURL, DLQURL, EventsURL string
}

func sqsClient(t *testing.T, endpoint string) *sqs.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	if err != nil {
		t.Fatal(err)
	}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func NewQueues(t *testing.T, visibility time.Duration, maxReceives int) *Queues {
	t.Helper()
	ctx := context.Background()
	c := sqsClient(t, awsEndpoint)
	s := randSuffix()
	q := &Queues{Client: c, Input: "it-" + s + ".fifo", DLQ: "it-" + s + "-dlq.fifo", Events: "it-" + s + "-events.fifo"}
	create := func(name string, attrs map[string]string) string {
		attrs["FifoQueue"] = "true"
		out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
		if err != nil {
			t.Fatalf("create queue %s (is localstack up?): %v", name, err)
		}
		return aws.ToString(out.QueueUrl)
	}
	q.DLQURL = create(q.DLQ, map[string]string{})
	arn, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(q.DLQURL), AttributeNames: []types.QueueAttributeName{"QueueArn"}})
	if err != nil {
		t.Fatal(err)
	}
	redrive, _ := json.Marshal(map[string]string{"deadLetterTargetArn": arn.Attributes["QueueArn"], "maxReceiveCount": fmt.Sprint(maxReceives)})
	q.InputURL = create(q.Input, map[string]string{
		"VisibilityTimeout": fmt.Sprint(int(visibility.Seconds())), "RedrivePolicy": string(redrive),
	})
	q.EventsURL = create(q.Events, map[string]string{})
	t.Cleanup(func() {
		for _, u := range []string{q.InputURL, q.DLQURL, q.EventsURL} {
			_, _ = c.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
		}
	})
	return q
}

// Send publishes a WagerTransactionRequested envelope.
func (q *Queues) Send(t *testing.T, messageID string, data map[string]any) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"messageId": messageID, "type": "WagerTransactionRequested",
		"occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "data": data,
	})
	q.SendRaw(t, string(body), data["walletId"].(string), messageID+"-"+randSuffix())
}

// SendRaw sends any body. The SQS deduplication id is unique so that the
// application-level deduplication (inbox) is what gets exercised.
func (q *Queues) SendRaw(t *testing.T, body, group, dedup string) {
	t.Helper()
	_, err := q.Client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(q.InputURL), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup),
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Drain receives every currently available message from a queue.
func (q *Queues) Drain(t *testing.T, queueURL string, wait time.Duration) []types.Message {
	t.Helper()
	var out []types.Message
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		res, err := q.Client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 60,
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range res.Messages {
			out = append(out, m)
			_, _ = q.Client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: m.ReceiptHandle})
		}
	}
	return out
}

// Approx returns visible + in-flight messages of a queue.
func (q *Queues) Approx(t *testing.T, queueURL string) int {
	t.Helper()
	out, err := q.Client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL), AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var a, b int
	fmt.Sscan(out.Attributes["ApproximateNumberOfMessages"], &a)
	fmt.Sscan(out.Attributes["ApproximateNumberOfMessagesNotVisible"], &b)
	return a + b
}

// ---------------------------------------------------------------- settings

// Settings is the environment of one application instance.
type Settings map[string]string

func NewSettings(db *Database, q *Queues) Settings {
	return Settings{
		"INSTANCE_ID": "it-" + randSuffix(), "LOG_LEVEL": "warn", "HTTP_ADDR": "127.0.0.1:0",
		"DATABASE_URL": db.AppURL, "DB_MAX_CONNS": "20", "DB_LOCK_TIMEOUT": "5s",
		"OIDC_ISSUER": issuer, "OIDC_JWKS_URL": jwksURL, "OIDC_AUDIENCE": "jungle-api",
		"AWS_REGION": "us-east-1", "AWS_ENDPOINT_URL": awsEndpoint,
		"SQS_CONSUMER_ACCESS_KEY_ID": "test", "SQS_CONSUMER_SECRET_ACCESS_KEY": "test",
		"SQS_PUBLISHER_ACCESS_KEY_ID": "test", "SQS_PUBLISHER_SECRET_ACCESS_KEY": "test",
		"SQS_INPUT_QUEUE": q.Input, "SQS_DLQ_QUEUE": q.DLQ, "SQS_EVENTS_QUEUE": q.Events,
		"SQS_WAIT_TIME": "1s", "SQS_VISIBILITY_TIMEOUT": "4s", "SQS_PROCESS_TIMEOUT": "3s",
		"SQS_RETRY_BASE": "1s", "SQS_RETRY_MAX": "2s", "SQS_MAX_RECEIVES": "3",
		"OUTBOX_POLL_INTERVAL": "50ms", "OUTBOX_LEASE": "2s", "OUTBOX_RETRY_BASE": "200ms", "OUTBOX_RETRY_MAX": "1s",
		"PENDING_POLL_INTERVAL": "100ms", "REFERENCE_BASE_DELAY": "200ms", "REFERENCE_MAX_DELAY": "1s",
		"REFERENCE_MAX_ATTEMPTS": "20", "REFERENCE_TTL": "1m",
		"START_TIMEOUT": "30s", "SHUTDOWN_TIMEOUT": "10s",
	}
}

func (s Settings) With(kv ...string) Settings {
	out := Settings{}
	for k, v := range s {
		out[k] = v
	}
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	if _, ok := out["INSTANCE_ID"]; ok && !contains(kv, "INSTANCE_ID") {
		out["INSTANCE_ID"] = "it-" + randSuffix()
	}
	return out
}

func contains(kv []string, k string) bool {
	for i := 0; i < len(kv); i += 2 {
		if kv[i] == k {
			return true
		}
	}
	return false
}

func (s Settings) Config(t *testing.T) config.Config {
	t.Helper()
	c, err := config.FromLookup(func(k string) (string, bool) { v, ok := s[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// --------------------------------------------------------- in-process app

// App is an application instance running inside the test process.
type App struct {
	FX       *fx.App
	BaseURL  string
	Metrics  *observability.Metrics
	Pool     *pgxpool.Pool
	Consumer *sqsx.Consumer
	Relay    *worker.OutboxRelay
	Resolver *worker.PendingResolver
	stopped  bool
}

func StartApp(t *testing.T, s Settings) *App {
	t.Helper()
	a := &App{}
	var srv *httpapi.Server
	a.FX = fx.New(app.Options(s.Config(t), fx.Populate(&srv, &a.Metrics, &a.Pool, &a.Consumer, &a.Relay, &a.Resolver)))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := a.FX.Start(ctx); err != nil {
		t.Fatalf("start app: %v", err)
	}
	if addr := srv.Addr(); addr != "" {
		a.BaseURL = "http://" + addr
	}
	t.Cleanup(func() { a.Stop(t) })
	return a
}

func (a *App) Stop(t *testing.T) {
	if a.stopped {
		return
	}
	a.stopped = true
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.FX.Stop(ctx); err != nil {
		t.Errorf("stop app: %v", err)
	}
}

// ---------------------------------------------------- independent process

// Process is an application instance in its own OS process (own memory,
// own connection pool), started from the compiled binary.
type Process struct {
	t       *testing.T
	cmd     *exec.Cmd
	BaseURL string
	logPath string
	exited  chan struct{}
	exitErr error
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func StartProcess(t *testing.T, s Settings) *Process {
	t.Helper()
	port := freePort(t)
	s = s.With("HTTP_ADDR", fmt.Sprintf("127.0.0.1:%d", port), "INSTANCE_ID", s["INSTANCE_ID"])
	logFile, err := os.CreateTemp(t.TempDir(), "proc-*.log")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binaryPath)
	cmd.Env = append(os.Environ(), "LOG_LEVEL=info")
	for k, v := range s {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &Process{t: t, cmd: cmd, BaseURL: fmt.Sprintf("http://127.0.0.1:%d", port), logPath: logFile.Name(), exited: make(chan struct{})}
	go func() {
		p.exitErr = cmd.Wait()
		_ = logFile.Close()
		close(p.exited)
	}()
	t.Cleanup(func() {
		p.Kill()
		if t.Failed() {
			p.DumpLog(80)
		}
	})
	if s["APP_ROLES"] == "" || strings.Contains(s["APP_ROLES"], "api") {
		p.WaitReady(45 * time.Second)
	} else {
		time.Sleep(2 * time.Second)
	}
	return p
}

func (p *Process) WaitReady(timeout time.Duration) {
	p.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-p.exited:
			p.DumpLog(50)
			p.t.Fatalf("process exited during startup: %v", p.exitErr)
		default:
		}
		resp, err := http.Get(p.BaseURL + "/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	p.DumpLog(50)
	p.t.Fatal("process not ready")
}

// Terminate sends SIGTERM and waits for a graceful exit.
func (p *Process) Terminate(timeout time.Duration) error {
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.exited:
		return p.exitErr
	case <-time.After(timeout):
		p.Kill()
		return fmt.Errorf("did not exit after SIGTERM")
	}
}

// Kill simulates an abrupt crash (SIGKILL).
func (p *Process) Kill() {
	select {
	case <-p.exited:
		return
	default:
	}
	_ = p.cmd.Process.Kill()
	<-p.exited
}

// WaitExit waits for the process to die on its own (failpoint exit).
func (p *Process) WaitExit(timeout time.Duration) bool {
	select {
	case <-p.exited:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (p *Process) Log() string {
	b, _ := os.ReadFile(p.logPath)
	return string(b)
}

func (p *Process) DumpLog(lines int) {
	all := strings.Split(p.Log(), "\n")
	if len(all) > lines {
		all = all[len(all)-lines:]
	}
	p.t.Logf("---- log of %s ----\n%s", p.BaseURL, strings.Join(all, "\n"))
}

// ------------------------------------------------------------------- tokens

var (
	tokenMu    sync.Mutex
	tokenCache = map[string]struct {
		tok string
		exp time.Time
	}{}
)

// Token obtains a real access token from Keycloak with client_credentials.
func Token(t *testing.T, client string) string {
	t.Helper()
	tokenMu.Lock()
	defer tokenMu.Unlock()
	if c, ok := tokenCache[client]; ok && time.Until(c.exp) > 30*time.Second {
		return c.tok
	}
	tok, ttl := FreshToken(t, client)
	tokenCache[client] = struct {
		tok string
		exp time.Time
	}{tok, time.Now().Add(ttl)}
	return tok
}

// FreshToken always requests a new token.
func FreshToken(t *testing.T, client string) (string, time.Duration) {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {client + "-secret"}}
	resp, err := http.PostForm(issuer+"/protocol/openid-connect/token", form)
	if err != nil {
		t.Fatalf("token request (is keycloak up?): %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("token for %s: status %d %v", client, resp.StatusCode, err)
	}
	return body.AccessToken, time.Duration(body.ExpiresIn) * time.Second
}

// --------------------------------------------------------------------- HTTP

type Resp struct {
	Code    int
	Body    map[string]any
	Raw     string
	Headers http.Header
}

func (r Resp) Str(path ...string) string {
	var cur any = r.Body
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[p]
	}
	s, _ := cur.(string)
	return s
}

func (r Resp) ErrCode() string { return r.Str("error", "code") }

var httpClient = &http.Client{Timeout: 30 * time.Second}

func Do(t *testing.T, method, url, token string, body any, headers ...string) Resp {
	t.Helper()
	r, err := DoErr(method, url, token, body, headers...)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return r
}

func DoErr(method, url, token string, body any, headers ...string) (Resp, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return Resp{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return Resp{}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := Resp{Code: resp.StatusCode, Raw: string(raw), Headers: resp.Header}
	_ = json.Unmarshal(raw, &out.Body)
	return out, nil
}

// OpenWallet opens a wallet through the API with the internal identity.
func OpenWallet(t *testing.T, base, balance string) (walletID, playerID string) {
	t.Helper()
	playerID = uuid.NewString()
	r := Do(t, "POST", base+"/wallets", Token(t, "wallet-service"), map[string]any{
		"playerId": playerID, "initialBalance": map[string]string{"amount": balance, "currency": "BRL"},
	})
	if r.Code != http.StatusCreated {
		t.Fatalf("open wallet = %d %s", r.Code, r.Raw)
	}
	return r.Str("id"), playerID
}

// Op builds an operation body.
func Op(provider, ext, player, wallet, kind, amount, ref string) map[string]any {
	m := map[string]any{
		"providerId": provider, "externalTransactionId": ext, "playerId": player, "walletId": wallet,
		"roundId": "round-1", "gameId": "fortune-chimp", "kind": kind,
		"money": map[string]string{"amount": amount, "currency": "BRL"},
	}
	if ref != "" {
		m["referenceExternalTransactionId"] = ref
	}
	return m
}

// Submit posts an operation with the provider's token and key provider:ext.
func Submit(t *testing.T, base, provider string, body map[string]any) Resp {
	t.Helper()
	return Do(t, "POST", base+"/wagering/transactions", Token(t, provider), body,
		"Idempotency-Key", provider+":"+body["externalTransactionId"].(string))
}

// SQSData converts an operation body into SQS message data.
func SQSData(body map[string]any, key string) map[string]any {
	d := map[string]any{}
	for k, v := range body {
		d[k] = v
	}
	d["idempotencyKey"] = key
	return d
}

// Eventually polls cond until it is true.
func Eventually(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}

// Metric reads a counter/gauge value from a /metrics text exposition.
func Metric(t *testing.T, base, name string) float64 {
	t.Helper()
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var total float64
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{") {
			var v float64
			fields := strings.Fields(line)
			fmt.Sscan(fields[len(fields)-1], &v)
			total += v
		}
	}
	return total
}

// newFXApp builds (without starting) an application.
func newFXApp(t *testing.T, s Settings) *fx.App {
	t.Helper()
	return fx.New(app.Options(s.Config(t)), fx.NopLogger)
}
