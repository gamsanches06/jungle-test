//go:build integration

package integration

import (
	"context"
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"
)

// Proxy is a TCP proxy that can cut every connection to simulate an
// unavailable dependency, then restore it.
type Proxy struct {
	t      *testing.T
	ln     net.Listener
	target string
	mu     sync.Mutex
	down   bool
	conns  map[net.Conn]struct{}
	Addr   string
}

func NewProxy(t *testing.T, target string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{t: t, ln: ln, target: target, conns: map[net.Conn]struct{}{}, Addr: ln.Addr().String()}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close(); p.Cut() })
	return p
}

func (p *Proxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.down {
			p.mu.Unlock()
			_ = c.Close()
			continue
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			p.mu.Unlock()
			_ = c.Close()
			continue
		}
		p.conns[c], p.conns[up] = struct{}{}, struct{}{}
		p.mu.Unlock()
		go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
		go func() { _, _ = io.Copy(c, up); _ = c.Close() }()
	}
}

// Cut closes every open connection and refuses new ones.
func (p *Proxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = true
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
}

// Restore accepts connections again.
func (p *Proxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = false
}

func hostPort(raw string) string {
	u, _ := url.Parse(raw)
	return u.Host
}

func replaceHost(raw, host string) string {
	u, _ := url.Parse(raw)
	u.Host = host
	return u.String()
}

func TestPostgresTemporarilyUnavailable(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 10)
	proxy := NewProxy(t, hostPort(db.AppURL))
	s := NewSettings(db, q).With("DATABASE_URL", replaceHost(db.AppURL, proxy.Addr), "APP_ROLES", "api,consumer,pending,outbox",
		"DB_STATEMENT_TIMEOUT", "2s", "HTTP_HANDLER_TIMEOUT", "3s")
	a := StartApp(t, s)
	wid, pid := OpenWallet(t, a.BaseURL, "100.00")

	proxy.Cut()
	r := Submit(t, a.BaseURL, "provider-a", Op("provider-a", "down-bet", pid, wid, "BET", "10.00", ""))
	if r.Code != 503 || r.ErrCode() != "SERVICE_UNAVAILABLE" || r.Headers.Get("Retry-After") == "" {
		t.Fatalf("bet while postgres down = %d %s", r.Code, r.Raw)
	}
	if rd := Do(t, "GET", a.BaseURL+"/health/ready", "", nil); rd.Code != 503 || rd.Str("checks", "postgres") != "down" {
		t.Fatalf("readiness while postgres down = %d %s", rd.Code, rd.Raw)
	}
	if lv := Do(t, "GET", a.BaseURL+"/health/live", "", nil); lv.Code != 200 {
		t.Fatalf("liveness = %d", lv.Code)
	}
	// Give the consumer time to fail on this message while the database is down.
	q.Send(t, "down-msg", SQSData(Op("provider-a", "down-sqs", pid, wid, "BET", "5.00", ""), "provider-a:down-sqs"))
	time.Sleep(3 * time.Second)

	proxy.Restore()
	Eventually(t, 20*time.Second, "ready again", func() bool {
		return Do(t, "GET", a.BaseURL+"/health/ready", "", nil).Code == 200
	})
	if r := Submit(t, a.BaseURL, "provider-a", Op("provider-a", "down-bet", pid, wid, "BET", "10.00", "")); r.Code != 200 || r.Body["idempotentReplay"] != false {
		t.Fatalf("retry after recovery = %d %s", r.Code, r.Raw)
	}
	Eventually(t, 30*time.Second, "sqs message processed after recovery", func() bool { return statusOf(t, db, "down-sqs") == "PROCESSED" })
	Eventually(t, 20*time.Second, "outbox drained after recovery", func() bool {
		return db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
	if bal := db.Int(t, `SELECT balance_minor FROM wallets WHERE id = $1`, wid); bal != 8500 {
		t.Fatalf("balance = %d", bal)
	}
	db.AssertConsistent(t, wid)
}

func TestSQSTemporarilyUnavailable(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 10)
	proxy := NewProxy(t, hostPort(awsEndpoint))
	s := NewSettings(db, q)
	api := StartApp(t, s.With("APP_ROLES", "api"))
	wid, pid := OpenWallet(t, api.BaseURL, "100.00")
	a := StartApp(t, s.With("APP_ROLES", "api,consumer,outbox", "AWS_ENDPOINT_URL", "http://"+proxy.Addr))

	proxy.Cut()
	if rd := Do(t, "GET", a.BaseURL+"/health/ready", "", nil); rd.Code != 503 || rd.Str("checks", "sqs") != "down" {
		t.Fatalf("readiness while sqs down = %d %s", rd.Code, rd.Raw)
	}
	if r := Submit(t, a.BaseURL, "provider-a", Op("provider-a", "sqs-down-bet", pid, wid, "BET", "10.00", "")); r.Code != 200 {
		t.Fatalf("http during sqs outage = %d %s", r.Code, r.Raw)
	}
	q.Send(t, "while-down", SQSData(Op("provider-a", "sqs-down-msg", pid, wid, "BET", "1.00", ""), "provider-a:sqs-down-msg"))
	time.Sleep(2 * time.Second)
	if n := db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`); n == 0 {
		t.Fatal("events published while the broker was down")
	}
	proxy.Restore()
	Eventually(t, 30*time.Second, "outbox drained", func() bool {
		return db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
	Eventually(t, 30*time.Second, "message consumed", func() bool { return statusOf(t, db, "sqs-down-msg") == "PROCESSED" })
	db.AssertConsistent(t, wid)
}

// TestFxLifecycleReleasesResources starts the full composition, stops it and
// checks that every worker terminated and the pool was closed after them.
func TestFxLifecycleReleasesResources(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 3)
	a := StartApp(t, NewSettings(db, q))
	if r := Do(t, "GET", a.BaseURL+"/health/ready", "", nil); r.Code != 200 {
		t.Fatalf("ready = %d %s", r.Code, r.Raw)
	}
	for name, done := range map[string]<-chan struct{}{"consumer": a.Consumer.Done(), "relay": a.Relay.Done(), "resolver": a.Resolver.Done()} {
		select {
		case <-done:
			t.Fatalf("%s stopped before shutdown", name)
		default:
		}
	}
	base := a.BaseURL
	a.Stop(t)
	for name, done := range map[string]<-chan struct{}{"consumer": a.Consumer.Done(), "relay": a.Relay.Done(), "resolver": a.Resolver.Done()} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s still running after stop", name)
		}
	}
	if err := a.Pool.Ping(context.Background()); err == nil {
		t.Fatal("pool still open after stop")
	}
	if _, err := DoErr("GET", base+"/health/live", "", nil); err == nil {
		t.Fatal("http server still accepting after stop")
	}

	bad := NewSettings(db, q).With("DATABASE_URL", "postgres://jungle-app:x@127.0.0.1:1/none?sslmode=disable", "START_TIMEOUT", "3s")
	app := newFXApp(t, bad)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := app.Start(ctx); err == nil {
		_ = app.Stop(context.Background())
		t.Fatal("start succeeded without a database")
	}
}
