// Command loadtest drives authenticated BET traffic against one or more
// running instances and reports throughput, latency percentiles, outcomes,
// concurrency conflicts and outbox lag. It also reconciles every wallet used.
//
//	go run ./cmd/loadtest -targets http://localhost:8080,http://localhost:8082,http://localhost:8083
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type wallet struct{ id, player string }

type result struct {
	latency time.Duration
	code    int
	replay  bool
	err     bool
}

func main() {
	targets := flag.String("targets", "http://localhost:8080,http://localhost:8082,http://localhost:8083", "comma separated base URLs")
	keycloak := flag.String("keycloak", "http://localhost:8081/realms/jungle", "realm URL")
	wallets := flag.Int("wallets", 50, "number of wallets")
	concurrency := flag.Int("concurrency", 32, "concurrent clients")
	duration := flag.Duration("duration", 30*time.Second, "test duration")
	dupRatio := flag.Float64("dup-ratio", 0.1, "fraction of requests that resend an earlier operation")
	flag.Parse()

	bases := strings.Split(*targets, ",")
	client := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: *concurrency * 2}}
	internal := token(*keycloak, "wallet-service")
	provider := token(*keycloak, "provider-a")

	ws := make([]wallet, *wallets)
	for i := range ws {
		player := uuid.NewString()
		var w struct {
			ID string `json:"id"`
		}
		code := call(client, "POST", bases[i%len(bases)]+"/wallets", internal, map[string]any{
			"playerId": player, "initialBalance": map[string]string{"amount": "1000000.00", "currency": "BRL"},
		}, nil, &w)
		if code != 201 {
			fail("open wallet: %d", code)
		}
		ws[i] = wallet{w.ID, player}
	}

	var (
		mu      sync.Mutex
		results []result
		sent    []map[string]any
		seq     atomic.Int64
		wg      sync.WaitGroup
	)
	deadline := time.Now().Add(*duration)
	start := time.Now()
	for c := 0; c < *concurrency; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				var body map[string]any
				mu.Lock()
				if len(sent) > 0 && rand.Float64() < *dupRatio {
					body = sent[rand.IntN(len(sent))]
				}
				mu.Unlock()
				if body == nil {
					w := ws[rand.IntN(len(ws))]
					n := seq.Add(1)
					body = map[string]any{
						"providerId": "provider-a", "externalTransactionId": fmt.Sprintf("load-%s-%d", w.id[:8], n),
						"playerId": w.player, "walletId": w.id, "roundId": fmt.Sprintf("r-%d", n), "gameId": "load",
						"kind": "BET", "money": map[string]string{"amount": "1.00", "currency": "BRL"},
					}
				}
				base := bases[rand.IntN(len(bases))]
				var out struct {
					Replay bool `json:"idempotentReplay"`
				}
				t0 := time.Now()
				code := call(client, "POST", base+"/wagering/transactions", provider, body,
					map[string]string{"Idempotency-Key": "provider-a:" + body["externalTransactionId"].(string)}, &out)
				r := result{latency: time.Since(t0), code: code, replay: out.Replay, err: code == 0}
				mu.Lock()
				results = append(results, r)
				if code == 200 && !out.Replay {
					sent = append(sent, body)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	lat := make([]time.Duration, 0, len(results))
	codes := map[int]int{}
	replays, errs := 0, 0
	for _, r := range results {
		lat = append(lat, r.latency)
		codes[r.code]++
		if r.replay {
			replays++
		}
		if r.err {
			errs++
		}
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(float64(len(lat)-1)*p)]
	}
	fmt.Printf("targets=%d wallets=%d concurrency=%d duration=%s dup-ratio=%.2f\n", len(bases), *wallets, *concurrency, *duration, *dupRatio)
	fmt.Printf("requests=%d throughput=%.1f req/s\n", len(results), float64(len(results))/elapsed.Seconds())
	fmt.Printf("latency p50=%s p95=%s p99=%s max=%s\n", pct(.50), pct(.95), pct(.99), pct(1))
	fmt.Printf("status codes=%v idempotent replays=%d transport errors=%d\n", codes, replays, errs)

	var conflicts, retries float64
	for _, b := range bases {
		conflicts += metric(client, b, "wagering_concurrency_conflicts_total")
		retries += metric(client, b, "wagering_retries_total")
	}
	fmt.Printf("concurrency conflicts=%.0f retries=%.0f\n", conflicts, retries)
	lagStart := time.Now()
	fmt.Printf("outbox pending at end=%.0f lag=%.3fs\n", metric(client, bases[0], "wagering_outbox_pending_events"), metric(client, bases[0], "wagering_outbox_lag_seconds"))
	for time.Since(lagStart) < time.Minute {
		time.Sleep(2500 * time.Millisecond)
		if metric(client, bases[0], "wagering_outbox_pending_events") == 0 {
			break
		}
	}
	fmt.Printf("outbox drained %.1fs after load stopped (gauge refresh 2s)\n", time.Since(lagStart).Seconds())

	inconsistent := 0
	for _, w := range ws {
		var rec struct {
			Consistent bool `json:"consistent"`
		}
		if code := call(client, "POST", bases[0]+"/wallets/"+w.id+"/reconciliation", internal, nil, nil, &rec); code != 200 || !rec.Consistent {
			inconsistent++
		}
	}
	fmt.Printf("reconciliation: %d/%d wallets consistent\n", len(ws)-inconsistent, len(ws))
	if inconsistent > 0 {
		os.Exit(1)
	}
}

func token(realm, client string) string {
	resp, err := http.PostForm(realm+"/protocol/openid-connect/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {client + "-secret"},
	})
	if err != nil {
		fail("token: %v", err)
	}
	defer resp.Body.Close()
	var b struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&b)
	if b.AccessToken == "" {
		fail("token for %s: status %d", client, resp.StatusCode)
	}
	return b.AccessToken
}

func call(c *http.Client, method, u, tok string, body any, headers map[string]string, out any) int {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, u, rd)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+tok)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

func metric(c *http.Client, base, name string) float64 {
	resp, err := c.Get(base + "/metrics")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var total float64
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{") {
			var v float64
			f := strings.Fields(line)
			fmt.Sscan(f[len(f)-1], &v)
			total += v
		}
	}
	return total
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
