// Command e2e validates the scenarios of the technical test against a running
// stack (docker compose up --build) and prints one line per check.
//
//	go run ./cmd/e2e
//	go run ./cmd/e2e -targets http://localhost:8080,http://localhost:8082,http://localhost:8083
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

var (
	targets  = flag.String("targets", "http://localhost:8080,http://localhost:8082,http://localhost:8083", "base URLs of the instances")
	realm    = flag.String("realm", "http://localhost:8081/realms/jungle", "Keycloak realm URL")
	endpoint = flag.String("sqs", "http://localhost:4566", "SQS (LocalStack) endpoint")
	verbose  = flag.Bool("v", false, "print response bodies of failed checks")

	bases          []string
	client         = &http.Client{Timeout: 20 * time.Second}
	passed, failed int
	run            = strconv.FormatInt(time.Now().Unix(), 36)
)

type resp struct {
	code int
	body map[string]any
	raw  string
}

func (r resp) str(path ...string) string {
	var cur any = r.body
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[p]
	}
	switch v := cur.(type) {
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	return ""
}

func check(name string, ok bool, r ...resp) {
	if ok {
		passed++
		fmt.Printf("  ✔ %s\n", name)
		return
	}
	failed++
	fmt.Printf("  ✘ %s\n", name)
	if *verbose && len(r) > 0 {
		fmt.Printf("      got %d %s\n", r[0].code, r[0].raw)
	}
}

func section(s string) { fmt.Printf("\n%s\n", s) }

func do(method, u, token string, body any, headers ...string) resp {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, u, rd)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	res, err := client.Do(req)
	if err != nil {
		return resp{raw: err.Error()}
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	out := resp{code: res.StatusCode, raw: string(raw)}
	_ = json.Unmarshal(raw, &out.body)
	return out
}

func token(clientID string) string {
	res, err := http.PostForm(*realm+"/protocol/openid-connect/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {clientID + "-secret"},
	})
	if err != nil {
		fatal("keycloak unreachable at %s: %v", *realm, err)
	}
	defer res.Body.Close()
	var b struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.NewDecoder(res.Body).Decode(&b)
	if b.AccessToken == "" {
		fatal("no token for client %s (status %d)", clientID, res.StatusCode)
	}
	return b.AccessToken
}

func fatal(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "fatal: "+f+"\n", a...)
	os.Exit(2)
}

func brl(amount string) map[string]string {
	return map[string]string{"amount": amount, "currency": "BRL"}
}

type wallet struct{ id, player string }

func openWallet(internal, balance string) (wallet, resp) {
	player := uuid.NewString()
	r := do("POST", bases[0]+"/wallets", internal, map[string]any{"playerId": player, "initialBalance": brl(balance)})
	return wallet{r.str("id"), player}, r
}

func op(w wallet, ext, kind, amount, ref string) map[string]any {
	m := map[string]any{
		"providerId": "provider-a", "externalTransactionId": ext, "playerId": w.player, "walletId": w.id,
		"roundId": "round-" + run, "gameId": "fortune-chimp", "kind": kind, "money": brl(amount),
	}
	if ref != "" {
		m["referenceExternalTransactionId"] = ref
	}
	return m
}

func submit(base, tok string, body map[string]any) resp {
	return do("POST", base+"/wagering/transactions", tok, body, "Idempotency-Key", "provider-a:"+body["externalTransactionId"].(string))
}

func eventually(timeout time.Duration, f func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

func main() {
	flag.Parse()
	bases = strings.Split(*targets, ",")
	fmt.Printf("run id %s, instances %v\n", run, bases)

	section("1. Health checks")
	for _, b := range bases {
		check("live "+b, do("GET", b+"/health/live", "", nil).code == 200)
		r := do("GET", b+"/health/ready", "", nil)
		check("ready "+b+" (postgres+sqs up)", r.code == 200 && r.str("checks", "postgres") == "up" && r.str("checks", "sqs") == "up", r)
	}

	internal, provA, provB := token("wallet-service"), token("provider-a"), token("provider-b")

	section("2. Authentication and authorization")
	probe := map[string]any{"providerId": "provider-a"}
	r := do("POST", bases[0]+"/wagering/transactions", "", probe, "Idempotency-Key", "x")
	check("no token -> 401 UNAUTHENTICATED", r.code == 401 && r.str("error", "code") == "UNAUTHENTICATED", r)
	check("garbage token -> 401", do("POST", bases[0]+"/wagering/transactions", "garbage", probe, "Idempotency-Key", "x").code == 401)
	tampered := provA[:len(provA)-4] + "AAAA"
	check("tampered signature -> 401", do("POST", bases[0]+"/wagering/transactions", tampered, probe, "Idempotency-Key", "x").code == 401)
	check("token for another audience -> 401", do("POST", bases[0]+"/wagering/transactions", token("other-audience-client"), probe, "Idempotency-Key", "x").code == 401)
	check("authenticated without role -> 403", do("POST", bases[0]+"/wallets", token("no-role-client"), map[string]any{}).code == 403)
	check("provider on wallet operation -> 403", do("POST", bases[0]+"/wallets", provA, map[string]any{}).code == 403)
	check("internal service submitting provider operation -> 403", do("POST", bases[0]+"/wagering/transactions", internal, probe, "Idempotency-Key", "x").code == 403)
	shortLived := token("provider-a-shortlived")
	time.Sleep(5 * time.Second) // provider-a-shortlived tokens live 3s
	check("expired token -> 401", do("POST", bases[0]+"/wagering/transactions", shortLived, probe, "Idempotency-Key", "x").code == 401)

	section("3. Wallet opening")
	w, r := openWallet(internal, "1000.00")
	check("POST /wallets 1000.00 -> 201, version 1", r.code == 201 && r.str("version") == "1" && r.str("balance", "amount") == "1000.00", r)
	r = do("GET", bases[1]+"/wallets/"+w.id, internal, nil)
	check("GET /wallets/:id on another instance", r.code == 200 && r.str("balance", "amount") == "1000.00", r)
	r = do("POST", bases[0]+"/wallets", internal, map[string]any{"playerId": w.player, "initialBalance": brl("5.00")})
	check("same player and currency -> 409 WALLET_ALREADY_EXISTS", r.code == 409 && r.str("error", "code") == "WALLET_ALREADY_EXISTS", r)
	zw, r := openWallet(internal, "0.00")
	lz := do("GET", bases[0]+"/wallets/"+zw.id+"/ledger", internal, nil)
	check("zero opening -> no ledger entry", r.code == 201 && lz.code == 200 && strings.Contains(lz.raw, `"entries":[]`), lz)
	for _, bad := range []map[string]any{{"amount": "10", "currency": "BRL"}, {"amount": "1e3", "currency": "BRL"}, {"amount": 10.0, "currency": "BRL"}, {"amount": "-1.00", "currency": "BRL"}, {"amount": "1.00", "currency": "XYZ"}} {
		r = do("POST", bases[0]+"/wallets", internal, map[string]any{"playerId": uuid.NewString(), "initialBalance": bad})
		check(fmt.Sprintf("invalid money %#v %v -> 400", bad["amount"], bad["currency"]), r.code == 400, r)
	}

	section("4. Operations, idempotency and rejections")
	p := func(ext string) string { return run + "-" + ext }
	bet := op(w, p("bet-1"), "BET", "25.00", "")
	r = submit(bases[0], provA, bet)
	check("BET 25.00 -> 200 PROCESSED, balance 975.00", r.code == 200 && r.str("status") == "PROCESSED" && r.str("balance", "amount") == "975.00" && r.str("idempotentReplay") == "false", r)
	betTx := r.str("transactionId")
	r = submit(bases[2], provA, bet)
	check("same BET on another instance -> idempotentReplay true", r.code == 200 && r.str("idempotentReplay") == "true" && r.str("transactionId") == betTx, r)
	changed := op(w, p("bet-1"), "BET", "30.00", "")
	r = submit(bases[1], provA, changed)
	check("same key, different payload -> 409 IDEMPOTENCY_KEY_REUSED", r.code == 409 && r.str("error", "code") == "IDEMPOTENCY_KEY_REUSED", r)
	r = do("POST", bases[1]+"/wagering/transactions", provA, bet, "Idempotency-Key", "other-key-"+run)
	check("same externalTransactionId, other key -> 409 EXTERNAL_TRANSACTION_ID_REUSED", r.code == 409 && r.str("error", "code") == "EXTERNAL_TRANSACTION_ID_REUSED", r)
	r = do("POST", bases[0]+"/wagering/transactions", provA, bet)
	check("missing Idempotency-Key -> 400", r.code == 400, r)
	r = submit(bases[0], provA, op(w, p("open"), "OPENING", "10.00", ""))
	check("kind OPENING from provider -> 400 INTERNAL_KIND_NOT_ALLOWED", r.code == 400 && r.str("error", "code") == "INTERNAL_KIND_NOT_ALLOWED", r)
	r = submit(bases[0], provA, op(w, p("bet-0"), "BET", "0.00", ""))
	check("BET 0.00 -> 400", r.code == 400, r)
	r = submit(bases[0], provA, op(w, p("loss-x"), "LOSS", "1.00", ""))
	check("LOSS with amount != 0.00 -> 400", r.code == 400, r)
	r = submit(bases[0], provA, op(w, p("win-1"), "WIN", "50.00", p("bet-1")))
	check("WIN 50.00 referencing the BET -> 1025.00", r.code == 200 && r.str("balance", "amount") == "1025.00", r)
	r = submit(bases[1], provA, op(w, p("loss-1"), "LOSS", "0.00", ""))
	check("LOSS 0.00 -> PROCESSED, balance unchanged", r.code == 200 && r.str("balance", "amount") == "1025.00", r)
	r = submit(bases[2], provA, op(w, p("bet-2"), "BET", "100.00", ""))
	check("BET 100.00 -> 925.00", r.code == 200 && r.str("balance", "amount") == "925.00", r)
	r = submit(bases[0], provA, op(w, p("refund-2"), "REFUND", "100.00", p("bet-2")))
	check("REFUND of the BET -> 1025.00", r.code == 200 && r.str("balance", "amount") == "1025.00", r)
	r = submit(bases[1], provA, op(w, p("rb-2"), "ROLLBACK", "100.00", p("bet-2")))
	check("ROLLBACK of an already refunded BET -> 422 REFERENCE_ALREADY_REVERSED", r.code == 422 && r.str("failureCode") == "REFERENCE_ALREADY_REVERSED", r)
	r = submit(bases[2], provA, op(w, p("refund-2b"), "REFUND", "100.00", p("bet-2")))
	check("second REFUND of the same BET -> 422 REFERENCE_ALREADY_REVERSED", r.code == 422 && r.str("failureCode") == "REFERENCE_ALREADY_REVERSED", r)
	r = submit(bases[0], provA, op(w, p("partial"), "REFUND", "1.00", p("bet-1")))
	check("partial REFUND -> 422 REVERSAL_AMOUNT_MISMATCH", r.code == 422 && r.str("failureCode") == "REVERSAL_AMOUNT_MISMATCH", r)
	r = submit(bases[0], provA, op(w, p("rb-win"), "ROLLBACK", "50.00", p("win-1")))
	check("ROLLBACK of the WIN -> 975.00", r.code == 200 && r.str("balance", "amount") == "975.00", r)
	r = submit(bases[0], provA, op(w, p("big"), "BET", "5000.00", ""))
	check("BET above balance -> 422 INSUFFICIENT_FUNDS", r.code == 422 && r.str("status") == "REJECTED" && r.str("failureCode") == "INSUFFICIENT_FUNDS", r)
	submit(bases[1], provA, op(w, p("win-3"), "WIN", "10.00", ""))
	submit(bases[1], provA, op(w, p("drain"), "BET", "980.00", ""))
	r = submit(bases[2], provA, op(w, p("rb-win-3"), "ROLLBACK", "10.00", p("win-3")))
	check("ROLLBACK without funds -> 422 REVERSAL_INSUFFICIENT_FUNDS (code differs from BET)", r.code == 422 && r.str("failureCode") == "REVERSAL_INSUFFICIENT_FUNDS", r)
	r = submit(bases[2], provA, bet)
	check("replay of the first BET still returns its original balance 975.00", r.str("idempotentReplay") == "true" && r.str("balance", "amount") == "975.00", r)

	section("5. Queries, ledger and reconciliation")
	r = do("GET", bases[0]+"/wagering/transactions/"+betTx, provA, nil)
	check("GET /wagering/transactions/:id (owner)", r.code == 200 && r.str("status") == "PROCESSED", r)
	r = do("GET", bases[0]+"/providers/provider-a/wagering/transactions/"+p("rb-win-3"), provA, nil)
	check("GET /providers/:p/wagering/transactions/:ext shows failureCode", r.code == 200 && r.str("failureCode") == "REVERSAL_INSUFFICIENT_FUNDS" && r.str("referenceTransactionId") != "", r)
	check("provider-b reading provider-a tx by id -> 404", do("GET", bases[0]+"/wagering/transactions/"+betTx, provB, nil).code == 404)
	check("provider-b reading provider-a path -> 403", do("GET", bases[0]+"/providers/provider-a/wagering/transactions/"+p("bet-1"), provB, nil).code == 403)
	r = do("POST", bases[0]+"/wagering/transactions", provB, bet, "Idempotency-Key", "provider-a:"+p("bet-1"))
	check("provider-b replaying provider-a body/key -> 403, no data leaked", r.code == 403 && !strings.Contains(r.raw, betTx), r)
	var versions []string
	cursor, pages := "", 0
	for pages < 20 {
		path := bases[1] + "/wallets/" + w.id + "/ledger?limit=3"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		lr := do("GET", path, internal, nil)
		pages++
		entries, _ := lr.body["entries"].([]any)
		for _, e := range entries {
			versions = append(versions, fmt.Sprint(e.(map[string]any)["walletVersion"]))
		}
		next, ok := lr.body["nextCursor"].(string)
		if !ok {
			break
		}
		cursor = next
	}
	check(fmt.Sprintf("ledger paginated with opaque cursor in stable order (%d pages, versions %s)", pages, strings.Join(versions, ",")),
		strings.Join(versions, ",") == "1,2,3,4,5,6,7,8")
	check("invalid cursor -> 400", do("GET", bases[0]+"/wallets/"+w.id+"/ledger?cursor=zzz", internal, nil).code == 400)
	r = do("POST", bases[2]+"/wallets/"+w.id+"/reconciliation", internal, nil)
	check("reconciliation consistent, difference 0.00, 8 entries", r.code == 200 && r.str("consistent") == "true" && r.str("difference", "amount") == "0.00" && r.str("checkedEntries") == "8", r)

	section("6. Concurrency across instances")
	for round := 0; round < 5; round++ {
		cw, _ := openWallet(internal, "100.00")
		results := make([]resp, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i] = submit(bases[i%len(bases)], provA, op(cw, p(fmt.Sprintf("race-%d-%d", round, i)), "BET", "80.00", ""))
			}()
		}
		wg.Wait()
		okCount, rej := 0, 0
		for _, x := range results {
			if x.code == 200 {
				okCount++
			}
			if x.code == 422 && x.str("failureCode") == "INSUFFICIENT_FUNDS" {
				rej++
			}
		}
		rec := do("POST", bases[0]+"/wallets/"+cw.id+"/reconciliation", internal, nil)
		check(fmt.Sprintf("round %d: two BETs of 80.00 on 100.00 -> 1 processed, 1 INSUFFICIENT_FUNDS, balance 20.00, 2 entries", round+1),
			okCount == 1 && rej == 1 && rec.str("storedBalance", "amount") == "20.00" && rec.str("checkedEntries") == "2" && rec.str("consistent") == "true", rec)
	}
	dw, _ := openWallet(internal, "100.00")
	dup := op(dw, p("dup"), "BET", "10.00", "")
	var mu sync.Mutex
	replays, oks := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			x := submit(bases[i%len(bases)], provA, dup)
			mu.Lock()
			defer mu.Unlock()
			if x.code == 200 && x.str("balance", "amount") == "90.00" {
				oks++
			}
			if x.str("idempotentReplay") == "true" {
				replays++
			}
		}()
	}
	wg.Wait()
	rec := do("POST", bases[0]+"/wallets/"+dw.id+"/reconciliation", internal, nil)
	check(fmt.Sprintf("same BET 50x in parallel on 3 instances -> %d ok, %d replays, one debit", oks, replays),
		oks == 50 && replays == 49 && rec.str("checkedEntries") == "2" && rec.str("storedBalance", "amount") == "90.00", rec)

	section("7. References not yet available")
	pw, _ := openWallet(internal, "100.00")
	r = submit(bases[0], provA, op(pw, p("early-refund"), "REFUND", "40.00", p("late-bet")))
	check("REFUND before its BET -> 202 PENDING_REFERENCE", r.code == 202 && r.str("status") == "PENDING_REFERENCE", r)
	refundTx := r.str("transactionId")
	submit(bases[1], provA, op(pw, p("late-bet"), "BET", "40.00", ""))
	check("REFUND resolved after the BET arrives (worker on any instance)", eventually(20*time.Second, func() bool {
		x := do("GET", bases[2]+"/wagering/transactions/"+refundTx, provA, nil)
		return x.str("status") == "PROCESSED" && x.str("balance", "amount") == "100.00"
	}))

	section("8. SQS consumer")
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	if err != nil {
		fatal("aws config: %v", err)
	}
	q := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(*endpoint) })
	queueURL := func(name string) string {
		out, err := q.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
		if err != nil {
			fatal("queue %s: %v", name, err)
		}
		return aws.ToString(out.QueueUrl)
	}
	in, dlq, events := queueURL("wager-transactions.fifo"), queueURL("wager-transactions-dlq.fifo"), queueURL("wallet-events.fifo")
	count := func(u string) int {
		out, _ := q.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: aws.String(u),
			AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessages"}})
		n, _ := strconv.Atoi(out.Attributes["ApproximateNumberOfMessages"])
		return n
	}
	send := func(body, group string) {
		_, err := q.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: aws.String(in), MessageBody: aws.String(body),
			MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(uuid.NewString())})
		if err != nil {
			fatal("send: %v", err)
		}
	}
	envelope := func(msgID string, data map[string]any) string {
		b, _ := json.Marshal(map[string]any{"messageId": msgID, "type": "WagerTransactionRequested",
			"occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "data": data})
		return string(b)
	}
	sw, _ := openWallet(internal, "100.00")
	data := op(sw, p("sqs-bet"), "BET", "15.00", "")
	data["idempotencyKey"] = "provider-a:" + p("sqs-bet")
	send(envelope(p("msg-1"), data), sw.id)
	check("BET via SQS processed", eventually(15*time.Second, func() bool {
		return do("GET", bases[0]+"/providers/provider-a/wagering/transactions/"+p("sqs-bet"), provA, nil).str("status") == "PROCESSED"
	}))
	send(envelope(p("msg-1"), data), sw.id)
	send(envelope(p("msg-2"), data), sw.id)
	r = submit(bases[1], provA, op(sw, p("sqs-bet"), "BET", "15.00", ""))
	check("same operation via HTTP -> idempotentReplay true, balance 85.00", r.str("idempotentReplay") == "true" && r.str("balance", "amount") == "85.00", r)
	time.Sleep(3 * time.Second)
	rec = do("POST", bases[0]+"/wallets/"+sw.id+"/reconciliation", internal, nil)
	check("redelivered messageId and new messageId -> still one debit", rec.str("checkedEntries") == "2" && rec.str("storedBalance", "amount") == "85.00", rec)
	before := count(dlq)
	send(`{"this is":"not a valid envelope"}`, "invalid")
	check("invalid message forwarded to the DLQ", eventually(15*time.Second, func() bool { return count(dlq) > before }))
	check("integration events published to wallet-events.fifo", count(events) > 0)

	section("Summary")
	fmt.Printf("  %d passed, %d failed\n", passed, failed)
	if failed > 0 {
		fmt.Println("  rerun with -v to see the responses of failed checks")
		os.Exit(1)
	}
}
