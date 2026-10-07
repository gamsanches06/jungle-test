// Command wagering runs the wallet service (HTTP API, SQS consumer, outbox
// relay and pending resolver, selected by APP_ROLES).
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"go.uber.org/fx"

	"github.com/gamsanches06/jungle-test/internal/app"
	"github.com/gamsanches06/jungle-test/internal/config"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(2)
	}
	fx.New(
		app.Options(cfg),
		fx.StartTimeout(cfg.StartTimeout),
		fx.StopTimeout(cfg.ShutdownTimeout),
	).Run()
}

// healthcheck probes the local readiness endpoint (used by the container
// HEALTHCHECK, since the runtime image has no shell or curl).
func healthcheck() int {
	addr := os.Getenv("HEALTHCHECK_URL")
	if addr == "" {
		addr = "http://127.0.0.1:8080/health/ready"
	}
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "status", resp.StatusCode)
		return 1
	}
	return 0
}
