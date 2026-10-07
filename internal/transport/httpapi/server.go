package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"sync/atomic"
	"time"

	"go.uber.org/fx"

	"github.com/gamsanches06/jungle-test/internal/auth"
	"github.com/gamsanches06/jungle-test/internal/config"
	"github.com/gamsanches06/jungle-test/internal/observability"
)

// Check is a named readiness dependency check.
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// Readiness aggregates dependency checks; it reports not ready while the
// server is draining.
type Readiness struct {
	checks   []Check
	draining atomic.Bool
}

// NewReadiness builds the readiness probe.
func NewReadiness(checks []Check) *Readiness { return &Readiness{checks: checks} }

// SetDraining marks the instance as shutting down.
func (r *Readiness) SetDraining() { r.draining.Store(true) }

func (r *Readiness) handle(w http.ResponseWriter, req *http.Request) {
	result := map[string]string{}
	ok := true
	if r.draining.Load() {
		ok = false
		result["instance"] = "draining"
	}
	for _, c := range r.checks {
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		err := c.Fn(ctx)
		cancel()
		if err != nil {
			ok = false
			result[c.Name] = "down"
		} else {
			result[c.Name] = "up"
		}
	}
	status := http.StatusOK
	state := "ready"
	if !ok {
		status, state = http.StatusServiceUnavailable, "not_ready"
	}
	writeJSON(w, status, map[string]any{"status": state, "checks": result})
}

// NewRouter wires routes, authentication and authorization.
func NewRouter(h *Handlers, v auth.Verifier, ready *Readiness, m *observability.Metrics, cfg config.Config, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	authn := func(route string, allow func(auth.Principal) bool, fn http.HandlerFunc) {
		mux.Handle(route, instrument(m, route, authenticate(v, m, log, allow, fn)))
	}
	internal := func(p auth.Principal) bool { return p.IsInternal() }
	provider := func(p auth.Principal) bool { return p.IsProvider() }
	reader := func(p auth.Principal) bool { return p.IsInternal() || p.IsProvider() }

	authn("POST /wallets", internal, h.openWallet)
	authn("GET /wallets/{walletId}", internal, h.getWallet)
	authn("GET /wallets/{walletId}/ledger", internal, h.getLedger)
	authn("POST /wallets/{walletId}/reconciliation", internal, h.reconcile)
	authn("POST /wagering/transactions", provider, h.submitTransaction)
	authn("GET /wagering/transactions/{transactionId}", reader, h.getTransaction)
	authn("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", reader, h.getProviderTransaction)

	mux.Handle("GET /health/live", instrument(m, "GET /health/live", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
	})))
	mux.Handle("GET /health/ready", instrument(m, "GET /health/ready", http.HandlerFunc(ready.handle)))
	mux.Handle("GET /metrics", m.Handler())

	return withCorrelation(withTimeout(cfg.HTTPHandlerTimeout, recoverer(log, mux)))
}

func authenticate(v auth.Verifier, m *observability.Metrics, log *slog.Logger, allow func(auth.Principal) bool, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, ok := auth.BearerToken(r.Header.Get("Authorization"))
		if !ok {
			m.AuthFailures.WithLabelValues("missing_token").Inc()
			writeError(w, r, log, fmt.Errorf("%w: missing bearer token", auth.ErrUnauthenticated))
			return
		}
		p, err := v.Verify(r.Context(), raw)
		if err != nil {
			m.AuthFailures.WithLabelValues("invalid_token").Inc()
			writeError(w, r, log, err)
			return
		}
		if !allow(p) {
			m.AuthFailures.WithLabelValues("forbidden").Inc()
			writeError(w, r, log, fmt.Errorf("%w: client %s lacks the required role", auth.ErrForbidden, p.ClientID))
			return
		}
		ctx := auth.WithPrincipal(r.Context(), p)
		if p.ProviderID != "" {
			ctx = observability.WithFields(ctx, slog.String(observability.FieldProviderID, p.ProviderID))
		}
		next(w, r.WithContext(ctx))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func instrument(m *observability.Metrics, route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		m.HTTPRequests.WithLabelValues(route, fmt.Sprint(rec.status)).Inc()
		m.HTTPDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
	})
}

func withCorrelation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-Id")
		if id == "" || len(id) > 128 {
			id = newCorrelationID()
		}
		w.Header().Set("X-Correlation-Id", id)
		ctx := observability.WithFields(r.Context(), slog.String(observability.FieldCorrelationID, id))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func withTimeout(d time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func recoverer(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				log.ErrorContext(r.Context(), "panic in handler", slog.Any("panic", v), slog.String("stack", string(debug.Stack())))
				writeError(w, r, log, errors.New("internal error"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// Server owns the HTTP listener lifecycle.
type Server struct {
	srv   *http.Server
	ready *Readiness
	log   *slog.Logger
	addr  atomic.Value
}

// NewServer builds the server and binds it to the Fx lifecycle when the API
// role is enabled. OnStart opens the listener synchronously (so a port
// conflict fails startup); OnStop first reports not-ready, then stops
// accepting connections and waits for in-flight requests until the stop
// deadline.
func NewServer(lc fx.Lifecycle, cfg config.Config, handler http.Handler, ready *Readiness, log *slog.Logger) *Server {
	s := &Server{
		srv: &http.Server{
			Addr:              cfg.HTTPAddr,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       cfg.HTTPReadTimeout,
			WriteTimeout:      cfg.HTTPWriteTimeout,
			IdleTimeout:       60 * time.Second,
		},
		ready: ready,
		log:   log,
	}
	if !cfg.Roles.API {
		return s
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			ln, err := net.Listen("tcp", cfg.HTTPAddr)
			if err != nil {
				return fmt.Errorf("listen %s: %w", cfg.HTTPAddr, err)
			}
			s.addr.Store(ln.Addr().String())
			log.Info("http server listening", slog.String("addr", ln.Addr().String()))
			go func() {
				if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error("http server failed", slog.String("error", err.Error()))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			ready.SetDraining()
			log.Info("http server draining")
			err := s.srv.Shutdown(ctx)
			log.Info("http server stopped")
			return err
		},
	})
	return s
}

// Addr returns the bound address (useful with ":0" in tests).
func (s *Server) Addr() string {
	v, _ := s.addr.Load().(string)
	return v
}
