package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gamsanches06/jungle-test/internal/application"
)

// Metrics holds every Prometheus collector of the service. A dedicated
// registry is used so several application instances can live in one test
// process.
type Metrics struct {
	Registry *prometheus.Registry

	transactions     *prometheus.CounterVec
	duplicates       *prometheus.CounterVec
	retries          *prometheus.CounterVec
	conflicts        *prometheus.CounterVec
	processing       *prometheus.HistogramVec
	reconDivergences prometheus.Counter
	reconRuns        prometheus.Counter

	InboxDuplicates  prometheus.Counter
	DLQ              *prometheus.CounterVec
	SQSMessages      *prometheus.CounterVec
	OutboxPublished  prometheus.Counter
	OutboxFailures   prometheus.Counter
	OutboxLagSeconds prometheus.Gauge
	OutboxPending    prometheus.Gauge
	OutboxPublishLag prometheus.Histogram
	HTTPRequests     *prometheus.CounterVec
	HTTPDuration     *prometheus.HistogramVec
	AuthFailures     *prometheus.CounterVec
}

var _ application.Metrics = (*Metrics)(nil)

// NewMetrics registers the collectors.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { reg.MustRegister(c) }
	m := &Metrics{Registry: reg}

	m.transactions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wagering_transactions_total", Help: "Handled wager transactions by source, kind and resulting status.",
	}, []string{"source", "kind", "status", "replay"})
	m.duplicates = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wagering_idempotent_replays_total", Help: "Operations recognized as duplicates and answered from the persisted result.",
	}, []string{"source", "kind"})
	m.retries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wagering_retries_total", Help: "Retries by component (sql, process, sqs, outbox, pending-reference).",
	}, []string{"component"})
	m.conflicts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wagering_concurrency_conflicts_total", Help: "Serialization failures, deadlocks, version guard misses and uniqueness races.",
	}, []string{"component"})
	m.processing = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "wagering_processing_duration_seconds", Help: "Latency of the processing use case.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"source"})
	m.reconDivergences = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wagering_reconciliation_divergences_total", Help: "Reconciliations where stored balance differs from the ledger.",
	})
	m.reconRuns = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wagering_reconciliation_runs_total", Help: "Reconciliation executions.",
	})
	m.InboxDuplicates = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wagering_inbox_duplicates_total", Help: "SQS messages redelivered after being durably handled (deduplicated by the inbox).",
	})
	m.DLQ = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wagering_dlq_messages_total", Help: "Messages sent to the DLQ (explicitly or expected by redrive) by reason.",
	}, []string{"reason"})
	m.SQSMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wagering_sqs_messages_total", Help: "Consumed SQS messages by outcome.",
	}, []string{"outcome"})
	m.OutboxPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wagering_outbox_published_total", Help: "Outbox events published.",
	})
	m.OutboxFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wagering_outbox_publish_failures_total", Help: "Failed outbox publication attempts.",
	})
	m.OutboxLagSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wagering_outbox_lag_seconds", Help: "Age of the oldest unpublished outbox event.",
	})
	m.OutboxPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wagering_outbox_pending_events", Help: "Unpublished outbox events.",
	})
	m.OutboxPublishLag = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "wagering_outbox_publish_delay_seconds", Help: "Delay between event occurrence and publication.",
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	})
	m.HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wagering_http_requests_total", Help: "HTTP requests by route and status code.",
	}, []string{"route", "code"})
	m.HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "wagering_http_request_duration_seconds", Help: "HTTP latency by route.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route"})
	m.AuthFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wagering_auth_failures_total", Help: "Rejected requests by reason (unauthenticated, forbidden).",
	}, []string{"reason"})

	for _, c := range []prometheus.Collector{m.transactions, m.duplicates, m.retries, m.conflicts, m.processing,
		m.reconDivergences, m.reconRuns, m.InboxDuplicates, m.DLQ, m.SQSMessages, m.OutboxPublished, m.OutboxFailures,
		m.OutboxLagSeconds, m.OutboxPending, m.OutboxPublishLag, m.HTTPRequests, m.HTTPDuration, m.AuthFailures} {
		f(c)
	}
	return m
}

// Handler exposes /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

func (m *Metrics) TransactionOutcome(source, kind, status string, replay bool) {
	m.transactions.WithLabelValues(source, kind, status, strconv.FormatBool(replay)).Inc()
}
func (m *Metrics) Duplicate(source, kind string) { m.duplicates.WithLabelValues(source, kind).Inc() }
func (m *Metrics) Retry(component string)        { m.retries.WithLabelValues(component).Inc() }
func (m *Metrics) ConcurrencyConflict(component string) {
	m.conflicts.WithLabelValues(component).Inc()
}
func (m *Metrics) ProcessingDuration(source string, d time.Duration) {
	m.processing.WithLabelValues(source).Observe(d.Seconds())
}

// ReconciliationDivergence counts divergences; the wallet id goes to the log
// (not a label) to keep metric cardinality bounded.
func (m *Metrics) ReconciliationDivergence(string) { m.reconDivergences.Inc() }

// ReconciliationRun counts executions.
func (m *Metrics) ReconciliationRun() { m.reconRuns.Inc() }
