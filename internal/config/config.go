// Package config loads and validates the service configuration from
// environment variables.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Roles selects which components run in this process. Every role can run in
// any number of instances.
type Roles struct {
	API      bool
	Consumer bool
	Outbox   bool
	Pending  bool
}

// Config is the full service configuration.
type Config struct {
	InstanceID string
	Roles      Roles
	LogLevel   string

	HTTPAddr           string
	HTTPReadTimeout    time.Duration
	HTTPWriteTimeout   time.Duration
	HTTPHandlerTimeout time.Duration

	DatabaseURL        string
	DBMaxConns         int32
	DBStatementTimeout time.Duration
	DBLockTimeout      time.Duration

	OIDCIssuer   string
	OIDCJWKSURL  string
	OIDCAudience string

	AWSRegion   string
	AWSEndpoint string
	// Credentials per broker role; empty values fall back to the default
	// AWS credential chain (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).
	ConsumerAccessKey  string
	ConsumerSecretKey  string
	PublisherAccessKey string
	PublisherSecretKey string

	InputQueueName    string
	DLQName           string
	EventsQueueName   string
	ConsumerName      string
	SQSPollers        int
	SQSMaxMessages    int32
	SQSWaitTime       time.Duration
	SQSVisibility     time.Duration
	SQSProcessTimeout time.Duration
	SQSRetryBase      time.Duration
	SQSRetryMax       time.Duration
	SQSMaxReceives    int

	OutboxPollInterval time.Duration
	OutboxBatchSize    int
	OutboxLease        time.Duration
	OutboxRetryBase    time.Duration
	OutboxRetryMax     time.Duration

	PendingPollInterval  time.Duration
	PendingBatchSize     int
	ReferenceBaseDelay   time.Duration
	ReferenceMaxDelay    time.Duration
	ReferenceMaxAttempts int
	ReferenceTTL         time.Duration

	StartTimeout    time.Duration
	ShutdownTimeout time.Duration
}

// Load reads the configuration from the environment and validates it.
func Load() (Config, error) {
	return FromLookup(os.LookupEnv)
}

// FromLookup builds a configuration from a lookup function (tests).
func FromLookup(lookup func(string) (string, bool)) (Config, error) {
	r := reader{lookup: lookup}
	host, _ := os.Hostname()
	c := Config{
		InstanceID: r.str("INSTANCE_ID", fmt.Sprintf("%s-%d", host, os.Getpid())),
		LogLevel:   r.str("LOG_LEVEL", "info"),

		HTTPAddr:           r.str("HTTP_ADDR", ":8080"),
		HTTPReadTimeout:    r.dur("HTTP_READ_TIMEOUT", 10*time.Second),
		HTTPWriteTimeout:   r.dur("HTTP_WRITE_TIMEOUT", 15*time.Second),
		HTTPHandlerTimeout: r.dur("HTTP_HANDLER_TIMEOUT", 10*time.Second),

		DatabaseURL:        r.str("DATABASE_URL", ""),
		DBMaxConns:         int32(r.int("DB_MAX_CONNS", 20)),
		DBStatementTimeout: r.dur("DB_STATEMENT_TIMEOUT", 5*time.Second),
		DBLockTimeout:      r.dur("DB_LOCK_TIMEOUT", 3*time.Second),

		OIDCIssuer:   r.str("OIDC_ISSUER", ""),
		OIDCJWKSURL:  r.str("OIDC_JWKS_URL", ""),
		OIDCAudience: r.str("OIDC_AUDIENCE", "jungle-api"),

		AWSRegion:          r.str("AWS_REGION", "us-east-1"),
		AWSEndpoint:        r.str("AWS_ENDPOINT_URL", ""),
		ConsumerAccessKey:  r.str("SQS_CONSUMER_ACCESS_KEY_ID", ""),
		ConsumerSecretKey:  r.str("SQS_CONSUMER_SECRET_ACCESS_KEY", ""),
		PublisherAccessKey: r.str("SQS_PUBLISHER_ACCESS_KEY_ID", ""),
		PublisherSecretKey: r.str("SQS_PUBLISHER_SECRET_ACCESS_KEY", ""),

		InputQueueName:    r.str("SQS_INPUT_QUEUE", "wager-transactions.fifo"),
		DLQName:           r.str("SQS_DLQ_QUEUE", "wager-transactions-dlq.fifo"),
		EventsQueueName:   r.str("SQS_EVENTS_QUEUE", "wallet-events.fifo"),
		ConsumerName:      r.str("SQS_CONSUMER_NAME", "wager-transactions-consumer"),
		SQSPollers:        r.int("SQS_POLLERS", 2),
		SQSMaxMessages:    int32(r.int("SQS_MAX_MESSAGES", 10)),
		SQSWaitTime:       r.dur("SQS_WAIT_TIME", 10*time.Second),
		SQSVisibility:     r.dur("SQS_VISIBILITY_TIMEOUT", 30*time.Second),
		SQSProcessTimeout: r.dur("SQS_PROCESS_TIMEOUT", 20*time.Second),
		SQSRetryBase:      r.dur("SQS_RETRY_BASE", 2*time.Second),
		SQSRetryMax:       r.dur("SQS_RETRY_MAX", 5*time.Minute),
		SQSMaxReceives:    r.int("SQS_MAX_RECEIVES", 5),

		OutboxPollInterval: r.dur("OUTBOX_POLL_INTERVAL", 250*time.Millisecond),
		OutboxBatchSize:    r.int("OUTBOX_BATCH_SIZE", 50),
		OutboxLease:        r.dur("OUTBOX_LEASE", 30*time.Second),
		OutboxRetryBase:    r.dur("OUTBOX_RETRY_BASE", time.Second),
		OutboxRetryMax:     r.dur("OUTBOX_RETRY_MAX", time.Minute),

		PendingPollInterval:  r.dur("PENDING_POLL_INTERVAL", 500*time.Millisecond),
		PendingBatchSize:     r.int("PENDING_BATCH_SIZE", 50),
		ReferenceBaseDelay:   r.dur("REFERENCE_BASE_DELAY", time.Second),
		ReferenceMaxDelay:    r.dur("REFERENCE_MAX_DELAY", 30*time.Second),
		ReferenceMaxAttempts: r.int("REFERENCE_MAX_ATTEMPTS", 10),
		ReferenceTTL:         r.dur("REFERENCE_TTL", 10*time.Minute),

		StartTimeout:    r.dur("START_TIMEOUT", 60*time.Second),
		ShutdownTimeout: r.dur("SHUTDOWN_TIMEOUT", 25*time.Second),
	}
	roles, err := parseRoles(r.str("APP_ROLES", "api,consumer,outbox,pending"))
	if err != nil {
		r.errs = append(r.errs, err)
	}
	c.Roles = roles
	if len(r.errs) > 0 {
		return Config{}, errors.Join(r.errs...)
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func parseRoles(s string) (Roles, error) {
	var r Roles
	for _, part := range strings.Split(s, ",") {
		switch strings.TrimSpace(part) {
		case "api":
			r.API = true
		case "consumer":
			r.Consumer = true
		case "outbox":
			r.Outbox = true
		case "pending":
			r.Pending = true
		case "":
		default:
			return r, fmt.Errorf("APP_ROLES: unknown role %q", part)
		}
	}
	if !r.API && !r.Consumer && !r.Outbox && !r.Pending {
		return r, errors.New("APP_ROLES: at least one role is required")
	}
	return r, nil
}

// Validate checks required values and coherent timeouts.
func (c Config) Validate() error {
	var errs []error
	req := func(name, v string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	req("DATABASE_URL", c.DatabaseURL)
	if c.Roles.API {
		req("OIDC_ISSUER", c.OIDCIssuer)
		req("OIDC_JWKS_URL", c.OIDCJWKSURL)
		req("OIDC_AUDIENCE", c.OIDCAudience)
		for name, v := range map[string]string{"OIDC_ISSUER": c.OIDCIssuer, "OIDC_JWKS_URL": c.OIDCJWKSURL} {
			if v != "" {
				if u, err := url.Parse(v); err != nil || u.Scheme == "" || u.Host == "" {
					errs = append(errs, fmt.Errorf("%s must be an absolute URL", name))
				}
			}
		}
	}
	if c.SQSMaxMessages < 1 || c.SQSMaxMessages > 10 {
		errs = append(errs, errors.New("SQS_MAX_MESSAGES must be between 1 and 10"))
	}
	if c.SQSWaitTime > 20*time.Second {
		errs = append(errs, errors.New("SQS_WAIT_TIME must be at most 20s"))
	}
	if c.SQSProcessTimeout >= c.SQSVisibility {
		errs = append(errs, errors.New("SQS_PROCESS_TIMEOUT must be lower than SQS_VISIBILITY_TIMEOUT"))
	}
	if c.SQSPollers < 1 {
		errs = append(errs, errors.New("SQS_POLLERS must be >= 1"))
	}
	if c.OutboxBatchSize < 1 || c.PendingBatchSize < 1 {
		errs = append(errs, errors.New("batch sizes must be >= 1"))
	}
	if c.ReferenceMaxAttempts < 1 || c.ReferenceTTL <= 0 || c.ReferenceBaseDelay <= 0 || c.ReferenceMaxDelay < c.ReferenceBaseDelay {
		errs = append(errs, errors.New("invalid reference retry policy"))
	}
	if c.ShutdownTimeout <= 0 || c.StartTimeout <= 0 {
		errs = append(errs, errors.New("START_TIMEOUT and SHUTDOWN_TIMEOUT must be positive"))
	}
	return errors.Join(errs...)
}

type reader struct {
	lookup func(string) (string, bool)
	errs   []error
}

func (r *reader) str(key, def string) string {
	if v, ok := r.lookup(key); ok && v != "" {
		return v
	}
	return def
}

func (r *reader) int(key string, def int) int {
	v, ok := r.lookup(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}

func (r *reader) dur(key string, def time.Duration) time.Duration {
	v, ok := r.lookup(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return d
}
