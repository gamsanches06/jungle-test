// Package failpoint provides named fault-injection points used by tests to
// simulate crashes and failures between processing steps.
//
// Failpoints are configured with the FAILPOINTS environment variable
// ("name=action;name2=action") or programmatically with Enable. Actions:
//
//	exit        terminate the process immediately with code 137 (simulates SIGKILL)
//	error       return ErrInjected (treated as a transient failure)
//	once-exit   like exit (a process can only die once)
//	once-error  return ErrInjected on the first hit only
//
// With no configuration every Inject call is a cheap no-op.
package failpoint

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

// Names of the failpoints wired in the code base.
const (
	ProcessBeforeCommit        = "process.before_commit"
	ProcessAfterCommit         = "process.after_commit"
	ConsumerAfterCommit        = "consumer.after_commit_before_delete"
	OutboxAfterClaim           = "outbox.after_claim_before_publish"
	OutboxAfterPublish         = "outbox.after_publish_before_ack"
	PendingBeforeResolve       = "pending.before_resolve"
	PublisherSend              = "publisher.send"
	ConsumerProcessTransiently = "consumer.process"
)

// ErrInjected is returned by the "error" actions.
var ErrInjected = errors.New("failpoint: injected failure")

type point struct {
	action string
	hits   atomic.Int64
}

var (
	mu     sync.RWMutex
	points = map[string]*point{}
	active atomic.Bool
	// exitFn is replaceable in tests of this package.
	exitFn = func(code int) { os.Exit(code) }
)

func init() {
	if err := Parse(os.Getenv("FAILPOINTS")); err != nil {
		fmt.Fprintln(os.Stderr, "invalid FAILPOINTS:", err)
	}
}

// Parse loads a "name=action;..." specification.
func Parse(spec string) error {
	if strings.TrimSpace(spec) == "" {
		return nil
	}
	for _, item := range strings.Split(spec, ";") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, action, ok := strings.Cut(item, "=")
		if !ok {
			return fmt.Errorf("failpoint %q: missing action", item)
		}
		if err := Enable(name, action); err != nil {
			return err
		}
	}
	return nil
}

// Enable activates a failpoint.
func Enable(name, action string) error {
	switch action {
	case "exit", "once-exit", "error", "once-error":
	default:
		return fmt.Errorf("failpoint %q: unknown action %q", name, action)
	}
	mu.Lock()
	defer mu.Unlock()
	points[name] = &point{action: action}
	active.Store(true)
	return nil
}

// Disable deactivates a failpoint.
func Disable(name string) {
	mu.Lock()
	defer mu.Unlock()
	delete(points, name)
	active.Store(len(points) > 0)
}

// Reset removes every failpoint.
func Reset() {
	mu.Lock()
	defer mu.Unlock()
	points = map[string]*point{}
	active.Store(false)
}

// Hits returns how many times a failpoint fired.
func Hits(name string) int64 {
	mu.RLock()
	defer mu.RUnlock()
	if p, ok := points[name]; ok {
		return p.hits.Load()
	}
	return 0
}

// Inject evaluates the failpoint. It returns ErrInjected for error actions,
// terminates the process for exit actions and returns nil otherwise.
func Inject(name string) error {
	if !active.Load() {
		return nil
	}
	mu.RLock()
	p, ok := points[name]
	mu.RUnlock()
	if !ok {
		return nil
	}
	n := p.hits.Add(1)
	switch p.action {
	case "exit", "once-exit":
		fmt.Fprintf(os.Stderr, "failpoint %s: exiting process\n", name)
		exitFn(137)
	case "error":
		return fmt.Errorf("%w at %s", ErrInjected, name)
	case "once-error":
		if n == 1 {
			return fmt.Errorf("%w at %s", ErrInjected, name)
		}
	}
	return nil
}
