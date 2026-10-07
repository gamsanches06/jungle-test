package worker

import (
	"context"
	"fmt"
	"time"
)

var closedChan = func() chan struct{} { c := make(chan struct{}); close(c); return c }()

// Loop runs a function in a goroutine with cancellation and observable
// termination.
type Loop struct {
	name   string
	cancel context.CancelFunc
	done   chan struct{}
}

// StartLoop starts fn with a cancellable context.
func StartLoop(name string, fn func(ctx context.Context)) *Loop {
	ctx, cancel := context.WithCancel(context.Background())
	l := &Loop{name: name, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(l.done)
		fn(ctx)
	}()
	return l
}

// Stop cancels the loop and waits for it until ctx expires.
func (l *Loop) Stop(ctx context.Context) error {
	l.cancel()
	select {
	case <-l.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s did not stop in time: %w", l.name, ctx.Err())
	}
}

// Done is closed when fn returned.
func (l *Loop) Done() <-chan struct{} { return l.done }

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
