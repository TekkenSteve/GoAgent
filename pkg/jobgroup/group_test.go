package jobgroup_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/pkg/jobgroup"
)

const (
	// _stopBudget is generous for a happy-path stop and far below the test
	// binary's own timeout.
	_stopBudget = 10 * time.Second

	// _tinyBudget expires almost immediately, to force ErrStopTimeout.
	_tinyBudget = 50 * time.Millisecond

	// _releaseCount matches the releases registered by the LIFO test.
	_releaseCount = 3

	// _concurrentStops exercises Stop's idempotence under concurrency.
	_concurrentStops = 4
)

var (
	errFirstJob  = errors.New("first job failed")
	errSecondJob = errors.New("second job failed")
	errRelease   = errors.New("release failed")
)

// blockingJob returns a job that exits cleanly when its context is canceled.
func blockingJob() jobgroup.Job {
	return func(ctx context.Context) error {
		<-ctx.Done()

		return ctx.Err()
	}
}

// recordingObserver captures the group's events in order.
type recordingObserver struct {
	mu     sync.Mutex
	events []string
}

func (o *recordingObserver) JobStarted(name string) {
	o.record("started:" + name)
}

func (o *recordingObserver) JobStopped(name string, _ error) {
	o.record("stopped:" + name)
}

func (o *recordingObserver) JobPanicked(name string, _ any) {
	o.record("panicked:" + name)
}

func (o *recordingObserver) record(event string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.events = append(o.events, event)
}

func (o *recordingObserver) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([]string(nil), o.events...)
}

// assertPanics fails the test unless act panics.
func assertPanics(t *testing.T, act func()) {
	t.Helper()

	defer func() {
		if recover() == nil {
			t.Error("expected a panic")
		}
	}()

	act()
}

// stopBudget returns a fresh context bounding a happy-path Stop.
func stopBudget(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()

	return context.WithTimeout(context.Background(), _stopBudget)
}

func TestStopRunsReleasesInReverseOrder(t *testing.T) {
	t.Parallel()

	order := make(chan string, _releaseCount)

	g := jobgroup.New(context.Background())
	g.Go("worker", blockingJob())
	g.Defer("first", func() error {
		order <- "first"

		return nil
	})
	g.Defer("second", func() error {
		order <- "second"

		return nil
	})
	g.Defer("third", func() error {
		order <- "third"

		return nil
	})

	ctx, cancel := stopBudget(t)
	defer cancel()

	if err := g.Stop(ctx); err != nil {
		t.Fatalf("clean stop returned %v", err)
	}

	for _, want := range []string{"third", "second", "first"} {
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("release order broke: got %q, want %q", got, want)
			}
		default:
			t.Fatalf("release %q never ran", want)
		}
	}
}

func TestStopTimeoutNamesStuckJobAndStillReleases(t *testing.T) {
	t.Parallel()

	unstick := make(chan struct{})
	released := make(chan struct{}, 1)

	g := jobgroup.New(context.Background())
	g.Go("stuck", func(context.Context) error {
		// A genuinely stuck job ignores cancellation entirely: it is wedged
		// on something other than its context.
		<-unstick

		return nil
	})
	g.Defer("cleanup", func() error {
		released <- struct{}{}

		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), _tinyBudget)
	defer cancel()

	err := g.Stop(ctx)

	if !errors.Is(err, jobgroup.ErrStopTimeout) {
		t.Fatalf("expected ErrStopTimeout, got %v", err)
	}

	if !strings.Contains(err.Error(), "stuck") {
		t.Errorf("error should name the stuck job: %v", err)
	}

	select {
	case <-released:
	default:
		t.Error("releases must run despite the exhausted budget")
	}

	close(unstick)

	if err := g.Wait(); err != nil {
		t.Errorf("after unsticking, Wait should be clean: %v", err)
	}
}

func TestWaitJoinsNamedFailures(t *testing.T) {
	t.Parallel()

	g := jobgroup.New(context.Background())
	g.Go("first", func(context.Context) error {
		return errFirstJob
	})
	g.Go("second", func(context.Context) error {
		return errSecondJob
	})

	err := g.Wait()

	if !errors.Is(err, errFirstJob) || !errors.Is(err, errSecondJob) {
		t.Fatalf("Wait should join both failures, got %v", err)
	}

	if !strings.Contains(err.Error(), `"first"`) || !strings.Contains(err.Error(), `"second"`) {
		t.Errorf("joined error should carry both job names: %v", err)
	}
}

func TestParentCancelStopsJobsCleanly(t *testing.T) {
	t.Parallel()

	parent, cancelParent := context.WithCancel(context.Background())

	g := jobgroup.New(parent)
	g.Go("worker", blockingJob())
	g.Go("other", blockingJob())

	cancelParent()

	if err := g.Wait(); err != nil {
		t.Fatalf("canceled exits are clean, got %v", err)
	}
}

func TestStopIsIdempotentUnderConcurrency(t *testing.T) {
	t.Parallel()

	var releases atomic.Int32

	g := jobgroup.New(context.Background())
	g.Go("worker", blockingJob())
	g.Defer("cleanup", func() error {
		releases.Add(1)

		return nil
	})

	results := make(chan error, _concurrentStops)

	for range _concurrentStops {
		go func() {
			ctx, cancel := stopBudget(t)
			defer cancel()

			results <- g.Stop(ctx)
		}()
	}

	for range _concurrentStops {
		if err := <-results; err != nil {
			t.Fatalf("concurrent stop returned %v", err)
		}
	}

	if got := releases.Load(); got != 1 {
		t.Fatalf("release ran %d times, want exactly once", got)
	}
}

func TestGoRejectsProgrammerErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		act  func(*testing.T)
	}{
		{"nil job", func(t *testing.T) {
			t.Helper()

			g := jobgroup.New(context.Background())

			assertPanics(t, func() { g.Go("broken", nil) })
		}},
		{"empty name", func(t *testing.T) {
			t.Helper()

			g := jobgroup.New(context.Background())

			assertPanics(t, func() { g.Go("", blockingJob()) })
		}},
		{"duplicate name", func(t *testing.T) {
			t.Helper()

			g := jobgroup.New(context.Background())
			g.Go("twin", blockingJob())

			assertPanics(t, func() { g.Go("twin", blockingJob()) })
		}},
		{"after Stop", func(t *testing.T) {
			t.Helper()

			g := jobgroup.New(context.Background())

			ctx, cancel := stopBudget(t)
			defer cancel()

			if err := g.Stop(ctx); err != nil {
				t.Fatalf("empty group stop returned %v", err)
			}

			assertPanics(t, func() { g.Go("late", blockingJob()) })
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			test.act(t)
		})
	}
}

func TestDeferRejectsProgrammerErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		act  func(*testing.T)
	}{
		{"nil release", func(t *testing.T) {
			t.Helper()

			g := jobgroup.New(context.Background())

			assertPanics(t, func() { g.Defer("broken", nil) })
		}},
		{"empty name", func(t *testing.T) {
			t.Helper()

			g := jobgroup.New(context.Background())

			assertPanics(t, func() { g.Defer("", func() error { return nil }) })
		}},
		{"after Stop", func(t *testing.T) {
			t.Helper()

			g := jobgroup.New(context.Background())

			ctx, cancel := stopBudget(t)
			defer cancel()

			if err := g.Stop(ctx); err != nil {
				t.Fatalf("empty group stop returned %v", err)
			}

			assertPanics(t, func() { g.Defer("late", func() error { return nil }) })
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			test.act(t)
		})
	}
}

func TestPanicRecoveryReportsAndFails(t *testing.T) {
	t.Parallel()

	obs := &recordingObserver{}

	g := jobgroup.New(context.Background(),
		jobgroup.WithObserver(obs),
		jobgroup.WithPanicRecovery(),
	)
	g.Go("boom", func(context.Context) error {
		panic("kaboom")
	})

	err := g.Wait()

	if !errors.Is(err, jobgroup.ErrJobPanicked) {
		t.Fatalf("expected ErrJobPanicked, got %v", err)
	}

	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("failure should name the panicked job: %v", err)
	}

	events := obs.snapshot()
	panickedAt, stoppedAt := -1, -1

	for i, event := range events {
		if event == "panicked:boom" {
			panickedAt = i
		}

		if event == "stopped:boom" {
			stoppedAt = i
		}
	}

	if panickedAt < 0 || stoppedAt < 0 || panickedAt > stoppedAt {
		t.Errorf("panicked must be reported before stopped, got %v", events)
	}
}

func TestObserverSeesStartThenStop(t *testing.T) {
	t.Parallel()

	obs := &recordingObserver{}

	g := jobgroup.New(context.Background(), jobgroup.WithObserver(obs))
	g.Go("quick", func(context.Context) error {
		return nil
	})

	if err := g.Wait(); err != nil {
		t.Fatalf("quick job should exit clean, got %v", err)
	}

	want := []string{"started:quick", "stopped:quick"}
	got := obs.snapshot()

	if len(got) != len(want) {
		t.Fatalf("events %v, want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events %v, want %v", got, want)
		}
	}
}

func TestReleaseFailureIsReported(t *testing.T) {
	t.Parallel()

	g := jobgroup.New(context.Background())
	g.Go("worker", blockingJob())
	g.Defer("broken-release", func() error {
		return errRelease
	})

	ctx, cancel := stopBudget(t)
	defer cancel()

	err := g.Stop(ctx)

	if !errors.Is(err, errRelease) {
		t.Fatalf("expected the release failure, got %v", err)
	}

	if !strings.Contains(err.Error(), "broken-release") {
		t.Errorf("failure should name the release: %v", err)
	}
}
