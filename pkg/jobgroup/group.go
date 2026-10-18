// Package jobgroup runs a process's long-lived worker loops as one named,
// stoppable unit. A Group starts its jobs immediately, cancels them
// together, waits them out under a caller-supplied budget, and then runs the
// registered releases in reverse registration order.
//
// The group deliberately does not schedule (durable timing is Temporal's
// job), restart (a crashing loop should take the process down and let the
// orchestrator act on it), or order starts (construction order is the start
// order). A job must not call Stop or Wait on its own group: both wait for
// the job itself to exit.
package jobgroup

import (
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
)

// ErrStopTimeout reports that Stop's context budget expired while jobs were
// still running. The jobs still running are named in the wrapped error; the
// releases ran regardless, because the process is going down either way and
// a resource released late is still released.
var ErrStopTimeout = errors.New("jobgroup: stop budget exhausted while jobs were still running")

// ErrJobPanicked wraps the recovered value of a job that panicked under
// WithPanicRecovery. Without that option a panicking job is not recovered
// and takes the process down, as any unrecovered panic does.
var ErrJobPanicked = errors.New("jobgroup: job panicked")

// _pprofLabel is the pprof label key carrying a job's name, so a profile
// taken while the group runs attributes its samples to the job.
const _pprofLabel = "jobgroup.job"

// Job is one long-lived worker loop owned by a Group. It returns when its
// context is canceled or its work is over; returning the context's
// cancellation error is a clean exit, and anything else is a failure.
type Job func(ctx context.Context) error

// Release is a cleanup step registered with Defer and run at the end of
// Stop, after every job has been waited out or timed out.
type Release func() error

// finalizer pairs one release with the name its failure is reported under.
type finalizer struct {
	name    string
	release Release
}

// job is one started Job with the channel its exit is observed through. The
// error is written before done closes and read only after, so the channel,
// not a lock, carries the happens-before.
type job struct {
	name string
	done chan struct{}
	err  error
}

// Group owns a set of named jobs and the releases backing them. Go and Defer
// may be called from any goroutine until Stop; Stop and Wait are safe to
// call concurrently and repeatedly. The group holds no strategy of its own:
// the stop budget is the context handed to Stop.
type Group struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex
	jobs       []*job
	names      map[string]struct{}
	finalizers []finalizer
	stopped    bool

	stopOnce sync.Once
	stopErr  error

	observer      Observer
	recoverPanics bool
}

// New derives the group's job context from parent — canceling the parent
// stops the jobs exactly as Stop does, minus the releases — and applies the
// given options. The parent must not be nil.
func New(parent context.Context, opts ...Option) *Group {
	cfg := config{observer: silentObserver{}}

	for _, opt := range opts {
		opt(&cfg)
	}

	ctx, cancel := context.WithCancel(parent)

	return &Group{
		ctx:           ctx,
		names:         make(map[string]struct{}),
		cancel:        cancel,
		observer:      cfg.observer,
		recoverPanics: cfg.recoverPanics,
	}
}

// Go starts the job immediately under the group's context. An empty name, a
// nil job, a duplicate name, or a call after Stop is a programming error and
// panics: the group's registry is an invariant, not a runtime input.
func (g *Group) Go(name string, fn Job) {
	if fn == nil {
		panic("jobgroup: Go called with a nil job")
	}

	if name == "" {
		panic("jobgroup: Go called with an empty job name")
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.stopped {
		panic("jobgroup: Go called after Stop: " + name)
	}

	if _, duplicated := g.names[name]; duplicated {
		panic("jobgroup: Go called twice with the job name: " + name)
	}

	g.names[name] = struct{}{}

	j := &job{name: name, done: make(chan struct{})}
	g.jobs = append(g.jobs, j)

	go g.run(j, fn)
}

// Defer registers a release to run at the end of Stop, in reverse
// registration order, after every job has been waited out or timed out. A
// release must be prompt and must succeed on a best-effort basis even when
// the stop budget is already spent. An empty name, a nil release, or a call
// after Stop is a programming error and panics.
func (g *Group) Defer(name string, release Release) {
	if release == nil {
		panic("jobgroup: Defer called with a nil release")
	}

	if name == "" {
		panic("jobgroup: Defer called with an empty release name")
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.stopped {
		panic("jobgroup: Defer called after Stop: " + name)
	}

	g.finalizers = append(g.finalizers, finalizer{name: name, release: release})
}

// Wait blocks until every job has exited and returns their failures joined
// under their names. Cancellation exits are not failures. Wait does not
// cancel anything and does not run releases; only Stop does.
func (g *Group) Wait() error {
	g.mu.Lock()
	jobs := slices.Clone(g.jobs)
	g.mu.Unlock()

	for _, j := range jobs {
		<-j.done
	}

	return joinJobErrors(jobs)
}

// Stop cancels the group's context, waits for the jobs within ctx's budget,
// then runs the releases in reverse registration order, and returns the
// joined failures: jobs that exceeded the budget under ErrStopTimeout, job
// and release failures under their names. Stop is idempotent; every call
// returns the first call's result.
func (g *Group) Stop(ctx context.Context) error {
	g.stopOnce.Do(func() {
		g.mu.Lock()
		g.stopped = true
		jobs := slices.Clone(g.jobs)
		g.mu.Unlock()

		g.cancel()

		g.stopErr = g.stopJobs(ctx, jobs)
	})

	return g.stopErr
}

// run is the goroutine behind one job: it reports the start, runs the job
// under a pprof label naming it, reports the outcome, and closes the job's
// done channel so waiters can observe the exit.
func (g *Group) run(j *job, fn Job) {
	g.observer.JobStarted(j.name)

	err := g.execute(j, fn)

	j.err = err
	close(j.done)

	g.observer.JobStopped(j.name, err)
}

// execute runs the job's function under a pprof label naming it and, when
// recovery is enabled, converts a panic into a failure reported through
// ErrJobPanicked.
func (g *Group) execute(j *job, fn Job) (err error) {
	if g.recoverPanics {
		defer func() {
			if v := recover(); v != nil {
				g.observer.JobPanicked(j.name, v)

				err = fmt.Errorf("jobgroup - job %q: %w: %v", j.name, ErrJobPanicked, v)
			}
		}()
	}

	pprof.Do(g.ctx, pprof.Labels(_pprofLabel, j.name), func(ctx context.Context) {
		err = fn(ctx)
	})

	return err
}

// stopJobs waits the given jobs out within ctx's budget and then runs the
// releases. A job still running when the budget ends is named in an
// ErrStopTimeout wrap; the releases run regardless.
func (g *Group) stopJobs(ctx context.Context, jobs []*job) error {
	var stuck []string

	for _, j := range jobs {
		select {
		case <-j.done:
		case <-ctx.Done():
			stuck = append(stuck, j.name)
		}
	}

	var errs []error

	if len(stuck) > 0 {
		errs = append(errs, fmt.Errorf("jobgroup - Stop: %w: %s", ErrStopTimeout, strings.Join(stuck, ", ")))
	}

	errs = append(errs, joinJobErrors(jobs))
	errs = append(errs, g.runFinalizers()...)

	return errors.Join(errs...)
}

// runFinalizers runs the registered releases in reverse registration order
// and returns their failures wrapped with their names.
func (g *Group) runFinalizers() []error {
	g.mu.Lock()
	finalizers := slices.Clone(g.finalizers)
	g.mu.Unlock()

	var errs []error

	for i := len(finalizers) - 1; i >= 0; i-- {
		f := finalizers[i]

		if err := f.release(); err != nil {
			errs = append(errs, fmt.Errorf("jobgroup - release %q: %w", f.name, err))
		}
	}

	return errs
}

// joinJobErrors aggregates the given jobs' failures wrapped with their
// names. A cancellation exit is a clean stop and is left out.
func joinJobErrors(jobs []*job) error {
	var errs []error

	for _, j := range jobs {
		if j.err != nil && !errors.Is(j.err, context.Canceled) {
			errs = append(errs, fmt.Errorf("jobgroup - job %q: %w", j.name, j.err))
		}
	}

	return errors.Join(errs...)
}
