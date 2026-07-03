package temporal

import (
	"context"
	"errors"
	"sync"
	"time"
)

var errPlanCommandRecoveryRecovererRequired = errors.New("agentos temporal plan command recovery: recoverer is required")

const (
	defaultPlanCommandRecoveryInterval = time.Minute
	defaultPlanCommandRecoveryLimit    = 100
)

// PlanCommandRecoverer redelivers durable RunPlan command outbox records.
type PlanCommandRecoverer interface {
	RecoverPlanCommands(ctx context.Context, limit int) (PlanCommandRecoveryResult, error)
}

// PlanCommandRecoveryLoopConfig controls periodic durable command recovery.
type PlanCommandRecoveryLoopConfig struct {
	Interval           time.Duration
	Limit              int
	RecoverImmediately bool
}

// PlanCommandRecoveryObserver receives recovery pass outcomes.
type PlanCommandRecoveryObserver interface {
	PlanCommandRecoverySucceeded(PlanCommandRecoveryResult)
	PlanCommandRecoveryFailed(error)
}

// PlanCommandRecoveryLoop owns periodic recovery of pending/failed RunPlan
// command outbox records.
type PlanCommandRecoveryLoop struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// StartPlanCommandRecovery starts a background recovery loop.
func StartPlanCommandRecovery(parent context.Context, recoverer PlanCommandRecoverer, cfg PlanCommandRecoveryLoopConfig, observer PlanCommandRecoveryObserver) (*PlanCommandRecoveryLoop, error) {
	if recoverer == nil {
		return nil, errPlanCommandRecoveryRecovererRequired
	}

	normalized := normalizePlanCommandRecoveryLoopConfig(cfg)
	loop := &PlanCommandRecoveryLoop{
		done: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(parent)
	loop.cancel = cancel

	go loop.run(ctx, cancel, recoverer, normalized, observer)

	return loop, nil
}

// Stop cancels the loop and waits for it to exit.
func (l *PlanCommandRecoveryLoop) Stop() {
	if l == nil {
		return
	}

	l.once.Do(func() {
		l.cancel()
		<-l.done
	})
}

func (l *PlanCommandRecoveryLoop) run(ctx context.Context, cancel context.CancelFunc, recoverer PlanCommandRecoverer, cfg PlanCommandRecoveryLoopConfig, observer PlanCommandRecoveryObserver) {
	defer close(l.done)
	defer cancel()

	if cfg.RecoverImmediately {
		runPlanCommandRecoveryPass(ctx, recoverer, cfg.Limit, observer)
	}

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runPlanCommandRecoveryPass(ctx, recoverer, cfg.Limit, observer)
		}
	}
}

func runPlanCommandRecoveryPass(ctx context.Context, recoverer PlanCommandRecoverer, limit int, observer PlanCommandRecoveryObserver) {
	result, err := recoverer.RecoverPlanCommands(ctx, limit)
	if err != nil {
		if observer != nil {
			observer.PlanCommandRecoveryFailed(err)
		}

		return
	}

	if observer != nil {
		observer.PlanCommandRecoverySucceeded(result)
	}
}

func normalizePlanCommandRecoveryLoopConfig(cfg PlanCommandRecoveryLoopConfig) PlanCommandRecoveryLoopConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultPlanCommandRecoveryInterval
	}

	if cfg.Limit <= 0 {
		cfg.Limit = defaultPlanCommandRecoveryLimit
	}

	return cfg
}
