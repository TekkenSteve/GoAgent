package agentosplan

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	defaultPlanMetricsExporterInterval = time.Minute
	defaultPlanMetricsExporterLimit    = 100
)

// PlanMetricsExporterRunner projects durable RunPlan events into a metric sink.
type PlanMetricsExporterRunner interface {
	Export(ctx context.Context, scope PlanRefScope) (PlanMetricsExportResult, error)
}

// PlanMetricsExporterLoopConfig controls periodic metric projection.
type PlanMetricsExporterLoopConfig struct {
	Interval          time.Duration
	Scope             PlanRefScope
	ExportImmediately bool
}

// PlanMetricsExporterObserver receives metric export pass outcomes.
type PlanMetricsExporterObserver interface {
	PlanMetricsExportSucceeded(PlanMetricsExportResult)
	PlanMetricsExportFailed(error)
}

// PlanMetricsExporterLoop owns periodic durable RunPlan metric projection.
type PlanMetricsExporterLoop struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// StartPlanMetricsExporterLoop starts a background metric exporter loop.
func StartPlanMetricsExporterLoop(parent context.Context, exporter PlanMetricsExporterRunner, cfg PlanMetricsExporterLoopConfig, observer PlanMetricsExporterObserver) (*PlanMetricsExporterLoop, error) {
	if exporter == nil {
		return nil, errors.New("agentos plan metrics exporter: exporter is required")
	}
	if cfg.Interval < 0 {
		return nil, errors.New("agentos plan metrics exporter: interval must be non-negative")
	}
	if cfg.Scope.Limit < 0 {
		return nil, errors.New("agentos plan metrics exporter: plan ref limit must be non-negative")
	}
	normalized := normalizePlanMetricsExporterLoopConfig(cfg)
	ctx, cancel := context.WithCancel(parent)
	loop := &PlanMetricsExporterLoop{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go loop.run(ctx, exporter, normalized, observer)

	return loop, nil
}

// Stop cancels the loop and waits for it to exit.
func (l *PlanMetricsExporterLoop) Stop() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.cancel()
		<-l.done
	})
}

func (l *PlanMetricsExporterLoop) run(ctx context.Context, exporter PlanMetricsExporterRunner, cfg PlanMetricsExporterLoopConfig, observer PlanMetricsExporterObserver) {
	defer close(l.done)
	if cfg.ExportImmediately {
		runPlanMetricsExportPass(ctx, exporter, cfg.Scope, observer)
	}

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runPlanMetricsExportPass(ctx, exporter, cfg.Scope, observer)
		}
	}
}

func runPlanMetricsExportPass(ctx context.Context, exporter PlanMetricsExporterRunner, scope PlanRefScope, observer PlanMetricsExporterObserver) {
	result, err := exporter.Export(ctx, scope)
	if err != nil {
		if observer != nil {
			observer.PlanMetricsExportFailed(err)
		}

		return
	}
	if observer != nil {
		observer.PlanMetricsExportSucceeded(result)
	}
}

func normalizePlanMetricsExporterLoopConfig(cfg PlanMetricsExporterLoopConfig) PlanMetricsExporterLoopConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultPlanMetricsExporterInterval
	}
	if cfg.Scope.Limit <= 0 {
		cfg.Scope.Limit = defaultPlanMetricsExporterLimit
	}

	return cfg
}
