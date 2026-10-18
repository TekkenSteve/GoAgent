package app

import (
	"context"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/pkg/jobgroup"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

// eventLoopStopTimeout bounds how long Stop waits for the event-log loops —
// drainers, projections, ingress routers — to notice cancellation. Each
// loop's pass is a claim or a poll plus a bounded batch, so exceeding this
// means the log is not answering, and blocking process shutdown on it would
// be worse than letting the goroutine die with the process.
const eventLoopStopTimeout = 15 * time.Second

// jobObserver adapts the app logger onto a job group's reporting channel, so
// every loop a component owns stays visible under its name while it runs and
// when it stops. The component prefix keeps the backbone's jobs readable
// apart from the ingress bridge's.
type jobObserver struct {
	component string
	logger    logger.Interface
}

func (o jobObserver) JobStarted(name string) {
	o.logger.Info("app - %s - job started: %s", o.component, name)
}

func (o jobObserver) JobStopped(name string, err error) {
	if err != nil {
		o.logger.Error(fmt.Errorf("app - %s - job %s stopped: %w", o.component, name, err))
	}
}

func (o jobObserver) JobPanicked(name string, recovered any) {
	o.logger.Error(fmt.Errorf("app - %s - job %s: %w: %v", o.component, name, jobgroup.ErrJobPanicked, recovered))
}

// stopJobGroup stops a component's job group under the app's stop budget. A
// stop failure is reported, not propagated: the process is going down either
// way, and every caller's contract is a plain stop function.
func stopJobGroup(l logger.Interface, jobs *jobgroup.Group) {
	if jobs == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), eventLoopStopTimeout)
	defer cancel()

	if err := jobs.Stop(ctx); err != nil {
		l.Error(fmt.Errorf("app - stop jobs: %w", err))
	}
}
