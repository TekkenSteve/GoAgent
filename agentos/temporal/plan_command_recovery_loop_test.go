package temporal

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStartPlanCommandRecoveryRunsImmediatePass(t *testing.T) {
	recoverer := &recordingPlanCommandRecoverer{
		result: PlanCommandRecoveryResult{Scanned: 2, Delivered: 2},
		calls:  make(chan int, 1),
	}
	observer := &recordingPlanCommandRecoveryObserver{
		success: make(chan PlanCommandRecoveryResult, 1),
	}
	loop, err := StartPlanCommandRecovery(t.Context(), recoverer, PlanCommandRecoveryLoopConfig{
		Interval:           time.Hour,
		Limit:              7,
		RecoverImmediately: true,
	}, observer)
	if err != nil {
		t.Fatalf("StartPlanCommandRecovery: %v", err)
	}
	defer loop.Stop()

	select {
	case limit := <-recoverer.calls:
		if limit != 7 {
			t.Fatalf("limit = %d, want 7", limit)
		}
	case <-time.After(time.Second):
		t.Fatal("recovery pass was not called")
	}
	select {
	case result := <-observer.success:
		if result.Scanned != 2 || result.Delivered != 2 {
			t.Fatalf("result = %#v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not receive success")
	}
}

func TestStartPlanCommandRecoveryReportsImmediateFailure(t *testing.T) {
	recoveryErr := errors.New("recover failed")
	recoverer := &recordingPlanCommandRecoverer{
		err:   recoveryErr,
		calls: make(chan int, 1),
	}
	observer := &recordingPlanCommandRecoveryObserver{
		failed: make(chan error, 1),
	}
	loop, err := StartPlanCommandRecovery(t.Context(), recoverer, PlanCommandRecoveryLoopConfig{
		Interval:           time.Hour,
		RecoverImmediately: true,
	}, observer)
	if err != nil {
		t.Fatalf("StartPlanCommandRecovery: %v", err)
	}
	defer loop.Stop()

	select {
	case <-recoverer.calls:
	case <-time.After(time.Second):
		t.Fatal("recovery pass was not called")
	}
	select {
	case err := <-observer.failed:
		if !errors.Is(err, recoveryErr) {
			t.Fatalf("error = %v, want %v", err, recoveryErr)
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not receive failure")
	}
}

func TestStartPlanCommandRecoveryRequiresRecoverer(t *testing.T) {
	_, err := StartPlanCommandRecovery(t.Context(), nil, PlanCommandRecoveryLoopConfig{}, nil)
	if err == nil {
		t.Fatal("StartPlanCommandRecovery succeeded without recoverer")
	}
}

type recordingPlanCommandRecoverer struct {
	result PlanCommandRecoveryResult
	err    error
	calls  chan int
}

func (r *recordingPlanCommandRecoverer) RecoverPlanCommands(_ context.Context, limit int) (PlanCommandRecoveryResult, error) {
	r.calls <- limit

	return r.result, r.err
}

type recordingPlanCommandRecoveryObserver struct {
	success chan PlanCommandRecoveryResult
	failed  chan error
}

func (o *recordingPlanCommandRecoveryObserver) PlanCommandRecoverySucceeded(result PlanCommandRecoveryResult) {
	o.success <- result
}

func (o *recordingPlanCommandRecoveryObserver) PlanCommandRecoveryFailed(err error) {
	o.failed <- err
}
