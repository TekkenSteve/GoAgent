package agentosplan

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStartPlanMetricsExporterLoopRunsImmediatePass(t *testing.T) {
	exporter := &recordingPlanMetricsExporterRunner{
		result: PlanMetricsExportResult{PlansScanned: 2, EventsScanned: 3, SamplesRecorded: 4, CheckpointsSaved: 1},
	}
	observer := &recordingPlanMetricsExporterObserver{
		success: make(chan PlanMetricsExportResult, 1),
		failed:  make(chan error, 1),
	}

	loop, err := StartPlanMetricsExporterLoop(t.Context(), exporter, PlanMetricsExporterLoopConfig{
		Interval:          time.Hour,
		Scope:             PlanRefScope{Limit: 7},
		ExportImmediately: true,
	}, observer)
	if err != nil {
		t.Fatalf("StartPlanMetricsExporterLoop: %v", err)
	}
	defer loop.Stop()

	select {
	case result := <-observer.success:
		if result.SamplesRecorded != 4 {
			t.Fatalf("result = %#v", result)
		}
	case err := <-observer.failed:
		t.Fatalf("unexpected failure: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for immediate export")
	}
	if exporter.scope.Limit != 7 {
		t.Fatalf("scope limit = %d, want 7", exporter.scope.Limit)
	}
}

func TestStartPlanMetricsExporterLoopReportsImmediateFailure(t *testing.T) {
	exportErr := errors.New("metric sink unavailable")
	exporter := &recordingPlanMetricsExporterRunner{err: exportErr}
	observer := &recordingPlanMetricsExporterObserver{
		success: make(chan PlanMetricsExportResult, 1),
		failed:  make(chan error, 1),
	}

	loop, err := StartPlanMetricsExporterLoop(t.Context(), exporter, PlanMetricsExporterLoopConfig{
		Interval:          time.Hour,
		ExportImmediately: true,
	}, observer)
	if err != nil {
		t.Fatalf("StartPlanMetricsExporterLoop: %v", err)
	}
	defer loop.Stop()

	select {
	case err := <-observer.failed:
		if !errors.Is(err, exportErr) {
			t.Fatalf("failure = %v, want %v", err, exportErr)
		}
	case result := <-observer.success:
		t.Fatalf("unexpected success: %#v", result)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for immediate failure")
	}
}

func TestStartPlanMetricsExporterLoopRequiresExporter(t *testing.T) {
	_, err := StartPlanMetricsExporterLoop(t.Context(), nil, PlanMetricsExporterLoopConfig{}, nil)
	if err == nil {
		t.Fatal("StartPlanMetricsExporterLoop succeeded without exporter")
	}
}

func TestStartPlanMetricsExporterLoopRejectsNegativeLimit(t *testing.T) {
	_, err := StartPlanMetricsExporterLoop(t.Context(), &recordingPlanMetricsExporterRunner{}, PlanMetricsExporterLoopConfig{
		Scope: PlanRefScope{Limit: -1},
	}, nil)
	if err == nil {
		t.Fatal("StartPlanMetricsExporterLoop succeeded with negative limit")
	}
}

type recordingPlanMetricsExporterRunner struct {
	result PlanMetricsExportResult
	err    error
	scope  PlanRefScope
}

func (r *recordingPlanMetricsExporterRunner) Export(_ context.Context, scope PlanRefScope) (PlanMetricsExportResult, error) {
	r.scope = scope
	if r.err != nil {
		return PlanMetricsExportResult{}, r.err
	}

	return r.result, nil
}

type recordingPlanMetricsExporterObserver struct {
	success chan PlanMetricsExportResult
	failed  chan error
}

func (o *recordingPlanMetricsExporterObserver) PlanMetricsExportSucceeded(result PlanMetricsExportResult) {
	o.success <- result
}

func (o *recordingPlanMetricsExporterObserver) PlanMetricsExportFailed(err error) {
	o.failed <- err
}
