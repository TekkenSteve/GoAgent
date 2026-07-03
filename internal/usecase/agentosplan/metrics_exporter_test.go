package agentosplan

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

var errTestSinkUnavailable = errors.New("sink unavailable")

func TestPlanMetricsExporterProjectsTailEventsWithCheckpointState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := metricsExporterPlanSpec("plan-export")

	status := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
		UpdatedAt:      spec.RequestedAt,
	}

	if _, _, err := store.CreatePlan(ctx, &spec, &status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	appendMetricEventForTest(ctx, t, store, agentos.EventPlanStarted, spec.PlanID, "", "", spec.RequestedAt.Add(time.Second), "event-1")
	appendMetricEventForTest(ctx, t, store, agentos.EventPlanNodeStarted, spec.PlanID, "draft", "run-draft", spec.RequestedAt.Add(2*time.Second), "event-2")

	sink := &recordingPlanMetricsSink{}
	exporter := newTestPlanMetricsExporter(t, store, sink, 2)

	scope := PlanRefScope{AccountID: spec.AccountID, ProjectID: spec.ProjectID}
	first := exportPlanMetricsForTest(ctx, t, exporter, &scope)
	requirePlanMetricsExportResult(t, first, "first")

	requireMetric(t, sink.samples, PlanMetricPlanStartedTotal, "", 1)
	requireMetric(t, sink.samples, PlanMetricNodeStartedTotal, "draft", 1)

	appendMetricEventForTest(ctx, t, store, agentos.EventPlanNodeSucceeded, spec.PlanID, "draft", "run-draft", spec.RequestedAt.Add(12*time.Second), "event-3")
	appendMetricEventForTest(ctx, t, store, agentos.EventPlanSucceeded, spec.PlanID, "", "", spec.RequestedAt.Add(20*time.Second), "event-4")

	sink.samples = nil

	second := exportPlanMetricsForTest(ctx, t, exporter, &scope)
	requirePlanMetricsExportResult(t, second, "second")

	requireNoMetric(t, sink.samples, PlanMetricPlanStartedTotal, "")
	requireNoMetric(t, sink.samples, PlanMetricNodeStartedTotal, "draft")
	requireMetric(t, sink.samples, PlanMetricNodeDurationSeconds, "draft", 10)
	requireMetric(t, sink.samples, PlanMetricPlanDurationSeconds, "", 19)

	checkpoint, exists, err := store.GetPlanMetricCheckpoint(ctx, "test-exporter", agentos.PlanRef{
		PlanID:    spec.PlanID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	})
	if err != nil {
		t.Fatalf("GetPlanMetricCheckpoint: %v", err)
	}

	requireMetricsCheckpoint(t, &checkpoint, exists, 4)
}

func appendMetricEventForTest(ctx context.Context, t *testing.T, store *MemoryPlanStore, eventType agentos.EventType, planID, nodeID, runID string, at time.Time, key string) {
	t.Helper()

	if _, err := appendPlanEvent(ctx, store, metricEventPtr(eventType, planID, nodeID, runID, at), key); err != nil {
		t.Fatalf("AppendPlanEvent %s: %v", key, err)
	}
}

func requirePlanMetricsExportResult(t *testing.T, result PlanMetricsExportResult, label string) {
	t.Helper()

	if result.EventsScanned != 2 || result.CheckpointsSaved != 1 {
		t.Fatalf("%s result = %#v", label, result)
	}
}

func exportPlanMetricsForTest(
	ctx context.Context,
	t *testing.T,
	exporter *PlanMetricsExporter,
	scope *PlanRefScope,
) PlanMetricsExportResult {
	t.Helper()

	result, err := exporter.Export(ctx, scope)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	return result
}

func requireMetricsCheckpoint(t *testing.T, checkpoint *PlanMetricCheckpoint, exists bool, sequence int64) {
	t.Helper()

	if !exists || checkpoint.Sequence != sequence {
		t.Fatalf("checkpoint = %#v exists=%v, want sequence %d", checkpoint, exists, sequence)
	}

	if !checkpoint.Projection.PlanStartedAt.IsZero() || len(checkpoint.Projection.NodeStartedAt) != 0 {
		t.Fatalf("terminal projection = %#v, want empty", checkpoint.Projection)
	}
}

func TestPlanMetricsExporterDoesNotAdvanceCheckpointWhenSinkFails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryPlanStore()

	spec := metricsExporterPlanSpec("plan-export-fail")

	status := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
		UpdatedAt:      spec.RequestedAt,
	}
	if _, _, err := store.CreatePlan(ctx, &spec, &status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	if _, err := appendPlanEvent(ctx, store, metricEventPtr(agentos.EventPlanStarted, spec.PlanID, "", "", spec.RequestedAt.Add(time.Second)), "event-1"); err != nil {
		t.Fatalf("AppendPlanEvent: %v", err)
	}

	sinkErr := errTestSinkUnavailable
	exporter := newTestPlanMetricsExporter(t, store, &recordingPlanMetricsSink{err: sinkErr}, 10)

	scope := PlanRefScope{AccountID: spec.AccountID, ProjectID: spec.ProjectID}
	_, err := exporter.Export(ctx, &scope)

	if !errors.Is(err, sinkErr) {
		t.Fatalf("Export error = %v, want sink error", err)
	}

	_, exists, err := store.GetPlanMetricCheckpoint(ctx, "test-exporter", agentos.PlanRef{
		PlanID:    spec.PlanID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	})
	if err != nil {
		t.Fatalf("GetPlanMetricCheckpoint: %v", err)
	}

	if exists {
		t.Fatalf("checkpoint advanced after sink failure")
	}
}

func newTestPlanMetricsExporter(t *testing.T, store *MemoryPlanStore, sink PlanMetricsSink, batchSize int) *PlanMetricsExporter {
	t.Helper()

	exporter, err := NewPlanMetricsExporter(&PlanMetricsExporterConfig{
		ExporterID:  "test-exporter",
		PlanRefs:    store,
		Plans:       store,
		PlanEvents:  store,
		Checkpoints: store,
		Sink:        sink,
		BatchSize:   batchSize,
	})
	if err != nil {
		t.Fatalf("NewPlanMetricsExporter: %v", err)
	}

	return exporter
}

func metricsExporterPlanSpec(planID string) agentos.RunPlanSpec {
	requestedAt := time.Date(2026, 6, 19, 14, 0, 0, 0, time.UTC)

	return agentos.RunPlanSpec{
		PlanID:         planID,
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "start-" + planID,
		RequestedAt:    requestedAt,
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "draft",
				Run: agentos.RunSpec{
					RunID:   "run-draft",
					Backend: agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
				},
			},
		},
	}
}

type recordingPlanMetricsSink struct {
	samples []PlanMetricSample
	err     error
}

func (s *recordingPlanMetricsSink) RecordPlanMetric(_ context.Context, sample *PlanMetricSample) error {
	if s.err != nil {
		return s.err
	}

	s.samples = append(s.samples, *sample)

	return nil
}
