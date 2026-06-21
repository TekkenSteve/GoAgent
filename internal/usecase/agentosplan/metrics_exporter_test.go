package agentosplan

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestPlanMetricsExporterProjectsTailEventsWithCheckpointState(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := metricsExporterPlanSpec("plan-export")
	status := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
		UpdatedAt:      spec.RequestedAt,
	}
	if _, _, err := store.CreatePlan(ctx, spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	planStartedAt := spec.RequestedAt.Add(time.Second)
	nodeStartedAt := spec.RequestedAt.Add(2 * time.Second)
	if _, err := store.AppendPlanEvent(ctx, metricEvent(0, agentos.EventPlanStarted, spec.PlanID, "", "", planStartedAt, nil), "event-1"); err != nil {
		t.Fatalf("AppendPlanEvent plan start: %v", err)
	}
	if _, err := store.AppendPlanEvent(ctx, metricEvent(0, agentos.EventPlanNodeStarted, spec.PlanID, "draft", "run-draft", nodeStartedAt, nil), "event-2"); err != nil {
		t.Fatalf("AppendPlanEvent node start: %v", err)
	}

	sink := &recordingPlanMetricsSink{}
	exporter := newTestPlanMetricsExporter(t, store, sink, 2)
	first, err := exporter.Export(ctx, PlanRefScope{AccountID: spec.AccountID, ProjectID: spec.ProjectID})
	if err != nil {
		t.Fatalf("Export first: %v", err)
	}
	if first.EventsScanned != 2 || first.CheckpointsSaved != 1 {
		t.Fatalf("first result = %#v", first)
	}
	requireMetric(t, sink.samples, PlanMetricPlanStartedTotal, "", 1)
	requireMetric(t, sink.samples, PlanMetricNodeStartedTotal, "draft", 1)

	nodeSucceededAt := spec.RequestedAt.Add(12 * time.Second)
	planSucceededAt := spec.RequestedAt.Add(20 * time.Second)
	if _, err := store.AppendPlanEvent(ctx, metricEvent(0, agentos.EventPlanNodeSucceeded, spec.PlanID, "draft", "run-draft", nodeSucceededAt, nil), "event-3"); err != nil {
		t.Fatalf("AppendPlanEvent node succeeded: %v", err)
	}
	if _, err := store.AppendPlanEvent(ctx, metricEvent(0, agentos.EventPlanSucceeded, spec.PlanID, "", "", planSucceededAt, nil), "event-4"); err != nil {
		t.Fatalf("AppendPlanEvent plan succeeded: %v", err)
	}

	sink.samples = nil
	second, err := exporter.Export(ctx, PlanRefScope{AccountID: spec.AccountID, ProjectID: spec.ProjectID})
	if err != nil {
		t.Fatalf("Export second: %v", err)
	}
	if second.EventsScanned != 2 || second.CheckpointsSaved != 1 {
		t.Fatalf("second result = %#v", second)
	}
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
	if !exists || checkpoint.Sequence != 4 {
		t.Fatalf("checkpoint = %#v exists=%v, want sequence 4", checkpoint, exists)
	}
	if !checkpoint.Projection.PlanStartedAt.IsZero() || len(checkpoint.Projection.NodeStartedAt) != 0 {
		t.Fatalf("terminal projection = %#v, want empty", checkpoint.Projection)
	}
}

func TestPlanMetricsExporterDoesNotAdvanceCheckpointWhenSinkFails(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryPlanStore()
	spec := metricsExporterPlanSpec("plan-export-fail")
	if _, _, err := store.CreatePlan(ctx, spec, agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
		UpdatedAt:      spec.RequestedAt,
	}); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}
	if _, err := store.AppendPlanEvent(ctx, metricEvent(0, agentos.EventPlanStarted, spec.PlanID, "", "", spec.RequestedAt.Add(time.Second), nil), "event-1"); err != nil {
		t.Fatalf("AppendPlanEvent: %v", err)
	}

	sinkErr := errors.New("sink unavailable")
	exporter := newTestPlanMetricsExporter(t, store, &recordingPlanMetricsSink{err: sinkErr}, 10)
	_, err := exporter.Export(ctx, PlanRefScope{AccountID: spec.AccountID, ProjectID: spec.ProjectID})
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
	exporter, err := NewPlanMetricsExporter(PlanMetricsExporterConfig{
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

func (s *recordingPlanMetricsSink) RecordPlanMetric(_ context.Context, sample PlanMetricSample) error {
	if s.err != nil {
		return s.err
	}
	s.samples = append(s.samples, sample)

	return nil
}
