package agentosplan

import (
	"context"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestBuildPlanMetricSamplesProjectsDurationsAndBackendErrors(t *testing.T) {
	t.Parallel()

	requestedAt := time.Date(2026, 6, 19, 10, 0, 0, 0, time.UTC)
	planStartedAt := requestedAt.Add(2 * time.Second)
	nodeRequestedAt := requestedAt.Add(3 * time.Second)
	nodeStartedAt := requestedAt.Add(5 * time.Second)
	nodeFailedAt := requestedAt.Add(15 * time.Second)
	planFailedAt := requestedAt.Add(20 * time.Second)
	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: RESEARCH}
	spec := agentos.RunPlanSpec{
		PlanID:      "plan-metrics",
		AccountID:   "acct-1",
		ProjectID:   "proj-1",
		RequestedAt: requestedAt,
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: RESEARCH,
				Run: agentos.RunSpec{
					RunID:       "run-research",
					Backend:     ref,
					RequestedAt: nodeRequestedAt,
				},
			},
		},
	}
	events := []agentos.PlanEvent{
		metricEvent(4, agentoscore.EventPlanFailed, spec.PlanID, "", "", planFailedAt, nil),
		metricEvent(2, agentoscore.EventPlanNodeStarted, spec.PlanID, RESEARCH, "run-research", nodeStartedAt, nil),
		metricEvent(1, agentoscore.EventPlanStarted, spec.PlanID, "", "", planStartedAt, nil),
		metricEvent(3, agentoscore.EventPlanNodeFailed, spec.PlanID, RESEARCH, "run-research", nodeFailedAt, nil),
	}

	samples, err := BuildPlanMetricSamples(context.Background(), &spec, events)
	if err != nil {
		t.Fatalf("BuildPlanMetricSamples: %v", err)
	}

	requireMetric(t, samples, PlanMetricPlanStartedTotal, "", 1)
	requireMetric(t, samples, PlanMetricPlanQueueLatencySeconds, "", 2)
	requireMetric(t, samples, PlanMetricNodeStartedTotal, RESEARCH, 1)
	requireMetric(t, samples, PlanMetricNodeQueueLatencySeconds, RESEARCH, 2)
	requireMetric(t, samples, PlanMetricNodeCompletedTotal, RESEARCH, 1)
	requireMetric(t, samples, PlanMetricNodeDurationSeconds, RESEARCH, 10)
	requireMetric(t, samples, PlanMetricBackendErrorsTotal, RESEARCH, 1)
	requireMetric(t, samples, PlanMetricPlanCompletedTotal, "", 1)
	requireMetric(t, samples, PlanMetricPlanDurationSeconds, "", 18)

	nodeDuration := findMetric(samples, PlanMetricNodeDurationSeconds, RESEARCH)
	if nodeDuration.Labels["backend_kind"] != string(agentos.BackendKindHTTP) ||
		nodeDuration.Labels["backend_name"] != RESEARCH ||
		nodeDuration.Labels["lifecycle_state"] != agentos.PlanNodeFailed {
		t.Fatalf("node duration labels = %#v", nodeDuration.Labels)
	}

	if nodeDuration.PlanID != spec.PlanID || nodeDuration.AccountID != spec.AccountID || nodeDuration.ProjectID != spec.ProjectID {
		t.Fatalf("sample identity = %#v", nodeDuration)
	}

	if _, ok := nodeDuration.Labels["plan_id"]; ok {
		t.Fatalf("plan_id leaked into metric labels: %#v", nodeDuration.Labels)
	}
}

func TestBuildPlanMetricSamplesProjectsArtifactsBudgetExpansionAndRetries(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 19, 11, 0, 0, 0, time.UTC)
	spec := agentos.RunPlanSpec{
		PlanID: "plan-metrics-artifact",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "writer",
				Run: agentos.RunSpec{
					RunID:   "run-writer",
					Backend: agentos.BackendRef{Kind: agentos.BackendKindGRPC, Name: "writer"},
				},
			},
		},
	}
	events := []agentos.PlanEvent{
		metricEvent(1, agentoscore.EventNodeOutputPublished, spec.PlanID, "writer", "run-writer", now, map[string]any{
			planEventPayloadArtifacts: []agentoscore.ArtifactRef{
				{
					ArtifactID: "artifact-1",
					PlanID:     spec.PlanID,
					NodeID:     "writer",
					RunID:      "run-writer",
					Name:       "summary",
					Kind:       agentoscore.ArtifactKindObject,
					SizeBytes:  4096,
				},
			},
		}),
		metricEvent(2, agentoscore.EventUsageReported, spec.PlanID, "writer", "run-writer", now.Add(time.Second), map[string]any{
			planEventPayloadBudgetDelta: agentos.PlanBudgetUsage{SpentCents: 25},
			planEventPayloadBudgetUsage: agentos.PlanBudgetUsage{SpentCents: 75},
		}),
		metricEvent(3, agentoscore.EventPlanExpanded, spec.PlanID, "", "", now.Add(2*time.Second), nil),
		metricEvent(4, agentoscore.EventPlanNodeRetryScheduled, spec.PlanID, "writer", "run-writer", now.Add(3*time.Second), nil),
	}

	samples, err := BuildPlanMetricSamples(context.Background(), &spec, events)
	if err != nil {
		t.Fatalf("BuildPlanMetricSamples: %v", err)
	}

	artifact := requireMetric(t, samples, PlanMetricArtifactPublishedBytes, "writer", 4096)
	if artifact.Labels["artifact_kind"] != string(agentoscore.ArtifactKindObject) {
		t.Fatalf("artifact labels = %#v", artifact.Labels)
	}

	requireMetric(t, samples, PlanMetricBudgetDeltaCents, "writer", 25)
	requireMetric(t, samples, PlanMetricBudgetSpentCents, "writer", 75)
	requireMetric(t, samples, PlanMetricDynamicExpansionsTotal, "", 1)

	retry := requireMetric(t, samples, PlanMetricNodeRetryScheduledTotal, "writer", 1)
	if retry.Labels["backend_kind"] != string(agentos.BackendKindGRPC) || retry.Labels["backend_name"] != "writer" {
		t.Fatalf("retry labels = %#v", retry.Labels)
	}
}

func TestBuildPlanMetricSamplesAfterUsesEarlierHistoryForDurations(t *testing.T) {
	t.Parallel()

	requestedAt := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	planStartedAt := requestedAt.Add(1 * time.Second)
	nodeStartedAt := requestedAt.Add(3 * time.Second)
	nodeSucceededAt := requestedAt.Add(13 * time.Second)
	planSucceededAt := requestedAt.Add(21 * time.Second)
	spec := agentos.RunPlanSpec{
		PlanID:      "plan-metrics-checkpoint",
		AccountID:   "acct-1",
		ProjectID:   "proj-1",
		RequestedAt: requestedAt,
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
	events := []agentos.PlanEvent{
		metricEvent(1, agentoscore.EventPlanStarted, spec.PlanID, "", "", planStartedAt, nil),
		metricEvent(2, agentoscore.EventPlanNodeStarted, spec.PlanID, "draft", "run-draft", nodeStartedAt, nil),
		metricEvent(3, agentoscore.EventPlanNodeSucceeded, spec.PlanID, "draft", "run-draft", nodeSucceededAt, nil),
		metricEvent(4, agentoscore.EventPlanSucceeded, spec.PlanID, "", "", planSucceededAt, nil),
	}

	samples, err := BuildPlanMetricSamplesAfter(context.Background(), &spec, events, 2)
	if err != nil {
		t.Fatalf("BuildPlanMetricSamplesAfter: %v", err)
	}

	requireNoMetric(t, samples, PlanMetricPlanStartedTotal, "")
	requireNoMetric(t, samples, PlanMetricPlanQueueLatencySeconds, "")
	requireNoMetric(t, samples, PlanMetricNodeStartedTotal, "draft")
	requireNoMetric(t, samples, PlanMetricNodeQueueLatencySeconds, "draft")
	requireMetric(t, samples, PlanMetricNodeCompletedTotal, "draft", 1)
	requireMetric(t, samples, PlanMetricNodeDurationSeconds, "draft", 10)
	requireMetric(t, samples, PlanMetricPlanCompletedTotal, "", 1)
	requireMetric(t, samples, PlanMetricPlanDurationSeconds, "", 20)

	for _, sample := range samples {
		if sample.Sequence <= 2 {
			t.Fatalf("sample crossed checkpoint: %#v", sample)
		}
	}
}

func TestBuildPlanMetricSamplesFromStateProjectsTailBatch(t *testing.T) {
	t.Parallel()

	requestedAt := time.Date(2026, 6, 19, 13, 0, 0, 0, time.UTC)
	planStartedAt := requestedAt.Add(1 * time.Second)
	nodeStartedAt := requestedAt.Add(2 * time.Second)
	nodeSucceededAt := requestedAt.Add(7 * time.Second)
	planSucceededAt := requestedAt.Add(11 * time.Second)
	spec := agentos.RunPlanSpec{
		PlanID:    "plan-metrics-state",
		AccountID: "acct-1",
		ProjectID: "proj-1",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "review",
				Run: agentos.RunSpec{
					RunID:   "run-review",
					Backend: agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "reviewer"},
				},
			},
		},
	}
	state := PlanMetricProjectionState{
		PlanStartedAt: planStartedAt,
		NodeStartedAt: []PlanMetricNodeStartState{
			{NodeID: "review", RunID: "run-review", StartedAt: nodeStartedAt},
		},
	}
	events := []agentos.PlanEvent{
		metricEvent(3, agentoscore.EventPlanNodeSucceeded, spec.PlanID, "review", "run-review", nodeSucceededAt, nil),
		metricEvent(4, agentoscore.EventPlanSucceeded, spec.PlanID, "", "", planSucceededAt, nil),
	}

	samples, nextState, err := BuildPlanMetricSamplesFromState(context.Background(), &spec, state, events)
	if err != nil {
		t.Fatalf("BuildPlanMetricSamplesFromState: %v", err)
	}

	requireMetric(t, samples, PlanMetricNodeCompletedTotal, "review", 1)
	requireMetric(t, samples, PlanMetricNodeDurationSeconds, "review", 5)
	requireMetric(t, samples, PlanMetricPlanCompletedTotal, "", 1)
	requireMetric(t, samples, PlanMetricPlanDurationSeconds, "", 10)

	if !nextState.PlanStartedAt.IsZero() {
		t.Fatalf("next plan start = %s, want zero after terminal plan event", nextState.PlanStartedAt)
	}

	if len(nextState.NodeStartedAt) != 0 {
		t.Fatalf("next node starts = %#v, want empty after terminal node event", nextState.NodeStartedAt)
	}
}

func metricEvent(sequence int64, eventType agentoscore.EventType, planID, nodeID, runID string, at time.Time, payload map[string]any) agentos.PlanEvent {
	return agentos.PlanEvent{
		Event: agentoscore.Event{
			EventID:   "event-" + string(eventType) + "-" + nodeID,
			EventType: eventType,
			RunID:     runID,
			Sequence:  sequence,
			Timestamp: at,
			Payload:   payload,
		},
		PlanID: planID,
		NodeID: nodeID,
	}
}

func metricEventPtr(eventType agentoscore.EventType, planID, nodeID, runID string, at time.Time) *agentos.PlanEvent {
	event := metricEvent(0, eventType, planID, nodeID, runID, at, nil)

	return &event
}

func requireMetric(t *testing.T, samples []PlanMetricSample, name PlanMetricName, nodeID string, value float64) PlanMetricSample {
	t.Helper()

	sample := findMetric(samples, name, nodeID)
	if sample.Name == "" {
		t.Fatalf("missing metric %s node=%q in %#v", name, nodeID, samples)
	}

	if sample.Value != value {
		t.Fatalf("metric %s node=%q value = %v, want %v", name, nodeID, sample.Value, value)
	}

	return sample
}

func requireNoMetric(t *testing.T, samples []PlanMetricSample, name PlanMetricName, nodeID string) {
	t.Helper()

	if sample := findMetric(samples, name, nodeID); sample.Name != "" {
		t.Fatalf("unexpected metric %s node=%q in %#v", name, nodeID, samples)
	}
}

func findMetric(samples []PlanMetricSample, name PlanMetricName, nodeID string) PlanMetricSample {
	for i := range samples {
		sample := samples[i]
		if sample.Name == name && sample.NodeID == nodeID {
			return sample
		}
	}

	return PlanMetricSample{}
}
