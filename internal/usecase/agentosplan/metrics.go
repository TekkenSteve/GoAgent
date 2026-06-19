package agentosplan

import (
	"context"
	"sort"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// PlanMetricName identifies one durable RunPlan metric projection. Samples are
// projected from persisted PlanEvents, so exporters can consume them
// idempotently by event sequence without coupling metrics to Temporal replay.
type PlanMetricName string

const (
	PlanMetricPlanStartedTotal        PlanMetricName = "agentos_plan_started_total"
	PlanMetricPlanCompletedTotal      PlanMetricName = "agentos_plan_completed_total"
	PlanMetricPlanDurationSeconds     PlanMetricName = "agentos_plan_duration_seconds"
	PlanMetricPlanQueueLatencySeconds PlanMetricName = "agentos_plan_queue_latency_seconds"
	PlanMetricNodeStartedTotal        PlanMetricName = "agentos_plan_node_started_total"
	PlanMetricNodeCompletedTotal      PlanMetricName = "agentos_plan_node_completed_total"
	PlanMetricNodeDurationSeconds     PlanMetricName = "agentos_plan_node_duration_seconds"
	PlanMetricNodeQueueLatencySeconds PlanMetricName = "agentos_plan_node_queue_latency_seconds"
	PlanMetricBackendErrorsTotal      PlanMetricName = "agentos_plan_backend_errors_total"
	PlanMetricArtifactPublishedBytes  PlanMetricName = "agentos_plan_artifact_published_bytes"
	PlanMetricBudgetDeltaCents        PlanMetricName = "agentos_plan_budget_delta_cents"
	PlanMetricBudgetSpentCents        PlanMetricName = "agentos_plan_budget_spent_cents"
	PlanMetricDynamicExpansionsTotal  PlanMetricName = "agentos_plan_dynamic_expansions_total"
	PlanMetricNodeRetryScheduledTotal PlanMetricName = "agentos_plan_node_retry_scheduled_total"
)

const (
	metricUnitCount   = "count"
	metricUnitSeconds = "seconds"
	metricUnitBytes   = "bytes"
	metricUnitCents   = "cents"
)

// PlanMetricSample is a low-cardinality metric event. Plan/node/run/event
// identity fields are intentionally not labels; Prometheus exporters can use
// them for idempotent projection checkpoints without creating unbounded label
// cardinality.
type PlanMetricSample struct {
	Name      PlanMetricName
	Value     float64
	Unit      string
	PlanID    string
	AccountID string
	ProjectID string
	NodeID    string
	RunID     string
	EventID   string
	Sequence  int64
	Timestamp time.Time
	Labels    map[string]string
}

// BuildPlanMetricSamples projects operational metrics from a durable RunPlan
// event batch. The input may be a full history or an ordered tail; samples keep
// event identity so downstream exporters can de-duplicate on EventID/Sequence.
func BuildPlanMetricSamples(_ context.Context, spec agentos.RunPlanSpec, events []agentos.PlanEvent) ([]PlanMetricSample, error) {
	ordered := orderedPlanEvents(events)
	nodeByID := planNodeByID(spec)
	planStart := time.Time{}
	nodeStarts := map[string]time.Time{}
	samples := make([]PlanMetricSample, 0, len(ordered))

	for _, event := range ordered {
		switch event.EventType {
		case agentos.EventPlanStarted:
			planStart = event.Timestamp
			samples = append(samples, metricSample(spec, event, PlanMetricPlanStartedTotal, 1, metricUnitCount, nil))
			if !spec.RequestedAt.IsZero() && !event.Timestamp.IsZero() {
				samples = append(samples, metricSample(spec, event, PlanMetricPlanQueueLatencySeconds, secondsBetween(spec.RequestedAt, event.Timestamp), metricUnitSeconds, nil))
			}
		case agentos.EventPlanSucceeded, agentos.EventPlanFailed, agentos.EventPlanCanceled:
			labels := map[string]string{"lifecycle_state": planLifecycleForEvent(event.EventType)}
			samples = append(samples, metricSample(spec, event, PlanMetricPlanCompletedTotal, 1, metricUnitCount, labels))
			if !planStart.IsZero() && !event.Timestamp.IsZero() {
				samples = append(samples, metricSample(spec, event, PlanMetricPlanDurationSeconds, secondsBetween(planStart, event.Timestamp), metricUnitSeconds, labels))
			}
		case agentos.EventPlanNodeStarted:
			nodeStarts[nodeMetricKey(event.NodeID, event.RunID)] = event.Timestamp
			labels := backendLabels(nodeByID[event.NodeID])
			samples = append(samples, metricSample(spec, event, PlanMetricNodeStartedTotal, 1, metricUnitCount, labels))
			if requestedAt := nodeRequestedAt(spec, nodeByID[event.NodeID]); !requestedAt.IsZero() && !event.Timestamp.IsZero() {
				samples = append(samples, metricSample(spec, event, PlanMetricNodeQueueLatencySeconds, secondsBetween(requestedAt, event.Timestamp), metricUnitSeconds, labels))
			}
		case agentos.EventPlanNodeSucceeded, agentos.EventPlanNodeFailed, agentos.EventPlanNodeCanceled, agentos.EventPlanNodeSkipped:
			labels := mergeLabels(backendLabels(nodeByID[event.NodeID]), map[string]string{"lifecycle_state": nodeLifecycleForEvent(event.EventType)})
			samples = append(samples, metricSample(spec, event, PlanMetricNodeCompletedTotal, 1, metricUnitCount, labels))
			if start := nodeStarts[nodeMetricKey(event.NodeID, event.RunID)]; !start.IsZero() && !event.Timestamp.IsZero() {
				samples = append(samples, metricSample(spec, event, PlanMetricNodeDurationSeconds, secondsBetween(start, event.Timestamp), metricUnitSeconds, labels))
			}
			if event.EventType == agentos.EventPlanNodeFailed {
				samples = append(samples, metricSample(spec, event, PlanMetricBackendErrorsTotal, 1, metricUnitCount, backendLabels(nodeByID[event.NodeID])))
			}
		case agentos.EventNodeOutputPublished:
			artifactSamples, err := artifactMetricSamples(spec, event)
			if err != nil {
				return nil, err
			}
			samples = append(samples, artifactSamples...)
		case agentos.EventUsageReported:
			budgetSamples, err := budgetMetricSamples(spec, event)
			if err != nil {
				return nil, err
			}
			samples = append(samples, budgetSamples...)
		case agentos.EventPlanExpanded:
			samples = append(samples, metricSample(spec, event, PlanMetricDynamicExpansionsTotal, 1, metricUnitCount, nil))
		case agentos.EventPlanNodeRetryScheduled:
			samples = append(samples, metricSample(spec, event, PlanMetricNodeRetryScheduledTotal, 1, metricUnitCount, backendLabels(nodeByID[event.NodeID])))
		}
	}

	return samples, nil
}

func artifactMetricSamples(spec agentos.RunPlanSpec, event agentos.PlanEvent) ([]PlanMetricSample, error) {
	value, ok := event.Payload[planEventPayloadArtifacts]
	if !ok {
		return nil, nil
	}
	artifacts, err := decodePlanDebugPayload[[]agentos.ArtifactRef](value, planEventPayloadArtifacts)
	if err != nil {
		return nil, err
	}
	samples := make([]PlanMetricSample, 0, len(artifacts))
	for _, artifact := range artifacts {
		sample := metricSample(spec, event, PlanMetricArtifactPublishedBytes, float64(artifact.SizeBytes), metricUnitBytes, map[string]string{
			"artifact_kind": string(artifact.Kind),
		})
		sample.NodeID = artifact.NodeID
		sample.RunID = artifact.RunID
		samples = append(samples, sample)
	}

	return samples, nil
}

func budgetMetricSamples(spec agentos.RunPlanSpec, event agentos.PlanEvent) ([]PlanMetricSample, error) {
	samples := make([]PlanMetricSample, 0, 2)
	if value, ok := event.Payload[planEventPayloadBudgetDelta]; ok {
		delta, err := decodePlanDebugPayload[agentos.PlanBudgetUsage](value, planEventPayloadBudgetDelta)
		if err != nil {
			return nil, err
		}
		samples = append(samples, metricSample(spec, event, PlanMetricBudgetDeltaCents, float64(delta.SpentCents), metricUnitCents, nil))
	}
	if value, ok := event.Payload[planEventPayloadBudgetUsage]; ok {
		usage, err := decodePlanDebugPayload[agentos.PlanBudgetUsage](value, planEventPayloadBudgetUsage)
		if err != nil {
			return nil, err
		}
		samples = append(samples, metricSample(spec, event, PlanMetricBudgetSpentCents, float64(usage.SpentCents), metricUnitCents, nil))
	}

	return samples, nil
}

func metricSample(spec agentos.RunPlanSpec, event agentos.PlanEvent, name PlanMetricName, value float64, unit string, labels map[string]string) PlanMetricSample {
	return PlanMetricSample{
		Name:      name,
		Value:     value,
		Unit:      unit,
		PlanID:    event.PlanID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		NodeID:    event.NodeID,
		RunID:     event.RunID,
		EventID:   event.EventID,
		Sequence:  event.Sequence,
		Timestamp: event.Timestamp,
		Labels:    cloneMetricLabels(labels),
	}
}

func orderedPlanEvents(events []agentos.PlanEvent) []agentos.PlanEvent {
	ordered := append([]agentos.PlanEvent(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Sequence != 0 || ordered[j].Sequence != 0 {
			return ordered[i].Sequence < ordered[j].Sequence
		}

		return ordered[i].Timestamp.Before(ordered[j].Timestamp)
	})

	return ordered
}

func planNodeByID(spec agentos.RunPlanSpec) map[string]agentos.PlanNodeSpec {
	nodes := make(map[string]agentos.PlanNodeSpec, len(spec.Nodes))
	for _, node := range spec.Nodes {
		nodes[node.NodeID] = node
	}

	return nodes
}

func nodeRequestedAt(spec agentos.RunPlanSpec, node agentos.PlanNodeSpec) time.Time {
	if !node.Run.RequestedAt.IsZero() {
		return node.Run.RequestedAt
	}

	return spec.RequestedAt
}

func nodeMetricKey(nodeID string, runID string) string {
	return nodeID + "\x00" + runID
}

func secondsBetween(start time.Time, end time.Time) float64 {
	return end.Sub(start).Seconds()
}

func backendLabels(node agentos.PlanNodeSpec) map[string]string {
	labels := make(map[string]string, 2)
	if node.Run.Backend.Kind != "" {
		labels["backend_kind"] = string(node.Run.Backend.Kind)
	}
	if node.Run.Backend.Name != "" {
		labels["backend_name"] = node.Run.Backend.Name
	}

	return labels
}

func mergeLabels(left map[string]string, right map[string]string) map[string]string {
	merged := cloneMetricLabels(left)
	for key, value := range right {
		merged[key] = value
	}

	return merged
}

func cloneMetricLabels(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}

	return output
}

func planLifecycleForEvent(eventType agentos.EventType) string {
	switch eventType {
	case agentos.EventPlanSucceeded:
		return agentos.PlanLifecycleSucceeded
	case agentos.EventPlanFailed:
		return agentos.PlanLifecycleFailed
	case agentos.EventPlanCanceled:
		return agentos.PlanLifecycleCanceled
	default:
		return ""
	}
}

func nodeLifecycleForEvent(eventType agentos.EventType) string {
	switch eventType {
	case agentos.EventPlanNodeSucceeded:
		return agentos.PlanNodeSucceeded
	case agentos.EventPlanNodeFailed:
		return agentos.PlanNodeFailed
	case agentos.EventPlanNodeCanceled:
		return agentos.PlanNodeCanceled
	case agentos.EventPlanNodeSkipped:
		return agentos.PlanNodeSkipped
	default:
		return ""
	}
}
