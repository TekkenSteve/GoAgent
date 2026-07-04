package agentosplan

import (
	"context"
	"maps"
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
	metricSampleInitialCapacity = 2
	metricLabelInitialCapacity  = 2
	metricUnitCount             = "count"
	metricUnitSeconds           = "seconds"
	metricUnitBytes             = "bytes"
	metricUnitCents             = "cents"
)

const planMetricSampleTimestampPrecision = time.Microsecond

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

// PlanMetricSampleKey is the stable idempotency identity for a projected metric
// sample. A single PlanEvent can emit multiple metric names, so event identity
// alone is not sufficient.
type PlanMetricSampleKey struct {
	Name     PlanMetricName
	PlanID   string
	NodeID   string
	RunID    string
	EventID  string
	Sequence int64
}

// Key returns the durable idempotency identity for this sample.
func (s *PlanMetricSample) Key() PlanMetricSampleKey {
	return PlanMetricSampleKey{
		Name:     s.Name,
		PlanID:   s.PlanID,
		NodeID:   s.NodeID,
		RunID:    s.RunID,
		EventID:  s.EventID,
		Sequence: s.Sequence,
	}
}

// PlanMetricProjectionState is the durable state required to continue metric
// projection from a per-plan event checkpoint without rereading the full event
// history.
type PlanMetricProjectionState struct {
	PlanStartedAt time.Time                  `json:"plan_started_at"`
	NodeStartedAt []PlanMetricNodeStartState `json:"node_started_at,omitempty"`
}

// PlanMetricNodeStartState remembers the start timestamp for one backend-owned
// child run so duration metrics can be computed when its terminal event arrives
// in a later exporter batch.
type PlanMetricNodeStartState struct {
	NodeID    string    `json:"node_id"`
	RunID     string    `json:"run_id,omitempty"`
	StartedAt time.Time `json:"started_at"`
}

// BuildPlanMetricSamples projects operational metrics from a durable RunPlan
// event history. Samples keep event identity so downstream exporters can
// de-duplicate on EventID/Sequence.
func BuildPlanMetricSamples(ctx context.Context, spec *agentos.RunPlanSpec, events []agentos.PlanEvent) ([]PlanMetricSample, error) {
	return BuildPlanMetricSamplesAfter(ctx, spec, events, 0)
}

// BuildPlanMetricSamplesAfter projects metrics for events after a durable
// checkpoint while still reading earlier history to reconstruct durations and
// other stateful measurements.
func BuildPlanMetricSamplesAfter(ctx context.Context, spec *agentos.RunPlanSpec, events []agentos.PlanEvent, afterSequence int64) ([]PlanMetricSample, error) {
	samples, _, err := projectPlanMetricSamples(ctx, spec, PlanMetricProjectionState{}, events, afterSequence)

	return samples, err
}

// BuildPlanMetricSamplesFromState projects metrics from an incremental event
// batch and returns the next durable projection state.
func BuildPlanMetricSamplesFromState(ctx context.Context, spec *agentos.RunPlanSpec, state PlanMetricProjectionState, events []agentos.PlanEvent) ([]PlanMetricSample, PlanMetricProjectionState, error) {
	return projectPlanMetricSamples(ctx, spec, state, events, 0)
}

func projectPlanMetricSamples(_ context.Context, spec *agentos.RunPlanSpec, state PlanMetricProjectionState, events []agentos.PlanEvent, afterSequence int64) ([]PlanMetricSample, PlanMetricProjectionState, error) {
	ordered := orderedPlanEvents(events)
	projection := newMetricProjection(spec, state, len(ordered))

	for i := range ordered {
		event := &ordered[i]
		emit := shouldEmitMetricSample(event, afterSequence)

		if err := projection.apply(event, emit); err != nil {
			return nil, PlanMetricProjectionState{}, err
		}
	}

	nextState := metricProjectionStateFromStarts(projection.planStart, projection.nodeStarts)

	return projection.samples, nextState, nil
}

type metricProjection struct {
	spec       *agentos.RunPlanSpec
	nodeByID   map[string]*agentos.PlanNodeSpec
	planStart  time.Time
	nodeStarts map[planMetricNodeKey]time.Time
	samples    []PlanMetricSample
}

func newMetricProjection(spec *agentos.RunPlanSpec, state PlanMetricProjectionState, capacity int) metricProjection {
	return metricProjection{
		spec:       spec,
		nodeByID:   planNodeByID(spec),
		planStart:  state.PlanStartedAt,
		nodeStarts: nodeStartsFromMetricProjectionState(state),
		samples:    make([]PlanMetricSample, 0, capacity),
	}
}

func (p *metricProjection) apply(event *agentos.PlanEvent, emit bool) error {
	switch event.EventType {
	case agentos.EventPlanStarted:
		p.recordPlanStarted(event, emit)
	case agentos.EventPlanSucceeded, agentos.EventPlanFailed, agentos.EventPlanCanceled:
		p.recordPlanCompleted(event, emit)
	case agentos.EventPlanNodeStarted:
		p.recordNodeStarted(event, emit)
	case agentos.EventPlanNodeSucceeded, agentos.EventPlanNodeFailed, agentos.EventPlanNodeCanceled, agentos.EventPlanNodeSkipped:
		p.recordNodeCompleted(event, emit)
	case agentos.EventNodeOutputPublished:
		return p.recordArtifactPublished(event, emit)
	case agentos.EventUsageReported:
		return p.recordUsageReported(event, emit)
	case agentos.EventPlanExpanded:
		p.recordPlanExpanded(event, emit)
	case agentos.EventPlanNodeRetryScheduled:
		p.recordNodeRetryScheduled(event, emit)
	case agentos.EventRunStarted, agentos.EventRunCompleted, agentos.EventRunFailed,
		agentos.EventRunCancelled, agentos.EventRunPaused, agentos.EventRunResumed,
		agentos.EventAgentStepStarted, agentos.EventAgentStepCompleted, agentos.EventAgentStepFailed,
		agentos.EventAgentMessageDelta, agentos.EventAgentMessageCompleted,
		agentos.EventToolCallStarted, agentos.EventToolCallDelta, agentos.EventToolCallCompleted,
		agentos.EventToolCallFailed,
		agentos.EventApprovalRequested, agentos.EventApprovalResolved,
		agentos.EventCheckpointCreated, agentos.EventArtifactCreated,
		agentos.EventNodeInputResolved, agentos.EventCapabilitySelected, agentos.EventConditionEvaluated,
		agentos.EventPlanBlocked, agentos.EventPlanApproved, agentos.EventPlanRejected,
		agentos.EventPlanNodeReady,
		agentos.EventProcessStarted, agentos.EventProcessWaiting, agentos.EventProcessBlocked,
		agentos.EventProcessSucceeded, agentos.EventProcessFailed, agentos.EventProcessCanceled,
		agentos.EventProcessTimerScheduled, agentos.EventProcessTimerFired:
		return nil
	}

	return nil
}

func (p *metricProjection) recordPlanStarted(event *agentos.PlanEvent, emit bool) {
	p.planStart = event.Timestamp

	if !emit {
		return
	}

	p.samples = append(p.samples, metricSample(p.spec, event, PlanMetricPlanStartedTotal, 1, metricUnitCount, nil))
	if !p.spec.RequestedAt.IsZero() && !event.Timestamp.IsZero() {
		p.samples = append(p.samples, metricSample(p.spec, event, PlanMetricPlanQueueLatencySeconds, secondsBetween(p.spec.RequestedAt, event.Timestamp), metricUnitSeconds, nil))
	}
}

func (p *metricProjection) recordPlanCompleted(event *agentos.PlanEvent, emit bool) {
	if !emit {
		return
	}

	labels := map[string]string{"lifecycle_state": planLifecycleForEvent(event.EventType)}
	p.samples = append(p.samples, metricSample(p.spec, event, PlanMetricPlanCompletedTotal, 1, metricUnitCount, labels))

	if !p.planStart.IsZero() && !event.Timestamp.IsZero() {
		p.samples = append(p.samples, metricSample(p.spec, event, PlanMetricPlanDurationSeconds, secondsBetween(p.planStart, event.Timestamp), metricUnitSeconds, labels))
	}

	p.planStart = time.Time{}
}

func (p *metricProjection) recordNodeStarted(event *agentos.PlanEvent, emit bool) {
	p.nodeStarts[nodeMetricKey(event.NodeID, event.RunID)] = event.Timestamp

	if !emit {
		return
	}

	node := p.nodeByID[event.NodeID]
	labels := backendLabels(node)
	p.samples = append(p.samples, metricSample(p.spec, event, PlanMetricNodeStartedTotal, 1, metricUnitCount, labels))

	if requestedAt := nodeRequestedAt(p.spec, node); !requestedAt.IsZero() && !event.Timestamp.IsZero() {
		p.samples = append(p.samples, metricSample(p.spec, event, PlanMetricNodeQueueLatencySeconds, secondsBetween(requestedAt, event.Timestamp), metricUnitSeconds, labels))
	}
}

func (p *metricProjection) recordNodeCompleted(event *agentos.PlanEvent, emit bool) {
	if !emit {
		return
	}

	node := p.nodeByID[event.NodeID]
	labels := mergeLabels(backendLabels(node), map[string]string{"lifecycle_state": nodeLifecycleForEvent(event.EventType)})
	p.samples = append(p.samples, metricSample(p.spec, event, PlanMetricNodeCompletedTotal, 1, metricUnitCount, labels))

	key := nodeMetricKey(event.NodeID, event.RunID)
	if start := p.nodeStarts[key]; !start.IsZero() && !event.Timestamp.IsZero() {
		p.samples = append(p.samples, metricSample(p.spec, event, PlanMetricNodeDurationSeconds, secondsBetween(start, event.Timestamp), metricUnitSeconds, labels))
	}

	delete(p.nodeStarts, key)

	if event.EventType == agentos.EventPlanNodeFailed {
		p.samples = append(p.samples, metricSample(p.spec, event, PlanMetricBackendErrorsTotal, 1, metricUnitCount, backendLabels(node)))
	}
}

func (p *metricProjection) recordArtifactPublished(event *agentos.PlanEvent, emit bool) error {
	if !emit {
		return nil
	}

	samples, err := artifactMetricSamples(p.spec, event)
	if err != nil {
		return err
	}

	p.samples = append(p.samples, samples...)

	return nil
}

func (p *metricProjection) recordUsageReported(event *agentos.PlanEvent, emit bool) error {
	if !emit {
		return nil
	}

	samples, err := budgetMetricSamples(p.spec, event)
	if err != nil {
		return err
	}

	p.samples = append(p.samples, samples...)

	return nil
}

func (p *metricProjection) recordPlanExpanded(event *agentos.PlanEvent, emit bool) {
	if emit {
		p.samples = append(p.samples, metricSample(p.spec, event, PlanMetricDynamicExpansionsTotal, 1, metricUnitCount, nil))
	}
}

func (p *metricProjection) recordNodeRetryScheduled(event *agentos.PlanEvent, emit bool) {
	if emit {
		p.samples = append(p.samples, metricSample(p.spec, event, PlanMetricNodeRetryScheduledTotal, 1, metricUnitCount, backendLabels(p.nodeByID[event.NodeID])))
	}
}

func shouldEmitMetricSample(event *agentos.PlanEvent, afterSequence int64) bool {
	return afterSequence == 0 || event.Sequence > afterSequence
}

func nodeStartsFromMetricProjectionState(state PlanMetricProjectionState) map[planMetricNodeKey]time.Time {
	starts := make(map[planMetricNodeKey]time.Time, len(state.NodeStartedAt))
	for _, start := range state.NodeStartedAt {
		if start.NodeID == "" || start.StartedAt.IsZero() {
			continue
		}

		starts[nodeMetricKey(start.NodeID, start.RunID)] = start.StartedAt
	}

	return starts
}

func metricProjectionStateFromStarts(planStart time.Time, nodeStarts map[planMetricNodeKey]time.Time) PlanMetricProjectionState {
	state := PlanMetricProjectionState{PlanStartedAt: planStart}
	if len(nodeStarts) == 0 {
		return state
	}

	starts := make([]PlanMetricNodeStartState, 0, len(nodeStarts))
	for key, startedAt := range nodeStarts {
		starts = append(starts, PlanMetricNodeStartState{
			NodeID:    key.NodeID,
			RunID:     key.RunID,
			StartedAt: startedAt,
		})
	}

	sort.SliceStable(starts, func(i, j int) bool {
		if starts[i].NodeID != starts[j].NodeID {
			return starts[i].NodeID < starts[j].NodeID
		}

		return starts[i].RunID < starts[j].RunID
	})
	state.NodeStartedAt = starts

	return state
}

func artifactMetricSamples(spec *agentos.RunPlanSpec, event *agentos.PlanEvent) ([]PlanMetricSample, error) {
	value, ok := event.Payload[planEventPayloadArtifacts]
	if !ok {
		return nil, nil
	}

	artifacts, err := decodePlanDebugPayload[[]agentos.ArtifactRef](value, planEventPayloadArtifacts)
	if err != nil {
		return nil, err
	}

	samples := make([]PlanMetricSample, 0, len(artifacts))
	for i := range artifacts {
		artifact := &artifacts[i]

		sample := metricSample(spec, event, PlanMetricArtifactPublishedBytes, float64(artifact.SizeBytes), metricUnitBytes, map[string]string{
			"artifact_kind": string(artifact.Kind),
		})
		sample.NodeID = artifact.NodeID
		sample.RunID = artifact.RunID
		samples = append(samples, sample)
	}

	return samples, nil
}

func budgetMetricSamples(spec *agentos.RunPlanSpec, event *agentos.PlanEvent) ([]PlanMetricSample, error) {
	samples := make([]PlanMetricSample, 0, metricSampleInitialCapacity)

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

func metricSample(spec *agentos.RunPlanSpec, event *agentos.PlanEvent, name PlanMetricName, value float64, unit string, labels map[string]string) PlanMetricSample {
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

func planNodeByID(spec *agentos.RunPlanSpec) map[string]*agentos.PlanNodeSpec {
	nodes := make(map[string]*agentos.PlanNodeSpec, len(spec.Nodes))
	for i := range spec.Nodes {
		node := &spec.Nodes[i]
		nodes[node.NodeID] = node
	}

	return nodes
}

func nodeRequestedAt(spec *agentos.RunPlanSpec, node *agentos.PlanNodeSpec) time.Time {
	if !node.Run.RequestedAt.IsZero() {
		return node.Run.RequestedAt
	}

	return spec.RequestedAt
}

type planMetricNodeKey struct {
	NodeID string
	RunID  string
}

func nodeMetricKey(nodeID, runID string) planMetricNodeKey {
	return planMetricNodeKey{NodeID: nodeID, RunID: runID}
}

func secondsBetween(start, end time.Time) float64 {
	return end.Sub(start).Seconds()
}

func backendLabels(node *agentos.PlanNodeSpec) map[string]string {
	if node == nil {
		return nil
	}

	labels := make(map[string]string, metricLabelInitialCapacity)
	if node.Run.Backend.Kind != "" {
		labels["backend_kind"] = string(node.Run.Backend.Kind)
	}

	if node.Run.Backend.Name != "" {
		labels["backend_name"] = node.Run.Backend.Name
	}

	return labels
}

func mergeLabels(left, right map[string]string) map[string]string {
	merged := cloneMetricLabels(left)
	maps.Copy(merged, right)

	return merged
}

func cloneMetricLabels(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}

	output := make(map[string]string, len(input))
	maps.Copy(output, input)

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
	case agentos.EventProcessStarted, agentos.EventProcessWaiting, agentos.EventProcessBlocked,
		agentos.EventProcessSucceeded, agentos.EventProcessFailed, agentos.EventProcessCanceled,
		agentos.EventProcessTimerScheduled, agentos.EventProcessTimerFired:
		return ""
	case agentos.EventRunStarted, agentos.EventRunCompleted, agentos.EventRunFailed,
		agentos.EventRunCancelled, agentos.EventRunPaused, agentos.EventRunResumed,
		agentos.EventAgentStepStarted, agentos.EventAgentStepCompleted, agentos.EventAgentStepFailed,
		agentos.EventAgentMessageDelta, agentos.EventAgentMessageCompleted,
		agentos.EventToolCallStarted, agentos.EventToolCallDelta, agentos.EventToolCallCompleted,
		agentos.EventToolCallFailed, agentos.EventApprovalRequested, agentos.EventApprovalResolved,
		agentos.EventUsageReported, agentos.EventCheckpointCreated, agentos.EventArtifactCreated,
		agentos.EventNodeInputResolved, agentos.EventNodeOutputPublished,
		agentos.EventCapabilitySelected, agentos.EventConditionEvaluated,
		agentos.EventPlanStarted, agentos.EventPlanBlocked, agentos.EventPlanExpanded,
		agentos.EventPlanApproved, agentos.EventPlanRejected,
		agentos.EventPlanNodeReady, agentos.EventPlanNodeStarted,
		agentos.EventPlanNodeSucceeded, agentos.EventPlanNodeFailed,
		agentos.EventPlanNodeRetryScheduled, agentos.EventPlanNodeSkipped,
		agentos.EventPlanNodeCanceled:
		return ""
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
	case agentos.EventProcessStarted, agentos.EventProcessWaiting, agentos.EventProcessBlocked,
		agentos.EventProcessSucceeded, agentos.EventProcessFailed, agentos.EventProcessCanceled,
		agentos.EventProcessTimerScheduled, agentos.EventProcessTimerFired:
		return ""
	case agentos.EventRunStarted, agentos.EventRunCompleted, agentos.EventRunFailed,
		agentos.EventRunCancelled, agentos.EventRunPaused, agentos.EventRunResumed,
		agentos.EventAgentStepStarted, agentos.EventAgentStepCompleted, agentos.EventAgentStepFailed,
		agentos.EventAgentMessageDelta, agentos.EventAgentMessageCompleted,
		agentos.EventToolCallStarted, agentos.EventToolCallDelta, agentos.EventToolCallCompleted,
		agentos.EventToolCallFailed, agentos.EventApprovalRequested, agentos.EventApprovalResolved,
		agentos.EventUsageReported, agentos.EventCheckpointCreated, agentos.EventArtifactCreated,
		agentos.EventNodeInputResolved, agentos.EventNodeOutputPublished,
		agentos.EventCapabilitySelected, agentos.EventConditionEvaluated,
		agentos.EventPlanStarted, agentos.EventPlanBlocked, agentos.EventPlanExpanded,
		agentos.EventPlanApproved, agentos.EventPlanRejected,
		agentos.EventPlanSucceeded, agentos.EventPlanFailed, agentos.EventPlanCanceled,
		agentos.EventPlanNodeReady, agentos.EventPlanNodeStarted,
		agentos.EventPlanNodeRetryScheduled:
		return ""
	default:
		return ""
	}
}
