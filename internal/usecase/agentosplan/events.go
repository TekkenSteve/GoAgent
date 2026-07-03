package agentosplan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

const (
	idempotencyOperationNodeStart        = "node_start"
	idempotencyOperationNodeControl      = "node_control"
	idempotencyOperationNodeTimeout      = "node_timeout"
	idempotencyOperationPlanTimeout      = "plan_timeout"
	idempotencyOperationPlanSignalCancel = "plan_signal_cancel"
	idempotencyOperationArtifactPublish  = "artifact_publish"
	idempotencyOperationBudgetExceeded   = "budget_exceeded"
)

const (
	planEventPayloadLifecycleState  = "lifecycle_state"
	planEventPayloadPlanID          = "plan_id"
	planEventPayloadNodeID          = "node_id"
	planEventPayloadRunID           = "run_id"
	planEventPayloadReason          = "reason"
	planEventPayloadAttempt         = "attempt"
	planEventPayloadExpansion       = "expansion"
	planEventPayloadArtifacts       = "artifacts"
	planEventPayloadBudgetDelta     = "budget_delta"
	planEventPayloadBudgetUsage     = "budget_usage"
	planEventPayloadTransition      = "transition"
	planEventPayloadInputResolution = "input_resolution"
	planEventPayloadCapability      = "capability"
	planEventPayloadConditions      = "conditions"
)

// PlanEventFromStateEvent maps a deterministic reducer transition to the public
// PlanEvent envelope used by durable event stores and UI timelines.
func buildPlanEventPayload(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, event *StateEvent) map[string]any {
	payload := map[string]any{
		planEventPayloadLifecycleState: status.LifecycleState,
		planEventPayloadPlanID:         spec.PlanID,
	}

	addPlanEventIdentityPayload(payload, event)
	addPlanEventExpansionPayload(payload, event)
	addPlanEventArtifactPayload(payload, event)
	addPlanEventBudgetPayload(payload, status, event)
	addPlanEventTracePayload(payload, event)

	return payload
}

func addPlanEventIdentityPayload(payload map[string]any, event *StateEvent) {
	if event.NodeID != "" {
		payload[planEventPayloadNodeID] = event.NodeID
	}

	if event.RunID != "" {
		payload[planEventPayloadRunID] = event.RunID
	}

	if event.Reason != "" {
		payload[planEventPayloadReason] = event.Reason
	}

	if event.Attempt > 0 {
		payload[planEventPayloadAttempt] = event.Attempt
	}
}

func addPlanEventExpansionPayload(payload map[string]any, event *StateEvent) {
	if len(event.Expansion.Nodes) > 0 || len(event.Expansion.Edges) > 0 {
		payload[planEventPayloadExpansion] = planExpansionPayload(event.Expansion)
	}
}

func addPlanEventArtifactPayload(payload map[string]any, event *StateEvent) {
	if len(event.Artifacts) > 0 {
		payload[planEventPayloadArtifacts] = event.Artifacts
	}
}

func addPlanEventBudgetPayload(payload map[string]any, status *agentos.RunPlanStatus, event *StateEvent) {
	if event.BudgetDelta.SpentCents != 0 {
		payload[planEventPayloadBudgetDelta] = event.BudgetDelta
		payload[planEventPayloadBudgetUsage] = status.BudgetUsage
	}

	if event.PreviousLifecycleState != "" || event.NextLifecycleState != "" {
		payload[planEventPayloadTransition] = map[string]any{
			"previous_lifecycle_state": event.PreviousLifecycleState,
			"next_lifecycle_state":     event.NextLifecycleState,
		}
	}
}

func addPlanEventTracePayload(payload map[string]any, event *StateEvent) {
	if event.InputTrace.InputDigest != "" || event.InputTrace.MappingCount > 0 {
		payload[planEventPayloadInputResolution] = event.InputTrace
	}

	if event.Capability.Capability != "" {
		payload[planEventPayloadCapability] = event.Capability
	}

	if len(event.ConditionTraces) > 0 {
		payload[planEventPayloadConditions] = event.ConditionTraces
	}
}

func PlanEventFromStateEvent(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, event *StateEvent) (agentos.PlanEvent, string, error) {
	if err := ValidateRunPlanScope(spec); err != nil {
		return agentos.PlanEvent{}, "", err
	}

	eventType, err := planEventType(event.Kind)
	if err != nil {
		return agentos.PlanEvent{}, "", err
	}

	if event.At.IsZero() {
		return agentos.PlanEvent{}, "", fmt.Errorf("%w: state event timestamp is required", agentos.ErrInvalidPlanEvent)
	}

	payload := buildPlanEventPayload(spec, status, event)

	planEvent := agentos.PlanEvent{
		Event: agentos.Event{
			EventType: eventType,
			RunID:     event.RunID,
			ThreadID:  spec.ThreadID,
			Timestamp: event.At,
			Source:    "agentos.plan",
			Payload:   payload,
		},
		PlanID:    spec.PlanID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		NodeID:    event.NodeID,
	}

	key, err := StateEventIdempotencyKey(spec.PlanID, event)
	if err != nil {
		return agentos.PlanEvent{}, "", err
	}

	return planEvent, key, nil
}

func idempotencyHash(planID string, payload any) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("%w: marshal idempotency key: %w", agentos.ErrInvalidPlanEvent, err)
	}

	sum := sha256.Sum256(data)

	return planID + ":" + hex.EncodeToString(sum[:]), nil
}

// StateEventIdempotencyKey creates a stable key for activity retries and
// workflow replays that attempt to persist the same reducer transition.
func StateEventIdempotencyKey(planID string, event *StateEvent) (string, error) {
	if planID == "" {
		return "", fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}

	return idempotencyHash(planID, stateEventIdempotencyIdentity(event))
}

type stateEventIdempotencyFields struct {
	Kind                   EventKind                  `json:"kind"`
	NodeID                 string                     `json:"node_id,omitempty"`
	RunID                  string                     `json:"run_id,omitempty"`
	Reason                 string                     `json:"reason,omitempty"`
	Attempt                int32                      `json:"attempt,omitempty"`
	Expansion              *PlanDelta                 `json:"expansion,omitempty"`
	Artifacts              []agentos.ArtifactRef      `json:"artifacts,omitempty"`
	BudgetDelta            *agentos.PlanBudgetUsage   `json:"budget_delta,omitempty"`
	InputTrace             *InputResolutionTrace      `json:"input_trace,omitempty"`
	Capability             *CapabilitySelectionTrace  `json:"capability,omitempty"`
	ConditionTraces        []ConditionEvaluationTrace `json:"condition_traces,omitempty"`
	PreviousLifecycleState string                     `json:"previous_lifecycle_state,omitempty"`
	NextLifecycleState     string                     `json:"next_lifecycle_state,omitempty"`
	At                     *time.Time                 `json:"at,omitempty"`
}

func stateEventIdempotencyIdentity(event *StateEvent) stateEventIdempotencyFields {
	return stateEventIdempotencyFields{
		Kind:                   event.Kind,
		NodeID:                 event.NodeID,
		RunID:                  event.RunID,
		Reason:                 event.Reason,
		Attempt:                event.Attempt,
		Expansion:              idempotencyExpansion(event.Expansion),
		Artifacts:              event.Artifacts,
		BudgetDelta:            idempotencyBudgetDelta(event.BudgetDelta),
		InputTrace:             idempotencyInputTrace(event.InputTrace),
		Capability:             idempotencyCapability(&event.Capability),
		ConditionTraces:        event.ConditionTraces,
		PreviousLifecycleState: event.PreviousLifecycleState,
		NextLifecycleState:     event.NextLifecycleState,
		At:                     idempotencyTime(event.At),
	}
}

func idempotencyExpansion(expansion PlanDelta) *PlanDelta {
	if len(expansion.Nodes) == 0 && len(expansion.Edges) == 0 {
		return nil
	}

	return &expansion
}

func idempotencyBudgetDelta(delta agentos.PlanBudgetUsage) *agentos.PlanBudgetUsage {
	if delta.SpentCents == 0 {
		return nil
	}

	return &delta
}

func idempotencyInputTrace(trace InputResolutionTrace) *InputResolutionTrace {
	if trace.InputDigest == "" && len(trace.InputKeys) == 0 && trace.MappingCount == 0 && len(trace.Mappings) == 0 {
		return nil
	}

	return &trace
}

func idempotencyCapability(trace *CapabilitySelectionTrace) *CapabilitySelectionTrace {
	if trace.Capability == "" && trace.Backend.Kind == "" && trace.Backend.Name == "" && len(trace.Signals) == 0 && len(trace.Controls) == 0 && !trace.HasInputSchema && !trace.HasOutputSchema {
		return nil
	}

	return trace
}

func idempotencyTime(at time.Time) *time.Time {
	if at.IsZero() {
		return nil
	}

	return &at
}

// NodeStartIdempotencyKey creates the stable idempotency key for starting one
// backend-owned child run.
func NodeStartIdempotencyKey(planID, nodeID string, attempt int32) (string, error) {
	if planID == "" {
		return "", fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}

	if nodeID == "" {
		return "", fmt.Errorf("%w: node id is required", agentos.ErrInvalidRunPlan)
	}

	if attempt <= 0 {
		return "", fmt.Errorf("%w: node start attempt must be positive", agentos.ErrInvalidRunPlan)
	}

	return idempotencyHash(planID, struct {
		Operation string `json:"operation"`
		PlanID    string `json:"plan_id"`
		NodeID    string `json:"node_id"`
		Attempt   int32  `json:"attempt"`
	}{
		Operation: idempotencyOperationNodeStart,
		PlanID:    planID,
		NodeID:    nodeID,
		Attempt:   attempt,
	})
}

// NodeTimeoutControlIdempotencyKey creates the stable idempotency key for
// canceling a timed-out child run.
func NodeTimeoutControlIdempotencyKey(planID, nodeID, runID string) (string, error) {
	if planID == "" {
		return "", fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}

	if nodeID == "" {
		return "", fmt.Errorf("%w: node id is required", agentos.ErrInvalidRunPlan)
	}

	if runID == "" {
		return "", fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}

	return idempotencyHash(planID, struct {
		Operation string `json:"operation"`
		PlanID    string `json:"plan_id"`
		NodeID    string `json:"node_id"`
		RunID     string `json:"run_id"`
	}{
		Operation: idempotencyOperationNodeTimeout,
		PlanID:    planID,
		NodeID:    nodeID,
		RunID:     runID,
	})
}

// PlanTimeoutControlIdempotencyKey creates the parent idempotency key for
// propagating cancellation after a plan-level timeout guard trips.
func PlanTimeoutControlIdempotencyKey(planID string, startedAt time.Time, timeoutSeconds int64) (string, error) {
	if planID == "" {
		return "", fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}

	if startedAt.IsZero() {
		return "", fmt.Errorf("%w: plan started_at is required", agentos.ErrInvalidRunPlan)
	}

	if timeoutSeconds <= 0 {
		return "", fmt.Errorf("%w: plan timeout seconds must be positive", agentos.ErrInvalidRunPlan)
	}

	return idempotencyHash(planID, struct {
		Operation      string    `json:"operation"`
		PlanID         string    `json:"plan_id"`
		StartedAt      time.Time `json:"started_at"`
		TimeoutSeconds int64     `json:"timeout_seconds"`
	}{
		Operation:      idempotencyOperationPlanTimeout,
		PlanID:         planID,
		StartedAt:      startedAt,
		TimeoutSeconds: timeoutSeconds,
	})
}

// ArtifactPublishIdempotencyKey creates the stable idempotency key for publishing
// one node output artifact.
func ArtifactPublishIdempotencyKey(planID, nodeID, runID, artifactName string) (string, error) {
	if planID == "" {
		return "", fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}

	if nodeID == "" {
		return "", fmt.Errorf("%w: node id is required", agentos.ErrInvalidRunPlan)
	}

	if artifactName == "" {
		return "", fmt.Errorf("%w: artifact name is required", agentos.ErrInvalidArtifact)
	}

	return idempotencyHash(planID, struct {
		Operation    string `json:"operation"`
		PlanID       string `json:"plan_id"`
		NodeID       string `json:"node_id"`
		RunID        string `json:"run_id,omitempty"`
		ArtifactName string `json:"artifact_name"`
	}{
		Operation:    idempotencyOperationArtifactPublish,
		PlanID:       planID,
		NodeID:       nodeID,
		RunID:        runID,
		ArtifactName: artifactName,
	})
}

// BudgetExceededControlIdempotencyKey creates the parent idempotency key for
// propagating cancellation after a plan budget guard trips.
func BudgetExceededControlIdempotencyKey(planID string, usage agentos.PlanBudgetUsage, budgetCents int64) (string, error) {
	if planID == "" {
		return "", fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}

	return idempotencyHash(planID, struct {
		Operation   string `json:"operation"`
		PlanID      string `json:"plan_id"`
		SpentCents  int64  `json:"spent_cents"`
		BudgetCents int64  `json:"budget_cents"`
	}{
		Operation:   idempotencyOperationBudgetExceeded,
		PlanID:      planID,
		SpentCents:  usage.SpentCents,
		BudgetCents: budgetCents,
	})
}

// PlanSignalCancelControlIdempotencyKey creates the parent idempotency key for
// cancellation propagated by a terminal plan signal such as operator reject.
func PlanSignalCancelControlIdempotencyKey(planID string, signal *agentos.Signal) (string, error) {
	if planID == "" {
		return "", fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}

	if err := ValidatePlanSignal(signal); err != nil {
		return "", err
	}

	if signal.Type != agentos.SignalPlanReject {
		return "", fmt.Errorf("%w: signal %q does not cancel active plan nodes", agentos.ErrInvalidSignal, signal.Type)
	}

	return idempotencyHash(planID, struct {
		Operation      string             `json:"operation"`
		PlanID         string             `json:"plan_id"`
		SignalType     agentos.SignalType `json:"signal_type"`
		IdempotencyKey string             `json:"idempotency_key"`
		ActorID        string             `json:"actor_id"`
	}{
		Operation:      idempotencyOperationPlanSignalCancel,
		PlanID:         planID,
		SignalType:     signal.Type,
		IdempotencyKey: signal.IdempotencyKey,
		ActorID:        signal.ActorID,
	})
}

// NodeControlIdempotencyKey creates the stable idempotency key for propagating
// one plan-level control operation to one backend-owned child run.
func NodeControlIdempotencyKey(planID, nodeID string, operation agentos.ControlOperation, parentKey string) (string, error) {
	if planID == "" {
		return "", fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}

	if nodeID == "" {
		return "", fmt.Errorf("%w: node id is required", agentos.ErrInvalidRunPlan)
	}

	if err := agentos.ValidateControlRequest(&agentos.ControlRequest{Operation: operation}); err != nil {
		return "", err
	}

	return idempotencyHash(planID, struct {
		Operation string                   `json:"operation"`
		PlanID    string                   `json:"plan_id"`
		NodeID    string                   `json:"node_id"`
		Control   agentos.ControlOperation `json:"control"`
		ParentKey string                   `json:"parent_key,omitempty"`
	}{
		Operation: idempotencyOperationNodeControl,
		PlanID:    planID,
		NodeID:    nodeID,
		Control:   operation,
		ParentKey: parentKey,
	})
}

func planEventType(kind EventKind) (agentos.EventType, error) {
	eventTypes := map[EventKind]agentos.EventType{
		EventPlanStarted:         agentos.EventPlanStarted,
		EventPlanBlocked:         agentos.EventPlanBlocked,
		EventPlanExpanded:        agentos.EventPlanExpanded,
		EventPlanApproved:        agentos.EventPlanApproved,
		EventPlanRejected:        agentos.EventPlanRejected,
		EventPlanSucceeded:       agentos.EventPlanSucceeded,
		EventPlanFailed:          agentos.EventPlanFailed,
		EventPlanCanceled:        agentos.EventPlanCanceled,
		EventNodeReady:           agentos.EventPlanNodeReady,
		EventNodeStarted:         agentos.EventPlanNodeStarted,
		EventNodeSucceeded:       agentos.EventPlanNodeSucceeded,
		EventNodeFailed:          agentos.EventPlanNodeFailed,
		EventNodeRetryScheduled:  agentos.EventPlanNodeRetryScheduled,
		EventNodeSkipped:         agentos.EventPlanNodeSkipped,
		EventNodeCanceled:        agentos.EventPlanNodeCanceled,
		EventNodeInputResolved:   agentos.EventNodeInputResolved,
		EventCapabilitySelected:  agentos.EventCapabilitySelected,
		EventConditionsEvaluated: agentos.EventConditionEvaluated,
		EventArtifactsPublished:  agentos.EventNodeOutputPublished,
		EventBudgetReported:      agentos.EventUsageReported,
	}

	eventType, ok := eventTypes[kind]
	if !ok {
		return "", fmt.Errorf("%w: unknown plan event kind %q", agentos.ErrInvalidPlanEvent, kind)
	}

	return eventType, nil
}

func planExpansionPayload(delta PlanDelta) map[string]any {
	nodeIDs := make([]string, 0, len(delta.Nodes))
	for i := range delta.Nodes {
		node := &delta.Nodes[i]
		nodeIDs = append(nodeIDs, node.NodeID)
	}

	edges := make([]map[string]any, 0, len(delta.Edges))
	for _, edge := range delta.Edges {
		item := map[string]any{
			"from": edge.From,
			"to":   edge.To,
		}
		if edge.EdgeID != "" {
			item["edge_id"] = edge.EdgeID
		}

		if edge.On != "" {
			item["on"] = edge.On
		}

		edges = append(edges, item)
	}

	return map[string]any{
		"node_ids": nodeIDs,
		"edges":    edges,
	}
}
