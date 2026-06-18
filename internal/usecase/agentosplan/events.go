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
	idempotencyOperationNodeStart   = "node_start"
	idempotencyOperationNodeControl = "node_control"
)

// PlanEventFromStateEvent maps a deterministic reducer transition to the public
// PlanEvent envelope used by durable event stores and UI timelines.
func PlanEventFromStateEvent(spec agentos.RunPlanSpec, status agentos.RunPlanStatus, event StateEvent) (agentos.PlanEvent, string, error) {
	eventType, err := planEventType(event.Kind)
	if err != nil {
		return agentos.PlanEvent{}, "", err
	}

	at := event.At
	if at.IsZero() {
		at = status.UpdatedAt
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	payload := map[string]any{
		"lifecycle_state": status.LifecycleState,
		"plan_id":         spec.PlanID,
	}
	if event.NodeID != "" {
		payload["node_id"] = event.NodeID
	}
	if event.RunID != "" {
		payload["run_id"] = event.RunID
	}
	if event.Reason != "" {
		payload["reason"] = event.Reason
	}
	if len(event.Artifacts) > 0 {
		payload["artifacts"] = event.Artifacts
	}

	planEvent := agentos.PlanEvent{
		Event: agentos.Event{
			EventType: eventType,
			RunID:     event.RunID,
			ThreadID:  spec.ThreadID,
			Timestamp: at,
			Source:    "agentos.plan",
			Payload:   payload,
		},
		PlanID: spec.PlanID,
		NodeID: event.NodeID,
	}

	key, err := StateEventIdempotencyKey(spec.PlanID, event)
	if err != nil {
		return agentos.PlanEvent{}, "", err
	}

	return planEvent, key, nil
}

// StateEventIdempotencyKey creates a stable key for activity retries and
// workflow replays that attempt to persist the same reducer transition.
func StateEventIdempotencyKey(planID string, event StateEvent) (string, error) {
	if planID == "" {
		return "", fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	data, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("%w: marshal state event: %s", agentos.ErrInvalidPlanEvent, err)
	}
	sum := sha256.Sum256(data)

	return planID + ":" + hex.EncodeToString(sum[:]), nil
}

// NodeStartIdempotencyKey creates the stable idempotency key for starting one
// backend-owned child run.
func NodeStartIdempotencyKey(planID, nodeID string) (string, error) {
	if planID == "" {
		return "", fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if nodeID == "" {
		return "", fmt.Errorf("%w: node id is required", agentos.ErrInvalidRunPlan)
	}
	data, err := json.Marshal(struct {
		Operation string `json:"operation"`
		PlanID    string `json:"plan_id"`
		NodeID    string `json:"node_id"`
	}{
		Operation: idempotencyOperationNodeStart,
		PlanID:    planID,
		NodeID:    nodeID,
	})
	if err != nil {
		return "", fmt.Errorf("%w: marshal node start key: %s", agentos.ErrInvalidPlanEvent, err)
	}
	sum := sha256.Sum256(data)

	return planID + ":" + hex.EncodeToString(sum[:]), nil
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
	if err := agentos.ValidateControlRequest(agentos.ControlRequest{Operation: operation}); err != nil {
		return "", err
	}
	data, err := json.Marshal(struct {
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
	if err != nil {
		return "", fmt.Errorf("%w: marshal node control key: %s", agentos.ErrInvalidPlanEvent, err)
	}
	sum := sha256.Sum256(data)

	return planID + ":" + hex.EncodeToString(sum[:]), nil
}

func planEventType(kind EventKind) (agentos.EventType, error) {
	switch kind {
	case EventPlanStarted:
		return agentos.EventPlanStarted, nil
	case EventPlanBlocked:
		return agentos.EventPlanBlocked, nil
	case EventPlanSucceeded:
		return agentos.EventPlanSucceeded, nil
	case EventPlanFailed:
		return agentos.EventPlanFailed, nil
	case EventPlanCanceled:
		return agentos.EventPlanCanceled, nil
	case EventNodeReady:
		return agentos.EventPlanNodeReady, nil
	case EventNodeStarted:
		return agentos.EventPlanNodeStarted, nil
	case EventNodeSucceeded:
		return agentos.EventPlanNodeSucceeded, nil
	case EventNodeFailed:
		return agentos.EventPlanNodeFailed, nil
	case EventNodeSkipped:
		return agentos.EventPlanNodeSkipped, nil
	case EventNodeCanceled:
		return agentos.EventPlanNodeCanceled, nil
	case EventArtifactsPublished:
		return agentos.EventNodeOutputPublished, nil
	default:
		return "", fmt.Errorf("%w: unknown plan event kind %q", agentos.ErrInvalidPlanEvent, kind)
	}
}
