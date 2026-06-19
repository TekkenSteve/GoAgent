package agentosplan

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ValidatePlanStartIdempotency verifies that a repeated plan start request is
// the same durable request that originally claimed the idempotency key.
func ValidatePlanStartIdempotency(existing agentos.RunPlanSpec, requested agentos.RunPlanSpec) error {
	if existing.PlanID != requested.PlanID {
		return fmt.Errorf("%w: plan idempotency key belongs to plan %q", agentos.ErrInvalidRunPlan, existing.PlanID)
	}
	if existing.IdempotencyKey != requested.IdempotencyKey {
		return fmt.Errorf("%w: plan %q was started with a different idempotency key", agentos.ErrInvalidRunPlan, existing.PlanID)
	}

	existingJSON, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("%w: marshal existing plan start request: %s", agentos.ErrInvalidRunPlan, err)
	}
	requestedJSON, err := json.Marshal(requested)
	if err != nil {
		return fmt.Errorf("%w: marshal requested plan start request: %s", agentos.ErrInvalidRunPlan, err)
	}
	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: plan %q idempotency key was reused with a different request", agentos.ErrInvalidRunPlan, existing.PlanID)
	}

	return nil
}

// ValidateAuditIdempotency verifies that an audit idempotency key is replayed
// for the same control-plane action.
func ValidateAuditIdempotency(existing AuditRecord, requested AuditRecord) error {
	if existing.PlanID != requested.PlanID {
		return fmt.Errorf("%w: audit idempotency key belongs to plan %q", agentos.ErrInvalidRunPlan, existing.PlanID)
	}
	if existing.RunID != requested.RunID {
		return fmt.Errorf("%w: audit idempotency key belongs to run %q", agentos.ErrInvalidRunPlan, existing.RunID)
	}
	if existing.NodeID != requested.NodeID {
		return fmt.Errorf("%w: audit idempotency key belongs to node %q", agentos.ErrInvalidRunPlan, existing.NodeID)
	}
	if existing.ActorID != requested.ActorID {
		return fmt.Errorf("%w: audit idempotency key belongs to actor %q", agentos.ErrInvalidRunPlan, existing.ActorID)
	}
	if existing.Action != requested.Action {
		return fmt.Errorf("%w: audit idempotency key belongs to action %q", agentos.ErrInvalidRunPlan, existing.Action)
	}
	if existing.IdempotencyKey != requested.IdempotencyKey {
		return fmt.Errorf("%w: audit idempotency key mismatch", agentos.ErrInvalidRunPlan)
	}

	existingPayload, err := json.Marshal(normalizeAuditPayload(existing.Payload))
	if err != nil {
		return fmt.Errorf("%w: marshal existing audit payload: %s", agentos.ErrInvalidRunPlan, err)
	}
	requestedPayload, err := json.Marshal(normalizeAuditPayload(requested.Payload))
	if err != nil {
		return fmt.Errorf("%w: marshal requested audit payload: %s", agentos.ErrInvalidRunPlan, err)
	}
	if !bytes.Equal(existingPayload, requestedPayload) {
		return fmt.Errorf("%w: audit idempotency key was reused with a different payload", agentos.ErrInvalidRunPlan)
	}

	return nil
}

func normalizeAuditPayload(payload map[string]any) map[string]any {
	if payload == nil {
		return map[string]any{}
	}

	return payload
}
