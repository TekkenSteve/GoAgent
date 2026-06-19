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
