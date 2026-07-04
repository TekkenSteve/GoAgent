package agentosaction

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ValidateActionStartIdempotency verifies that a repeated action request is
// the same durable request that originally claimed the idempotency key.
func ValidateActionStartIdempotency(existing, requested *agentos.GovernedActionSpec) error {
	if existing.ActionID != requested.ActionID {
		return fmt.Errorf("%w: action idempotency key belongs to action %q", agentos.ErrInvalidGovernedAction, existing.ActionID)
	}

	existingJSON, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("%w: marshal existing action request: %w", agentos.ErrInvalidGovernedAction, err)
	}

	requestedJSON, err := json.Marshal(requested)
	if err != nil {
		return fmt.Errorf("%w: marshal requested action request: %w", agentos.ErrInvalidGovernedAction, err)
	}

	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: action %q idempotency key was reused with a different request", agentos.ErrInvalidGovernedAction, existing.ActionID)
	}

	return nil
}
