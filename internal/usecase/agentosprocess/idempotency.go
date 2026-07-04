package agentosprocess

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ValidateProcessStartIdempotency verifies that a repeated process start
// request is the same durable request that originally claimed the idempotency
// key.
func ValidateProcessStartIdempotency(existing, requested *agentos.ProcessSpec) error {
	if existing.ProcessID != requested.ProcessID {
		return fmt.Errorf("%w: process idempotency key belongs to process %q", agentos.ErrInvalidProcess, existing.ProcessID)
	}

	if existing.IdempotencyKey != requested.IdempotencyKey {
		return fmt.Errorf("%w: process %q was started with a different idempotency key", agentos.ErrInvalidProcess, existing.ProcessID)
	}

	existingJSON, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("%w: marshal existing process start request: %w", agentos.ErrInvalidProcess, err)
	}

	requestedJSON, err := json.Marshal(requested)
	if err != nil {
		return fmt.Errorf("%w: marshal requested process start request: %w", agentos.ErrInvalidProcess, err)
	}

	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: process %q idempotency key was reused with a different request", agentos.ErrInvalidProcess, existing.ProcessID)
	}

	return nil
}

// ValidateProcessEventIdempotency verifies that a repeated durable event append
// request carries the same event body.
func ValidateProcessEventIdempotency(existing, requested *agentos.ProcessEvent) error {
	if existing.EventID != requested.EventID {
		return fmt.Errorf("%w: process event idempotency key belongs to event %q", agentos.ErrInvalidProcess, existing.EventID)
	}

	if existing.Sequence != requested.Sequence {
		return fmt.Errorf("%w: process event idempotency key belongs to sequence %d", agentos.ErrInvalidProcess, existing.Sequence)
	}

	existingJSON, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("%w: marshal existing process event: %w", agentos.ErrInvalidProcess, err)
	}

	requestedJSON, err := json.Marshal(requested)
	if err != nil {
		return fmt.Errorf("%w: marshal requested process event: %w", agentos.ErrInvalidProcess, err)
	}

	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: process event idempotency key was reused with a different event", agentos.ErrInvalidProcess)
	}

	return nil
}
