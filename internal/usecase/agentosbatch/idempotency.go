package agentosbatch

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ValidateWorksetStartIdempotency verifies a repeated start request.
func ValidateWorksetStartIdempotency(existing, requested *agentos.WorksetSpec) error {
	if existing.WorksetID != requested.WorksetID {
		return fmt.Errorf("%w: workset idempotency key belongs to workset %q", agentos.ErrInvalidWorkset, existing.WorksetID)
	}

	existingJSON, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("%w: marshal existing workset request: %w", agentos.ErrInvalidWorkset, err)
	}

	requestedJSON, err := json.Marshal(requested)
	if err != nil {
		return fmt.Errorf("%w: marshal requested workset request: %w", agentos.ErrInvalidWorkset, err)
	}

	if !bytes.Equal(existingJSON, requestedJSON) {
		return fmt.Errorf("%w: workset %q idempotency key was reused with a different request", agentos.ErrInvalidWorkset, existing.WorksetID)
	}

	return nil
}
