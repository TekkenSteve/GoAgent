package agentosplan

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// ValidateCapability checks the public capability declaration before it is
// inserted into a catalog or used by a RunPlan validator.
func ValidateCapability(capability agentos.Capability) error {
	if capability.Backend.Kind == "" || capability.Backend.Name == "" {
		return fmt.Errorf("%w: capability backend is required", agentos.ErrInvalidBackendRef)
	}
	if capability.Name == "" {
		return fmt.Errorf("%w: capability name is required", agentos.ErrCapabilityNotFound)
	}
	if err := validateRawJSON("input schema", capability.InputSchema); err != nil {
		return err
	}
	if err := validateRawJSON("output schema", capability.OutputSchema); err != nil {
		return err
	}

	return nil
}

// RegisterCapabilities registers a batch of backend capabilities into a
// durable catalog using content-addressed idempotency keys.
func RegisterCapabilities(ctx context.Context, registry CapabilityRegistry, capabilities []agentos.Capability) error {
	if registry == nil {
		return fmt.Errorf("%w: capability registry is required", agentos.ErrCapabilityNotFound)
	}
	for _, capability := range capabilities {
		key, err := CapabilityRegistrationIdempotencyKey(capability)
		if err != nil {
			return err
		}
		if _, _, err := registry.RegisterCapability(ctx, capability, key); err != nil {
			return err
		}
	}

	return nil
}

// CapabilityRegistrationIdempotencyKey returns a stable key for one capability
// declaration. A changed declaration intentionally produces a different key.
func CapabilityRegistrationIdempotencyKey(capability agentos.Capability) (string, error) {
	if err := ValidateCapability(capability); err != nil {
		return "", err
	}
	data, err := json.Marshal(capability)
	if err != nil {
		return "", fmt.Errorf("%w: marshal capability registration: %s", agentos.ErrInvalidRunPlan, err)
	}
	sum := sha256.Sum256(data)

	return "capability:" + hex.EncodeToString(sum[:]), nil
}

// ValidateCapabilityRegistrationIdempotency rejects replaying one
// idempotency key for a different capability declaration.
func ValidateCapabilityRegistrationIdempotency(existing, requested agentos.Capability) error {
	existingJSON, err := json.Marshal(existing)
	if err != nil {
		return fmt.Errorf("%w: marshal existing capability: %s", agentos.ErrInvalidRunPlan, err)
	}
	requestedJSON, err := json.Marshal(requested)
	if err != nil {
		return fmt.Errorf("%w: marshal requested capability: %s", agentos.ErrInvalidRunPlan, err)
	}
	if string(existingJSON) != string(requestedJSON) {
		return fmt.Errorf("%w: capability registration idempotency key reused with different declaration", agentos.ErrInvalidRunPlan)
	}

	return nil
}

func validateRawJSON(label string, raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	if !json.Valid(raw) {
		return fmt.Errorf("%w: invalid capability %s", agentos.ErrInvalidRunPlan, label)
	}

	return nil
}
