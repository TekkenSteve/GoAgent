package agentosplan

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestCapabilityRegistrationIdempotencyKeyIsContentAddressed(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}
	capability := agentos.Capability{
		Backend:     ref,
		Name:        "run",
		InputSchema: json.RawMessage(`{"type":"object"}`),
	}

	first, err := CapabilityRegistrationIdempotencyKey(&capability)
	if err != nil {
		t.Fatalf("CapabilityRegistrationIdempotencyKey first: %v", err)
	}

	second, err := CapabilityRegistrationIdempotencyKey(&capability)
	if err != nil {
		t.Fatalf("CapabilityRegistrationIdempotencyKey second: %v", err)
	}

	if first != second {
		t.Fatalf("idempotency key changed: %q != %q", first, second)
	}

	capability.Description = "updated"

	changed, err := CapabilityRegistrationIdempotencyKey(&capability)
	if err != nil {
		t.Fatalf("CapabilityRegistrationIdempotencyKey changed: %v", err)
	}

	if changed == first {
		t.Fatalf("changed capability reused idempotency key %q", changed)
	}
}

func TestValidateCapabilityRejectsInvalidDeclaration(t *testing.T) {
	t.Parallel()

	if err := ValidateCapability(&agentos.Capability{Name: "run"}); !errors.Is(err, agentoscore.ErrInvalidBackendRef) {
		t.Fatalf("missing backend error = %v, want ErrInvalidBackendRef", err)
	}

	if err := ValidateCapability(&agentos.Capability{
		Backend: agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"},
	}); !errors.Is(err, agentoscore.ErrCapabilityNotFound) {
		t.Fatalf("missing name error = %v, want ErrCapabilityNotFound", err)
	}

	if err := ValidateCapability(&agentos.Capability{
		Backend:     agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"},
		Name:        "run",
		InputSchema: json.RawMessage(`{`),
	}); !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("invalid schema error = %v, want ErrInvalidRunPlan", err)
	}

	if err := ValidateCapability(&agentos.Capability{
		Backend: agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"},
		Name:    agentos.CapabilityRunBatch,
		Limits:  agentos.CapabilityLimits{MaxBatchItems: -1},
	}); !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("invalid batch limit error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestStaticCapabilityCatalogRegistersAndReads(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}

	catalog, err := NewStaticCapabilityCatalog(nil)
	if err != nil {
		t.Fatalf("NewStaticCapabilityCatalog: %v", err)
	}

	if _, created, err := catalog.RegisterCapability(context.Background(), &agentos.Capability{
		Backend:  ref,
		Name:     "run",
		Controls: []agentoscore.ControlOperation{agentoscore.ControlCancel},
	}, ""); err != nil || !created {
		t.Fatalf("RegisterCapability created=%v err=%v", created, err)
	}

	capability, ok, err := catalog.GetCapability(context.Background(), ref, "run")
	if err != nil {
		t.Fatalf("GetCapability: %v", err)
	}

	if !ok || len(capability.Controls) != 1 || capability.Controls[0] != agentoscore.ControlCancel {
		t.Fatalf("capability = %#v ok=%v", capability, ok)
	}
}

func TestRegisterCapabilitiesRequiresRegistry(t *testing.T) {
	t.Parallel()

	err := RegisterCapabilities(context.Background(), nil, []agentos.Capability{
		{
			Backend: agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"},
			Name:    "run",
		},
	})
	if !errors.Is(err, agentoscore.ErrCapabilityNotFound) {
		t.Fatalf("error = %v, want ErrCapabilityNotFound", err)
	}
}

func TestValidateCapabilityRegistrationIdempotencyRejectsDifferentDeclaration(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}
	existing := agentos.Capability{Backend: ref, Name: "run", Description: "v1"}
	requested := agentos.Capability{Backend: ref, Name: "run", Description: "v2"}

	err := ValidateCapabilityRegistrationIdempotency(&existing, &requested)
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}
