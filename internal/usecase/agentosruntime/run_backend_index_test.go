package agentosruntime

import (
	"errors"
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

func TestValidateRunBackendIndexIdempotencyAcceptsSameOwnership(t *testing.T) {
	t.Parallel()

	record := testRunBackendIndexRecord()
	if err := ValidateRunBackendIndexIdempotency(&record, &record); err != nil {
		t.Fatalf("ValidateRunBackendIndexIdempotency: %v", err)
	}
}

func TestValidateRunBackendIndexIdempotencyRejectsDifferentRun(t *testing.T) {
	t.Parallel()

	existing := testRunBackendIndexRecord()
	requested := existing
	requested.RunID = "run-2"

	err := ValidateRunBackendIndexIdempotency(&existing, &requested)
	if !errors.Is(err, agentoscore.ErrInvalidRunSpec) {
		t.Fatalf("error = %v, want ErrInvalidRunSpec", err)
	}
}

func TestValidateRunBackendIndexIdempotencyRejectsDifferentPlanNode(t *testing.T) {
	t.Parallel()

	existing := testRunBackendIndexRecord()
	requested := existing
	requested.NodeID = "node-2"

	err := ValidateRunBackendIndexIdempotency(&existing, &requested)
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidateRunBackendIndexIdempotencyRejectsDifferentScope(t *testing.T) {
	t.Parallel()

	existing := testRunBackendIndexRecord()
	requested := existing
	requested.AccountID = "account-2"

	err := ValidateRunBackendIndexIdempotency(&existing, &requested)
	if !errors.Is(err, agentoscore.ErrInvalidRunSpec) {
		t.Fatalf("error = %v, want ErrInvalidRunSpec", err)
	}
}

func TestValidateRunBackendIndexIdempotencyRejectsDifferentBackend(t *testing.T) {
	t.Parallel()

	existing := testRunBackendIndexRecord()
	requested := existing
	requested.BackendName = "other"

	err := ValidateRunBackendIndexIdempotency(&existing, &requested)
	if !errors.Is(err, agentoscore.ErrInvalidBackendRef) {
		t.Fatalf("error = %v, want ErrInvalidBackendRef", err)
	}
}

func TestValidateRunBackendIndexIdempotencyRejectsDifferentKey(t *testing.T) {
	t.Parallel()

	existing := testRunBackendIndexRecord()
	requested := existing
	requested.IdempotencyKey = "other-key"

	err := ValidateRunBackendIndexIdempotency(&existing, &requested)
	if !errors.Is(err, agentoscore.ErrInvalidRunSpec) {
		t.Fatalf("error = %v, want ErrInvalidRunSpec", err)
	}
}

func testRunBackendIndexRecord() entity.RunBackendIndexRecord {
	return entity.RunBackendIndexRecord{
		RunID:          "run-1",
		PlanID:         "plan-1",
		NodeID:         "node-1",
		ThreadID:       "thread-1",
		AccountID:      "account-1",
		ProjectID:      "project-1",
		BackendKind:    string(agentos.BackendKindNative),
		BackendName:    agentos.BackendNameGoAgentNative,
		IdempotencyKey: "node-start-key",
		LifecycleState: "running",
	}
}
