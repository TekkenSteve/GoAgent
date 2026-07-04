package memory

import (
	"errors"
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
)

const RUNNING = "running"

func TestAgentOSRunIndexRejectsRunOwnershipOverwrite(t *testing.T) {
	t.Parallel()

	index := NewAgentOSRunIndex()

	spec := nativeRunSpec("run-start-1")
	if err := bindRun(t, index, &spec, "created"); err != nil {
		t.Fatalf("Bind first: %v", err)
	}

	if err := bindRun(t, index, &spec, "created"); err != nil {
		t.Fatalf("Bind replay: %v", err)
	}

	spec.Backend = agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "http-agent"}
	if err := bindRun(t, index, &spec, "created"); !errors.Is(err, agentoscore.ErrInvalidBackendRef) {
		t.Fatalf("Bind changed backend error = %v, want ErrInvalidBackendRef", err)
	}
}

func TestAgentOSRunIndexRequiresIdempotencyKey(t *testing.T) {
	t.Parallel()

	index := NewAgentOSRunIndex()

	spec := nativeRunSpec("")
	err := bindRun(t, index, &spec, "created")

	if !errors.Is(err, agentoscore.ErrInvalidRunSpec) {
		t.Fatalf("Bind error = %v, want ErrInvalidRunSpec", err)
	}
}

func testAgentOSRunIndexRejectsMutation(t *testing.T, mutate func(*agentos.RunSpec), errMsg string) {
	t.Helper()

	index := NewAgentOSRunIndex()

	spec := nativeRunSpec("run-start-1")
	if err := bindRun(t, index, &spec, "created"); err != nil {
		t.Fatalf("Bind first: %v", err)
	}

	mutate(&spec)

	if err := bindRun(t, index, &spec, "created"); !errors.Is(err, agentoscore.ErrInvalidRunSpec) {
		t.Fatalf("%s error = %v, want ErrInvalidRunSpec", errMsg, err)
	}
}

func TestAgentOSRunIndexRejectsRunIDWithDifferentIdempotencyKey(t *testing.T) {
	t.Parallel()
	testAgentOSRunIndexRejectsMutation(t, func(spec *agentos.RunSpec) {
		spec.IdempotencyKey = "run-start-2"
	}, "Bind changed key")
}

func TestAgentOSRunIndexRejectsIdempotencyKeyWithDifferentRunID(t *testing.T) {
	t.Parallel()
	testAgentOSRunIndexRejectsMutation(t, func(spec *agentos.RunSpec) {
		spec.RunID = "run-2"
	}, "Bind reused key")
}

func TestAgentOSRunIndexUpdatesStandaloneLifecycleWithoutDowngrade(t *testing.T) {
	t.Parallel()

	index := NewAgentOSRunIndex()

	spec := nativeRunSpec("run-start-1")
	if err := bindRun(t, index, &spec, agentosruntime.RunBackendLifecycleClaiming); err != nil {
		t.Fatalf("Bind claim: %v", err)
	}

	if err := bindRun(t, index, &spec, "running"); err != nil {
		t.Fatalf("Bind running: %v", err)
	}

	ownership, exists, err := index.GetRunBackend(t.Context(), spec.RunID)
	if err != nil || !exists {
		t.Fatalf("GetRunBackend exists=%v err=%v", exists, err)
	}

	if ownership.LifecycleState != RUNNING {
		t.Fatalf("lifecycle = %q, want running", ownership.LifecycleState)
	}

	if err := bindRun(t, index, &spec, agentosruntime.RunBackendLifecycleClaiming); err != nil {
		t.Fatalf("Bind replay claim: %v", err)
	}

	ownership, exists, err = index.GetRunBackend(t.Context(), spec.RunID)
	if err != nil || !exists {
		t.Fatalf("GetRunBackend replay exists=%v err=%v", exists, err)
	}

	if ownership.LifecycleState != RUNNING {
		t.Fatalf("lifecycle after replay claim = %q, want running", ownership.LifecycleState)
	}
}

func TestAgentOSRunIndexRejectsPlanNodeOwnershipChange(t *testing.T) {
	t.Parallel()

	index := NewAgentOSRunIndex()
	spec := nativeRunSpec("node-start-1")

	if err := bindPlanNode(t, index, &spec, "node-1", "running"); err != nil {
		t.Fatalf("BindPlanNode first: %v", err)
	}

	if err := bindPlanNode(t, index, &spec, "node-1", "running"); err != nil {
		t.Fatalf("BindPlanNode replay: %v", err)
	}

	if err := bindPlanNode(t, index, &spec, "node-2", "running"); !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("BindPlanNode changed node error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestAgentOSRunIndexUpdatesPlanNodeLifecycleWithoutDowngrade(t *testing.T) {
	t.Parallel()

	index := NewAgentOSRunIndex()

	spec := nativeRunSpec("node-start-1")
	if err := bindPlanNode(t, index, &spec, "node-1", agentosruntime.RunBackendLifecycleClaiming); err != nil {
		t.Fatalf("BindPlanNode claim: %v", err)
	}

	if err := bindPlanNode(t, index, &spec, "node-1", "running"); err != nil {
		t.Fatalf("BindPlanNode running: %v", err)
	}

	ownership, exists, err := index.GetRunBackend(t.Context(), spec.RunID)
	if err != nil || !exists {
		t.Fatalf("GetRunBackend exists=%v err=%v", exists, err)
	}

	if ownership.LifecycleState != RUNNING {
		t.Fatalf("lifecycle = %q, want running", ownership.LifecycleState)
	}

	if err := bindPlanNode(t, index, &spec, "node-1", agentosruntime.RunBackendLifecycleClaiming); err != nil {
		t.Fatalf("BindPlanNode replay claim: %v", err)
	}

	ownership, exists, err = index.GetRunBackend(t.Context(), spec.RunID)
	if err != nil || !exists {
		t.Fatalf("GetRunBackend replay exists=%v err=%v", exists, err)
	}

	if ownership.LifecycleState != RUNNING {
		t.Fatalf("lifecycle after replay claim = %q, want running", ownership.LifecycleState)
	}
}

func nativeRunSpec(idempotencyKey string) agentos.RunSpec {
	return agentos.RunSpec{
		RunID:          "run-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: idempotencyKey,
		Backend:        agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}
}

func bindRun(t *testing.T, index *AgentOSRunIndex, spec *agentos.RunSpec, lifecycle string) error {
	t.Helper()

	status := agentos.RunStatus{RunID: spec.RunID, LifecycleState: lifecycle}

	return index.Bind(t.Context(), spec, &status)
}

func bindPlanNode(t *testing.T, index *AgentOSRunIndex, spec *agentos.RunSpec, nodeID, lifecycle string) error {
	t.Helper()

	status := agentos.RunStatus{RunID: spec.RunID, LifecycleState: lifecycle}

	return index.BindPlanNode(t.Context(), "plan-1", nodeID, spec, &status)
}
