package memory

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
)

func TestAgentOSRunIndexRejectsRunOwnershipOverwrite(t *testing.T) {
	index := NewAgentOSRunIndex()
	spec := agentos.RunSpec{
		RunID:          "run-1",
		IdempotencyKey: "run-start-1",
		Backend:        agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}
	if err := index.Bind(t.Context(), spec, agentos.RunStatus{RunID: spec.RunID, LifecycleState: "created"}); err != nil {
		t.Fatalf("Bind first: %v", err)
	}
	if err := index.Bind(t.Context(), spec, agentos.RunStatus{RunID: spec.RunID, LifecycleState: "created"}); err != nil {
		t.Fatalf("Bind replay: %v", err)
	}

	spec.Backend = agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "http-agent"}
	if err := index.Bind(t.Context(), spec, agentos.RunStatus{RunID: spec.RunID, LifecycleState: "created"}); !errors.Is(err, agentos.ErrInvalidBackendRef) {
		t.Fatalf("Bind changed backend error = %v, want ErrInvalidBackendRef", err)
	}
}

func TestAgentOSRunIndexRequiresIdempotencyKey(t *testing.T) {
	index := NewAgentOSRunIndex()
	err := index.Bind(t.Context(), agentos.RunSpec{
		RunID:   "run-1",
		Backend: agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}, agentos.RunStatus{RunID: "run-1", LifecycleState: "created"})
	if !errors.Is(err, agentos.ErrInvalidRunSpec) {
		t.Fatalf("Bind error = %v, want ErrInvalidRunSpec", err)
	}
}

func TestAgentOSRunIndexRejectsRunIDWithDifferentIdempotencyKey(t *testing.T) {
	index := NewAgentOSRunIndex()
	spec := agentos.RunSpec{
		RunID:          "run-1",
		IdempotencyKey: "run-start-1",
		Backend:        agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}
	if err := index.Bind(t.Context(), spec, agentos.RunStatus{RunID: spec.RunID, LifecycleState: "created"}); err != nil {
		t.Fatalf("Bind first: %v", err)
	}

	spec.IdempotencyKey = "run-start-2"
	if err := index.Bind(t.Context(), spec, agentos.RunStatus{RunID: spec.RunID, LifecycleState: "created"}); !errors.Is(err, agentos.ErrInvalidRunSpec) {
		t.Fatalf("Bind changed key error = %v, want ErrInvalidRunSpec", err)
	}
}

func TestAgentOSRunIndexRejectsIdempotencyKeyWithDifferentRunID(t *testing.T) {
	index := NewAgentOSRunIndex()
	spec := agentos.RunSpec{
		RunID:          "run-1",
		IdempotencyKey: "run-start-1",
		Backend:        agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}
	if err := index.Bind(t.Context(), spec, agentos.RunStatus{RunID: spec.RunID, LifecycleState: "created"}); err != nil {
		t.Fatalf("Bind first: %v", err)
	}

	spec.RunID = "run-2"
	if err := index.Bind(t.Context(), spec, agentos.RunStatus{RunID: spec.RunID, LifecycleState: "created"}); !errors.Is(err, agentos.ErrInvalidRunSpec) {
		t.Fatalf("Bind reused key error = %v, want ErrInvalidRunSpec", err)
	}
}

func TestAgentOSRunIndexUpdatesStandaloneLifecycleWithoutDowngrade(t *testing.T) {
	index := NewAgentOSRunIndex()
	spec := agentos.RunSpec{
		RunID:          "run-1",
		IdempotencyKey: "run-start-1",
		Backend:        agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}
	if err := index.Bind(t.Context(), spec, agentos.RunStatus{
		RunID:          spec.RunID,
		LifecycleState: agentosruntime.RunBackendLifecycleClaiming,
	}); err != nil {
		t.Fatalf("Bind claim: %v", err)
	}
	if err := index.Bind(t.Context(), spec, agentos.RunStatus{
		RunID:          spec.RunID,
		LifecycleState: "running",
	}); err != nil {
		t.Fatalf("Bind running: %v", err)
	}
	ownership, exists, err := index.GetRunBackend(t.Context(), spec.RunID)
	if err != nil || !exists {
		t.Fatalf("GetRunBackend exists=%v err=%v", exists, err)
	}
	if ownership.LifecycleState != "running" {
		t.Fatalf("lifecycle = %q, want running", ownership.LifecycleState)
	}

	if err := index.Bind(t.Context(), spec, agentos.RunStatus{
		RunID:          spec.RunID,
		LifecycleState: agentosruntime.RunBackendLifecycleClaiming,
	}); err != nil {
		t.Fatalf("Bind replay claim: %v", err)
	}
	ownership, exists, err = index.GetRunBackend(t.Context(), spec.RunID)
	if err != nil || !exists {
		t.Fatalf("GetRunBackend replay exists=%v err=%v", exists, err)
	}
	if ownership.LifecycleState != "running" {
		t.Fatalf("lifecycle after replay claim = %q, want running", ownership.LifecycleState)
	}
}

func TestAgentOSRunIndexRejectsPlanNodeOwnershipChange(t *testing.T) {
	index := NewAgentOSRunIndex()
	spec := agentos.RunSpec{
		RunID:          "run-1",
		IdempotencyKey: "node-start-1",
		Backend:        agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}
	status := agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running"}
	if err := index.BindPlanNode(t.Context(), "plan-1", "node-1", spec, status); err != nil {
		t.Fatalf("BindPlanNode first: %v", err)
	}
	if err := index.BindPlanNode(t.Context(), "plan-1", "node-1", spec, status); err != nil {
		t.Fatalf("BindPlanNode replay: %v", err)
	}

	if err := index.BindPlanNode(t.Context(), "plan-1", "node-2", spec, status); !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("BindPlanNode changed node error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestAgentOSRunIndexUpdatesPlanNodeLifecycleWithoutDowngrade(t *testing.T) {
	index := NewAgentOSRunIndex()
	spec := agentos.RunSpec{
		RunID:          "run-1",
		IdempotencyKey: "node-start-1",
		Backend:        agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}
	if err := index.BindPlanNode(t.Context(), "plan-1", "node-1", spec, agentos.RunStatus{
		RunID:          spec.RunID,
		LifecycleState: agentosruntime.RunBackendLifecycleClaiming,
	}); err != nil {
		t.Fatalf("BindPlanNode claim: %v", err)
	}
	if err := index.BindPlanNode(t.Context(), "plan-1", "node-1", spec, agentos.RunStatus{
		RunID:          spec.RunID,
		LifecycleState: "running",
	}); err != nil {
		t.Fatalf("BindPlanNode running: %v", err)
	}
	ownership, exists, err := index.GetRunBackend(t.Context(), spec.RunID)
	if err != nil || !exists {
		t.Fatalf("GetRunBackend exists=%v err=%v", exists, err)
	}
	if ownership.LifecycleState != "running" {
		t.Fatalf("lifecycle = %q, want running", ownership.LifecycleState)
	}

	if err := index.BindPlanNode(t.Context(), "plan-1", "node-1", spec, agentos.RunStatus{
		RunID:          spec.RunID,
		LifecycleState: agentosruntime.RunBackendLifecycleClaiming,
	}); err != nil {
		t.Fatalf("BindPlanNode replay claim: %v", err)
	}
	ownership, exists, err = index.GetRunBackend(t.Context(), spec.RunID)
	if err != nil || !exists {
		t.Fatalf("GetRunBackend replay exists=%v err=%v", exists, err)
	}
	if ownership.LifecycleState != "running" {
		t.Fatalf("lifecycle after replay claim = %q, want running", ownership.LifecycleState)
	}
}
