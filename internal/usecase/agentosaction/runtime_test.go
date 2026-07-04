package agentosaction

import (
	"errors"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
)

func TestRuntimeImplementsGovernedActionRuntime(t *testing.T) {
	t.Parallel()

	var _ agentos.GovernedActionRuntime = (*Runtime)(nil)
}

func TestRuntimeRequestActionInitializesGates(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleActionSpec()

	status, err := runtime.RequestAction(t.Context(), &spec)
	if err != nil {
		t.Fatalf("RequestAction: %v", err)
	}

	if status.LifecycleState != agentos.ActionWaitingDryRun ||
		status.DryRunState != agentos.ActionDryRunPending ||
		status.ApprovalState != agentos.ActionApprovalPending {
		t.Fatalf("status = %#v, want waiting dry-run with approval pending", status)
	}
}

func TestRuntimeActionLifecycleDryRunApprovalExecute(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleActionSpec()
	ref := actionRefFromSpec(&spec)

	if _, err := runtime.RequestAction(t.Context(), &spec); err != nil {
		t.Fatalf("RequestAction: %v", err)
	}

	dryRun, err := runtime.RecordActionDryRun(t.Context(), ref, &agentos.ActionDryRunResult{
		IdempotencyKey: "dry-run-1",
		Succeeded:      true,
		RecordedAt:     spec.RequestedAt.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("RecordActionDryRun: %v", err)
	}

	if dryRun.LifecycleState != agentos.ActionWaitingApproval {
		t.Fatalf("dry-run lifecycle = %q, want waiting approval", dryRun.LifecycleState)
	}

	approved, err := runtime.ResolveActionApproval(t.Context(), ref, &agentos.ActionApprovalDecision{
		IdempotencyKey: "approval-1",
		Approved:       true,
		DecidedAt:      spec.RequestedAt.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("ResolveActionApproval: %v", err)
	}

	if approved.LifecycleState != agentos.ActionReady {
		t.Fatalf("approved lifecycle = %q, want ready", approved.LifecycleState)
	}

	executed, err := runtime.CompleteAction(t.Context(), ref, &agentos.ActionExecutionResult{
		IdempotencyKey: "execute-1",
		Succeeded:      true,
		RecordedAt:     spec.RequestedAt.Add(3 * time.Minute),
	})
	if err != nil {
		t.Fatalf("CompleteAction: %v", err)
	}

	if executed.LifecycleState != agentos.ActionExecuted || executed.ExecutionState != agentos.ActionExecutionSucceeded {
		t.Fatalf("executed status = %#v, want executed", executed)
	}
}

func TestRuntimeRejectsExecutionBeforeGates(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleActionSpec()
	ref := actionRefFromSpec(&spec)

	if _, err := runtime.RequestAction(t.Context(), &spec); err != nil {
		t.Fatalf("RequestAction: %v", err)
	}

	_, err := runtime.CompleteAction(t.Context(), ref, &agentos.ActionExecutionResult{
		IdempotencyKey: "execute-1",
		Succeeded:      true,
	})
	if !errors.Is(err, agentoscore.ErrInvalidGovernedAction) {
		t.Fatalf("CompleteAction error = %v, want ErrInvalidGovernedAction", err)
	}
}

func TestRuntimeCancelAction(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleActionSpec()
	ref := actionRefFromSpec(&spec)

	if _, err := runtime.RequestAction(t.Context(), &spec); err != nil {
		t.Fatalf("RequestAction: %v", err)
	}

	status, err := runtime.CancelAction(t.Context(), ref, &agentos.ActionCancelRequest{
		IdempotencyKey: "cancel-1",
		Reason:         "operator canceled",
		RequestedAt:    spec.RequestedAt.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("CancelAction: %v", err)
	}

	if status.LifecycleState != agentos.ActionCanceled {
		t.Fatalf("lifecycle = %q, want canceled", status.LifecycleState)
	}
}

func TestRuntimeListActionsFiltersByLifecycle(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleActionSpec()
	ready := spec
	ready.ActionID = "action-2"
	ready.IdempotencyKey = "action-key-2"
	ready.DryRunRequired = false
	ready.ApprovalRequired = false

	if _, err := runtime.RequestAction(t.Context(), &spec); err != nil {
		t.Fatalf("RequestAction gated: %v", err)
	}

	if _, err := runtime.RequestAction(t.Context(), &ready); err != nil {
		t.Fatalf("RequestAction ready: %v", err)
	}

	statuses, err := runtime.ListActions(t.Context(), &agentos.ActionScope{
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		LifecycleState: agentos.ActionReady,
	})
	if err != nil {
		t.Fatalf("ListActions: %v", err)
	}

	if len(statuses) != 1 || statuses[0].ActionID != ready.ActionID {
		t.Fatalf("statuses = %#v, want ready action only", statuses)
	}
}

func newSampleRuntime(t *testing.T) *Runtime {
	t.Helper()

	runtime, err := NewRuntime(NewMemoryStore())
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	return runtime
}

func sampleActionSpec() agentos.GovernedActionSpec {
	return agentos.GovernedActionSpec{
		ActionID:       "action-1",
		IdempotencyKey: "action-key-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		ProcessID:      "process-1",
		Resource: agentos.ResourceRef{
			Kind:       "resource-kind",
			ResourceID: "resource-1",
			AccountID:  "acct-1",
			ProjectID:  "proj-1",
		},
		Kind:             "governed-change",
		RequestedAt:      time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
		DryRunRequired:   true,
		ApprovalRequired: true,
		Risk: agentos.ActionRiskAssessment{
			Level:  agentos.ActionRiskHigh,
			Reason: "mutates external system",
		},
	}
}

func actionRefFromSpec(spec *agentos.GovernedActionSpec) agentos.ActionRef {
	return agentos.ActionRef{
		ActionID:  spec.ActionID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	}
}
