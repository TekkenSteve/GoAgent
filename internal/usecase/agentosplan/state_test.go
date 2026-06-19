package agentosplan

import (
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestStateRetryScheduledKeepsAttemptsAndClearsActiveRun(t *testing.T) {
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	state := NewState(agentos.RunPlanSpec{
		PlanID: "plan-1",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "node-1",
				Run: agentos.RunSpec{
					RunID:   "run-1",
					Backend: agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
				},
			},
		},
	}, now)

	if err := state.Apply(StateEvent{Kind: EventNodeStarted, NodeID: "node-1", RunID: "run-1", Attempt: 1, At: now}); err != nil {
		t.Fatalf("Apply started: %v", err)
	}
	if err := state.Apply(StateEvent{Kind: EventNodeStarted, NodeID: "node-1", RunID: "run-backend", Attempt: 1, At: now.Add(time.Second)}); err != nil {
		t.Fatalf("Apply started run id update: %v", err)
	}
	running, _ := state.NodeStatus("node-1")
	if running.Attempts != 1 || running.RunID != "run-backend" {
		t.Fatalf("running node = %#v", running)
	}

	if err := state.Apply(StateEvent{
		Kind:    EventNodeRetryScheduled,
		NodeID:  "node-1",
		RunID:   "run-backend",
		Reason:  "first attempt failed",
		Attempt: 2,
		At:      now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("Apply retry: %v", err)
	}

	retry, _ := state.NodeStatus("node-1")
	if retry.LifecycleState != agentos.PlanNodeReady {
		t.Fatalf("lifecycle = %q", retry.LifecycleState)
	}
	if retry.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", retry.Attempts)
	}
	if retry.RunID != "" {
		t.Fatalf("run id = %q, want empty", retry.RunID)
	}
	if len(state.Status.ActiveRunIDs) != 0 {
		t.Fatalf("active runs = %#v", state.Status.ActiveRunIDs)
	}

	if err := state.Apply(StateEvent{Kind: EventNodeStarted, NodeID: "node-1", RunID: "run-2", Attempt: 2, At: now.Add(3 * time.Second)}); err != nil {
		t.Fatalf("Apply second attempt: %v", err)
	}
	second, _ := state.NodeStatus("node-1")
	if second.Attempts != 2 || second.RunID != "run-2" {
		t.Fatalf("second attempt node = %#v", second)
	}
}

func TestStatePlanApprovalAndRejection(t *testing.T) {
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	state := NewState(agentos.RunPlanSpec{PlanID: "plan-1"}, now)

	if err := state.Apply(StateEvent{Kind: EventPlanBlocked, Reason: "waiting approval", At: now}); err != nil {
		t.Fatalf("Apply blocked: %v", err)
	}
	if err := state.Apply(StateEvent{Kind: EventPlanApproved, At: now.Add(time.Second)}); err != nil {
		t.Fatalf("Apply approved: %v", err)
	}
	if state.Status.LifecycleState != agentos.PlanLifecycleRunning || state.Status.Reason != "" {
		t.Fatalf("approved status = %#v", state.Status)
	}
	if err := state.Apply(StateEvent{Kind: EventPlanRejected, Reason: "operator rejected", At: now.Add(2 * time.Second)}); err != nil {
		t.Fatalf("Apply rejected: %v", err)
	}
	if state.Status.LifecycleState != agentos.PlanLifecycleFailed || state.Status.Reason != "operator rejected" {
		t.Fatalf("rejected status = %#v", state.Status)
	}
}

func TestStateRestoresFromSnapshotStatus(t *testing.T) {
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-restore",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "a", Run: agentos.RunSpec{RunID: "run-a", Backend: ref}},
			{NodeID: "b", Run: agentos.RunSpec{RunID: "run-b", Backend: ref}},
		},
	}
	status := agentos.RunPlanStatus{
		PlanID:         "plan-restore",
		LifecycleState: agentos.PlanLifecycleRunning,
		Nodes: []agentos.PlanNodeStatus{
			{NodeID: "b", RunID: "run-b", Backend: ref, LifecycleState: agentos.PlanNodePending, UpdatedAt: now},
			{NodeID: "a", RunID: "run-a", Backend: ref, LifecycleState: agentos.PlanNodeRunning, UpdatedAt: now},
		},
		UpdatedAt: now,
	}

	state, err := NewStateFromStatus(spec, status)
	if err != nil {
		t.Fatalf("NewStateFromStatus: %v", err)
	}
	if got := state.Status.ActiveRunIDs; len(got) != 1 || got[0] != "run-a" {
		t.Fatalf("active runs = %#v", got)
	}
	if got := state.Status.Nodes; got[0].NodeID != "a" || got[1].NodeID != "b" {
		t.Fatalf("nodes not sorted/restored = %#v", got)
	}
}

func TestStateRestoresRejectsSnapshotMismatch(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-restore",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "known", Run: agentos.RunSpec{RunID: "run-known", Backend: ref}},
		},
	}
	status := agentos.RunPlanStatus{
		PlanID: "plan-restore",
		Nodes: []agentos.PlanNodeStatus{
			{NodeID: "unknown", Backend: ref, LifecycleState: agentos.PlanNodePending},
		},
	}

	if _, err := NewStateFromStatus(spec, status); err == nil {
		t.Fatal("NewStateFromStatus succeeded with unknown snapshot node")
	}
}

func TestStateBudgetReportedAccumulatesPlanAndNodeUsage(t *testing.T) {
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	state := NewState(agentos.RunPlanSpec{
		PlanID: "plan-budget",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "node-1", Run: agentos.RunSpec{RunID: "run-1", Backend: ref}},
		},
	}, now)

	if err := state.Apply(StateEvent{
		Kind:        EventBudgetReported,
		NodeID:      "node-1",
		RunID:       "run-1",
		BudgetDelta: agentos.PlanBudgetUsage{SpentCents: 25},
		At:          now,
	}); err != nil {
		t.Fatalf("Apply budget: %v", err)
	}
	if state.Status.BudgetUsage.SpentCents != 25 {
		t.Fatalf("plan spent = %d, want 25", state.Status.BudgetUsage.SpentCents)
	}
	node, _ := state.NodeStatus("node-1")
	if node.BudgetUsage.SpentCents != 25 {
		t.Fatalf("node spent = %d, want 25", node.BudgetUsage.SpentCents)
	}
	if state.AppliedTransitions() != 1 {
		t.Fatalf("transitions = %d, want 1", state.AppliedTransitions())
	}
}

func TestStateDebugTraceEventsTouchNodeWithoutChangingLifecycle(t *testing.T) {
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	state := NewState(agentos.RunPlanSpec{
		PlanID: "plan-debug",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "node-1", Run: agentos.RunSpec{RunID: "run-1", Backend: ref}},
		},
	}, now)

	if err := state.Apply(StateEvent{Kind: EventCapabilitySelected, NodeID: "node-1", At: now.Add(time.Second)}); err != nil {
		t.Fatalf("Apply capability: %v", err)
	}
	if err := state.Apply(StateEvent{Kind: EventNodeInputResolved, NodeID: "node-1", RunID: "run-1", At: now.Add(2 * time.Second)}); err != nil {
		t.Fatalf("Apply input: %v", err)
	}

	node, _ := state.NodeStatus("node-1")
	if node.LifecycleState != agentos.PlanNodePending {
		t.Fatalf("lifecycle = %q, want pending", node.LifecycleState)
	}
	if state.AppliedTransitions() != 2 {
		t.Fatalf("transitions = %d, want 2", state.AppliedTransitions())
	}
}
