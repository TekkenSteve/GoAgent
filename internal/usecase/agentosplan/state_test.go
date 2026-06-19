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
