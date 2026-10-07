package agentosplan

import (
	"errors"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/stretchr/testify/require"
)

func approvalTestSpec(planID string, policy agentos.PlanPolicy) *agentos.RunPlanSpec {
	return &agentos.RunPlanSpec{
		PlanID: planID,
		Policy: policy,
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "node-1", Run: agentos.RunSpec{RunID: "run-1"}},
			{NodeID: "node-2", Run: agentos.RunSpec{RunID: "run-2"}},
		},
	}
}

func requireApprovalGate(t *testing.T, spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, now time.Time) agentos.PlanApprovalGate {
	t.Helper()

	gate, err := NewPlanApprovalGate(spec, status, "pause requested", now)
	require.NoError(t, err)

	return gate
}

func TestStateApprovalGateLifecycle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	spec := approvalTestSpec("plan-approval-lifecycle", agentos.PlanPolicy{ApprovalTimeoutSeconds: 600})
	state := NewState(spec, now)

	require.NoError(t, state.Apply(&StateEvent{Kind: EventPlanStarted, At: now}))
	require.NoError(t, state.Apply(&StateEvent{Kind: EventNodeStarted, NodeID: "node-1", RunID: "run-1", Attempt: 1, At: now.Add(time.Second)}))

	gate := requireApprovalGate(t, spec, &state.Status, now.Add(2*time.Second))
	require.Equal(t, []string{"node-1", "node-2"}, gate.NodeIDs)
	require.Equal(t, "pause requested", gate.RiskReason)
	require.Equal(t, now.Add(2*time.Second), gate.RequestedAt)
	require.Equal(t, now.Add(2*time.Second+600*time.Second), gate.ExpiresAt)
	require.NotEmpty(t, gate.PolicyVersion)

	require.NoError(t, state.Apply(&StateEvent{
		Kind:     EventPlanBlocked,
		Reason:   "pause requested",
		Approval: &StateEventApproval{Gate: gate},
		At:       now.Add(2 * time.Second),
	}))

	require.NotNil(t, state.Status.Approval)
	require.Equal(t, agentos.PlanApprovalPending, state.Status.Approval.LifecycleState)
	require.Equal(t, gate, state.Status.Approval.Gate)

	decision, err := PlanApprovalDecisionFromSignal(spec, &state.Status, &agentoscore.Signal{
		Type: agentoscore.SignalPlanApprove, ActorID: "operator-1",
		Payload: map[string]any{agentoscore.SignalPayloadReason: "looks good"},
	}, true, now.Add(3*time.Second))
	require.NoError(t, err)
	require.True(t, decision.Approved)
	require.Equal(t, "operator-1", decision.ActorID)
	require.Equal(t, "looks good", decision.Reason)
	require.Equal(t, gate.PolicyVersion, decision.PolicyVersion)
	require.Equal(t, now.Add(3*time.Second), decision.DecidedAt)

	require.NoError(t, state.Apply(&StateEvent{
		Kind:     EventPlanApproved,
		Reason:   decision.Reason,
		Approval: &StateEventApproval{Decision: &decision},
		At:       now.Add(3 * time.Second),
	}))

	require.Equal(t, agentos.PlanApprovalApproved, state.Status.Approval.LifecycleState)
	require.NotNil(t, state.Status.Approval.Decision)
	require.Equal(t, "operator-1", state.Status.Approval.Decision.ActorID)
	require.Equal(t, gate.PolicyVersion, state.Status.Approval.Decision.PolicyVersion)
}

func TestStateApprovalPendingClearedOnResumeWithoutDecision(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	spec := approvalTestSpec("plan-approval-resume", agentos.PlanPolicy{})
	state := NewState(spec, now)
	require.NoError(t, state.Apply(&StateEvent{Kind: EventPlanStarted, At: now}))

	gate := requireApprovalGate(t, spec, &state.Status, now.Add(time.Second))
	require.NoError(t, state.Apply(&StateEvent{
		Kind: EventPlanBlocked, Reason: "pause requested",
		Approval: &StateEventApproval{Gate: gate}, At: now.Add(time.Second),
	}))
	require.Equal(t, agentos.PlanApprovalPending, state.Status.Approval.LifecycleState)

	// Resume without a decision closes the pending gate.
	require.NoError(t, state.Apply(&StateEvent{Kind: EventPlanStarted, At: now.Add(2 * time.Second)}))
	require.Nil(t, state.Status.Approval)
}

func TestStateApprovalMarkedStaleOnExpansion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	spec := approvalTestSpec("plan-approval-replan", agentos.PlanPolicy{})
	state := NewState(spec, now)
	require.NoError(t, state.Apply(&StateEvent{Kind: EventPlanStarted, At: now}))
	require.NoError(t, state.Apply(&StateEvent{Kind: EventNodeStarted, NodeID: "node-1", RunID: "run-1", Attempt: 1, At: now.Add(time.Second)}))

	gate := requireApprovalGate(t, spec, &state.Status, now.Add(2*time.Second))
	require.NoError(t, state.Apply(&StateEvent{
		Kind: EventPlanBlocked, Reason: "pause requested",
		Approval: &StateEventApproval{Gate: gate}, At: now.Add(2 * time.Second),
	}))

	decision, err := PlanApprovalDecisionFromSignal(spec, &state.Status, &agentoscore.Signal{Type: agentoscore.SignalPlanApprove, ActorID: "operator-1"}, true, now.Add(3*time.Second))
	require.NoError(t, err)
	require.NoError(t, state.Apply(&StateEvent{
		Kind: EventPlanApproved, Reason: decision.Reason,
		Approval: &StateEventApproval{Decision: &decision}, At: now.Add(3 * time.Second),
	}))
	require.Equal(t, agentos.PlanApprovalApproved, state.Status.Approval.LifecycleState)

	// Replanning the topology invalidates the recorded approval.
	require.NoError(t, state.Apply(&StateEvent{
		Kind: EventPlanExpanded,
		Expansion: PlanDelta{Nodes: []agentos.PlanNodeSpec{
			{NodeID: "node-3", Run: agentos.RunSpec{RunID: "run-3"}},
		}},
		At: now.Add(4 * time.Second),
	}))
	require.Equal(t, agentos.PlanApprovalStale, state.Status.Approval.LifecycleState)

	// A stale gate must not be decided.
	_, err = PlanApprovalDecisionFromSignal(spec, &state.Status, &agentoscore.Signal{Type: agentoscore.SignalPlanApprove, ActorID: "operator-1"}, true, now.Add(5*time.Second))
	require.Error(t, err)
	require.True(t, errors.Is(err, agentoscore.ErrInvalidSignal))
}

func TestPlanApprovalPolicyVersionChangesOnExpansion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	spec := approvalTestSpec("plan-approval-version", agentos.PlanPolicy{})
	state := NewState(spec, now)
	require.NoError(t, state.Apply(&StateEvent{Kind: EventPlanStarted, At: now}))

	before, err := PlanApprovalPolicyVersion(spec, &state.Status)
	require.NoError(t, err)

	require.NoError(t, state.Apply(&StateEvent{
		Kind: EventPlanExpanded,
		Expansion: PlanDelta{Nodes: []agentos.PlanNodeSpec{
			{NodeID: "node-3", Run: agentos.RunSpec{RunID: "run-3"}},
		}},
		At: now.Add(time.Second),
	}))

	after, err := PlanApprovalPolicyVersion(spec, &state.Status)
	require.NoError(t, err)
	require.NotEqual(t, before, after)
}

func TestNewPlanApprovalGateNoExpiryWithoutPolicy(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	spec := approvalTestSpec("plan-approval-no-expiry", agentos.PlanPolicy{})
	state := NewState(spec, now)

	gate := requireApprovalGate(t, spec, &state.Status, now)
	require.True(t, gate.ExpiresAt.IsZero())
	require.NotEmpty(t, gate.Summary)
}

func TestPlanApprovalDecisionBackwardCompatibleWithoutGate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	spec := approvalTestSpec("plan-approval-legacy", agentos.PlanPolicy{})
	state := NewState(spec, now)
	require.NoError(t, state.Apply(&StateEvent{Kind: EventPlanStarted, At: now}))

	// A blocked plan persisted before the auditable-approval feature has no
	// gate snapshot; the decision is derived against the live topology so the
	// in-flight plan keeps working.
	decision, err := PlanApprovalDecisionFromSignal(spec, &state.Status, &agentoscore.Signal{
		Type: agentoscore.SignalPlanReject, ActorID: "operator-1",
	}, false, now.Add(time.Second))
	require.NoError(t, err)
	require.False(t, decision.Approved)
	require.Equal(t, "operator-1", decision.ActorID)
	require.NotEmpty(t, decision.PolicyVersion)
}

func TestPlanApprovalDecisionRejectsAlreadyDecidedGate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	spec := approvalTestSpec("plan-approval-twice", agentos.PlanPolicy{})
	state := NewState(spec, now)
	require.NoError(t, state.Apply(&StateEvent{Kind: EventPlanStarted, At: now}))

	gate := requireApprovalGate(t, spec, &state.Status, now.Add(time.Second))
	require.NoError(t, state.Apply(&StateEvent{
		Kind: EventPlanBlocked, Reason: "pause requested",
		Approval: &StateEventApproval{Gate: gate}, At: now.Add(time.Second),
	}))

	decision, err := PlanApprovalDecisionFromSignal(spec, &state.Status, &agentoscore.Signal{Type: agentoscore.SignalPlanApprove, ActorID: "operator-1"}, true, now.Add(2*time.Second))
	require.NoError(t, err)
	require.NoError(t, state.Apply(&StateEvent{
		Kind: EventPlanApproved, Reason: decision.Reason,
		Approval: &StateEventApproval{Decision: &decision}, At: now.Add(2 * time.Second),
	}))

	_, err = PlanApprovalDecisionFromSignal(spec, &state.Status, &agentoscore.Signal{Type: agentoscore.SignalPlanApprove, ActorID: "operator-2"}, true, now.Add(3*time.Second))
	require.Error(t, err)
	require.Contains(t, err.Error(), "already decided")
}
