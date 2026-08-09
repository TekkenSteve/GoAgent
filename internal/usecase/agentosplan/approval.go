package agentosplan

import (
	"fmt"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// PlanApprovalPolicyVersion derives a deterministic fingerprint of the plan
// policy and active node set. It anchors a plan approval gate: when the plan is
// replanned (PlanDelta expands the node set) or the policy changes, the version
// changes and the recorded gate/decision no longer matches the active topology.
func PlanApprovalPolicyVersion(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus) (string, error) {
	nodeIDs := make([]string, 0, len(status.Nodes))
	for i := range status.Nodes {
		nodeIDs = append(nodeIDs, status.Nodes[i].NodeID)
	}

	return idempotencyHash(spec.PlanID, struct {
		Policy  agentos.PlanPolicy `json:"policy"`
		NodeIDs []string           `json:"node_ids"`
	}{Policy: spec.Policy, NodeIDs: nodeIDs})
}

// NewPlanApprovalGate builds the auditable gate snapshot for a plan entering
// the blocked lifecycle. The gate covers the nodes that would resume or start
// on approval, so an operator decision applies to exactly the pending scope.
func NewPlanApprovalGate(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, reason string, now time.Time) (agentos.PlanApprovalGate, error) {
	nodeIDs := make([]string, 0, len(status.Nodes))
	for i := range status.Nodes {
		node := &status.Nodes[i]
		if planNodeGatedByApproval(node.LifecycleState) {
			nodeIDs = append(nodeIDs, node.NodeID)
		}
	}

	policyVersion, err := PlanApprovalPolicyVersion(spec, status)
	if err != nil {
		return agentos.PlanApprovalGate{}, err
	}

	gate := agentos.PlanApprovalGate{
		Summary:       fmt.Sprintf("approve execution of %d plan node(s) for plan %q", len(nodeIDs), spec.PlanID),
		NodeIDs:       nodeIDs,
		RiskReason:    reason,
		PolicyVersion: policyVersion,
		RequestedAt:   now,
	}

	if spec.Policy.ApprovalTimeoutSeconds > 0 {
		gate.ExpiresAt = now.Add(time.Duration(spec.Policy.ApprovalTimeoutSeconds) * time.Second)
	}

	return gate, nil
}

// planNodeGatedByApproval reports whether a node still needs the approval gate
// to run: terminal nodes are outside the gated scope.
func planNodeGatedByApproval(lifecycleState string) bool {
	switch lifecycleState {
	case agentos.PlanNodeSucceeded, agentos.PlanNodeSkipped, agentos.PlanNodeFailed, agentos.PlanNodeCanceled:
		return false
	default:
		return true
	}
}

// PlanApprovalDecisionFromSignal records an approve/reject decision for the
// current gate. It validates that a gate exists and is still pending against
// the live policy version, so a stale decision (after a topology change) is
// rejected rather than silently applied. A blocked plan persisted before the
// auditable-approval feature has no gate snapshot; the decision is derived
// against the live topology so in-flight plans keep working.
func PlanApprovalDecisionFromSignal(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, signal *agentoscore.Signal, approved bool, now time.Time) (agentos.PlanApprovalDecision, error) {
	liveVersion, err := PlanApprovalPolicyVersion(spec, status)
	if err != nil {
		return agentos.PlanApprovalDecision{}, err
	}

	if current := status.Approval; current != nil {
		if current.LifecycleState != agentos.PlanApprovalPending {
			return agentos.PlanApprovalDecision{}, fmt.Errorf("%w: approval already decided (%s)", agentoscore.ErrInvalidSignal, current.LifecycleState)
		}

		if current.Gate.PolicyVersion != liveVersion {
			return agentos.PlanApprovalDecision{}, fmt.Errorf("%w: approval gate is stale (topology changed after the gate was created)", agentoscore.ErrInvalidSignal)
		}
	}

	return agentos.PlanApprovalDecision{
		Approved:      approved,
		ActorID:       signal.ActorID,
		Reason:        PlanSignalReason(signal),
		PolicyVersion: liveVersion,
		DecidedAt:     now,
	}, nil
}
