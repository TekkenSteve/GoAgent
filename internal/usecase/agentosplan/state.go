package agentosplan

import (
	"fmt"
	"sort"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// State is the deterministic reducer state for a RunPlan.
type State struct {
	Status      agentos.RunPlanStatus
	nodes       map[string]agentos.PlanNodeStatus
	transitions int32
}

// NewState initializes state from a validated or unvalidated plan spec.
func NewState(spec *agentos.RunPlanSpec, now time.Time) State {
	nodes := make(map[string]agentos.PlanNodeStatus, len(spec.Nodes))
	for i := range spec.Nodes {
		node := &spec.Nodes[i]
		status := agentos.PlanNodeStatus{
			NodeID:         node.NodeID,
			RunID:          node.Run.RunID,
			Backend:        node.Run.Backend,
			LifecycleState: agentos.PlanNodePending,
			UpdatedAt:      now,
		}
		nodes[node.NodeID] = status
	}

	status := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecyclePending,
		Nodes:          sortedNodeStatuses(nodes),
		Metadata:       spec.Metadata,
		UpdatedAt:      now,
	}

	return State{Status: status, nodes: nodes}
}

// NewStateFromStatus restores reducer state from a durable snapshot status.
func NewStateFromStatus(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus) (State, error) {
	if spec.PlanID == "" {
		return State{}, fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidRunPlan)
	}

	restored := *status
	if restored.PlanID == "" {
		restored.PlanID = spec.PlanID
	}

	if restored.PlanID != spec.PlanID {
		return State{}, fmt.Errorf("%w: snapshot plan id %q does not match spec plan id %q", agentoscore.ErrInvalidRunPlan, restored.PlanID, spec.PlanID)
	}

	expectedNodes, err := expectedPlanNodeSet(spec)
	if err != nil {
		return State{}, err
	}

	nodes, err := restorePlanNodeStatuses(&restored, expectedNodes)
	if err != nil {
		return State{}, err
	}

	state := State{Status: restored, nodes: nodes}
	state.refresh(restored.UpdatedAt)

	return state, nil
}

func expectedPlanNodeSet(spec *agentos.RunPlanSpec) (map[string]struct{}, error) {
	expectedNodes := make(map[string]struct{}, len(spec.Nodes))
	for i := range spec.Nodes {
		node := &spec.Nodes[i]
		if node.NodeID == "" {
			return nil, fmt.Errorf("%w: node id is required", agentoscore.ErrInvalidRunPlan)
		}

		if _, exists := expectedNodes[node.NodeID]; exists {
			return nil, fmt.Errorf("%w: duplicate node %q", agentoscore.ErrInvalidRunPlan, node.NodeID)
		}

		expectedNodes[node.NodeID] = struct{}{}
	}

	return expectedNodes, nil
}

func restorePlanNodeStatuses(status *agentos.RunPlanStatus, expectedNodes map[string]struct{}) (map[string]agentos.PlanNodeStatus, error) {
	nodes := make(map[string]agentos.PlanNodeStatus, len(status.Nodes))
	for i := range status.Nodes {
		node := &status.Nodes[i]
		if err := restorePlanNodeStatus(nodes, expectedNodes, node); err != nil {
			return nil, err
		}
	}

	for nodeID := range expectedNodes {
		if _, exists := nodes[nodeID]; !exists {
			return nil, fmt.Errorf("%w: snapshot missing node %q", agentoscore.ErrInvalidRunPlan, nodeID)
		}
	}

	return nodes, nil
}

func restorePlanNodeStatus(nodes map[string]agentos.PlanNodeStatus, expectedNodes map[string]struct{}, node *agentos.PlanNodeStatus) error {
	if node.NodeID == "" {
		return fmt.Errorf("%w: snapshot node id is required", agentoscore.ErrInvalidRunPlan)
	}

	if _, expected := expectedNodes[node.NodeID]; !expected {
		return fmt.Errorf("%w: snapshot contains unknown node %q", agentoscore.ErrInvalidRunPlan, node.NodeID)
	}

	if _, exists := nodes[node.NodeID]; exists {
		return fmt.Errorf("%w: snapshot contains duplicate node %q", agentoscore.ErrInvalidRunPlan, node.NodeID)
	}

	nodes[node.NodeID] = *node

	return nil
}

// EventKind identifies one reducer event.
type EventKind string

const (
	// EventPlanStarted is the reducer event for a plan entering the running lifecycle.
	EventPlanStarted EventKind = "plan.started"
	// EventPlanBlocked is the reducer event for a plan entering the blocked lifecycle.
	EventPlanBlocked EventKind = "plan.blocked"
	// EventPlanExpanded is the reducer event for a plan expansion.
	EventPlanExpanded EventKind = "plan.expanded"
	// EventPlanApproved is the reducer event for a blocked plan receiving approval.
	EventPlanApproved EventKind = "plan.approved"
	// EventPlanRejected is the reducer event for a blocked plan receiving rejection.
	EventPlanRejected EventKind = "plan.rejected"
	// EventPlanSucceeded is the reducer event for a plan reaching the succeeded lifecycle.
	EventPlanSucceeded EventKind = "plan.succeeded"
	// EventPlanFailed is the reducer event for a plan reaching the failed lifecycle.
	EventPlanFailed EventKind = "plan.failed"
	// EventPlanCanceled is the reducer event for a plan reaching the canceled lifecycle.
	EventPlanCanceled EventKind = "plan.canceled"
	// EventNodeReady is the reducer event for a node entering the ready lifecycle.
	EventNodeReady EventKind = "node.ready"
	// EventNodeStarted is the reducer event for a node starting a backend run.
	EventNodeStarted EventKind = "node.started"
	// EventNodeSucceeded is the reducer event for a node completing successfully.
	EventNodeSucceeded EventKind = "node.succeeded"
	// EventNodeFailed is the reducer event for a node failing.
	EventNodeFailed EventKind = "node.failed"
	// EventNodeRetryScheduled is the reducer event for a node retry being scheduled.
	EventNodeRetryScheduled EventKind = "node.retry_scheduled"
	// EventNodeSkipped is the reducer event for a node being skipped.
	EventNodeSkipped EventKind = "node.skipped"
	// EventNodeCanceled is the reducer event for a node being canceled.
	EventNodeCanceled EventKind = "node.canceled"
	// EventNodeInputResolved is the reducer event for a node's inputs being resolved.
	EventNodeInputResolved EventKind = "node.input_resolved"
	// EventCapabilitySelected is the reducer event for a capability being selected for a node.
	EventCapabilitySelected EventKind = "capability.selected"
	// EventConditionsEvaluated is the reducer event for node conditions being evaluated.
	EventConditionsEvaluated EventKind = "conditions.evaluated"
	// EventArtifactsPublished is the reducer event for node artifacts being published.
	EventArtifactsPublished EventKind = "artifacts.published"
	// EventBudgetReported is the reducer event for plan budget usage being reported.
	EventBudgetReported EventKind = "budget.reported"
)

// StateEvent transitions plan state.
type StateEvent struct {
	Kind                   EventKind                  `json:"kind"`
	NodeID                 string                     `json:"node_id,omitempty"`
	RunID                  string                     `json:"run_id,omitempty"`
	Reason                 string                     `json:"reason,omitempty"`
	Attempt                int32                      `json:"attempt,omitempty"`
	Expansion              PlanDelta                  `json:"expansion"`
	Artifacts              []agentoscore.ArtifactRef  `json:"artifacts,omitempty"`
	BudgetDelta            agentos.PlanBudgetUsage    `json:"budget_delta"`
	InputTrace             InputResolutionTrace       `json:"input_trace"`
	Capability             CapabilitySelectionTrace   `json:"capability"`
	ConditionTraces        []ConditionEvaluationTrace `json:"condition_traces,omitempty"`
	PreviousLifecycleState string                     `json:"previous_lifecycle_state,omitempty"`
	NextLifecycleState     string                     `json:"next_lifecycle_state,omitempty"`
	At                     time.Time                  `json:"at"`
	// Approval carries the auditable approval-gate context for plan.blocked
	// (the gate snapshot), plan.approved and plan.rejected (the decision).
	// It is nil for events that do not touch the approval gate.
	Approval *StateEventApproval `json:"approval,omitempty"`
}

// StateEventApproval carries the gate snapshot or decision on approval-gate
// events. Exactly one of Gate/Decision is populated per event kind.
type StateEventApproval struct {
	Gate     agentos.PlanApprovalGate      `json:"gate,omitzero" schema:"optional"`
	Decision *agentos.PlanApprovalDecision `json:"decision,omitempty"`
}

// Apply applies one deterministic state transition.
func (s *State) Apply(event *StateEvent) error {
	at := event.At
	if at.IsZero() {
		at = time.Now().UTC()
	}

	if event.Kind == EventPlanExpanded {
		return s.expand(event.Expansion, at)
	}

	if lifecycle, ok := reducerPlanLifecycleForEvent(event.Kind); ok {
		s.applyPlanEvent(lifecycle, event, at)
		s.refresh(at)
		s.transitions++

		return nil
	}

	if lifecycle, ok := reducerNodeLifecycleForEvent(event.Kind); ok {
		return s.transitionNode(event.NodeID, lifecycle, event, at)
	}

	if event.Kind == EventNodeRetryScheduled {
		return s.retryNode(event.NodeID, event, at)
	}

	if touchNodeEvent(event.Kind) {
		return s.touchNode(event.NodeID, at)
	}

	if event.Kind == EventArtifactsPublished {
		return s.publishArtifacts(event, at)
	}

	if event.Kind == EventBudgetReported {
		return s.reportBudget(event, at)
	}

	return fmt.Errorf("%w: unknown plan event %q", agentoscore.ErrInvalidRunPlan, event.Kind)
}

func reducerPlanLifecycleForEvent(kind EventKind) (string, bool) {
	lifecycle, ok := map[EventKind]string{
		EventPlanStarted:   agentos.PlanLifecycleRunning,
		EventPlanBlocked:   agentos.PlanLifecycleBlocked,
		EventPlanApproved:  agentos.PlanLifecycleRunning,
		EventPlanRejected:  agentos.PlanLifecycleFailed,
		EventPlanSucceeded: agentos.PlanLifecycleSucceeded,
		EventPlanFailed:    agentos.PlanLifecycleFailed,
		EventPlanCanceled:  agentos.PlanLifecycleCanceled,
	}[kind]

	return lifecycle, ok
}

func reducerNodeLifecycleForEvent(kind EventKind) (string, bool) {
	lifecycle, ok := map[EventKind]string{
		EventNodeReady:     agentos.PlanNodeReady,
		EventNodeStarted:   agentos.PlanNodeRunning,
		EventNodeSucceeded: agentos.PlanNodeSucceeded,
		EventNodeFailed:    agentos.PlanNodeFailed,
		EventNodeSkipped:   agentos.PlanNodeSkipped,
		EventNodeCanceled:  agentos.PlanNodeCanceled,
	}[kind]

	return lifecycle, ok
}

func touchNodeEvent(kind EventKind) bool {
	_, ok := map[EventKind]struct{}{
		EventNodeInputResolved:   {},
		EventCapabilitySelected:  {},
		EventConditionsEvaluated: {},
	}[kind]

	return ok
}

func (s *State) applyPlanEvent(lifecycle string, event *StateEvent, at time.Time) {
	s.Status.LifecycleState = lifecycle
	s.Status.Reason = event.Reason

	if event.Kind == EventPlanStarted && s.Status.StartedAt.IsZero() {
		s.Status.StartedAt = at
	}

	// BlockedAt anchors the ApprovalTimeoutSeconds gate: set when entering
	// the blocked state, cleared on any transition out of it (node lifecycle
	// events do not pass through here, so they never advance the clock).
	if lifecycle == agentos.PlanLifecycleBlocked {
		s.Status.BlockedAt = at
	} else if !s.Status.BlockedAt.IsZero() {
		s.Status.BlockedAt = time.Time{}
	}

	s.applyApprovalGateEvent(event, lifecycle, at)
}

// applyApprovalGateEvent keeps the approval-gate projection in sync with plan
// lifecycle events. plan.blocked opens a pending gate; plan.approved and
// plan.rejected record a decision; any other transition out of blocked closes
// the gate without a decision. Topology expansion (plan.expanded) is handled
// separately in expand and marks a decided gate stale.
func (s *State) applyApprovalGateEvent(event *StateEvent, lifecycle string, at time.Time) {
	switch {
	case event.Kind == EventPlanBlocked:
		if event.Approval == nil {
			return
		}

		s.Status.Approval = &agentos.PlanApprovalStatus{
			LifecycleState: agentos.PlanApprovalPending,
			Gate:           event.Approval.Gate,
		}
	case event.Kind == EventPlanApproved:
		s.recordPlanApprovalDecision(true, event, at)
	case event.Kind == EventPlanRejected:
		s.recordPlanApprovalDecision(false, event, at)
	case lifecycle != agentos.PlanLifecycleBlocked && s.Status.Approval != nil && s.Status.Approval.LifecycleState == agentos.PlanApprovalPending:
		// Leaving the blocked lifecycle without approve/reject closes a pending
		// gate (resume or terminal without a decision). A decided gate is kept
		// in the projection for audit; only a topology replan marks it stale.
		s.Status.Approval = nil
	}
}

// recordPlanApprovalDecision records an approve/reject decision on a pending
// gate. The first decision wins; an explicit actor decision takes precedence
// over the system-synthesized one (used by the approval-timeout auto-reject).
// When no gate exists (a plan decided before the auditable-approval feature, or
// an operator abort of a running plan), the event carries a gate snapshot so
// the decision is still auditable.
func (s *State) recordPlanApprovalDecision(approved bool, event *StateEvent, at time.Time) {
	if s.Status.Approval == nil {
		if event.Approval == nil || event.Approval.Gate.PolicyVersion == "" {
			return
		}

		s.Status.Approval = &agentos.PlanApprovalStatus{
			LifecycleState: agentos.PlanApprovalPending,
			Gate:           event.Approval.Gate,
		}
	}

	if s.Status.Approval.LifecycleState != agentos.PlanApprovalPending {
		return
	}

	if approved {
		s.Status.Approval.LifecycleState = agentos.PlanApprovalApproved
	} else {
		s.Status.Approval.LifecycleState = agentos.PlanApprovalRejected
	}

	if event.Approval != nil && event.Approval.Decision != nil {
		decision := *event.Approval.Decision
		s.Status.Approval.Decision = &decision

		return
	}

	// System decision without an explicit actor (e.g. approval timeout, manual
	// node retry that unblocks the plan). The gate's policy version anchors it.
	s.Status.Approval.Decision = &agentos.PlanApprovalDecision{
		Approved:      approved,
		Reason:        event.Reason,
		PolicyVersion: s.Status.Approval.Gate.PolicyVersion,
		DecidedAt:     at,
	}
}

func (s *State) publishArtifacts(event *StateEvent, at time.Time) error {
	if event.NodeID != "" {
		node, ok := s.nodes[event.NodeID]
		if !ok {
			return fmt.Errorf("%w: unknown node %q", agentoscore.ErrInvalidRunPlan, event.NodeID)
		}

		node.Artifacts = append(node.Artifacts, event.Artifacts...)
		node.UpdatedAt = at
		s.nodes[event.NodeID] = node
	}

	s.Status.Artifacts = append(s.Status.Artifacts, event.Artifacts...)
	s.refresh(at)
	s.transitions++

	return nil
}

func (s *State) touchNode(nodeID string, at time.Time) error {
	if nodeID != "" {
		node, ok := s.nodes[nodeID]
		if !ok {
			return fmt.Errorf("%w: unknown node %q", agentoscore.ErrInvalidRunPlan, nodeID)
		}

		node.UpdatedAt = at
		s.nodes[nodeID] = node
	}

	s.refresh(at)
	s.transitions++

	return nil
}

func (s *State) transitionNode(nodeID, lifecycle string, event *StateEvent, at time.Time) error {
	node, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("%w: unknown node %q", agentoscore.ErrInvalidRunPlan, nodeID)
	}

	previousLifecycle := node.LifecycleState
	node.LifecycleState = lifecycle

	node.Reason = event.Reason
	if event.RunID != "" {
		node.RunID = event.RunID
	}

	switch lifecycle {
	case agentos.PlanNodeRunning:
		if previousLifecycle != agentos.PlanNodeRunning {
			if event.Attempt > 0 {
				node.Attempts = event.Attempt
			} else {
				node.Attempts++
			}

			node.StartedAt = at
			node.CompletedAt = time.Time{}
		}
	case agentos.PlanNodeSucceeded, agentos.PlanNodeFailed, agentos.PlanNodeSkipped, agentos.PlanNodeCanceled:
		node.CompletedAt = at
	}

	node.Artifacts = append(node.Artifacts, event.Artifacts...)
	node.UpdatedAt = at
	s.nodes[nodeID] = node
	s.refresh(at)
	s.transitions++

	return nil
}

func (s *State) retryNode(nodeID string, event *StateEvent, at time.Time) error {
	node, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("%w: unknown node %q", agentoscore.ErrInvalidRunPlan, nodeID)
	}

	if node.Attempts == 0 && event.Attempt > 1 {
		node.Attempts = event.Attempt - 1
	}

	node.LifecycleState = agentos.PlanNodeReady
	node.RunID = ""
	node.Reason = event.Reason
	node.StartedAt = time.Time{}
	node.CompletedAt = time.Time{}
	node.UpdatedAt = at
	s.nodes[nodeID] = node
	s.refresh(at)
	s.transitions++

	return nil
}

func (s *State) reportBudget(event *StateEvent, at time.Time) error {
	if event.BudgetDelta.SpentCents < 0 {
		return fmt.Errorf("%w: budget delta cannot be negative", agentoscore.ErrInvalidRunPlan)
	}

	if event.NodeID != "" {
		node, ok := s.nodes[event.NodeID]
		if !ok {
			return fmt.Errorf("%w: unknown node %q", agentoscore.ErrInvalidRunPlan, event.NodeID)
		}

		node.BudgetUsage.SpentCents += event.BudgetDelta.SpentCents
		node.UpdatedAt = at
		s.nodes[event.NodeID] = node
	}

	s.Status.BudgetUsage.SpentCents += event.BudgetDelta.SpentCents
	s.refresh(at)
	s.transitions++

	return nil
}

func (s *State) expand(delta PlanDelta, at time.Time) error {
	if len(delta.Nodes) == 0 && len(delta.Edges) == 0 {
		return fmt.Errorf("%w: expansion delta is empty", agentoscore.ErrInvalidRunPlan)
	}

	for i := range delta.Nodes {
		node := &delta.Nodes[i]

		if node.NodeID == "" {
			return fmt.Errorf("%w: expansion node id is required", agentoscore.ErrInvalidRunPlan)
		}

		if _, exists := s.nodes[node.NodeID]; exists {
			return fmt.Errorf("%w: expansion node %q already exists", agentoscore.ErrInvalidRunPlan, node.NodeID)
		}

		s.nodes[node.NodeID] = agentos.PlanNodeStatus{
			NodeID:         node.NodeID,
			RunID:          node.Run.RunID,
			Backend:        node.Run.Backend,
			LifecycleState: agentos.PlanNodePending,
			UpdatedAt:      at,
		}
	}

	// Replanning (PlanDelta) changes the gated scope: the approval gate's
	// policy version no longer describes the active topology, so any recorded
	// decision is invalid and a fresh gate is required before the plan may
	// pause again.
	if s.Status.Approval != nil {
		s.Status.Approval.LifecycleState = agentos.PlanApprovalStale
	}

	s.refresh(at)
	s.transitions++

	return nil
}

func (s *State) refresh(at time.Time) {
	s.Status.Nodes = sortedNodeStatuses(s.nodes)
	activeRunIDs := make([]string, 0)

	for i := range s.Status.Nodes {
		node := &s.Status.Nodes[i]
		if node.LifecycleState == agentos.PlanNodeRunning && node.RunID != "" {
			activeRunIDs = append(activeRunIDs, node.RunID)
		}
	}

	s.Status.ActiveRunIDs = activeRunIDs
	s.Status.UpdatedAt = at
}

func sortedNodeStatuses(nodes map[string]agentos.PlanNodeStatus) []agentos.PlanNodeStatus {
	keys := make([]string, 0, len(nodes))
	for key := range nodes {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	statuses := make([]agentos.PlanNodeStatus, 0, len(keys))
	for _, key := range keys {
		statuses = append(statuses, nodes[key])
	}

	return statuses
}

// NodeStatus returns one node status.
func (s *State) NodeStatus(nodeID string) (agentos.PlanNodeStatus, bool) {
	status, ok := s.nodes[nodeID]

	return status, ok
}

// AppliedTransitions returns the reducer transitions applied since this State
// was created or restored.
func (s *State) AppliedTransitions() int32 {
	return s.transitions
}
