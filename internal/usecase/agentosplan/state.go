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
	EventPlanStarted         EventKind = "plan.started"
	EventPlanBlocked         EventKind = "plan.blocked"
	EventPlanExpanded        EventKind = "plan.expanded"
	EventPlanApproved        EventKind = "plan.approved"
	EventPlanRejected        EventKind = "plan.rejected"
	EventPlanSucceeded       EventKind = "plan.succeeded"
	EventPlanFailed          EventKind = "plan.failed"
	EventPlanCanceled        EventKind = "plan.canceled"
	EventNodeReady           EventKind = "node.ready"
	EventNodeStarted         EventKind = "node.started"
	EventNodeSucceeded       EventKind = "node.succeeded"
	EventNodeFailed          EventKind = "node.failed"
	EventNodeRetryScheduled  EventKind = "node.retry_scheduled"
	EventNodeSkipped         EventKind = "node.skipped"
	EventNodeCanceled        EventKind = "node.canceled"
	EventNodeInputResolved   EventKind = "node.input_resolved"
	EventCapabilitySelected  EventKind = "capability.selected"
	EventConditionsEvaluated EventKind = "conditions.evaluated"
	EventArtifactsPublished  EventKind = "artifacts.published"
	EventBudgetReported      EventKind = "budget.reported"
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
