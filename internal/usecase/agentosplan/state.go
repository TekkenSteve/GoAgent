package agentosplan

import (
	"fmt"
	"sort"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// State is the deterministic reducer state for a RunPlan.
type State struct {
	Status      agentos.RunPlanStatus
	nodes       map[string]agentos.PlanNodeStatus
	transitions int32
}

// NewState initializes state from a validated or unvalidated plan spec.
func NewState(spec agentos.RunPlanSpec, now time.Time) State {
	nodes := make(map[string]agentos.PlanNodeStatus, len(spec.Nodes))
	for _, node := range spec.Nodes {
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
func NewStateFromStatus(spec agentos.RunPlanSpec, status agentos.RunPlanStatus) (State, error) {
	if spec.PlanID == "" {
		return State{}, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if status.PlanID == "" {
		status.PlanID = spec.PlanID
	}
	if status.PlanID != spec.PlanID {
		return State{}, fmt.Errorf("%w: snapshot plan id %q does not match spec plan id %q", agentos.ErrInvalidRunPlan, status.PlanID, spec.PlanID)
	}

	expectedNodes := make(map[string]struct{}, len(spec.Nodes))
	for _, node := range spec.Nodes {
		if node.NodeID == "" {
			return State{}, fmt.Errorf("%w: node id is required", agentos.ErrInvalidRunPlan)
		}
		if _, exists := expectedNodes[node.NodeID]; exists {
			return State{}, fmt.Errorf("%w: duplicate node %q", agentos.ErrInvalidRunPlan, node.NodeID)
		}
		expectedNodes[node.NodeID] = struct{}{}
	}

	nodes := make(map[string]agentos.PlanNodeStatus, len(status.Nodes))
	for _, node := range status.Nodes {
		if node.NodeID == "" {
			return State{}, fmt.Errorf("%w: snapshot node id is required", agentos.ErrInvalidRunPlan)
		}
		if _, expected := expectedNodes[node.NodeID]; !expected {
			return State{}, fmt.Errorf("%w: snapshot contains unknown node %q", agentos.ErrInvalidRunPlan, node.NodeID)
		}
		if _, exists := nodes[node.NodeID]; exists {
			return State{}, fmt.Errorf("%w: snapshot contains duplicate node %q", agentos.ErrInvalidRunPlan, node.NodeID)
		}
		nodes[node.NodeID] = node
	}
	for nodeID := range expectedNodes {
		if _, exists := nodes[nodeID]; !exists {
			return State{}, fmt.Errorf("%w: snapshot missing node %q", agentos.ErrInvalidRunPlan, nodeID)
		}
	}

	state := State{Status: status, nodes: nodes}
	state.refresh(status.UpdatedAt)

	return state, nil
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
	Expansion              PlanDelta                  `json:"expansion,omitempty"`
	Artifacts              []agentos.ArtifactRef      `json:"artifacts,omitempty"`
	BudgetDelta            agentos.PlanBudgetUsage    `json:"budget_delta,omitempty"`
	InputTrace             InputResolutionTrace       `json:"input_trace,omitempty"`
	Capability             CapabilitySelectionTrace   `json:"capability,omitempty"`
	ConditionTraces        []ConditionEvaluationTrace `json:"condition_traces,omitempty"`
	PreviousLifecycleState string                     `json:"previous_lifecycle_state,omitempty"`
	NextLifecycleState     string                     `json:"next_lifecycle_state,omitempty"`
	At                     time.Time                  `json:"at,omitempty"`
}

// Apply applies one deterministic state transition.
func (s *State) Apply(event StateEvent) error {
	at := event.At
	if at.IsZero() {
		at = time.Now().UTC()
	}
	switch event.Kind {
	case EventPlanStarted:
		s.Status.LifecycleState = agentos.PlanLifecycleRunning
		s.Status.Reason = event.Reason
		if s.Status.StartedAt.IsZero() {
			s.Status.StartedAt = at
		}
	case EventPlanBlocked:
		s.Status.LifecycleState = agentos.PlanLifecycleBlocked
		s.Status.Reason = event.Reason
	case EventPlanExpanded:
		return s.expand(event.Expansion, at)
	case EventPlanApproved:
		s.Status.LifecycleState = agentos.PlanLifecycleRunning
		s.Status.Reason = event.Reason
	case EventPlanRejected:
		s.Status.LifecycleState = agentos.PlanLifecycleFailed
		s.Status.Reason = event.Reason
	case EventPlanSucceeded:
		s.Status.LifecycleState = agentos.PlanLifecycleSucceeded
	case EventPlanFailed:
		s.Status.LifecycleState = agentos.PlanLifecycleFailed
		s.Status.Reason = event.Reason
	case EventPlanCanceled:
		s.Status.LifecycleState = agentos.PlanLifecycleCanceled
		s.Status.Reason = event.Reason
	case EventNodeReady:
		return s.transitionNode(event.NodeID, agentos.PlanNodeReady, event, at)
	case EventNodeStarted:
		return s.transitionNode(event.NodeID, agentos.PlanNodeRunning, event, at)
	case EventNodeSucceeded:
		return s.transitionNode(event.NodeID, agentos.PlanNodeSucceeded, event, at)
	case EventNodeFailed:
		return s.transitionNode(event.NodeID, agentos.PlanNodeFailed, event, at)
	case EventNodeRetryScheduled:
		return s.retryNode(event.NodeID, event, at)
	case EventNodeSkipped:
		return s.transitionNode(event.NodeID, agentos.PlanNodeSkipped, event, at)
	case EventNodeCanceled:
		return s.transitionNode(event.NodeID, agentos.PlanNodeCanceled, event, at)
	case EventNodeInputResolved, EventCapabilitySelected, EventConditionsEvaluated:
		return s.touchNode(event.NodeID, at)
	case EventArtifactsPublished:
		if event.NodeID != "" {
			node, ok := s.nodes[event.NodeID]
			if !ok {
				return fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, event.NodeID)
			}
			node.Artifacts = append(node.Artifacts, event.Artifacts...)
			node.UpdatedAt = at
			s.nodes[event.NodeID] = node
		}
		s.Status.Artifacts = append(s.Status.Artifacts, event.Artifacts...)
	case EventBudgetReported:
		return s.reportBudget(event, at)
	default:
		return fmt.Errorf("%w: unknown plan event %q", agentos.ErrInvalidRunPlan, event.Kind)
	}
	s.refresh(at)
	s.transitions++

	return nil
}

func (s *State) touchNode(nodeID string, at time.Time) error {
	if nodeID != "" {
		node, ok := s.nodes[nodeID]
		if !ok {
			return fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, nodeID)
		}
		node.UpdatedAt = at
		s.nodes[nodeID] = node
	}
	s.refresh(at)
	s.transitions++

	return nil
}

func (s *State) transitionNode(nodeID string, lifecycle string, event StateEvent, at time.Time) error {
	node, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, nodeID)
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

func (s *State) retryNode(nodeID string, event StateEvent, at time.Time) error {
	node, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, nodeID)
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

func (s *State) reportBudget(event StateEvent, at time.Time) error {
	if event.BudgetDelta.SpentCents < 0 {
		return fmt.Errorf("%w: budget delta cannot be negative", agentos.ErrInvalidRunPlan)
	}
	if event.NodeID != "" {
		node, ok := s.nodes[event.NodeID]
		if !ok {
			return fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, event.NodeID)
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
		return fmt.Errorf("%w: expansion delta is empty", agentos.ErrInvalidRunPlan)
	}
	for _, node := range delta.Nodes {
		if node.NodeID == "" {
			return fmt.Errorf("%w: expansion node id is required", agentos.ErrInvalidRunPlan)
		}
		if _, exists := s.nodes[node.NodeID]; exists {
			return fmt.Errorf("%w: expansion node %q already exists", agentos.ErrInvalidRunPlan, node.NodeID)
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
	for _, node := range s.Status.Nodes {
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
func (s State) NodeStatus(nodeID string) (agentos.PlanNodeStatus, bool) {
	status, ok := s.nodes[nodeID]

	return status, ok
}

// AppliedTransitions returns the reducer transitions applied since this State
// was created or restored.
func (s State) AppliedTransitions() int32 {
	return s.transitions
}
