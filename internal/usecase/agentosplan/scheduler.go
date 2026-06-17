package agentosplan

import (
	"context"
	"fmt"
	"sort"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// Scheduler calculates ready nodes from deterministic plan state.
type Scheduler struct {
	Expressions ExpressionCompiler
}

// SchedulerDecision contains deterministic scheduling transitions for one tick.
type SchedulerDecision struct {
	Ready   []agentos.PlanNodeSpec
	Skipped []SkippedNode
}

// SkippedNode records one node that can no longer become runnable.
type SkippedNode struct {
	NodeID string
	Reason string
}

// ReadyNodes returns pending nodes whose active dependencies and conditions are satisfied.
func (s Scheduler) ReadyNodes(ctx context.Context, plan ExecutablePlan, status agentos.RunPlanStatus, vars map[string]any) ([]agentos.PlanNodeSpec, error) {
	decision, err := s.Decide(ctx, plan, status, vars)
	if err != nil {
		return nil, err
	}

	return decision.Ready, nil
}

// Decide returns ready and skipped nodes for one deterministic scheduler tick.
func (s Scheduler) Decide(ctx context.Context, plan ExecutablePlan, status agentos.RunPlanStatus, vars map[string]any) (SchedulerDecision, error) {
	statusByNode := make(map[string]agentos.PlanNodeStatus, len(status.Nodes))
	for _, node := range status.Nodes {
		statusByNode[node.NodeID] = node
	}

	decision := SchedulerDecision{
		Ready:   make([]agentos.PlanNodeSpec, 0),
		Skipped: make([]SkippedNode, 0),
	}
	for _, nodeID := range plan.Order {
		node := plan.NodeByID[nodeID]
		current := statusByNode[nodeID]
		if current.LifecycleState != agentos.PlanNodePending && current.LifecycleState != agentos.PlanNodeReady {
			continue
		}
		ok, err := s.nodeConditionsTrue(ctx, node, vars)
		if err != nil {
			return SchedulerDecision{}, err
		}
		if !ok {
			decision.Skipped = append(decision.Skipped, SkippedNode{NodeID: node.NodeID, Reason: "node conditions evaluated false"})
			continue
		}
		dependencies, err := s.evaluateDependencies(ctx, node, plan.EdgesByTo[nodeID], statusByNode, vars)
		if err != nil {
			return SchedulerDecision{}, err
		}
		if dependencies.Ready {
			decision.Ready = append(decision.Ready, node)
		}
		if dependencies.Impossible {
			decision.Skipped = append(decision.Skipped, SkippedNode{NodeID: node.NodeID, Reason: dependencies.Reason})
		}
	}
	sort.Slice(decision.Ready, func(i, j int) bool { return decision.Ready[i].NodeID < decision.Ready[j].NodeID })
	sort.Slice(decision.Skipped, func(i, j int) bool { return decision.Skipped[i].NodeID < decision.Skipped[j].NodeID })

	return decision, nil
}

func (s Scheduler) nodeConditionsTrue(ctx context.Context, node agentos.PlanNodeSpec, vars map[string]any) (bool, error) {
	for _, condition := range node.Conditions {
		ok, err := s.evaluate(ctx, condition, vars)
		if err != nil || !ok {
			return ok, err
		}
	}

	return true, nil
}

type dependencyEvaluation struct {
	Ready      bool
	Impossible bool
	Reason     string
}

func (s Scheduler) evaluateDependencies(ctx context.Context, node agentos.PlanNodeSpec, edges []agentos.PlanEdgeSpec, statusByNode map[string]agentos.PlanNodeStatus, vars map[string]any) (dependencyEvaluation, error) {
	if len(edges) == 0 {
		return dependencyEvaluation{Ready: true}, nil
	}

	join := normalizeJoin(node.Policy.Join)
	waiting := 0
	active := 0
	satisfied := 0
	impossible := 0
	for _, edge := range edges {
		parent, ok := statusByNode[edge.From]
		if !ok {
			return dependencyEvaluation{}, fmt.Errorf("%w: edge %q from unknown node %q", agentos.ErrInvalidRunPlan, edge.EdgeID, edge.From)
		}
		if !nodeTerminal(parent.LifecycleState) {
			waiting++
			continue
		}
		isActive, err := s.evaluate(ctx, edge.Condition, vars)
		if err != nil {
			return dependencyEvaluation{}, fmt.Errorf("edge %q condition: %w", edge.EdgeID, err)
		}
		if !isActive {
			continue
		}
		active++
		if edgeSatisfied(edgeTrigger(edge.On), parent.LifecycleState) {
			satisfied++
		} else {
			impossible++
		}
	}

	switch join {
	case agentos.PlanJoinAll:
		if impossible > 0 {
			return dependencyEvaluation{Impossible: true, Reason: "required dependency edge cannot be satisfied"}, nil
		}
		if waiting > 0 {
			return dependencyEvaluation{}, nil
		}
		if active == 0 {
			return dependencyEvaluation{Impossible: true, Reason: "no active incoming dependency edges"}, nil
		}

		return dependencyEvaluation{Ready: satisfied == active}, nil
	case agentos.PlanJoinAny, agentos.PlanJoinFirst:
		if satisfied > 0 {
			return dependencyEvaluation{Ready: true}, nil
		}
		if waiting > 0 {
			return dependencyEvaluation{}, nil
		}
		if active == 0 {
			return dependencyEvaluation{Impossible: true, Reason: "no active incoming dependency edges"}, nil
		}
		if impossible == active {
			return dependencyEvaluation{Impossible: true, Reason: "no dependency edge can be satisfied"}, nil
		}

		return dependencyEvaluation{}, nil
	default:
		return dependencyEvaluation{}, fmt.Errorf("%w: invalid join strategy %q", agentos.ErrInvalidRunPlan, node.Policy.Join)
	}
}

func (s Scheduler) evaluate(ctx context.Context, expression string, vars map[string]any) (bool, error) {
	if expression == "" {
		return true, nil
	}
	if s.Expressions == nil {
		return false, fmt.Errorf("%w: expression compiler is required", agentos.ErrInvalidExpression)
	}
	compiled, err := s.Expressions.Compile(expression)
	if err != nil {
		return false, err
	}

	return compiled.Evaluate(ctx, vars)
}

func edgeTrigger(trigger agentos.EdgeTrigger) agentos.EdgeTrigger {
	if trigger == "" {
		return agentos.EdgeOnSuccess
	}

	return trigger
}

func edgeSatisfied(trigger agentos.EdgeTrigger, lifecycle string) bool {
	switch trigger {
	case agentos.EdgeOnSuccess:
		return lifecycle == agentos.PlanNodeSucceeded
	case agentos.EdgeOnError:
		return lifecycle == agentos.PlanNodeFailed
	case agentos.EdgeOnComplete:
		return lifecycle == agentos.PlanNodeSucceeded || lifecycle == agentos.PlanNodeFailed || lifecycle == agentos.PlanNodeSkipped || lifecycle == agentos.PlanNodeCanceled
	case agentos.EdgeOnAlways:
		return lifecycle != agentos.PlanNodePending && lifecycle != agentos.PlanNodeReady && lifecycle != agentos.PlanNodeRunning
	default:
		return false
	}
}

func normalizeJoin(join agentos.PlanJoinStrategy) agentos.PlanJoinStrategy {
	if join == "" {
		return agentos.PlanJoinAll
	}

	return join
}

func nodeTerminal(lifecycle string) bool {
	switch lifecycle {
	case agentos.PlanNodeSucceeded, agentos.PlanNodeFailed, agentos.PlanNodeSkipped, agentos.PlanNodeCanceled:
		return true
	default:
		return false
	}
}
