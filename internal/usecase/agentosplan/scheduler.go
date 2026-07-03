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
	Ready           []agentos.PlanNodeSpec
	Skipped         []SkippedNode
	ConditionTraces []ConditionEvaluationTrace
}

// SkippedNode records one node that can no longer become runnable.
type SkippedNode struct {
	NodeID string
	Reason string
}

// ReadyNodes returns pending nodes whose active dependencies and conditions are satisfied.
func (s Scheduler) ReadyNodes(ctx context.Context, plan *ExecutablePlan, status *agentos.RunPlanStatus, vars map[string]any) ([]agentos.PlanNodeSpec, error) {
	decision, err := s.Decide(ctx, plan, status, vars)
	if err != nil {
		return nil, err
	}

	return decision.Ready, nil
}

// Decide returns ready and skipped nodes for one deterministic scheduler tick.
func (s Scheduler) Decide(ctx context.Context, plan *ExecutablePlan, status *agentos.RunPlanStatus, vars map[string]any) (SchedulerDecision, error) {
	statusByNode := make(map[string]agentos.PlanNodeStatus, len(status.Nodes))
	for i := range status.Nodes {
		node := &status.Nodes[i]
		statusByNode[node.NodeID] = *node
	}

	decision := SchedulerDecision{
		Ready:           make([]agentos.PlanNodeSpec, 0),
		Skipped:         make([]SkippedNode, 0),
		ConditionTraces: make([]ConditionEvaluationTrace, 0),
	}

	for _, nodeID := range plan.Order {
		node := plan.NodeByID[nodeID]

		current := statusByNode[nodeID]
		if current.LifecycleState != agentos.PlanNodePending && current.LifecycleState != agentos.PlanNodeReady {
			continue
		}

		ok, traces, err := s.nodeConditionsTrue(ctx, &node, vars)
		if err != nil {
			return SchedulerDecision{}, err
		}

		decision.ConditionTraces = append(decision.ConditionTraces, traces...)
		if !ok {
			decision.Skipped = append(decision.Skipped, SkippedNode{NodeID: node.NodeID, Reason: "node conditions evaluated false"})

			continue
		}

		dependencies, err := s.evaluateDependencies(ctx, &node, plan.EdgesByTo[nodeID], statusByNode, vars)
		if err != nil {
			return SchedulerDecision{}, err
		}

		decision.ConditionTraces = append(decision.ConditionTraces, dependencies.Traces...)
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

func (s Scheduler) nodeConditionsTrue(ctx context.Context, node *agentos.PlanNodeSpec, vars map[string]any) (bool, []ConditionEvaluationTrace, error) {
	traces := make([]ConditionEvaluationTrace, 0, len(node.Conditions))
	for _, condition := range node.Conditions {
		ok, err := s.evaluate(ctx, condition, vars)
		if err != nil {
			return false, traces, err
		}

		traces = append(traces, ConditionEvaluationTrace{
			Scope:      "node",
			NodeID:     node.NodeID,
			Expression: condition,
			Result:     ok,
		})
		if !ok {
			return false, traces, nil
		}
	}

	return true, traces, nil
}

type dependencyEvaluation struct {
	Ready      bool
	Impossible bool
	Reason     string
	Traces     []ConditionEvaluationTrace
}

func (s Scheduler) evaluateDependencies(ctx context.Context, node *agentos.PlanNodeSpec, edges []agentos.PlanEdgeSpec, statusByNode map[string]agentos.PlanNodeStatus, vars map[string]any) (dependencyEvaluation, error) {
	if len(edges) == 0 {
		return dependencyEvaluation{Ready: true}, nil
	}

	join := normalizeJoin(node.Policy.Join)

	waiting, satisfied, impossible, traces, err := s.evaluateEdges(ctx, node, edges, statusByNode, vars)
	if err != nil {
		return dependencyEvaluation{}, err
	}

	active := satisfied + impossible

	switch join {
	case agentos.PlanJoinAll:
		return evaluateAllJoin(waiting, satisfied, impossible, active, traces), nil
	case agentos.PlanJoinAny, agentos.PlanJoinFirst:
		return evaluateAnyJoin(waiting, satisfied, impossible, active, traces), nil
	default:
		return dependencyEvaluation{}, fmt.Errorf("%w: invalid join strategy %q", agentos.ErrInvalidRunPlan, node.Policy.Join)
	}
}

func evaluateAllJoin(waiting, satisfied, impossible, active int, traces []ConditionEvaluationTrace) dependencyEvaluation {
	if impossible > 0 {
		return dependencyEvaluation{Impossible: true, Reason: "required dependency edge cannot be satisfied", Traces: traces}
	}

	if waiting > 0 {
		return dependencyEvaluation{Traces: traces}
	}

	if active == 0 {
		return dependencyEvaluation{Impossible: true, Reason: "no active incoming dependency edges", Traces: traces}
	}

	return dependencyEvaluation{Ready: satisfied == active, Traces: traces}
}

func evaluateAnyJoin(waiting, satisfied, impossible, active int, traces []ConditionEvaluationTrace) dependencyEvaluation {
	if satisfied > 0 {
		return dependencyEvaluation{Ready: true, Traces: traces}
	}

	if waiting > 0 {
		return dependencyEvaluation{Traces: traces}
	}

	if active == 0 {
		return dependencyEvaluation{Impossible: true, Reason: "no active incoming dependency edges", Traces: traces}
	}

	if impossible == active {
		return dependencyEvaluation{Impossible: true, Reason: "no dependency edge can be satisfied", Traces: traces}
	}

	return dependencyEvaluation{Traces: traces}
}

func (s Scheduler) evaluateEdges(ctx context.Context, node *agentos.PlanNodeSpec, edges []agentos.PlanEdgeSpec, statusByNode map[string]agentos.PlanNodeStatus, vars map[string]any) (waiting, satisfied, impossible int, traces []ConditionEvaluationTrace, err error) {
	for _, edge := range edges {
		parent, ok := statusByNode[edge.From]
		if !ok {
			return 0, 0, 0, nil, fmt.Errorf("%w: edge %q from unknown node %q", agentos.ErrInvalidRunPlan, edge.EdgeID, edge.From)
		}

		if !nodeTerminal(parent.LifecycleState) {
			waiting++

			continue
		}

		isActive, evalErr := s.evaluate(ctx, edge.Condition, vars)
		if evalErr != nil {
			return 0, 0, 0, nil, fmt.Errorf("edge %q condition: %w", edge.EdgeID, evalErr)
		}

		if edge.Condition != "" {
			traces = append(traces, ConditionEvaluationTrace{
				Scope:       "edge",
				NodeID:      node.NodeID,
				EdgeID:      edge.EdgeID,
				From:        edge.From,
				To:          edge.To,
				Expression:  edge.Condition,
				Result:      isActive,
				On:          edgeTrigger(edge.On),
				ParentState: parent.LifecycleState,
			})
		}

		if !isActive {
			continue
		}

		if edgeSatisfied(edgeTrigger(edge.On), parent.LifecycleState) {
			satisfied++
		} else {
			impossible++
		}
	}

	return waiting, satisfied, impossible, traces, nil
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
		return edgeOnCompleteSatisfied(lifecycle)
	case agentos.EdgeOnAlways:
		return edgeOnAlwaysSatisfied(lifecycle)
	default:
		return false
	}
}

func edgeOnCompleteSatisfied(lifecycle string) bool {
	switch lifecycle {
	case agentos.PlanNodeSucceeded, agentos.PlanNodeFailed, agentos.PlanNodeSkipped, agentos.PlanNodeCanceled:
		return true
	default:
		return false
	}
}

func edgeOnAlwaysSatisfied(lifecycle string) bool {
	switch lifecycle {
	case agentos.PlanNodePending, agentos.PlanNodeReady, agentos.PlanNodeRunning:
		return false
	default:
		return true
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
