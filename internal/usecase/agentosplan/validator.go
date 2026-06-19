package agentosplan

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/google/jsonschema-go/jsonschema"
)

const (
	defaultMaxNodes      int32 = 256
	defaultMaxDepth      int32 = 32
	defaultMaxExpansions int32 = 64
)

// Validator validates RunPlan specs against topology, policy, expressions,
// capabilities, and artifact contracts.
type Validator struct {
	Expressions  ExpressionCompiler
	Capabilities CapabilityCatalog
}

// ExecutablePlan is a validated, deterministic RunPlan view.
type ExecutablePlan struct {
	Spec        agentos.RunPlanSpec
	NodeByID    map[string]agentos.PlanNodeSpec
	EdgesByTo   map[string][]agentos.PlanEdgeSpec
	EdgesByFrom map[string][]agentos.PlanEdgeSpec
	Order       []string
}

// Validate returns a deterministic executable plan or a precise validation error.
func (v Validator) Validate(ctx context.Context, spec agentos.RunPlanSpec) (ExecutablePlan, error) {
	if spec.PlanID == "" {
		return ExecutablePlan{}, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	policy := normalizePolicy(spec.Policy)
	if policy.MaxHistoryEvents > 0 && policy.ContinueAsNewEvents > policy.MaxHistoryEvents {
		return ExecutablePlan{}, fmt.Errorf("%w: continue-as-new events %d exceeds max history events %d", agentos.ErrInvalidRunPlan, policy.ContinueAsNewEvents, policy.MaxHistoryEvents)
	}
	if len(spec.Nodes) == 0 {
		return ExecutablePlan{}, fmt.Errorf("%w: nodes are required", agentos.ErrInvalidRunPlan)
	}
	if int32(len(spec.Nodes)) > policy.MaxNodes {
		return ExecutablePlan{}, fmt.Errorf("%w: node count %d exceeds max %d", agentos.ErrInvalidRunPlan, len(spec.Nodes), policy.MaxNodes)
	}

	nodeByID := make(map[string]agentos.PlanNodeSpec, len(spec.Nodes))
	for _, node := range spec.Nodes {
		if err := v.validateNode(ctx, spec, node); err != nil {
			return ExecutablePlan{}, err
		}
		if _, exists := nodeByID[node.NodeID]; exists {
			return ExecutablePlan{}, fmt.Errorf("%w: duplicate node %q", agentos.ErrInvalidRunPlan, node.NodeID)
		}
		nodeByID[node.NodeID] = node
	}

	edgesByFrom := make(map[string][]agentos.PlanEdgeSpec)
	edgesByTo := make(map[string][]agentos.PlanEdgeSpec)
	edgeIDs := make(map[string]struct{}, len(spec.Edges))
	for _, edge := range spec.Edges {
		if err := v.validateEdge(edge, nodeByID); err != nil {
			return ExecutablePlan{}, err
		}
		if edge.EdgeID != "" {
			if _, exists := edgeIDs[edge.EdgeID]; exists {
				return ExecutablePlan{}, fmt.Errorf("%w: duplicate edge %q", agentos.ErrInvalidRunPlan, edge.EdgeID)
			}
			edgeIDs[edge.EdgeID] = struct{}{}
		}
		if edge.Condition != "" {
			if err := v.compileExpression(edge.Condition); err != nil {
				return ExecutablePlan{}, fmt.Errorf("edge %q: %w", edge.EdgeID, err)
			}
		}
		edgesByFrom[edge.From] = append(edgesByFrom[edge.From], edge)
		edgesByTo[edge.To] = append(edgesByTo[edge.To], edge)
	}
	sortEdges(edgesByFrom)
	sortEdges(edgesByTo)

	order, err := topoSort(nodeByID, spec.Edges)
	if err != nil {
		return ExecutablePlan{}, err
	}
	if int32(maxDepth(order, edgesByFrom)) > policy.MaxDepth {
		return ExecutablePlan{}, fmt.Errorf("%w: plan depth exceeds max %d", agentos.ErrInvalidRunPlan, policy.MaxDepth)
	}

	return ExecutablePlan{
		Spec:        spec,
		NodeByID:    nodeByID,
		EdgesByTo:   edgesByTo,
		EdgesByFrom: edgesByFrom,
		Order:       order,
	}, nil
}

func (v Validator) validateNode(ctx context.Context, spec agentos.RunPlanSpec, node agentos.PlanNodeSpec) error {
	if node.NodeID == "" {
		return fmt.Errorf("%w: node id is required", agentos.ErrInvalidRunPlan)
	}
	if node.Run.RunID == "" {
		return fmt.Errorf("%w: node %q run id is required", agentos.ErrInvalidRunSpec, node.NodeID)
	}
	if node.Run.Backend.Kind == "" || node.Run.Backend.Name == "" {
		return fmt.Errorf("%w: node %q backend is required", agentos.ErrInvalidBackendRef, node.NodeID)
	}
	for _, condition := range node.Conditions {
		if err := v.compileExpression(condition); err != nil {
			return fmt.Errorf("node %q: %w", node.NodeID, err)
		}
	}
	switch node.Policy.Join {
	case "", agentos.PlanJoinAll, agentos.PlanJoinAny, agentos.PlanJoinFirst:
	default:
		return fmt.Errorf("%w: node %q has invalid join strategy %q", agentos.ErrInvalidRunPlan, node.NodeID, node.Policy.Join)
	}
	for _, mapping := range node.Inputs {
		if mapping.Target == "" {
			return fmt.Errorf("%w: node %q input mapping target is required", agentos.ErrInvalidRunPlan, node.NodeID)
		}
		if mapping.Expression != "" {
			if err := v.compileExpression(mapping.Expression); err != nil {
				return fmt.Errorf("node %q input %q: %w", node.NodeID, mapping.Target, err)
			}
		}
	}
	if err := ValidateArtifactSpecs(node.NodeID, node.Outputs); err != nil {
		return err
	}
	if node.Capability == "" {
		return nil
	}
	if v.Capabilities == nil {
		return fmt.Errorf("%w: node %q capability catalog is required", agentos.ErrCapabilityNotFound, node.NodeID)
	}

	capability, ok, err := v.Capabilities.GetCapability(ctx, node.Run.Backend, node.Capability)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: node %q capability %s/%s/%s", agentos.ErrCapabilityNotFound, node.NodeID, node.Run.Backend.Kind, node.Run.Backend.Name, node.Capability)
	}
	if len(capability.InputSchema) > 0 {
		if err := validateRawSchema(capability.InputSchema, node.Run.Input); err != nil {
			return fmt.Errorf("%w: node %q input schema: %s", agentos.ErrInvalidRunPlan, node.NodeID, err)
		}
	}
	_ = spec

	return nil
}

func (v Validator) validateEdge(edge agentos.PlanEdgeSpec, nodeByID map[string]agentos.PlanNodeSpec) error {
	if edge.From == "" || edge.To == "" {
		return fmt.Errorf("%w: edge endpoints are required", agentos.ErrInvalidRunPlan)
	}
	if _, ok := nodeByID[edge.From]; !ok {
		return fmt.Errorf("%w: edge from unknown node %q", agentos.ErrInvalidRunPlan, edge.From)
	}
	if _, ok := nodeByID[edge.To]; !ok {
		return fmt.Errorf("%w: edge to unknown node %q", agentos.ErrInvalidRunPlan, edge.To)
	}
	if edge.From == edge.To {
		return fmt.Errorf("%w: self edge on node %q", agentos.ErrInvalidRunPlan, edge.From)
	}
	switch edge.On {
	case "", agentos.EdgeOnSuccess, agentos.EdgeOnError, agentos.EdgeOnComplete, agentos.EdgeOnAlways:
	default:
		return fmt.Errorf("%w: edge %q has invalid trigger %q", agentos.ErrInvalidRunPlan, edge.EdgeID, edge.On)
	}
	for _, mapping := range edge.InputMapping {
		if mapping.Target == "" {
			return fmt.Errorf("%w: edge %q input mapping target is required", agentos.ErrInvalidRunPlan, edge.EdgeID)
		}
		if mapping.SourceNodeID != "" && mapping.SourceNodeID != edge.From {
			return fmt.Errorf("%w: edge %q mapping source must match edge source", agentos.ErrInvalidRunPlan, edge.EdgeID)
		}
	}

	return nil
}

func (v Validator) compileExpression(expression string) error {
	if expression == "" || v.Expressions == nil {
		return nil
	}
	_, err := v.Expressions.Compile(expression)

	return err
}

func normalizePolicy(policy agentos.PlanPolicy) agentos.PlanPolicy {
	if policy.MaxNodes <= 0 {
		policy.MaxNodes = defaultMaxNodes
	}
	if policy.MaxDepth <= 0 {
		policy.MaxDepth = defaultMaxDepth
	}
	if policy.MaxExpansions <= 0 {
		policy.MaxExpansions = defaultMaxExpansions
	}

	return policy
}

func validateRawSchema(raw json.RawMessage, value any) error {
	var schema jsonschema.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		return err
	}
	resolved, err := schema.Resolve(nil)
	if err != nil {
		return err
	}

	return resolved.Validate(value)
}

func topoSort(nodeByID map[string]agentos.PlanNodeSpec, edges []agentos.PlanEdgeSpec) ([]string, error) {
	incoming := make(map[string]int, len(nodeByID))
	children := make(map[string][]string, len(nodeByID))
	for id := range nodeByID {
		incoming[id] = 0
	}
	for _, edge := range edges {
		incoming[edge.To]++
		children[edge.From] = append(children[edge.From], edge.To)
	}
	for id := range children {
		sort.Strings(children[id])
	}

	queue := make([]string, 0, len(nodeByID))
	for id, count := range incoming {
		if count == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)

	order := make([]string, 0, len(nodeByID))
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		order = append(order, current)
		for _, child := range children[current] {
			incoming[child]--
			if incoming[child] == 0 {
				queue = append(queue, child)
				sort.Strings(queue)
			}
		}
	}
	if len(order) != len(nodeByID) {
		return nil, fmt.Errorf("%w: plan contains a cycle", agentos.ErrInvalidRunPlan)
	}

	return order, nil
}

func maxDepth(order []string, edgesByFrom map[string][]agentos.PlanEdgeSpec) int {
	depth := make(map[string]int, len(order))
	maxValue := 0
	for _, nodeID := range order {
		current := depth[nodeID]
		if current > maxValue {
			maxValue = current
		}
		for _, edge := range edgesByFrom[nodeID] {
			if depth[edge.To] < current+1 {
				depth[edge.To] = current + 1
			}
		}
	}

	return maxValue + 1
}

func sortEdges(edges map[string][]agentos.PlanEdgeSpec) {
	for key := range edges {
		sort.Slice(edges[key], func(i, j int) bool {
			if edges[key][i].From != edges[key][j].From {
				return edges[key][i].From < edges[key][j].From
			}

			return edges[key][i].To < edges[key][j].To
		})
	}
}
