package agentosplan

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/google/jsonschema-go/jsonschema"
)

const (
	defaultMaxNodes      int32 = 256
	defaultMaxDepth      int32 = 32
	defaultMaxExpansions int32 = 64
	defaultBatchInputKey       = "items"
)

// Validator validates RunPlan specs against topology, policy, expressions,
// capabilities, and artifact contracts.
type Validator struct {
	Expressions     ExpressionCompiler
	Capabilities    CapabilityCatalog
	ArtifactSchemas ArtifactSchemaCatalog
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
func (v Validator) Validate(ctx context.Context, spec *agentos.RunPlanSpec) (ExecutablePlan, error) {
	policy, err := validatePlanEnvelope(spec)
	if err != nil {
		return ExecutablePlan{}, err
	}

	nodeByID, err := v.validateNodes(ctx, spec)
	if err != nil {
		return ExecutablePlan{}, err
	}

	edgesByFrom, edgesByTo, err := v.validateEdges(spec, nodeByID)
	if err != nil {
		return ExecutablePlan{}, err
	}

	order, err := topoSort(nodeByID, spec.Edges)
	if err != nil {
		return ExecutablePlan{}, err
	}

	if err := validateInputMappingContracts(spec, nodeByID, edgesByFrom); err != nil {
		return ExecutablePlan{}, err
	}

	depth := maxDepth(order, edgesByFrom)
	if depth > int(policy.MaxDepth) {
		return ExecutablePlan{}, fmt.Errorf("%w: plan depth exceeds max %d", agentoscore.ErrInvalidRunPlan, policy.MaxDepth)
	}

	return ExecutablePlan{
		Spec:        *spec,
		NodeByID:    nodeByID,
		EdgesByTo:   edgesByTo,
		EdgesByFrom: edgesByFrom,
		Order:       order,
	}, nil
}

func validatePlanEnvelope(spec *agentos.RunPlanSpec) (agentos.PlanPolicy, error) {
	if err := ValidateRunPlanScope(spec); err != nil {
		return agentos.PlanPolicy{}, err
	}

	policy := normalizePolicy(spec.Policy)
	if policy.MaxHistoryEvents > 0 && policy.ContinueAsNewEvents > policy.MaxHistoryEvents {
		return agentos.PlanPolicy{}, fmt.Errorf("%w: continue-as-new events %d exceeds max history events %d", agentoscore.ErrInvalidRunPlan, policy.ContinueAsNewEvents, policy.MaxHistoryEvents)
	}

	if len(spec.Nodes) == 0 {
		return agentos.PlanPolicy{}, fmt.Errorf("%w: nodes are required", agentoscore.ErrInvalidRunPlan)
	}

	if len(spec.Nodes) > int(policy.MaxNodes) {
		return agentos.PlanPolicy{}, fmt.Errorf("%w: node count %d exceeds max %d", agentoscore.ErrInvalidRunPlan, len(spec.Nodes), policy.MaxNodes)
	}

	return policy, nil
}

func (v Validator) validateNodes(ctx context.Context, spec *agentos.RunPlanSpec) (map[string]agentos.PlanNodeSpec, error) {
	nodeByID := make(map[string]agentos.PlanNodeSpec, len(spec.Nodes))
	for i := range spec.Nodes {
		node := &spec.Nodes[i]
		if err := v.validateNode(ctx, node); err != nil {
			return nil, err
		}

		if _, exists := nodeByID[node.NodeID]; exists {
			return nil, fmt.Errorf("%w: duplicate node %q", agentoscore.ErrInvalidRunPlan, node.NodeID)
		}

		nodeByID[node.NodeID] = *node
	}

	return nodeByID, nil
}

func (v Validator) validateEdges(spec *agentos.RunPlanSpec, nodeByID map[string]agentos.PlanNodeSpec) (edgesByFrom, edgesByTo map[string][]agentos.PlanEdgeSpec, err error) {
	edgesByFrom = make(map[string][]agentos.PlanEdgeSpec)
	edgesByTo = make(map[string][]agentos.PlanEdgeSpec)
	edgeIDs := make(map[string]struct{}, len(spec.Edges))

	for i := range spec.Edges {
		edge := &spec.Edges[i]
		if err := v.validateEdge(edge, nodeByID, edgeIDs); err != nil {
			return nil, nil, err
		}

		edgesByFrom[edge.From] = append(edgesByFrom[edge.From], *edge)
		edgesByTo[edge.To] = append(edgesByTo[edge.To], *edge)
	}

	sortEdges(edgesByFrom)
	sortEdges(edgesByTo)

	return edgesByFrom, edgesByTo, nil
}

func (v Validator) validateNode(ctx context.Context, node *agentos.PlanNodeSpec) error {
	if err := validateNodeIdentity(node); err != nil {
		return err
	}

	if err := v.validateNodeExpressions(node); err != nil {
		return err
	}

	if err := ValidateArtifactSpecs(node.NodeID, node.Outputs); err != nil {
		return err
	}

	if err := ValidateArtifactSchemaRefs(ctx, v.ArtifactSchemas, node.NodeID, node.Outputs); err != nil {
		return err
	}

	return v.validateNodeCapability(ctx, node)
}

func validateNodeIdentity(node *agentos.PlanNodeSpec) error {
	if node.NodeID == "" {
		return fmt.Errorf("%w: node id is required", agentoscore.ErrInvalidRunPlan)
	}

	if node.Run.RunID == "" {
		return fmt.Errorf("%w: node %q run id is required", agentoscore.ErrInvalidRunSpec, node.NodeID)
	}

	if node.Run.Backend.Kind == "" || node.Run.Backend.Name == "" {
		return fmt.Errorf("%w: node %q backend is required", agentoscore.ErrInvalidBackendRef, node.NodeID)
	}

	switch node.Policy.Join {
	case "", agentos.PlanJoinAll, agentos.PlanJoinAny, agentos.PlanJoinFirst:
	default:
		return fmt.Errorf("%w: node %q has invalid join strategy %q", agentoscore.ErrInvalidRunPlan, node.NodeID, node.Policy.Join)
	}

	return nil
}

func (v Validator) validateNodeExpressions(node *agentos.PlanNodeSpec) error {
	for _, condition := range node.Conditions {
		if err := v.compileExpression(condition); err != nil {
			return fmt.Errorf("node %q: %w", node.NodeID, err)
		}
	}

	for i := range node.Inputs {
		mapping := &node.Inputs[i]

		mode, err := validateInputMappingShape(fmt.Sprintf("node %q", node.NodeID), mapping)
		if err != nil {
			return err
		}

		if mode == inputMappingSourceExpression {
			if err := v.compileExpression(mapping.Expression); err != nil {
				return fmt.Errorf("node %q input %q: %w", node.NodeID, mapping.Target, err)
			}
		}
	}

	return nil
}

func (v Validator) validateNodeCapability(ctx context.Context, node *agentos.PlanNodeSpec) error {
	if node.Capability == "" {
		return nil
	}

	if v.Capabilities == nil {
		return fmt.Errorf("%w: node %q capability catalog is required", agentoscore.ErrCapabilityNotFound, node.NodeID)
	}

	capability, ok, err := v.Capabilities.GetCapability(ctx, node.Run.Backend, node.Capability)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("%w: node %q capability %s/%s/%s", agentoscore.ErrCapabilityNotFound, node.NodeID, node.Run.Backend.Kind, node.Run.Backend.Name, node.Capability)
	}

	if len(capability.InputSchema) > 0 {
		if err := validateRawSchema(capability.InputSchema, node.Run.Input); err != nil {
			return fmt.Errorf("%w: node %q input schema: %w", agentoscore.ErrInvalidRunPlan, node.NodeID, err)
		}
	}

	if err := validateNodeBatchLimits(node, &capability); err != nil {
		return err
	}

	return nil
}

func validateNodeBatchLimits(node *agentos.PlanNodeSpec, capability *agentos.Capability) error {
	if capability.Limits.MaxBatchItems == 0 {
		return nil
	}

	key := capability.Limits.BatchInputKey
	if key == "" {
		key = defaultBatchInputKey
	}

	value, ok := node.Run.Input[key]
	if !ok {
		return fmt.Errorf("%w: node %q batch input %q is required", agentoscore.ErrInvalidRunPlan, node.NodeID, key)
	}

	size, ok := batchSize(value)
	if !ok {
		return fmt.Errorf("%w: node %q batch input %q must be an array", agentoscore.ErrInvalidRunPlan, node.NodeID, key)
	}

	if size > int(capability.Limits.MaxBatchItems) {
		return fmt.Errorf("%w: node %q batch input %q has %d items, exceeds max %d", agentoscore.ErrInvalidRunPlan, node.NodeID, key, size, capability.Limits.MaxBatchItems)
	}

	return nil
}

func batchSize(value any) (int, bool) {
	if value == nil {
		return 0, false
	}

	v := reflect.ValueOf(value)

	kind := v.Kind()
	if kind == reflect.Array || kind == reflect.Slice {
		return v.Len(), true
	}

	return 0, false
}

func (v Validator) validateEdge(edge *agentos.PlanEdgeSpec, nodeByID map[string]agentos.PlanNodeSpec, edgeIDs map[string]struct{}) error {
	if err := validateEdgeIdentity(edge, nodeByID); err != nil {
		return err
	}

	if err := validateUniqueEdgeID(edge, edgeIDs); err != nil {
		return err
	}

	if edge.Condition != "" {
		if err := v.compileExpression(edge.Condition); err != nil {
			return fmt.Errorf("edge %q: %w", edge.EdgeID, err)
		}
	}

	for i := range edge.InputMapping {
		mapping := &edge.InputMapping[i]

		mode, err := validateInputMappingShape(fmt.Sprintf("edge %q", edge.EdgeID), mapping)
		if err != nil {
			return err
		}

		if mode == inputMappingSourceArtifact && mapping.SourceNodeID != edge.From {
			return fmt.Errorf("%w: edge %q mapping source must match edge source", agentoscore.ErrInvalidRunPlan, edge.EdgeID)
		}
	}

	return nil
}

func validateEdgeIdentity(edge *agentos.PlanEdgeSpec, nodeByID map[string]agentos.PlanNodeSpec) error {
	if edge.From == "" || edge.To == "" {
		return fmt.Errorf("%w: edge endpoints are required", agentoscore.ErrInvalidRunPlan)
	}

	if _, ok := nodeByID[edge.From]; !ok {
		return fmt.Errorf("%w: edge from unknown node %q", agentoscore.ErrInvalidRunPlan, edge.From)
	}

	if _, ok := nodeByID[edge.To]; !ok {
		return fmt.Errorf("%w: edge to unknown node %q", agentoscore.ErrInvalidRunPlan, edge.To)
	}

	if edge.From == edge.To {
		return fmt.Errorf("%w: self edge on node %q", agentoscore.ErrInvalidRunPlan, edge.From)
	}

	switch edge.On {
	case "", agentos.EdgeOnSuccess, agentos.EdgeOnError, agentos.EdgeOnComplete, agentos.EdgeOnAlways:
	default:
		return fmt.Errorf("%w: edge %q has invalid trigger %q", agentoscore.ErrInvalidRunPlan, edge.EdgeID, edge.On)
	}

	return nil
}

func validateUniqueEdgeID(edge *agentos.PlanEdgeSpec, edgeIDs map[string]struct{}) error {
	if edge.EdgeID == "" {
		return nil
	}

	if _, exists := edgeIDs[edge.EdgeID]; exists {
		return fmt.Errorf("%w: duplicate edge %q", agentoscore.ErrInvalidRunPlan, edge.EdgeID)
	}

	edgeIDs[edge.EdgeID] = struct{}{}

	return nil
}

func validateInputMappingContracts(spec *agentos.RunPlanSpec, nodeByID map[string]agentos.PlanNodeSpec, edgesByFrom map[string][]agentos.PlanEdgeSpec) error {
	for i := range spec.Nodes {
		if err := validateNodeInputMappingContracts(&spec.Nodes[i], nodeByID, edgesByFrom); err != nil {
			return err
		}
	}

	for i := range spec.Edges {
		if err := validateEdgeInputMappingContracts(&spec.Edges[i], nodeByID); err != nil {
			return err
		}
	}

	return nil
}

func validateNodeInputMappingContracts(node *agentos.PlanNodeSpec, nodeByID map[string]agentos.PlanNodeSpec, edgesByFrom map[string][]agentos.PlanEdgeSpec) error {
	for _, mapping := range node.Inputs {
		if mapping.SourceArtifact == "" {
			continue
		}

		source, ok := nodeByID[mapping.SourceNodeID]
		if !ok {
			return fmt.Errorf("%w: node %q input %q source node %q is unknown", agentoscore.ErrInvalidRunPlan, node.NodeID, mapping.Target, mapping.SourceNodeID)
		}

		if err := validateArtifactMappingSource(node, &source, &mapping, edgesByFrom); err != nil {
			return err
		}
	}

	return nil
}

func validateArtifactMappingSource(node, source *agentos.PlanNodeSpec, mapping *agentos.InputMapping, edgesByFrom map[string][]agentos.PlanEdgeSpec) error {
	if source.NodeID == node.NodeID {
		return fmt.Errorf("%w: node %q input %q cannot map an artifact from the same node", agentoscore.ErrInvalidRunPlan, node.NodeID, mapping.Target)
	}

	if !nodeDeclaresArtifact(source, mapping.SourceArtifact) {
		return artifactContractError(source.NodeID, "artifact %q is not declared for mapping into node %q", mapping.SourceArtifact, node.NodeID)
	}

	if !hasDependencyPath(edgesByFrom, source.NodeID, node.NodeID) {
		return fmt.Errorf("%w: node %q input %q requires a dependency path from source node %q", agentoscore.ErrInvalidRunPlan, node.NodeID, mapping.Target, source.NodeID)
	}

	return nil
}

func validateEdgeInputMappingContracts(edge *agentos.PlanEdgeSpec, nodeByID map[string]agentos.PlanNodeSpec) error {
	source := nodeByID[edge.From]
	for _, mapping := range edge.InputMapping {
		if mapping.SourceArtifact == "" {
			continue
		}

		if !nodeDeclaresArtifact(&source, mapping.SourceArtifact) {
			return artifactContractError(source.NodeID, "artifact %q is not declared for edge %q mapping into node %q", mapping.SourceArtifact, edge.EdgeID, edge.To)
		}
	}

	return nil
}

func nodeDeclaresArtifact(node *agentos.PlanNodeSpec, name string) bool {
	for _, output := range node.Outputs {
		if output.Name == name {
			return true
		}
	}

	return false
}

func hasDependencyPath(edgesByFrom map[string][]agentos.PlanEdgeSpec, from, to string) bool {
	visited := map[string]struct{}{from: {}}

	queue := []string{from}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		for _, edge := range edgesByFrom[current] {
			if edge.To == to {
				return true
			}

			if _, ok := visited[edge.To]; ok {
				continue
			}

			visited[edge.To] = struct{}{}
			queue = append(queue, edge.To)
		}
	}

	return false
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
	resolved, err := resolveRawSchema(raw)
	if err != nil {
		return err
	}

	return resolved.Validate(value)
}

func validateRawSchemaSyntax(raw json.RawMessage) error {
	_, err := resolveRawSchema(raw)

	return err
}

func resolveRawSchema(raw json.RawMessage) (*jsonschema.Resolved, error) {
	var schema jsonschema.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil, err
	}

	resolved, err := schema.Resolve(nil)
	if err != nil {
		return nil, err
	}

	return resolved, nil
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
		return nil, fmt.Errorf("%w: plan contains a cycle", agentoscore.ErrInvalidRunPlan)
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
