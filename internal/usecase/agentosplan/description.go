package agentosplan

import (
	"fmt"
	"maps"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// DescribeRunPlan builds the public topology/control-plane view from the latest
// durable plan snapshot.
func DescribeRunPlan(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus) (agentos.RunPlanDescription, error) {
	if err := validateDescribePlanInput(spec, status); err != nil {
		return agentos.RunPlanDescription{}, err
	}

	statusByNode, err := planStatusByNode(status)
	if err != nil {
		return agentos.RunPlanDescription{}, err
	}

	nodeByID, err := buildNodeIDMap(spec, statusByNode)
	if err != nil {
		return agentos.RunPlanDescription{}, err
	}

	if err := validateTopologyEdges(spec.Edges, nodeByID); err != nil {
		return agentos.RunPlanDescription{}, err
	}

	order, err := topoSort(nodeByID, spec.Edges)
	if err != nil {
		return agentos.RunPlanDescription{}, err
	}

	nodes := make([]agentos.PlanTopologyNode, 0, len(order))
	for i := range order {
		nodeID := order[i]
		node := nodeByID[nodeID]
		nodeStatus := statusByNode[nodeID]
		nodes = append(nodes, agentos.PlanTopologyNode{
			NodeID:     node.NodeID,
			RunID:      nodeStatus.RunID,
			Backend:    nodeStatus.Backend,
			Capability: node.Capability,
			Conditions: append([]string(nil), node.Conditions...),
			Inputs:     append([]agentos.InputMapping(nil), node.Inputs...),
			Outputs:    append([]agentos.ArtifactSpec(nil), node.Outputs...),
			Policy:     node.Policy,
			Status:     nodeStatus,
		})
	}

	return agentos.RunPlanDescription{
		PlanID:    spec.PlanID,
		ThreadID:  spec.ThreadID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		Status:    *status,
		Topology: agentos.PlanTopology{
			Nodes: nodes,
			Edges: planTopologyEdges(spec.Edges),
			Order: append([]string(nil), order...),
		},
		Policy:    spec.Policy,
		Metadata:  cloneStringMap(spec.Metadata),
		UpdatedAt: status.UpdatedAt,
	}, nil
}

func validateDescribePlanInput(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus) error {
	if spec.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidRunPlan)
	}

	if status.PlanID == "" {
		status.PlanID = spec.PlanID
	}

	if status.PlanID != spec.PlanID {
		return fmt.Errorf("%w: status plan id %q does not match spec plan id %q", agentoscore.ErrInvalidRunPlan, status.PlanID, spec.PlanID)
	}

	return nil
}

func buildNodeIDMap(spec *agentos.RunPlanSpec, statusByNode map[string]agentos.PlanNodeStatus) (map[string]agentos.PlanNodeSpec, error) {
	nodeByID := make(map[string]agentos.PlanNodeSpec, len(spec.Nodes))
	for i := range spec.Nodes {
		node := spec.Nodes[i]
		if node.NodeID == "" {
			return nil, fmt.Errorf("%w: node id is required", agentoscore.ErrInvalidRunPlan)
		}

		if _, exists := nodeByID[node.NodeID]; exists {
			return nil, fmt.Errorf("%w: duplicate node %q", agentoscore.ErrInvalidRunPlan, node.NodeID)
		}

		nodeByID[node.NodeID] = node
		if _, ok := statusByNode[node.NodeID]; !ok {
			return nil, fmt.Errorf("%w: status missing node %q", agentoscore.ErrInvalidRunPlan, node.NodeID)
		}
	}

	for nodeID := range statusByNode {
		if _, ok := nodeByID[nodeID]; !ok {
			return nil, fmt.Errorf("%w: status contains unknown node %q", agentoscore.ErrInvalidRunPlan, nodeID)
		}
	}

	return nodeByID, nil
}

func validateTopologyEdges(edges []agentos.PlanEdgeSpec, nodeByID map[string]agentos.PlanNodeSpec) error {
	for i := range edges {
		if err := validateTopologyEdge(&edges[i], nodeByID); err != nil {
			return err
		}
	}

	return nil
}

func planStatusByNode(status *agentos.RunPlanStatus) (map[string]agentos.PlanNodeStatus, error) {
	statusByNode := make(map[string]agentos.PlanNodeStatus, len(status.Nodes))
	for i := range status.Nodes {
		node := status.Nodes[i]
		if node.NodeID == "" {
			return nil, fmt.Errorf("%w: status node id is required", agentoscore.ErrInvalidRunPlan)
		}

		if _, exists := statusByNode[node.NodeID]; exists {
			return nil, fmt.Errorf("%w: status contains duplicate node %q", agentoscore.ErrInvalidRunPlan, node.NodeID)
		}

		statusByNode[node.NodeID] = node
	}

	return statusByNode, nil
}

func validateTopologyEdge(edge *agentos.PlanEdgeSpec, nodeByID map[string]agentos.PlanNodeSpec) error {
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

	return nil
}

func planTopologyEdges(edges []agentos.PlanEdgeSpec) []agentos.PlanTopologyEdge {
	topologyEdges := make([]agentos.PlanTopologyEdge, 0, len(edges))
	for i := range edges {
		edge := edges[i]
		topologyEdges = append(topologyEdges, agentos.PlanTopologyEdge{
			EdgeID:       edge.EdgeID,
			From:         edge.From,
			To:           edge.To,
			On:           edge.On,
			Condition:    edge.Condition,
			InputMapping: append([]agentos.InputMapping(nil), edge.InputMapping...),
		})
	}

	return topologyEdges
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}

	output := make(map[string]string, len(input))
	maps.Copy(output, input)

	return output
}
