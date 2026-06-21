package agentosplan

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// DescribeRunPlan builds the public topology/control-plane view from the latest
// durable plan snapshot.
func DescribeRunPlan(spec agentos.RunPlanSpec, status agentos.RunPlanStatus) (agentos.RunPlanDescription, error) {
	if spec.PlanID == "" {
		return agentos.RunPlanDescription{}, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if status.PlanID == "" {
		status.PlanID = spec.PlanID
	}
	if status.PlanID != spec.PlanID {
		return agentos.RunPlanDescription{}, fmt.Errorf("%w: status plan id %q does not match spec plan id %q", agentos.ErrInvalidRunPlan, status.PlanID, spec.PlanID)
	}

	statusByNode, err := planStatusByNode(status)
	if err != nil {
		return agentos.RunPlanDescription{}, err
	}
	nodeByID := make(map[string]agentos.PlanNodeSpec, len(spec.Nodes))
	for _, node := range spec.Nodes {
		if node.NodeID == "" {
			return agentos.RunPlanDescription{}, fmt.Errorf("%w: node id is required", agentos.ErrInvalidRunPlan)
		}
		if _, exists := nodeByID[node.NodeID]; exists {
			return agentos.RunPlanDescription{}, fmt.Errorf("%w: duplicate node %q", agentos.ErrInvalidRunPlan, node.NodeID)
		}
		nodeByID[node.NodeID] = node
		if _, ok := statusByNode[node.NodeID]; !ok {
			return agentos.RunPlanDescription{}, fmt.Errorf("%w: status missing node %q", agentos.ErrInvalidRunPlan, node.NodeID)
		}
	}
	for nodeID := range statusByNode {
		if _, ok := nodeByID[nodeID]; !ok {
			return agentos.RunPlanDescription{}, fmt.Errorf("%w: status contains unknown node %q", agentos.ErrInvalidRunPlan, nodeID)
		}
	}
	for _, edge := range spec.Edges {
		if err := validateTopologyEdge(edge, nodeByID); err != nil {
			return agentos.RunPlanDescription{}, err
		}
	}
	order, err := topoSort(nodeByID, spec.Edges)
	if err != nil {
		return agentos.RunPlanDescription{}, err
	}

	nodes := make([]agentos.PlanTopologyNode, 0, len(order))
	for _, nodeID := range order {
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
		Status:    status,
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

func planStatusByNode(status agentos.RunPlanStatus) (map[string]agentos.PlanNodeStatus, error) {
	statusByNode := make(map[string]agentos.PlanNodeStatus, len(status.Nodes))
	for _, node := range status.Nodes {
		if node.NodeID == "" {
			return nil, fmt.Errorf("%w: status node id is required", agentos.ErrInvalidRunPlan)
		}
		if _, exists := statusByNode[node.NodeID]; exists {
			return nil, fmt.Errorf("%w: status contains duplicate node %q", agentos.ErrInvalidRunPlan, node.NodeID)
		}
		statusByNode[node.NodeID] = node
	}

	return statusByNode, nil
}

func validateTopologyEdge(edge agentos.PlanEdgeSpec, nodeByID map[string]agentos.PlanNodeSpec) error {
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

	return nil
}

func planTopologyEdges(edges []agentos.PlanEdgeSpec) []agentos.PlanTopologyEdge {
	topologyEdges := make([]agentos.PlanTopologyEdge, 0, len(edges))
	for _, edge := range edges {
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
	for key, value := range input {
		output[key] = value
	}

	return output
}
