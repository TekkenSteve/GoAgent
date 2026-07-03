package agentosplan

import (
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

const RESEARCH = "research"

func TestDescribeRunPlanBuildsTopologicalGraph(t *testing.T) {
	t.Parallel()

	selectedRef := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research-http"}
	spec := descriptionTestSpec()
	status := descriptionTestStatus(spec.PlanID, selectedRef)

	description, err := DescribeRunPlan(&spec, &status)
	if err != nil {
		t.Fatalf("DescribeRunPlan: %v", err)
	}

	requireDescriptionScope(t, &description, &spec)
	requireDescriptionTopology(t, &description, selectedRef)
}

func descriptionTestSpec() agentos.RunPlanSpec {
	plannedRef := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "planned-http"}

	return agentos.RunPlanSpec{
		PlanID:    "plan-1",
		ThreadID:  "thread-1",
		AccountID: "account-1",
		ProjectID: "project-1",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "write", Capability: "run", Run: agentos.RunSpec{RunID: "run-write-planned", Backend: plannedRef}, Policy: agentos.NodePolicy{Join: agentos.PlanJoinAll}},
			{NodeID: RESEARCH, Capability: "run", Run: agentos.RunSpec{RunID: "run-research-planned", Backend: plannedRef}, Outputs: []agentos.ArtifactSpec{{Name: "summary", Kind: agentos.ArtifactKindObject, Required: true}}},
		},
		Edges:    []agentos.PlanEdgeSpec{{EdgeID: "research-write", From: RESEARCH, To: "write", On: agentos.EdgeOnSuccess}},
		Metadata: map[string]string{"purpose": "test"},
	}
}

func descriptionTestStatus(planID string, selectedRef agentos.BackendRef) agentos.RunPlanStatus {
	return agentos.RunPlanStatus{
		PlanID:         planID,
		LifecycleState: agentos.PlanLifecycleRunning,
		Nodes: []agentos.PlanNodeStatus{
			{NodeID: "write", RunID: "run-write-started", Backend: selectedRef, LifecycleState: agentos.PlanNodePending},
			{NodeID: RESEARCH, RunID: "run-research-started", Backend: selectedRef, LifecycleState: agentos.PlanNodeSucceeded},
		},
		UpdatedAt: time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
	}
}

func requireDescriptionScope(t *testing.T, description *agentos.RunPlanDescription, spec *agentos.RunPlanSpec) {
	t.Helper()

	if description.PlanID != spec.PlanID || description.AccountID != spec.AccountID || description.ProjectID != spec.ProjectID {
		t.Fatalf("description scope = %#v", description)
	}
}

func requireDescriptionTopology(t *testing.T, description *agentos.RunPlanDescription, selectedRef agentos.BackendRef) {
	t.Helper()

	requireDescriptionTopologyOrder(t, description)
	requireDescriptionTopologyNodes(t, description, selectedRef)
	requireDescriptionTopologyEdges(t, description)
}

func requireDescriptionTopologyOrder(t *testing.T, description *agentos.RunPlanDescription) {
	t.Helper()

	if got := description.Topology.Order; len(got) != 2 || got[0] != RESEARCH || got[1] != "write" {
		t.Fatalf("topology order = %#v", got)
	}
}

func requireDescriptionTopologyNodes(t *testing.T, description *agentos.RunPlanDescription, selectedRef agentos.BackendRef) {
	t.Helper()

	if len(description.Topology.Nodes) != 2 ||
		description.Topology.Nodes[0].NodeID != RESEARCH ||
		description.Topology.Nodes[0].RunID != "run-research-started" ||
		description.Topology.Nodes[0].Backend != selectedRef ||
		description.Topology.Nodes[0].Status.LifecycleState != agentos.PlanNodeSucceeded ||
		description.Topology.Nodes[1].Policy.Join != agentos.PlanJoinAll {
		t.Fatalf("topology nodes = %#v", description.Topology.Nodes)
	}
}

func requireDescriptionTopologyEdges(t *testing.T, description *agentos.RunPlanDescription) {
	t.Helper()

	if len(description.Topology.Edges) != 1 || description.Topology.Edges[0].EdgeID != "research-write" {
		t.Fatalf("topology edges = %#v", description.Topology.Edges)
	}
}

func TestDescribeRunPlanRejectsInconsistentStatus(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}

	spec := agentos.RunPlanSpec{
		PlanID: "plan-1",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "known", Run: agentos.RunSpec{RunID: "run-known", Backend: ref}},
		},
	}
	status := agentos.RunPlanStatus{
		PlanID: "plan-1",
		Nodes: []agentos.PlanNodeStatus{
			{NodeID: "unknown", RunID: "run-unknown", Backend: ref},
		},
	}

	_, err := DescribeRunPlan(&spec, &status)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}
