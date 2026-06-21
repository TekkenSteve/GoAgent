package agentosplan

import (
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestDescribeRunPlanBuildsTopologicalGraph(t *testing.T) {
	plannedRef := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "planned-http"}
	selectedRef := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research-http"}
	spec := agentos.RunPlanSpec{
		PlanID:    "plan-1",
		ThreadID:  "thread-1",
		AccountID: "account-1",
		ProjectID: "project-1",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID:     "write",
				Capability: "run",
				Run:        agentos.RunSpec{RunID: "run-write-planned", Backend: plannedRef},
				Policy:     agentos.NodePolicy{Join: agentos.PlanJoinAll},
			},
			{
				NodeID:     "research",
				Capability: "run",
				Run:        agentos.RunSpec{RunID: "run-research-planned", Backend: plannedRef},
				Outputs: []agentos.ArtifactSpec{
					{Name: "summary", Kind: agentos.ArtifactKindObject, Required: true},
				},
			},
		},
		Edges: []agentos.PlanEdgeSpec{
			{EdgeID: "research-write", From: "research", To: "write", On: agentos.EdgeOnSuccess},
		},
		Metadata: map[string]string{"purpose": "test"},
	}
	status := agentos.RunPlanStatus{
		PlanID:         spec.PlanID,
		LifecycleState: agentos.PlanLifecycleRunning,
		Nodes: []agentos.PlanNodeStatus{
			{
				NodeID:         "write",
				RunID:          "run-write-started",
				Backend:        selectedRef,
				LifecycleState: agentos.PlanNodePending,
			},
			{
				NodeID:         "research",
				RunID:          "run-research-started",
				Backend:        selectedRef,
				LifecycleState: agentos.PlanNodeSucceeded,
			},
		},
		UpdatedAt: time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
	}

	description, err := DescribeRunPlan(spec, status)
	if err != nil {
		t.Fatalf("DescribeRunPlan: %v", err)
	}
	if description.PlanID != spec.PlanID || description.AccountID != spec.AccountID || description.ProjectID != spec.ProjectID {
		t.Fatalf("description scope = %#v", description)
	}
	if got := description.Topology.Order; len(got) != 2 || got[0] != "research" || got[1] != "write" {
		t.Fatalf("topology order = %#v", got)
	}
	if len(description.Topology.Nodes) != 2 ||
		description.Topology.Nodes[0].NodeID != "research" ||
		description.Topology.Nodes[0].RunID != "run-research-started" ||
		description.Topology.Nodes[0].Backend != selectedRef ||
		description.Topology.Nodes[0].Status.LifecycleState != agentos.PlanNodeSucceeded ||
		description.Topology.Nodes[1].Policy.Join != agentos.PlanJoinAll {
		t.Fatalf("topology nodes = %#v", description.Topology.Nodes)
	}
	if len(description.Topology.Edges) != 1 || description.Topology.Edges[0].EdgeID != "research-write" {
		t.Fatalf("topology edges = %#v", description.Topology.Edges)
	}
}

func TestDescribeRunPlanRejectsInconsistentStatus(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	_, err := DescribeRunPlan(
		agentos.RunPlanSpec{
			PlanID: "plan-1",
			Nodes: []agentos.PlanNodeSpec{
				{NodeID: "known", Run: agentos.RunSpec{RunID: "run-known", Backend: ref}},
			},
		},
		agentos.RunPlanStatus{
			PlanID: "plan-1",
			Nodes: []agentos.PlanNodeStatus{
				{NodeID: "unknown", RunID: "run-unknown", Backend: ref},
			},
		},
	)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}
