package agentosplan

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestArtifactPlanDeltaProviderDecodesPlanDeltaArtifact(t *testing.T) {
	store := NewMemoryArtifactStore()
	ref, err := store.Put(context.Background(), agentos.ArtifactRef{
		ArtifactID: "delta-1",
		PlanID:     "plan-1",
		NodeID:     "seed",
		RunID:      "run-seed",
		Name:       "expand",
		Kind:       agentos.ArtifactKindPlanDelta,
	}, PlanDelta{
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "expanded",
				Run: agentos.RunSpec{
					RunID: "run-expanded",
					Backend: agentos.BackendRef{
						Kind: agentos.BackendKindNative,
						Name: agentos.BackendNameGoAgentNative,
					},
				},
			},
		},
		Edges: []agentos.PlanEdgeSpec{
			{EdgeID: "seed-expanded", From: "seed", To: "expanded", On: agentos.EdgeOnSuccess},
		},
	}, "delta-key")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	delta, ok, err := NewArtifactPlanDeltaProvider(store).NextPlanDelta(context.Background(), PlanDeltaInput{
		Artifacts: []agentos.ArtifactRef{ref},
	})
	if err != nil {
		t.Fatalf("NextPlanDelta: %v", err)
	}
	if !ok {
		t.Fatal("NextPlanDelta did not detect plan_delta artifact")
	}
	if len(delta.Nodes) != 1 || delta.Nodes[0].NodeID != "expanded" {
		t.Fatalf("delta nodes = %#v", delta.Nodes)
	}
	if len(delta.Edges) != 1 || delta.Edges[0].EdgeID != "seed-expanded" {
		t.Fatalf("delta edges = %#v", delta.Edges)
	}
}

func TestArtifactPlanDeltaProviderFailsWhenPayloadMissing(t *testing.T) {
	store := NewMemoryArtifactStore()
	ref, err := store.Put(context.Background(), agentos.ArtifactRef{
		ArtifactID: "delta-1",
		Name:       "expand",
		Kind:       agentos.ArtifactKindPlanDelta,
	}, nil, "delta-key")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	_, _, err = NewArtifactPlanDeltaProvider(store).NextPlanDelta(context.Background(), PlanDeltaInput{
		Artifacts: []agentos.ArtifactRef{ref},
	})
	if !errors.Is(err, agentos.ErrArtifactNotFound) {
		t.Fatalf("error = %v, want ErrArtifactNotFound", err)
	}
}

func TestStateReducerAppliesPlanExpansion(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-1",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "seed", Run: agentos.RunSpec{RunID: "run-seed", Backend: ref}},
		},
	}
	state := NewState(spec, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC))

	if err := state.Apply(StateEvent{
		Kind: EventPlanExpanded,
		Expansion: PlanDelta{Nodes: []agentos.PlanNodeSpec{
			{NodeID: "expanded", Run: agentos.RunSpec{RunID: "run-expanded", Backend: ref}},
		}},
	}); err != nil {
		t.Fatalf("Apply expansion: %v", err)
	}

	status, ok := state.NodeStatus("expanded")
	if !ok {
		t.Fatal("expanded node was not added")
	}
	if status.LifecycleState != agentos.PlanNodePending || status.RunID != "run-expanded" {
		t.Fatalf("expanded status = %#v", status)
	}
}
