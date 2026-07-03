package agentosplan

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestArtifactPlanDeltaProviderDecodesPlanDeltaArtifact(t *testing.T) {
	t.Parallel()

	store := NewMemoryArtifactStore()

	ref, err := putArtifact(context.Background(), store, &agentos.ArtifactRef{
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

	input := PlanDeltaInput{
		Spec:      agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"},
		Artifacts: []agentos.ArtifactRef{ref},
	}

	delta, ok, err := NewArtifactPlanDeltaProvider(store).NextPlanDelta(context.Background(), &input)
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

func TestArtifactPlanDeltaProviderReadsArtifactWithTenantScope(t *testing.T) {
	t.Parallel()

	store := &recordingPlanDeltaArtifactStore{
		ref: agentos.ArtifactRef{
			ArtifactID: "delta-1",
			PlanID:     "plan-1",
			Name:       "expand",
			Kind:       agentos.ArtifactKindPlanDelta,
		},
		payload: PlanDelta{
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
		},
	}

	input := PlanDeltaInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    "plan-1",
			AccountID: "acct-1",
			ProjectID: "proj-1",
		},
		Artifacts: []agentos.ArtifactRef{store.ref},
	}

	_, ok, err := NewArtifactPlanDeltaProvider(store).NextPlanDelta(context.Background(), &input)
	if err != nil {
		t.Fatalf("NextPlanDelta: %v", err)
	}

	if !ok {
		t.Fatal("NextPlanDelta did not detect plan_delta artifact")
	}

	if store.scope.PlanID != "plan-1" || store.scope.AccountID != "acct-1" || store.scope.ProjectID != "proj-1" || store.scope.ArtifactID != "delta-1" {
		t.Fatalf("artifact scope = %#v", store.scope)
	}
}

func TestArtifactPlanDeltaProviderFailsWhenPayloadMissing(t *testing.T) {
	t.Parallel()

	store := NewMemoryArtifactStore()

	ref, err := putArtifact(context.Background(), store, &agentos.ArtifactRef{
		ArtifactID: "delta-1",
		PlanID:     "plan-1",
		Name:       "expand",
		Kind:       agentos.ArtifactKindPlanDelta,
	}, nil, "delta-key")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	input := PlanDeltaInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    "plan-1",
			AccountID: "acct-1",
			ProjectID: "proj-1",
		},
		Artifacts: []agentos.ArtifactRef{ref},
	}

	_, _, err = NewArtifactPlanDeltaProvider(store).NextPlanDelta(context.Background(), &input)
	if !errors.Is(err, agentos.ErrArtifactNotFound) {
		t.Fatalf("error = %v, want ErrArtifactNotFound", err)
	}
}

func TestDecodePlanDeltaPayloadRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	_, err := DecodePlanDeltaPayload(map[string]any{
		"nodes": []any{
			map[string]any{
				"node_id":    "expanded",
				"unexpected": true,
				"run": map[string]any{
					"run_id": "run-expanded",
					"backend": map[string]any{
						"kind": "native",
						"name": "goagent-native",
					},
				},
			},
		},
	})
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}

	if !strings.Contains(err.Error(), `unknown field "unexpected"`) {
		t.Fatalf("error = %v, want unknown field", err)
	}
}

type recordingPlanDeltaArtifactStore struct {
	scope   agentos.PlanArtifactScope
	ref     agentos.ArtifactRef
	payload any
}

func (s *recordingPlanDeltaArtifactStore) Put(context.Context, *agentos.ArtifactRef, any, string) (agentos.ArtifactRef, error) {
	return agentos.ArtifactRef{}, nil
}

func (s *recordingPlanDeltaArtifactStore) Get(_ context.Context, scope *agentos.PlanArtifactScope) (agentos.ArtifactRef, any, error) {
	s.scope = *scope

	return s.ref, s.payload, nil
}

func (s *recordingPlanDeltaArtifactStore) List(context.Context, *agentos.PlanArtifactScope) ([]agentos.ArtifactRef, error) {
	return nil, nil
}

func TestStateReducerAppliesPlanExpansion(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-1",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "seed", Run: agentos.RunSpec{RunID: "run-seed", Backend: ref}},
		},
	}
	state := NewState(&spec, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC))

	if err := state.Apply(&StateEvent{
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
