package agentosplan

import (
	"context"
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestResolveRunInputMapsPlanInputsArtifactsAndExpressions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryArtifactStore()
	status := mappingTestPlanStatus([]agentos.ArtifactRef{putMappingTestArtifact(ctx, t, store)})

	compiler, err := NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}

	spec := mappingPlanSpecWithInputs()
	node := mappingNodeWithInputs()
	edges := mappingEdgesWithArtifactInput()

	resolved, err := ResolveRunInput(ctx, store, compiler, &spec, &status, &node, edges)
	if err != nil {
		t.Fatalf("ResolveRunInput: %v", err)
	}

	requireResolvedMappingValue(t, resolved, "existing", true)
	requireResolvedMappingValue(t, resolved, "topic", "orchestration")
	requireResolvedMappingValue(t, resolved, "summary_title", "Durable agents")
	requireResolvedMappingValue(t, resolved, "priority", float64(3))
}

func TestResolveRunInputFailsWhenRequiredArtifactIsMissing(t *testing.T) {
	t.Parallel()

	spec := mappingTestPlanSpec()
	status := mappingTestPlanStatus(nil)
	mapping := agentos.InputMapping{Target: "summary", SourceNodeID: "research", SourceArtifact: "summary", Required: true}
	node := mappingTestNode(&mapping)

	_, err := ResolveRunInput(context.Background(), NewMemoryArtifactStore(), nil, &spec, &status, &node, nil)
	if !errors.Is(err, agentos.ErrArtifactNotFound) {
		t.Fatalf("error = %v, want ErrArtifactNotFound", err)
	}
}

func TestResolveRunInputRejectsArtifactMappingWithoutSourceNode(t *testing.T) {
	t.Parallel()

	spec := mappingTestPlanSpec()
	status := mappingTestPlanStatus(nil)
	node := agentos.PlanNodeSpec{
		NodeID: "verify",
		Inputs: []agentos.InputMapping{
			{Target: "summary", SourceArtifact: "summary", Required: true},
		},
	}

	_, err := ResolveRunInput(context.Background(), NewMemoryArtifactStore(), nil, &spec, &status, &node, nil)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}

func TestResolveRunInputSkipsOptionalMissingArtifact(t *testing.T) {
	t.Parallel()

	spec := mappingTestPlanSpec()
	status := mappingTestPlanStatus(nil)
	mapping := agentos.InputMapping{Target: "summary", SourceNodeID: "research", SourceArtifact: "summary"}
	node := mappingTestNode(&mapping)

	resolved, err := ResolveRunInput(context.Background(), NewMemoryArtifactStore(), nil, &spec, &status, &node, nil)
	if err != nil {
		t.Fatalf("ResolveRunInput optional artifact: %v", err)
	}

	if resolved["existing"] != true {
		t.Fatalf("existing = %#v", resolved["existing"])
	}

	if _, ok := resolved["summary"]; ok {
		t.Fatalf("optional missing artifact wrote target: %#v", resolved)
	}
}

func TestResolveRunInputFailsWhenArtifactPayloadIsMissing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryArtifactStore()

	ref, err := putArtifact(ctx, store, &agentos.ArtifactRef{
		ArtifactID: "artifact-summary",
		PlanID:     "plan-1",
		NodeID:     "research",
		RunID:      "run-research",
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, nil, "plan-1:artifact-summary")
	if err != nil {
		t.Fatalf("Put nil artifact: %v", err)
	}

	spec := mappingTestPlanSpec()
	status := mappingTestPlanStatus([]agentos.ArtifactRef{ref})
	mapping := agentos.InputMapping{Target: "summary", SourceNodeID: "research", SourceArtifact: "summary", Required: true}
	node := mappingTestNode(&mapping)

	_, err = ResolveRunInput(ctx, store, nil, &spec, &status, &node, nil)
	if !errors.Is(err, agentos.ErrArtifactNotFound) {
		t.Fatalf("error = %v, want ErrArtifactNotFound", err)
	}
}

func TestResolveRunInputRejectsArtifactRefFromDifferentPlan(t *testing.T) {
	t.Parallel()

	spec := mappingTestPlanSpec()
	status := mappingTestPlanStatus([]agentos.ArtifactRef{
		{ArtifactID: "artifact-summary", PlanID: "other-plan", NodeID: "research", Name: "summary", Kind: agentos.ArtifactKindObject},
	})
	mapping := agentos.InputMapping{Target: "summary", SourceNodeID: "research", SourceArtifact: "summary", Required: true}
	node := mappingTestNode(&mapping)

	_, err := ResolveRunInput(context.Background(), NewMemoryArtifactStore(), nil, &spec, &status, &node, nil)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}

func TestResolveRunInputRejectsArtifactRefWithoutID(t *testing.T) {
	t.Parallel()

	spec := mappingTestPlanSpec()
	status := mappingTestPlanStatus([]agentos.ArtifactRef{
		{PlanID: "plan-1", NodeID: "research", Name: "summary", Kind: agentos.ArtifactKindObject},
	})
	mapping := agentos.InputMapping{Target: "summary", SourceNodeID: "research", SourceArtifact: "summary", Required: true}
	node := mappingTestNode(&mapping)

	_, err := ResolveRunInput(context.Background(), NewMemoryArtifactStore(), nil, &spec, &status, &node, nil)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}

func mappingTestPlanSpec() agentos.RunPlanSpec {
	return agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"}
}

func mappingTestPlanStatus(artifacts []agentos.ArtifactRef) agentos.RunPlanStatus {
	return agentos.RunPlanStatus{PlanID: "plan-1", Artifacts: artifacts}
}

func putMappingTestArtifact(ctx context.Context, t *testing.T, store *MemoryArtifactStore) agentos.ArtifactRef {
	t.Helper()

	ref, err := putArtifact(ctx, store, &agentos.ArtifactRef{
		ArtifactID: "artifact-summary",
		PlanID:     "plan-1",
		NodeID:     "research",
		RunID:      "run-research",
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{
		"body": map[string]any{
			"title": "Durable agents",
		},
	}, "plan-1:artifact-summary")
	if err != nil {
		t.Fatalf("Put artifact: %v", err)
	}

	return ref
}

func mappingPlanSpecWithInputs() agentos.RunPlanSpec {
	spec := mappingTestPlanSpec()
	spec.Inputs = map[string]any{
		"task": map[string]any{
			"topic": "orchestration",
		},
		"priority": 2,
	}

	return spec
}

func mappingNodeWithInputs() agentos.PlanNodeSpec {
	node := mappingTestNode(nil)
	node.Inputs = []agentos.InputMapping{
		{Target: "topic", SourcePath: "task.topic", Required: true},
		{Target: "priority", Expression: "inputs.priority + 1", Required: true},
	}

	return node
}

func mappingEdgesWithArtifactInput() []agentos.PlanEdgeSpec {
	return []agentos.PlanEdgeSpec{
		{
			From: "research",
			To:   "verify",
			InputMapping: []agentos.InputMapping{
				{Target: "summary_title", SourceNodeID: "research", SourceArtifact: "summary", SourcePath: "body.title", Required: true},
			},
		},
	}
}

func requireResolvedMappingValue(t *testing.T, resolved map[string]any, key string, want any) {
	t.Helper()

	if resolved[key] != want {
		t.Fatalf("%s = %#v, want %#v", key, resolved[key], want)
	}
}

func mappingTestNode(mapping *agentos.InputMapping) agentos.PlanNodeSpec {
	inputs := []agentos.InputMapping(nil)
	if mapping != nil {
		inputs = append(inputs, *mapping)
	}

	return agentos.PlanNodeSpec{
		NodeID: "verify",
		Run: agentos.RunSpec{
			RunID: "run-verify",
			Input: map[string]any{"existing": true},
		},
		Inputs: inputs,
	}
}
