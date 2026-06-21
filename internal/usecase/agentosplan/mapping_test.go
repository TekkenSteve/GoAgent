package agentosplan

import (
	"context"
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestResolveRunInputMapsPlanInputsArtifactsAndExpressions(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryArtifactStore()
	compiler, err := NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}

	ref, err := store.Put(ctx, agentos.ArtifactRef{
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

	status := agentos.RunPlanStatus{
		PlanID: "plan-1",
		Artifacts: []agentos.ArtifactRef{
			ref,
		},
	}
	spec := agentos.RunPlanSpec{
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
		Inputs: map[string]any{
			"task": map[string]any{
				"topic": "orchestration",
			},
			"priority": 2,
		},
	}
	node := agentos.PlanNodeSpec{
		NodeID: "verify",
		Run: agentos.RunSpec{
			RunID: "run-verify",
			Input: map[string]any{
				"existing": true,
			},
		},
		Inputs: []agentos.InputMapping{
			{Target: "topic", SourcePath: "task.topic", Required: true},
			{Target: "priority", Expression: "inputs.priority + 1", Required: true},
		},
	}
	edges := []agentos.PlanEdgeSpec{
		{
			From: "research",
			To:   "verify",
			InputMapping: []agentos.InputMapping{
				{
					Target:         "summary_title",
					SourceNodeID:   "research",
					SourceArtifact: "summary",
					SourcePath:     "body.title",
					Required:       true,
				},
			},
		},
	}

	resolved, err := ResolveRunInput(ctx, store, compiler, spec, status, node, edges)
	if err != nil {
		t.Fatalf("ResolveRunInput: %v", err)
	}

	if resolved["existing"] != true {
		t.Fatalf("existing = %#v", resolved["existing"])
	}
	if resolved["topic"] != "orchestration" {
		t.Fatalf("topic = %#v", resolved["topic"])
	}
	if resolved["summary_title"] != "Durable agents" {
		t.Fatalf("summary_title = %#v", resolved["summary_title"])
	}
	if resolved["priority"] != float64(3) {
		t.Fatalf("priority = %#v", resolved["priority"])
	}
}

func TestResolveRunInputFailsWhenRequiredArtifactIsMissing(t *testing.T) {
	_, err := ResolveRunInput(
		context.Background(),
		NewMemoryArtifactStore(),
		nil,
		agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"},
		agentos.RunPlanStatus{PlanID: "plan-1"},
		agentos.PlanNodeSpec{
			NodeID: "verify",
			Run:    agentos.RunSpec{RunID: "run-verify"},
			Inputs: []agentos.InputMapping{
				{Target: "summary", SourceNodeID: "research", SourceArtifact: "summary", Required: true},
			},
		},
		nil,
	)
	if !errors.Is(err, agentos.ErrArtifactNotFound) {
		t.Fatalf("error = %v, want ErrArtifactNotFound", err)
	}
}

func TestResolveRunInputRejectsArtifactMappingWithoutSourceNode(t *testing.T) {
	_, err := ResolveRunInput(
		context.Background(),
		NewMemoryArtifactStore(),
		nil,
		agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"},
		agentos.RunPlanStatus{PlanID: "plan-1"},
		agentos.PlanNodeSpec{
			NodeID: "verify",
			Inputs: []agentos.InputMapping{
				{Target: "summary", SourceArtifact: "summary", Required: true},
			},
		},
		nil,
	)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}

func TestResolveRunInputSkipsOptionalMissingArtifact(t *testing.T) {
	resolved, err := ResolveRunInput(
		context.Background(),
		NewMemoryArtifactStore(),
		nil,
		agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"},
		agentos.RunPlanStatus{PlanID: "plan-1"},
		agentos.PlanNodeSpec{
			NodeID: "verify",
			Run: agentos.RunSpec{
				RunID: "run-verify",
				Input: map[string]any{
					"existing": true,
				},
			},
			Inputs: []agentos.InputMapping{
				{Target: "summary", SourceNodeID: "research", SourceArtifact: "summary"},
			},
		},
		nil,
	)
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
	ctx := context.Background()
	store := NewMemoryArtifactStore()
	ref, err := store.Put(ctx, agentos.ArtifactRef{
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

	_, err = ResolveRunInput(
		ctx,
		store,
		nil,
		agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"},
		agentos.RunPlanStatus{
			PlanID:    "plan-1",
			Artifacts: []agentos.ArtifactRef{ref},
		},
		agentos.PlanNodeSpec{
			NodeID: "verify",
			Run:    agentos.RunSpec{RunID: "run-verify"},
			Inputs: []agentos.InputMapping{
				{Target: "summary", SourceNodeID: "research", SourceArtifact: "summary", Required: true},
			},
		},
		nil,
	)
	if !errors.Is(err, agentos.ErrArtifactNotFound) {
		t.Fatalf("error = %v, want ErrArtifactNotFound", err)
	}
}

func TestResolveRunInputRejectsArtifactRefFromDifferentPlan(t *testing.T) {
	_, err := ResolveRunInput(
		context.Background(),
		NewMemoryArtifactStore(),
		nil,
		agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"},
		agentos.RunPlanStatus{
			PlanID: "plan-1",
			Artifacts: []agentos.ArtifactRef{
				{
					ArtifactID: "artifact-summary",
					PlanID:     "other-plan",
					NodeID:     "research",
					Name:       "summary",
					Kind:       agentos.ArtifactKindObject,
				},
			},
		},
		agentos.PlanNodeSpec{
			NodeID: "verify",
			Run:    agentos.RunSpec{RunID: "run-verify"},
			Inputs: []agentos.InputMapping{
				{Target: "summary", SourceNodeID: "research", SourceArtifact: "summary", Required: true},
			},
		},
		nil,
	)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}

func TestResolveRunInputRejectsArtifactRefWithoutID(t *testing.T) {
	_, err := ResolveRunInput(
		context.Background(),
		NewMemoryArtifactStore(),
		nil,
		agentos.RunPlanSpec{PlanID: "plan-1", AccountID: "acct-1", ProjectID: "proj-1"},
		agentos.RunPlanStatus{
			PlanID: "plan-1",
			Artifacts: []agentos.ArtifactRef{
				{
					PlanID: "plan-1",
					NodeID: "research",
					Name:   "summary",
					Kind:   agentos.ArtifactKindObject,
				},
			},
		},
		agentos.PlanNodeSpec{
			NodeID: "verify",
			Run:    agentos.RunSpec{RunID: "run-verify"},
			Inputs: []agentos.InputMapping{
				{Target: "summary", SourceNodeID: "research", SourceArtifact: "summary", Required: true},
			},
		},
		nil,
	)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}
