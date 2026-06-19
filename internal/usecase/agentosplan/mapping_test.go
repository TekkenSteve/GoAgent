package agentosplan

import (
	"context"
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

	resolved, err := ResolveRunInput(ctx, store, compiler, map[string]any{
		"task": map[string]any{
			"topic": "orchestration",
		},
		"priority": 2,
	}, status, node, edges)
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
