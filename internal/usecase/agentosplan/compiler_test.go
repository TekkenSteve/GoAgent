package agentosplan

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestRunPlanCompilerRejectsUnknownJSONField(t *testing.T) {
	t.Parallel()

	compiler := testRunPlanCompiler(t)
	_, err := compiler.CompileJSON(context.Background(), []byte(`{
	  "plan_id": "plan-strict-json",
	  "account_id": "acct-strict",
	  "project_id": "proj-strict",
	  "nodes": [
	    {
	      "node_id": "research",
	      "capabilty": "typo",
	      "run": {
	        "run_id": "run-research",
	        "backend": {"kind": "native", "name": "goagent-native"}
	      }
	    }
	  ]
	}`))
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
	if !strings.Contains(err.Error(), `unknown field "capabilty"`) {
		t.Fatalf("error = %v, want unknown field", err)
	}
}

func TestRunPlanCompilerRejectsUnknownYAMLField(t *testing.T) {
	t.Parallel()

	compiler := testRunPlanCompiler(t)
	_, err := compiler.CompileYAML(context.Background(), []byte(`
plan_id: plan-strict-yaml
account_id: acct-strict
project_id: proj-strict
nodes:
  - node_id: research
    capabilty: typo
    run:
      run_id: run-research
      backend:
        kind: native
        name: goagent-native
`))
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
	if !strings.Contains(err.Error(), `unknown field "capabilty"`) {
		t.Fatalf("error = %v, want unknown field", err)
	}
}

func TestRunPlanCompilerAllowsFreeFormRunInput(t *testing.T) {
	t.Parallel()

	compiler := testRunPlanCompiler(t)
	plan, err := compiler.CompileJSON(context.Background(), []byte(`{
	  "plan_id": "plan-free-form-input",
	  "account_id": "acct-strict",
	  "project_id": "proj-strict",
	  "nodes": [
	    {
	      "node_id": "research",
	      "run": {
	        "run_id": "run-research",
	        "backend": {"kind": "native", "name": "goagent-native"},
	        "input": {
	          "arbitrary": {"nested": true},
	          "count": 2
	        }
	      }
	    }
	  ]
	}`))
	if err != nil {
		t.Fatalf("CompileJSON: %v", err)
	}
	if got := plan.Spec.Nodes[0].Run.Input["arbitrary"]; got == nil {
		t.Fatalf("free-form input was not decoded: %#v", plan.Spec.Nodes[0].Run.Input)
	}
}

func TestRunPlanCompilerRejectsUnknownPlanDeltaField(t *testing.T) {
	t.Parallel()

	compiler := testRunPlanCompiler(t)
	base := agentos.RunPlanSpec{
		PlanID:    "plan-delta-strict",
		AccountID: "acct-strict",
		ProjectID: "proj-strict",
		Policy:    agentos.PlanPolicy{MaxNodes: 2},
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "seed",
				Run: agentos.RunSpec{
					RunID:   "run-seed",
					Backend: agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
				},
			},
		},
	}
	_, _, err := compiler.CompileDeltaYAML(context.Background(), base, []byte(`
nodes:
  - node_id: expanded
    unexpected: true
    run:
      run_id: run-expanded
      backend:
        kind: native
        name: goagent-native
edges:
  - edge_id: seed-expanded
    from: seed
    to: expanded
    on: success
`), 0)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
	if !strings.Contains(err.Error(), `unknown field "unexpected"`) {
		t.Fatalf("error = %v, want unknown field", err)
	}
}

func TestRunPlanCompilerAcceptsUnquotedOnEdgeTriggerYAML(t *testing.T) {
	t.Parallel()

	compiler := testRunPlanCompiler(t)
	plan, err := compiler.CompileYAML(context.Background(), []byte(`
plan_id: plan-yaml-on
account_id: acct-strict
project_id: proj-strict
nodes:
  - node_id: seed
    run:
      run_id: run-seed
      backend:
        kind: native
        name: goagent-native
  - node_id: expanded
    run:
      run_id: run-expanded
      backend:
        kind: native
        name: goagent-native
edges:
  - edge_id: seed-expanded
    from: seed
    to: expanded
    on: success
`))
	if err != nil {
		t.Fatalf("CompileYAML: %v", err)
	}
	if len(plan.Spec.Edges) != 1 || plan.Spec.Edges[0].On != agentos.EdgeOnSuccess {
		t.Fatalf("edges = %#v", plan.Spec.Edges)
	}
}

func testRunPlanCompiler(t *testing.T) RunPlanCompiler {
	t.Helper()

	compiler, err := NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}

	return RunPlanCompiler{
		Validator: Validator{
			Expressions: compiler,
		},
	}
}
