package agentosplan

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestValidatorAcceptsPlanWithCapabilityAndCEL(t *testing.T) {
	ctx := context.Background()
	compiler, err := NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}
	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}
	catalog, err := NewStaticCapabilityCatalog([]agentos.Capability{
		{
			Backend: ref,
			Name:    "research",
			InputSchema: json.RawMessage(`{
				"type":"object",
				"properties":{"topic":{"type":"string"}},
				"required":["topic"]
			}`),
		},
	})
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	validator := Validator{Expressions: compiler, Capabilities: catalog}

	plan, err := validator.Validate(ctx, samplePlan(ref))
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := plan.Order; len(got) != 2 || got[0] != "research" || got[1] != "verify" {
		t.Fatalf("order = %#v", got)
	}
}

func TestValidatorRejectsCycle(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	spec.Edges = append(spec.Edges, agentos.PlanEdgeSpec{
		EdgeID: "cycle",
		From:   "verify",
		To:     "research",
	})

	_, err := Validator{}.Validate(context.Background(), spec)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatorRejectsSchemaMismatch(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}
	catalog, err := NewStaticCapabilityCatalog([]agentos.Capability{
		{
			Backend: ref,
			Name:    "research",
			InputSchema: json.RawMessage(`{
				"type":"object",
				"properties":{"topic":{"type":"string"}},
				"required":["topic"]
			}`),
		},
	})
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	spec := samplePlan(ref)
	spec.Nodes[0].Run.Input = map[string]any{"topic": 42}

	_, err = Validator{Capabilities: catalog}.Validate(context.Background(), spec)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestSchedulerReadyNodes(t *testing.T) {
	ctx := context.Background()
	compiler, err := NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	plan, err := Validator{Expressions: compiler}.Validate(ctx, spec)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	state := NewState(spec, time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC))
	if err := state.Apply(StateEvent{Kind: EventPlanStarted}); err != nil {
		t.Fatalf("plan started: %v", err)
	}

	ready, err := Scheduler{Expressions: compiler}.ReadyNodes(ctx, plan, state.Status, map[string]any{
		"inputs": map[string]any{"enabled": true},
	})
	if err != nil {
		t.Fatalf("ReadyNodes: %v", err)
	}
	if len(ready) != 1 || ready[0].NodeID != "research" {
		t.Fatalf("ready = %#v", ready)
	}

	if err := state.Apply(StateEvent{Kind: EventNodeSucceeded, NodeID: "research"}); err != nil {
		t.Fatalf("node succeeded: %v", err)
	}
	ready, err = Scheduler{Expressions: compiler}.ReadyNodes(ctx, plan, state.Status, map[string]any{
		"inputs": map[string]any{"enabled": true},
	})
	if err != nil {
		t.Fatalf("ReadyNodes second: %v", err)
	}
	if len(ready) != 1 || ready[0].NodeID != "verify" {
		t.Fatalf("ready second = %#v", ready)
	}
}

func TestSchedulerSkipsNodeWhenConditionIsFalse(t *testing.T) {
	ctx := context.Background()
	compiler, err := NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	plan, err := Validator{Expressions: compiler}.Validate(ctx, spec)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	state := NewState(spec, time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC))
	if err := state.Apply(StateEvent{Kind: EventPlanStarted}); err != nil {
		t.Fatalf("plan started: %v", err)
	}

	decision, err := Scheduler{Expressions: compiler}.Decide(ctx, plan, state.Status, map[string]any{
		"inputs": map[string]any{"enabled": false},
	})
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if len(decision.Ready) != 0 {
		t.Fatalf("ready = %#v", decision.Ready)
	}
	if len(decision.Skipped) != 1 || decision.Skipped[0].NodeID != "research" {
		t.Fatalf("skipped = %#v", decision.Skipped)
	}
}

func TestSchedulerActivatesErrorEdge(t *testing.T) {
	ctx := context.Background()
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-error-edge",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "attempt",
				Run: agentos.RunSpec{
					RunID:   "run-attempt",
					Backend: ref,
				},
			},
			{
				NodeID: "recover",
				Run: agentos.RunSpec{
					RunID:   "run-recover",
					Backend: ref,
				},
			},
		},
		Edges: []agentos.PlanEdgeSpec{
			{EdgeID: "attempt-failed", From: "attempt", To: "recover", On: agentos.EdgeOnError},
		},
	}
	plan, err := Validator{}.Validate(ctx, spec)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	state := NewState(spec, time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC))
	if err := state.Apply(StateEvent{Kind: EventPlanStarted}); err != nil {
		t.Fatalf("plan started: %v", err)
	}
	if err := state.Apply(StateEvent{Kind: EventNodeFailed, NodeID: "attempt", Reason: "backend failed"}); err != nil {
		t.Fatalf("node failed: %v", err)
	}

	ready, err := Scheduler{}.ReadyNodes(ctx, plan, state.Status, nil)
	if err != nil {
		t.Fatalf("ReadyNodes: %v", err)
	}
	if len(ready) != 1 || ready[0].NodeID != "recover" {
		t.Fatalf("ready = %#v", ready)
	}
}

func TestSchedulerJoinAny(t *testing.T) {
	ctx := context.Background()
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-join-any",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "left", Run: agentos.RunSpec{RunID: "run-left", Backend: ref}},
			{NodeID: "right", Run: agentos.RunSpec{RunID: "run-right", Backend: ref}},
			{
				NodeID: "join",
				Run:    agentos.RunSpec{RunID: "run-join", Backend: ref},
				Policy: agentos.NodePolicy{Join: agentos.PlanJoinAny},
			},
		},
		Edges: []agentos.PlanEdgeSpec{
			{EdgeID: "left-join", From: "left", To: "join", On: agentos.EdgeOnSuccess},
			{EdgeID: "right-join", From: "right", To: "join", On: agentos.EdgeOnSuccess},
		},
	}
	plan, err := Validator{}.Validate(ctx, spec)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	state := NewState(spec, time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC))
	if err := state.Apply(StateEvent{Kind: EventPlanStarted}); err != nil {
		t.Fatalf("plan started: %v", err)
	}
	if err := state.Apply(StateEvent{Kind: EventNodeSucceeded, NodeID: "left"}); err != nil {
		t.Fatalf("left succeeded: %v", err)
	}
	if err := state.Apply(StateEvent{Kind: EventNodeStarted, NodeID: "right"}); err != nil {
		t.Fatalf("right started: %v", err)
	}

	ready, err := Scheduler{}.ReadyNodes(ctx, plan, state.Status, nil)
	if err != nil {
		t.Fatalf("ReadyNodes: %v", err)
	}
	if len(ready) != 1 || ready[0].NodeID != "join" {
		t.Fatalf("ready = %#v", ready)
	}
}

func TestStateReducerTransitions(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	state := NewState(spec, time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC))

	if err := state.Apply(StateEvent{Kind: EventPlanStarted}); err != nil {
		t.Fatalf("plan started: %v", err)
	}
	if err := state.Apply(StateEvent{Kind: EventNodeStarted, NodeID: "research", RunID: "run-research"}); err != nil {
		t.Fatalf("node started: %v", err)
	}
	if state.Status.LifecycleState != agentos.PlanLifecycleRunning {
		t.Fatalf("plan lifecycle = %q", state.Status.LifecycleState)
	}
	if len(state.Status.ActiveRunIDs) != 1 || state.Status.ActiveRunIDs[0] != "run-research" {
		t.Fatalf("active run ids = %#v", state.Status.ActiveRunIDs)
	}
}

func TestApplyDeltaRespectsLimits(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	spec.Policy.MaxNodes = 2
	delta := PlanDelta{Nodes: []agentos.PlanNodeSpec{
		{
			NodeID: "extra",
			Run: agentos.RunSpec{
				RunID:   "run-extra",
				Backend: ref,
			},
		},
	}}

	_, _, err := ApplyDelta(context.Background(), Validator{}, spec, delta, 0)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestCompilerYAML(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	data := []byte(`
plan_id: plan-yaml
nodes:
  - node_id: only
    run:
      run_id: run-only
      backend:
        kind: native
        name: goagent-native
`)
	plan, err := RunPlanCompiler{Validator: Validator{}}.CompileYAML(context.Background(), data)
	if err != nil {
		t.Fatalf("CompileYAML: %v", err)
	}
	if plan.NodeByID["only"].Run.Backend != ref {
		t.Fatalf("backend = %#v", plan.NodeByID["only"].Run.Backend)
	}
}

func samplePlan(ref agentos.BackendRef) agentos.RunPlanSpec {
	return agentos.RunPlanSpec{
		PlanID: "plan-1",
		Inputs: map[string]any{
			"enabled": true,
		},
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID:     "research",
				Capability: "research",
				Run: agentos.RunSpec{
					RunID:   "run-research",
					Backend: ref,
					Input: map[string]any{
						"topic": "agent orchestration",
					},
				},
				Conditions: []string{"inputs.enabled == true"},
				Outputs: []agentos.ArtifactSpec{
					{Name: "summary", Kind: agentos.ArtifactKindObject},
				},
			},
			{
				NodeID: "verify",
				Run: agentos.RunSpec{
					RunID:   "run-verify",
					Backend: ref,
				},
			},
		},
		Edges: []agentos.PlanEdgeSpec{
			{
				EdgeID: "research-to-verify",
				From:   "research",
				To:     "verify",
				On:     agentos.EdgeOnSuccess,
			},
		},
	}
}
