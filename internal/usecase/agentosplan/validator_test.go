package agentosplan

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

const (
	VERIFY        = "verify"
	SchemaSummary = "schema:summary"
)

func TestValidatorAcceptsPlanWithCapabilityAndCEL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	compiler, err := NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}

	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: RESEARCH}

	catalog, err := NewStaticCapabilityCatalog([]agentos.Capability{
		{
			Backend: ref,
			Name:    RESEARCH,
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

	plan, err := validator.Validate(ctx, samplePlanPtr(ref))
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if got := plan.Order; len(got) != 2 || got[0] != RESEARCH || got[1] != VERIFY {
		t.Fatalf("order = %#v", got)
	}
}

func TestValidatorRejectsCycle(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	spec.Edges = append(spec.Edges, agentos.PlanEdgeSpec{
		EdgeID: "cycle",
		From:   "verify",
		To:     RESEARCH,
	})

	_, err := Validator{Capabilities: sampleCapabilityCatalog(t, ref)}.Validate(context.Background(), &spec)
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatorRejectsCapabilityWithoutCatalog(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}

	_, err := Validator{}.Validate(context.Background(), samplePlanPtr(ref))
	if !errors.Is(err, agentoscore.ErrCapabilityNotFound) {
		t.Fatalf("error = %v, want ErrCapabilityNotFound", err)
	}
}

func TestValidatorRejectsSchemaMismatch(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: RESEARCH}

	catalog, err := NewStaticCapabilityCatalog([]agentos.Capability{
		{
			Backend: ref,
			Name:    RESEARCH,
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

	_, err = Validator{Capabilities: catalog}.Validate(context.Background(), &spec)
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatorAcceptsRunBatchWithinCapabilityLimit(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "batch-runtime"}
	spec := agentos.RunPlanSpec{
		PlanID:    "plan-batch",
		AccountID: "acct-plan-batch",
		ProjectID: "proj-plan-batch",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID:     "batch",
				Capability: agentos.CapabilityRunBatch,
				Run: agentos.RunSpec{
					RunID:   "run-batch",
					Backend: ref,
					Input: map[string]any{
						"records": []string{"alert-1", "alert-2"},
					},
				},
			},
		},
	}
	catalog := mustCapabilityCatalog(t, []agentos.Capability{
		{
			Backend: ref,
			Name:    agentos.CapabilityRunBatch,
			Limits: agentos.CapabilityLimits{
				MaxBatchItems: 3,
				BatchInputKey: "records",
			},
		},
	})

	_, err := Validator{Capabilities: catalog}.Validate(context.Background(), &spec)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidatorRejectsRunBatchOverCapabilityLimit(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "batch-runtime"}
	spec := agentos.RunPlanSpec{
		PlanID:    "plan-batch",
		AccountID: "acct-plan-batch",
		ProjectID: "proj-plan-batch",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID:     "batch",
				Capability: agentos.CapabilityRunBatch,
				Run: agentos.RunSpec{
					RunID:   "run-batch",
					Backend: ref,
					Input: map[string]any{
						"items": []any{"alert-1", "alert-2", "alert-3"},
					},
				},
			},
		},
	}
	catalog := mustCapabilityCatalog(t, []agentos.Capability{
		{
			Backend: ref,
			Name:    agentos.CapabilityRunBatch,
			Limits:  agentos.CapabilityLimits{MaxBatchItems: 2},
		},
	})

	_, err := Validator{Capabilities: catalog}.Validate(context.Background(), &spec)
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatorRejectsArtifactSchemaRefWithoutCatalog(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	spec.Nodes[0].Outputs[0].SchemaRef = SchemaSummary

	_, err := Validator{Capabilities: sampleCapabilityCatalog(t, ref)}.Validate(context.Background(), &spec)
	if !errors.Is(err, agentoscore.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}

func TestValidatorAcceptsArtifactSchemaRefWithCatalog(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	spec.Nodes[0].Outputs[0].SchemaRef = "schema:summary"

	schemas, err := NewStaticArtifactSchemaCatalog([]agentos.ArtifactSchema{
		{Ref: "schema:summary", Schema: json.RawMessage(`{"type":"object"}`)},
	})
	if err != nil {
		t.Fatalf("NewStaticArtifactSchemaCatalog: %v", err)
	}

	_, err = Validator{Capabilities: sampleCapabilityCatalog(t, ref), ArtifactSchemas: schemas}.Validate(context.Background(), &spec)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidatorAcceptsDeclaredArtifactMappingWithDependency(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	spec.Nodes[1].Inputs = []agentos.InputMapping{
		{Target: "summary", SourceNodeID: RESEARCH, SourceArtifact: "summary", Required: true},
	}

	_, err := Validator{Capabilities: sampleCapabilityCatalog(t, ref)}.Validate(context.Background(), &spec)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidatorRejectsArtifactMappingWithoutSourceNode(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	spec.Edges[0].InputMapping = []agentos.InputMapping{
		{Target: "summary", SourceArtifact: "summary", Required: true},
	}

	_, err := Validator{Capabilities: sampleCapabilityCatalog(t, ref)}.Validate(context.Background(), &spec)
	if !errors.Is(err, agentoscore.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}

func TestValidatorRejectsArtifactMappingForUndeclaredOutput(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	spec.Edges[0].InputMapping = []agentos.InputMapping{
		{Target: "missing", SourceNodeID: RESEARCH, SourceArtifact: "missing", Required: true},
	}

	_, err := Validator{Capabilities: sampleCapabilityCatalog(t, ref)}.Validate(context.Background(), &spec)
	if !errors.Is(err, agentoscore.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}

func TestValidatorRejectsNodeArtifactMappingWithoutDependencyPath(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	spec.Edges = nil
	spec.Nodes[1].Inputs = []agentos.InputMapping{
		{Target: "summary", SourceNodeID: RESEARCH, SourceArtifact: "summary", Required: true},
	}

	_, err := Validator{Capabilities: sampleCapabilityCatalog(t, ref)}.Validate(context.Background(), &spec)
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestValidatorRejectsContinuationThresholdAboveHistoryGuard(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	spec.Policy = agentos.PlanPolicy{
		ContinueAsNewEvents: 20,
		MaxHistoryEvents:    10,
	}

	_, err := Validator{Capabilities: sampleCapabilityCatalog(t, ref)}.Validate(context.Background(), &spec)
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestSchedulerReadyNodes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	compiler, err := NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)

	plan, err := Validator{Expressions: compiler, Capabilities: sampleCapabilityCatalog(t, ref)}.Validate(ctx, &spec)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}

	state := NewState(&spec, time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC))
	if err := state.Apply(&StateEvent{Kind: EventPlanStarted}); err != nil {
		t.Fatalf("plan started: %v", err)
	}

	ready, err := Scheduler{Expressions: compiler}.ReadyNodes(ctx, &plan, &state.Status, map[string]any{
		"inputs": map[string]any{"enabled": true},
	})
	if err != nil {
		t.Fatalf("ReadyNodes: %v", err)
	}

	requireReadyNode(t, ready, RESEARCH)

	if err := state.Apply(&StateEvent{Kind: EventNodeSucceeded, NodeID: RESEARCH}); err != nil {
		t.Fatalf("node succeeded: %v", err)
	}

	ready, err = Scheduler{Expressions: compiler}.ReadyNodes(ctx, &plan, &state.Status, map[string]any{
		"inputs": map[string]any{"enabled": true},
	})
	if err != nil {
		t.Fatalf("ReadyNodes second: %v", err)
	}

	requireReadyNode(t, ready, "verify")
}

func TestSchedulerDecisionIncludesConditionTraces(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	compiler, err := NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)

	plan, err := Validator{Expressions: compiler, Capabilities: sampleCapabilityCatalog(t, ref)}.Validate(ctx, &spec)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}

	state := NewState(&spec, time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC))

	decision, err := Scheduler{Expressions: compiler}.Decide(ctx, &plan, &state.Status, map[string]any{
		"inputs": map[string]any{"enabled": true},
	})
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}

	requireReadyNode(t, decision.Ready, RESEARCH)

	if len(decision.ConditionTraces) != 1 {
		t.Fatalf("condition traces = %#v", decision.ConditionTraces)
	}

	trace := decision.ConditionTraces[0]
	requireNodeConditionTrace(t, &trace, RESEARCH)
}

func requireReadyNode(t *testing.T, ready []agentos.PlanNodeSpec, nodeID string) {
	t.Helper()

	if len(ready) != 1 || ready[0].NodeID != nodeID {
		t.Fatalf("ready = %#v, want %s", ready, nodeID)
	}
}

func requireNodeConditionTrace(t *testing.T, trace *ConditionEvaluationTrace, nodeID string) {
	t.Helper()

	if trace.Scope != "node" || trace.NodeID != nodeID || trace.Expression == "" || !trace.Result {
		t.Fatalf("trace = %#v", trace)
	}
}

func TestSchedulerSkipsNodeWhenConditionIsFalse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	compiler, err := NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)

	plan, err := Validator{Expressions: compiler, Capabilities: sampleCapabilityCatalog(t, ref)}.Validate(ctx, &spec)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}

	state := NewState(&spec, time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC))
	if err := state.Apply(&StateEvent{Kind: EventPlanStarted}); err != nil {
		t.Fatalf("plan started: %v", err)
	}

	decision, err := Scheduler{Expressions: compiler}.Decide(ctx, &plan, &state.Status, map[string]any{
		"inputs": map[string]any{"enabled": false},
	})
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}

	if len(decision.Ready) != 0 {
		t.Fatalf("ready = %#v", decision.Ready)
	}

	if len(decision.Skipped) != 1 || decision.Skipped[0].NodeID != RESEARCH {
		t.Fatalf("skipped = %#v", decision.Skipped)
	}
}

func TestSchedulerActivatesErrorEdge(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:    "plan-error-edge",
		AccountID: "acct-plan-error-edge",
		ProjectID: "proj-plan-error-edge",
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

	plan, err := Validator{}.Validate(ctx, &spec)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}

	state := NewState(&spec, time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC))
	if err := state.Apply(&StateEvent{Kind: EventPlanStarted}); err != nil {
		t.Fatalf("plan started: %v", err)
	}

	if err := state.Apply(&StateEvent{Kind: EventNodeFailed, NodeID: "attempt", Reason: "backend failed"}); err != nil {
		t.Fatalf("node failed: %v", err)
	}

	ready, err := Scheduler{}.ReadyNodes(ctx, &plan, &state.Status, nil)
	if err != nil {
		t.Fatalf("ReadyNodes: %v", err)
	}

	if len(ready) != 1 || ready[0].NodeID != "recover" {
		t.Fatalf("ready = %#v", ready)
	}
}

func TestSchedulerJoinAny(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:    "plan-join-any",
		AccountID: "acct-plan-join-any",
		ProjectID: "proj-plan-join-any",
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

	plan, err := Validator{}.Validate(ctx, &spec)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}

	state := NewState(&spec, time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC))
	if err := state.Apply(&StateEvent{Kind: EventPlanStarted}); err != nil {
		t.Fatalf("plan started: %v", err)
	}

	if err := state.Apply(&StateEvent{Kind: EventNodeSucceeded, NodeID: "left"}); err != nil {
		t.Fatalf("left succeeded: %v", err)
	}

	if err := state.Apply(&StateEvent{Kind: EventNodeStarted, NodeID: "right"}); err != nil {
		t.Fatalf("right started: %v", err)
	}

	ready, err := Scheduler{}.ReadyNodes(ctx, &plan, &state.Status, nil)
	if err != nil {
		t.Fatalf("ReadyNodes: %v", err)
	}

	if len(ready) != 1 || ready[0].NodeID != "join" {
		t.Fatalf("ready = %#v", ready)
	}
}

func TestStateReducerTransitions(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := samplePlan(ref)
	state := NewState(&spec, time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC))

	if err := state.Apply(&StateEvent{Kind: EventPlanStarted}); err != nil {
		t.Fatalf("plan started: %v", err)
	}

	if err := state.Apply(&StateEvent{Kind: EventNodeStarted, NodeID: RESEARCH, RunID: "run-research"}); err != nil {
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
	t.Parallel()

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

	_, _, err := ApplyDelta(context.Background(), Validator{}, &spec, delta, 0)
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestCompilerYAML(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	data := []byte(`
plan_id: plan-yaml
account_id: acct-plan-yaml
project_id: proj-plan-yaml
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
		PlanID:    "plan-1",
		AccountID: "acct-plan-1",
		ProjectID: "proj-plan-1",
		Inputs: map[string]any{
			"enabled": true,
		},
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID:     RESEARCH,
				Capability: RESEARCH,
				Run: agentos.RunSpec{
					RunID:   "run-research",
					Backend: ref,
					Input: map[string]any{
						"topic": "agent orchestration",
					},
				},
				Conditions: []string{"inputs.enabled == true"},
				Outputs: []agentos.ArtifactSpec{
					{Name: "summary", Kind: agentoscore.ArtifactKindObject},
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
				From:   RESEARCH,
				To:     "verify",
				On:     agentos.EdgeOnSuccess,
			},
		},
	}
}

func samplePlanPtr(ref agentos.BackendRef) *agentos.RunPlanSpec {
	spec := samplePlan(ref)

	return &spec
}

func sampleCapabilityCatalog(t *testing.T, ref agentos.BackendRef) *StaticCapabilityCatalog {
	t.Helper()

	return mustCapabilityCatalog(t, []agentos.Capability{
		{
			Backend: ref,
			Name:    RESEARCH,
			InputSchema: json.RawMessage(`{
				"type":"object",
				"properties":{"topic":{"type":"string"}},
				"required":["topic"]
			}`),
		},
	})
}

func mustCapabilityCatalog(t *testing.T, capabilities []agentos.Capability) *StaticCapabilityCatalog {
	t.Helper()

	catalog, err := NewStaticCapabilityCatalog(capabilities)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}

	return catalog
}
