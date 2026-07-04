package serverlessworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

func TestAdapterJSONRoundTrip(t *testing.T) {
	t.Parallel()
	adapter := testAdapter(t)
	spec := testRunPlanSpec()

	workflow, err := adapter.Export(context.Background(), &spec)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	if workflow.Document.DSL != DSLVersion {
		t.Fatalf("dsl = %q", workflow.Document.DSL)
	}

	assertTasks(t, workflow.Do)

	data, err := MarshalJSON(&workflow)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	decoded, err := UnmarshalJSON(data)
	if err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}

	imported, err := adapter.Import(context.Background(), &decoded)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}

	assertImportedSpec(t, &imported, &spec)
}

func TestAdapterYAMLRoundTrip(t *testing.T) {
	t.Parallel()
	adapter := testAdapter(t)

	spec := testRunPlanSpec()

	workflow, err := adapter.Export(context.Background(), &spec)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	data, err := MarshalYAML(&workflow)
	if err != nil {
		t.Fatalf("MarshalYAML: %v", err)
	}

	decoded, err := UnmarshalYAML(data)
	if err != nil {
		t.Fatalf("UnmarshalYAML: %v", err)
	}

	imported, err := adapter.Import(context.Background(), &decoded)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}

	if imported.PlanID != "plan-swf" {
		t.Fatalf("plan id = %q", imported.PlanID)
	}
}

func TestAdapterRejectsUnsupportedDSLVersion(t *testing.T) {
	t.Parallel()
	adapter := testAdapter(t)

	spec := testRunPlanSpec()

	workflow, err := adapter.Export(context.Background(), &spec)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	workflow.Document.DSL = "0.8"

	_, err = adapter.Import(context.Background(), &workflow)
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("Import error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestAdapterRejectsMissingRunPlanExtension(t *testing.T) {
	t.Parallel()
	adapter := testAdapter(t)

	_, err := adapter.Import(context.Background(), &Workflow{
		Document: Document{DSL: DSLVersion, Name: "missing"},
		Do:       TaskList{},
	})
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("Import error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestTaskListRejectsAmbiguousTaskObjects(t *testing.T) {
	t.Parallel()

	data := []byte(`[{"a":{"call":"agentos.run"},"b":{"call":"agentos.run"}}]`)

	var tasks TaskList

	err := json.Unmarshal(data, &tasks)
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("Unmarshal error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestUnmarshalRejectsUnknownWorkflowFields(t *testing.T) {
	t.Parallel()

	_, err := UnmarshalJSON([]byte(`{
	  "document": {"dsl": "1.0.3", "name": "unknown"},
	  "do": [],
	  "listen": {}
	}`))
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("UnmarshalJSON error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestTaskListRejectsUnknownTaskDefinitionFields(t *testing.T) {
	t.Parallel()

	data := []byte(`[{"a":{"call":"agentos.run","foreach":[]}}]`)

	var tasks TaskList

	err := json.Unmarshal(data, &tasks)
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("Unmarshal error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestAdapterRejectsUnknownRunPlanExtensionFields(t *testing.T) {
	t.Parallel()
	adapter := testAdapter(t)

	specPayload, err := json.Marshal(testRunPlanSpec())
	if err != nil {
		t.Fatalf("Marshal spec: %v", err)
	}

	raw := json.RawMessage(`{"spec":` + string(specPayload) + `,"specc":{}}`)

	_, err = adapter.Import(context.Background(), &Workflow{
		Document: Document{DSL: DSLVersion, Name: "bad-extension"},
		Do:       TaskList{},
		Use: Use{
			Extensions: map[string]json.RawMessage{
				AgentOSRunPlanKey: raw,
			},
		},
	})
	if !errors.Is(err, agentoscore.ErrInvalidRunPlan) {
		t.Fatalf("Import error = %v, want ErrInvalidRunPlan", err)
	}
}

func assertTasks(t *testing.T, tasks TaskList) {
	t.Helper()

	if len(tasks) != 2 || tasks[0].Name != "research" || tasks[0].Definition.Then != "verify" {
		t.Fatalf("tasks = %#v", tasks)
	}
}

func assertImportedSpec(t *testing.T, imported, spec *agentos.RunPlanSpec) {
	t.Helper()

	if imported.PlanID != spec.PlanID || len(imported.Nodes) != len(spec.Nodes) || len(imported.Edges) != len(spec.Edges) {
		t.Fatalf("imported = %#v", imported)
	}
}

func testAdapter(t *testing.T) Adapter {
	t.Helper()

	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}

	catalog, err := agentosplan.NewStaticCapabilityCatalog([]agentos.Capability{
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

	compiler, err := agentosplan.NewCELCompiler()
	if err != nil {
		t.Fatalf("NewCELCompiler: %v", err)
	}

	return Adapter{
		Validator: agentosplan.Validator{
			Expressions:  compiler,
			Capabilities: catalog,
		},
	}
}

func testRunPlanSpec() agentos.RunPlanSpec {
	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}

	return agentos.RunPlanSpec{
		PlanID:    "plan-swf",
		AccountID: "account-1",
		ProjectID: "project-1",
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
					{Name: "summary", Kind: agentoscore.ArtifactKindObject, Required: true},
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
				EdgeID: "research-verify",
				From:   "research",
				To:     "verify",
				On:     agentos.EdgeOnSuccess,
			},
		},
	}
}
