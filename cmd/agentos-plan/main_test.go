package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testSchemaCommand(t *testing.T, kind, outName string) {
	t.Helper()

	dir := t.TempDir()
	out := filepath.Join(dir, outName)

	args := []string{"schema", "--out", out}
	if kind != "" {
		args = append(args, "--kind", kind)
	}

	var stdout, stderr bytes.Buffer

	code := run(args, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run schema code = %d stderr = %s", code, stderr.String())
	}

	file, err := os.OpenInRoot(dir, outName)
	if err != nil {
		t.Fatalf("OpenInRoot: %v", err)
	}

	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Errorf("close schema file: %v", err)
		}
	})

	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("schema json: %v", err)
	}

	if len(schema) == 0 {
		t.Fatal("schema is empty")
	}
}

func TestSchemaCommandWritesRunPlanSchema(t *testing.T) {
	t.Parallel()
	testSchemaCommand(t, "", "run_plan.schema.json")
}

func TestSchemaCommandWritesPlanDeltaSchema(t *testing.T) {
	t.Parallel()
	testSchemaCommand(t, "plan-delta", "plan_delta.schema.json")
}

func TestSchemaCommandWritesCapabilityCatalogSchema(t *testing.T) {
	t.Parallel()
	testSchemaCommand(t, "capability-catalog", "capability_catalog.schema.json")
}

func TestSchemaCommandWritesArtifactSchemaCatalogSchema(t *testing.T) {
	t.Parallel()
	testSchemaCommand(t, "artifact-schema-catalog", "artifact_schema_catalog.schema.json")
}

func TestValidateCommandRequiresCapabilityCatalogForCapabilityPlan(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	planPath := filepath.Join(dir, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(capabilityPlanYAML), 0o600); err != nil {
		t.Fatalf("WriteFile plan: %v", err)
	}

	var stdout, stderr bytes.Buffer

	code := run([]string{"validate", "--file", planPath, "--format", "yaml"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("validate succeeded without capability catalog; stdout=%s", stdout.String())
	}

	if !strings.Contains(stderr.String(), "capability catalog is required") {
		t.Fatalf("stderr = %s", stderr.String())
	}
}

func TestCompileCommandEmitsValidatedPlanOrder(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.yaml")
	capabilitiesPath := filepath.Join(dir, "capabilities.yaml")

	if err := os.WriteFile(planPath, []byte(capabilityPlanYAML), 0o600); err != nil {
		t.Fatalf("WriteFile plan: %v", err)
	}

	if err := os.WriteFile(capabilitiesPath, []byte(capabilityCatalogYAML), 0o600); err != nil {
		t.Fatalf("WriteFile capabilities: %v", err)
	}

	var stdout, stderr bytes.Buffer

	code := run([]string{
		"compile",
		"--file", planPath,
		"--format", "yaml",
		"--capabilities", capabilitiesPath,
		"--capabilities-format", "yaml",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("compile code = %d stderr = %s", code, stderr.String())
	}

	var output compiledPlanOutput
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatalf("compiled json: %v\n%s", err, stdout.String())
	}

	if output.Spec.PlanID != planCLI {
		t.Fatalf("plan id = %q", output.Spec.PlanID)
	}

	if len(output.Order) != 1 || output.Order[0] != RESEARCH {
		t.Fatalf("order = %#v", output.Order)
	}
}

func TestCompileCommandAcceptsJSONWireFormat(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	capabilitiesPath := filepath.Join(dir, "capabilities.json")

	if err := os.WriteFile(planPath, []byte(capabilityPlanJSON), 0o600); err != nil {
		t.Fatalf("WriteFile plan: %v", err)
	}

	if err := os.WriteFile(capabilitiesPath, []byte(capabilityCatalogJSON), 0o600); err != nil {
		t.Fatalf("WriteFile capabilities: %v", err)
	}

	var stdout, stderr bytes.Buffer

	code := run([]string{
		"compile",
		"--file", planPath,
		"--format", "json",
		"--capabilities", capabilitiesPath,
		"--capabilities-format", "json",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("compile json code = %d stderr = %s", code, stderr.String())
	}

	var output compiledPlanOutput
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatalf("compiled json: %v\n%s", err, stdout.String())
	}

	if output.Spec.PlanID != "plan-cli-json" {
		t.Fatalf("plan id = %q", output.Spec.PlanID)
	}

	if len(output.Order) != 1 || output.Order[0] != RESEARCH {
		t.Fatalf("order = %#v", output.Order)
	}
}

func TestCompileCommandRejectsUnknownCapabilityCatalogField(t *testing.T) {
	t.Parallel()

	assertRejectsUnknownCatalogField(t, &unknownCatalogFieldCase{
		command:         "compile",
		planYAML:        capabilityPlanYAML,
		catalogFileName: "capabilities.yaml",
		catalogYAML: `
capabilities:
  - backend:
      kind: http
      name: research
    name: research
    capabilty: typo
`,
		catalogFlag:       "--capabilities",
		catalogFormatFlag: "--capabilities-format",
		unknownField:      "capabilty",
	})
}

func TestValidateCommandUsesArtifactSchemaCatalog(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.yaml")
	schemasPath := filepath.Join(dir, "artifact-schemas.yaml")

	if err := os.WriteFile(planPath, []byte(artifactSchemaPlanYAML), 0o600); err != nil {
		t.Fatalf("WriteFile plan: %v", err)
	}

	if err := os.WriteFile(schemasPath, []byte(artifactSchemaCatalogYAML), 0o600); err != nil {
		t.Fatalf("WriteFile artifact schemas: %v", err)
	}

	var stdout, stderr bytes.Buffer

	code := run([]string{"validate", "--file", planPath, "--format", "yaml"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("validate succeeded without artifact schema catalog; stdout=%s", stdout.String())
	}

	if !strings.Contains(stderr.String(), "artifact schema catalog") {
		t.Fatalf("stderr = %s", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()

	code = run([]string{
		"validate",
		"--file", planPath,
		"--format", "yaml",
		"--artifact-schemas", schemasPath,
		"--artifact-schemas-format", "yaml",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("validate with artifact schemas code = %d stderr = %s", code, stderr.String())
	}
}

func TestValidateCommandRejectsUnknownArtifactSchemaCatalogField(t *testing.T) {
	t.Parallel()

	assertRejectsUnknownCatalogField(t, &unknownCatalogFieldCase{
		command:         "validate",
		planYAML:        artifactSchemaPlanYAML,
		catalogFileName: "artifact-schemas.yaml",
		catalogYAML: `
artifact_schemas:
  - ref: schema:summary
    description: Summary artifact
    schema:
      type: object
    scheam: typo
`,
		catalogFlag:       "--artifact-schemas",
		catalogFormatFlag: "--artifact-schemas-format",
		unknownField:      "scheam",
	})
}

type unknownCatalogFieldCase struct {
	command           string
	planYAML          string
	catalogFileName   string
	catalogYAML       string
	catalogFlag       string
	catalogFormatFlag string
	unknownField      string
}

func assertRejectsUnknownCatalogField(t *testing.T, tc *unknownCatalogFieldCase) {
	t.Helper()

	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.yaml")
	catalogPath := filepath.Join(dir, tc.catalogFileName)

	if err := os.WriteFile(planPath, []byte(tc.planYAML), 0o600); err != nil {
		t.Fatalf("WriteFile plan: %v", err)
	}

	if err := os.WriteFile(catalogPath, []byte(tc.catalogYAML), 0o600); err != nil {
		t.Fatalf("WriteFile catalog: %v", err)
	}

	var stdout, stderr bytes.Buffer

	code := run([]string{
		tc.command,
		"--file", planPath,
		"--format", "yaml",
		tc.catalogFlag, catalogPath,
		tc.catalogFormatFlag, "yaml",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("%s succeeded with unknown catalog field; stdout=%s", tc.command, stdout.String())
	}

	if !strings.Contains(stderr.String(), `unknown field "`+tc.unknownField+`"`) {
		t.Fatalf("stderr = %s", stderr.String())
	}
}

func TestCompileDeltaCommandAppliesValidatedPlanDelta(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.yaml")
	deltaPath := filepath.Join(dir, "delta.yaml")

	if err := os.WriteFile(basePath, []byte(deltaBasePlanYAML), 0o600); err != nil {
		t.Fatalf("WriteFile base: %v", err)
	}

	if err := os.WriteFile(deltaPath, []byte(planDeltaYAML), 0o600); err != nil {
		t.Fatalf("WriteFile delta: %v", err)
	}

	var stdout, stderr bytes.Buffer

	code := run([]string{
		"compile-delta",
		"--base", basePath,
		"--base-format", "yaml",
		"--file", deltaPath,
		"--format", "yaml",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("compile-delta code = %d stderr = %s", code, stderr.String())
	}

	var output compiledPlanOutput
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatalf("compiled delta json: %v\n%s", err, stdout.String())
	}

	if len(output.Spec.Nodes) != 2 {
		t.Fatalf("node count = %d", len(output.Spec.Nodes))
	}

	if len(output.Order) != 2 || output.Order[0] != "seed" || output.Order[1] != "expanded" {
		t.Fatalf("order = %#v", output.Order)
	}
}

func TestValidateDeltaCommandRejectsPolicyViolation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.yaml")
	deltaPath := filepath.Join(dir, "delta.yaml")

	if err := os.WriteFile(basePath, []byte(deltaBasePlanMaxOneYAML), 0o600); err != nil {
		t.Fatalf("WriteFile base: %v", err)
	}

	if err := os.WriteFile(deltaPath, []byte(planDeltaYAML), 0o600); err != nil {
		t.Fatalf("WriteFile delta: %v", err)
	}

	var stdout, stderr bytes.Buffer

	code := run([]string{
		"validate-delta",
		"--base", basePath,
		"--base-format", "yaml",
		"--file", deltaPath,
		"--format", "yaml",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("validate-delta succeeded; stdout=%s", stdout.String())
	}

	if !strings.Contains(stderr.String(), "delta exceeds max nodes") {
		t.Fatalf("stderr = %s", stderr.String())
	}
}

func TestServerlessWorkflowCommandsRoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.yaml")
	capabilitiesPath := filepath.Join(dir, "capabilities.yaml")
	workflowPath := filepath.Join(dir, "workflow.yaml")

	if err := os.WriteFile(planPath, []byte(capabilityPlanYAML), 0o600); err != nil {
		t.Fatalf("WriteFile plan: %v", err)
	}

	if err := os.WriteFile(capabilitiesPath, []byte(capabilityCatalogYAML), 0o600); err != nil {
		t.Fatalf("WriteFile capabilities: %v", err)
	}

	exportServerlessWorkflow(t, planPath, capabilitiesPath, workflowPath)
	assertWorkflowContainsAgentOSExtension(t, dir, "workflow.yaml")
	output := importServerlessWorkflow(t, workflowPath, capabilitiesPath)

	if output.Spec.PlanID != planCLI {
		t.Fatalf("plan id = %q", output.Spec.PlanID)
	}

	if len(output.Order) != 1 || output.Order[0] != RESEARCH {
		t.Fatalf("order = %#v", output.Order)
	}
}

func exportServerlessWorkflow(t *testing.T, planPath, capabilitiesPath, workflowPath string) {
	t.Helper()

	var stdout, stderr bytes.Buffer

	code := run([]string{
		"export-serverless",
		"--file", planPath,
		"--format", "yaml",
		"--capabilities", capabilitiesPath,
		"--capabilities-format", "yaml",
		"--out-format", "yaml",
		"--out", workflowPath,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("export-serverless code = %d stderr = %s", code, stderr.String())
	}
}

func assertWorkflowContainsAgentOSExtension(t *testing.T, dir, name string) {
	t.Helper()

	workflow, err := os.OpenInRoot(dir, name)
	if err != nil {
		t.Fatalf("OpenInRoot workflow: %v", err)
	}

	t.Cleanup(func() {
		if err := workflow.Close(); err != nil {
			t.Errorf("close workflow file: %v", err)
		}
	})

	workflowData, err := io.ReadAll(workflow)
	if err != nil {
		t.Fatalf("ReadAll workflow: %v", err)
	}

	if !strings.Contains(string(workflowData), "agentos.io/run_plan") {
		t.Fatalf("workflow yaml missing AgentOS extension:\n%s", string(workflowData))
	}
}

func importServerlessWorkflow(t *testing.T, workflowPath, capabilitiesPath string) compiledPlanOutput {
	t.Helper()

	var stdout, stderr bytes.Buffer

	code := run([]string{
		"import-serverless",
		"--file", workflowPath,
		"--format", "yaml",
		"--capabilities", capabilitiesPath,
		"--capabilities-format", "yaml",
		"--out-format", "json",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("import-serverless code = %d stderr = %s", code, stderr.String())
	}

	var output compiledPlanOutput
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatalf("imported json: %v\n%s", err, stdout.String())
	}

	return output
}

func TestImportServerlessCommandRejectsMissingAgentOSExtension(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	workflowPath := filepath.Join(dir, "workflow.yaml")
	if err := os.WriteFile(workflowPath, []byte(`
document:
  dsl: 1.0.3
  name: missing-agentos-extension
do: []
`), 0o600); err != nil {
		t.Fatalf("WriteFile workflow: %v", err)
	}

	var stdout, stderr bytes.Buffer

	code := run([]string{
		"import-serverless",
		"--file", workflowPath,
		"--format", "yaml",
		"--out-format", "json",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("import-serverless succeeded; stdout=%s", stdout.String())
	}

	if !strings.Contains(stderr.String(), "missing agentos.io/run_plan extension") {
		t.Fatalf("stderr = %s", stderr.String())
	}
}

const capabilityPlanYAML = `
plan_id: plan-cli
account_id: acct-cli
project_id: proj-cli
nodes:
  - node_id: research
    capability: research
    run:
      run_id: run-research
      backend:
        kind: http
        name: research
      input:
        topic: agent orchestration
`

const deltaBasePlanYAML = `
plan_id: plan-delta-cli
account_id: acct-delta-cli
project_id: proj-delta-cli
policy:
  max_nodes: 2
nodes:
  - node_id: seed
    run:
      run_id: run-seed
      backend:
        kind: native
        name: goagent-native
`

const deltaBasePlanMaxOneYAML = `
plan_id: plan-delta-cli
account_id: acct-delta-cli
project_id: proj-delta-cli
policy:
  max_nodes: 1
nodes:
  - node_id: seed
    run:
      run_id: run-seed
      backend:
        kind: native
        name: goagent-native
`

const planDeltaYAML = `
nodes:
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
`

const capabilityCatalogYAML = `
capabilities:
  - backend:
      kind: http
      name: research
    name: research
    input_schema:
      type: object
      properties:
        topic:
          type: string
      required:
        - topic
`

const capabilityPlanJSON = `{
  "plan_id": "plan-cli-json",
  "account_id": "acct-cli",
  "project_id": "proj-cli",
  "nodes": [
    {
      "node_id": "research",
      "capability": "research",
      "run": {
        "run_id": "run-research",
        "backend": {
          "kind": "http",
          "name": "research"
        },
        "input": {
          "topic": "agent orchestration"
        }
      }
    }
  ]
}`

const capabilityCatalogJSON = `{
  "capabilities": [
    {
      "backend": {
        "kind": "http",
        "name": "research"
      },
      "name": "research",
      "input_schema": {
        "type": "object",
        "properties": {
          "topic": {
            "type": "string"
          }
        },
        "required": ["topic"]
      }
    }
  ]
}`

const artifactSchemaPlanYAML = `
plan_id: plan-schema-cli
account_id: acct-cli
project_id: proj-cli
nodes:
  - node_id: summarize
    run:
      run_id: run-summarize
      backend:
        kind: native
        name: goagent-native
    outputs:
      - name: summary
        kind: object
        schema_ref: schema:summary
`

const (
	planCLI  = "plan-cli"
	RESEARCH = "research"
)

const artifactSchemaCatalogYAML = `
artifact_schemas:
  - ref: schema:summary
    description: Summary artifact
    schema:
      type: object
      properties:
        summary:
          type: string
      required:
        - summary
`
