package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSchemaCommandWritesRunPlanSchema(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "run_plan.schema.json")
	var stdout, stderr bytes.Buffer

	code := run([]string{"schema", "--out", out}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run schema code = %d stderr = %s", code, stderr.String())
	}

	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("schema json: %v", err)
	}
	if len(schema) == 0 {
		t.Fatal("schema is empty")
	}
}

func TestSchemaCommandWritesPlanDeltaSchema(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "plan_delta.schema.json")
	var stdout, stderr bytes.Buffer

	code := run([]string{"schema", "--kind", "plan-delta", "--out", out}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run schema code = %d stderr = %s", code, stderr.String())
	}

	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("schema json: %v", err)
	}
	if len(schema) == 0 {
		t.Fatal("schema is empty")
	}
}

func TestValidateCommandRequiresCapabilityCatalogForCapabilityPlan(t *testing.T) {
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(capabilityPlanYAML), 0o644); err != nil {
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
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.yaml")
	capabilitiesPath := filepath.Join(dir, "capabilities.yaml")
	if err := os.WriteFile(planPath, []byte(capabilityPlanYAML), 0o644); err != nil {
		t.Fatalf("WriteFile plan: %v", err)
	}
	if err := os.WriteFile(capabilitiesPath, []byte(capabilityCatalogYAML), 0o644); err != nil {
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
	if output.Spec.PlanID != "plan-cli" {
		t.Fatalf("plan id = %q", output.Spec.PlanID)
	}
	if len(output.Order) != 1 || output.Order[0] != "research" {
		t.Fatalf("order = %#v", output.Order)
	}
}

func TestCompileDeltaCommandAppliesValidatedPlanDelta(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.yaml")
	deltaPath := filepath.Join(dir, "delta.yaml")
	if err := os.WriteFile(basePath, []byte(deltaBasePlanYAML), 0o644); err != nil {
		t.Fatalf("WriteFile base: %v", err)
	}
	if err := os.WriteFile(deltaPath, []byte(planDeltaYAML), 0o644); err != nil {
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
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.yaml")
	deltaPath := filepath.Join(dir, "delta.yaml")
	if err := os.WriteFile(basePath, []byte(deltaBasePlanMaxOneYAML), 0o644); err != nil {
		t.Fatalf("WriteFile base: %v", err)
	}
	if err := os.WriteFile(deltaPath, []byte(planDeltaYAML), 0o644); err != nil {
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

const capabilityPlanYAML = `
plan_id: plan-cli
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
