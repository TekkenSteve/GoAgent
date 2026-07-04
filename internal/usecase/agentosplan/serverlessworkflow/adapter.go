package serverlessworkflow

import (
	"context"
	"encoding/json"
	"fmt"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"sigs.k8s.io/yaml"
)

const (
	DSLVersion            = "1.0.3"
	AgentOSRunPlanKey     = "agentos.io/run_plan"
	AgentOSPlanNodeKey    = "agentos.io/plan_node"
	AgentOSRunCall        = "agentos.run"
	defaultExportVersion  = "1.0.0"
	defaultExportName     = "agentos-run-plan"
	defaultExportNSPrefix = "agentos"
)

// Adapter imports and exports RunPlanSpec through the Serverless Workflow DSL
// as an edge interoperability format. AgentOS RunPlanSpec remains the source of
// truth; Serverless Workflow is not used as the internal plan model.
type Adapter struct {
	Validator agentosplan.Validator
}

// Workflow is the Serverless Workflow subset needed for lossless AgentOS plan
// import/export.
type Workflow struct {
	Document   Document                   `json:"document"`
	Use        Use                        `json:"use"`
	Do         TaskList                   `json:"do"`
	Extensions map[string]json.RawMessage `json:"extensions,omitempty"`
}

// Document identifies a Serverless Workflow document.
type Document struct {
	DSL       string `json:"dsl"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
	Version   string `json:"version,omitempty"`
}

// Use contains reusable Serverless Workflow components.
type Use struct {
	Extensions map[string]json.RawMessage `json:"extensions,omitempty"`
}

// TaskList is the Serverless Workflow do-array, encoded as a list of one-key
// task objects.
type TaskList []Task

// Task is one named Serverless Workflow task.
type Task struct {
	Name       string
	Definition TaskDefinition
}

// TaskDefinition is the Serverless Workflow call task subset AgentOS exports.
type TaskDefinition struct {
	Call     string         `json:"call,omitempty"`
	With     map[string]any `json:"with,omitempty"`
	Then     string         `json:"then,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type runPlanExtension struct {
	Spec agentos.RunPlanSpec `json:"spec"`
}

type planNodeExtension struct {
	Node agentos.PlanNodeSpec `json:"node"`
}

// Export converts a validated RunPlanSpec to a Serverless Workflow document.
func (a Adapter) Export(ctx context.Context, spec *agentos.RunPlanSpec) (Workflow, error) {
	plan, err := a.Validator.Validate(ctx, spec)
	if err != nil {
		return Workflow{}, err
	}

	spec = &plan.Spec

	runPlanPayload, err := json.Marshal(runPlanExtension{Spec: *spec})
	if err != nil {
		return Workflow{}, fmt.Errorf("%w: encode run plan extension: %w", agentoscore.ErrInvalidRunPlan, err)
	}

	tasks := make(TaskList, 0, len(spec.Nodes))
	for i := range spec.Nodes {
		node := &spec.Nodes[i]

		nodePayload, err := json.Marshal(planNodeExtension{Node: *node})
		if err != nil {
			return Workflow{}, fmt.Errorf("%w: encode node extension: %w", agentoscore.ErrInvalidRunPlan, err)
		}

		tasks = append(tasks, Task{
			Name: node.NodeID,
			Definition: TaskDefinition{
				Call: AgentOSRunCall,
				With: map[string]any{
					AgentOSPlanNodeKey: json.RawMessage(nodePayload),
				},
				Then: nextSuccessNode(spec, node.NodeID),
			},
		})
	}

	return Workflow{
		Document: Document{
			DSL:       DSLVersion,
			Namespace: exportNamespace(spec),
			Name:      exportName(spec),
			Version:   defaultExportVersion,
		},
		Do: tasks,
		Use: Use{
			Extensions: map[string]json.RawMessage{
				AgentOSRunPlanKey: runPlanPayload,
			},
		},
	}, nil
}

// Import converts an AgentOS-exported Serverless Workflow document back to a
// validated RunPlanSpec.
func (a Adapter) Import(ctx context.Context, workflow *Workflow) (agentos.RunPlanSpec, error) {
	if workflow.Document.DSL != DSLVersion {
		return agentos.RunPlanSpec{}, fmt.Errorf("%w: unsupported serverless workflow dsl %q", agentoscore.ErrInvalidRunPlan, workflow.Document.DSL)
	}

	raw, ok := workflow.Use.Extensions[AgentOSRunPlanKey]
	if !ok {
		return agentos.RunPlanSpec{}, fmt.Errorf("%w: missing %s extension", agentoscore.ErrInvalidRunPlan, AgentOSRunPlanKey)
	}

	var extension runPlanExtension

	extension, err := agentosplan.DecodeWireJSON[runPlanExtension](raw)
	if err != nil {
		return agentos.RunPlanSpec{}, fmt.Errorf("%w: decode run plan extension: %w", agentoscore.ErrInvalidRunPlan, err)
	}

	if _, err := a.Validator.Validate(ctx, &extension.Spec); err != nil {
		return agentos.RunPlanSpec{}, err
	}

	return extension.Spec, nil
}

// MarshalJSON serializes a workflow using the canonical JSON wire format.
func MarshalJSON(workflow *Workflow) ([]byte, error) {
	return json.MarshalIndent(workflow, "", "  ")
}

// UnmarshalJSON parses a Serverless Workflow JSON document.
func UnmarshalJSON(data []byte) (Workflow, error) {
	workflow, err := agentosplan.DecodeWireJSON[Workflow](data)
	if err != nil {
		return Workflow{}, fmt.Errorf("%w: decode serverless workflow json: %w", agentoscore.ErrInvalidRunPlan, err)
	}

	return workflow, nil
}

// MarshalYAML serializes a workflow using Serverless Workflow YAML.
func MarshalYAML(workflow *Workflow) ([]byte, error) {
	data, err := MarshalJSON(workflow)
	if err != nil {
		return nil, err
	}

	return yaml.JSONToYAML(data)
}

// UnmarshalYAML parses a Serverless Workflow YAML document.
func UnmarshalYAML(data []byte) (Workflow, error) {
	workflow, err := agentosplan.DecodeWireYAML[Workflow](data)
	if err != nil {
		return Workflow{}, fmt.Errorf("%w: decode serverless workflow yaml: %w", agentoscore.ErrInvalidRunPlan, err)
	}

	return workflow, nil
}

func (tasks TaskList) MarshalJSON() ([]byte, error) {
	rawTasks := make([]map[string]TaskDefinition, 0, len(tasks))
	for i := range tasks {
		task := tasks[i]
		if task.Name == "" {
			return nil, fmt.Errorf("%w: serverless workflow task name is required", agentoscore.ErrInvalidRunPlan)
		}

		rawTasks = append(rawTasks, map[string]TaskDefinition{task.Name: task.Definition})
	}

	return json.Marshal(rawTasks)
}

func (tasks *TaskList) UnmarshalJSON(data []byte) error {
	rawTasks, err := agentosplan.DecodeWireJSON[[]map[string]TaskDefinition](data)
	if err != nil {
		return fmt.Errorf("%w: decode serverless workflow tasks: %w", agentoscore.ErrInvalidRunPlan, err)
	}

	decoded := make(TaskList, 0, len(rawTasks))
	for i := range rawTasks {
		rawTask := rawTasks[i]
		if len(rawTask) != 1 {
			return fmt.Errorf("%w: serverless workflow task must have exactly one name", agentoscore.ErrInvalidRunPlan)
		}

		for name, definition := range rawTask {
			if name == "" {
				return fmt.Errorf("%w: serverless workflow task name is required", agentoscore.ErrInvalidRunPlan)
			}

			decoded = append(decoded, Task{Name: name, Definition: definition})
		}
	}

	*tasks = decoded

	return nil
}

func nextSuccessNode(spec *agentos.RunPlanSpec, nodeID string) string {
	next := ""

	for _, edge := range spec.Edges {
		if edge.From != nodeID {
			continue
		}

		if edge.On != "" && edge.On != agentos.EdgeOnSuccess {
			continue
		}

		if next != "" {
			return ""
		}

		next = edge.To
	}

	return next
}

func exportNamespace(spec *agentos.RunPlanSpec) string {
	if spec.ProjectID != "" {
		return spec.ProjectID
	}

	if spec.AccountID != "" {
		return spec.AccountID
	}

	return defaultExportNSPrefix
}

func exportName(spec *agentos.RunPlanSpec) string {
	if spec.PlanID != "" {
		return spec.PlanID
	}

	return defaultExportName
}
