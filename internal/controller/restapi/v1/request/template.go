package request

// TemplateImport -.
type TemplateImport struct {
	YAMLData string `json:"yaml_data"  validate:"required" example:"name: my-workflow\nagents:\n  - id: agent-1\n    name: Agent 1\n    model: gpt-4.1-mini\nsteps:\n  - id: step-1\n    agent_ref: agent-1\n"`
}
