package entity

const DelegateToolName = "delegate_to_agent"

// DelegateTaskInput defines the input for a delegate_to_agent tool call.
type DelegateTaskInput struct {
	SystemPrompt string   `json:"system_prompt"`
	Task         string   `json:"task"`
	ModelRef     string   `json:"model_ref,omitempty"`
	Tools        []string `json:"tools,omitempty"`
}
