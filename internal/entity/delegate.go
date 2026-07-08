package entity

import "encoding/json"

const DelegateToolName = "delegate_to_agent"

// DelegateTaskInput defines the input for a delegate_to_agent tool call.
// Used both for the tool's JSON schema and for parsing incoming tool call arguments.
type DelegateTaskInput struct {
	SystemPrompt string   `json:"system_prompt"`
	Task         string   `json:"task"`
	ModelRef     string   `json:"model,omitempty"`
	Tools        []string `json:"tools,omitempty"`
}

// ParseDelegateArgs unmarshals a tool call's JSON arguments into a typed struct.
func ParseDelegateArgs(rawJSON string) (*DelegateTaskInput, error) {
	var input DelegateTaskInput
	if err := json.Unmarshal([]byte(rawJSON), &input); err != nil {
		return nil, err
	}

	return &input, nil
}
