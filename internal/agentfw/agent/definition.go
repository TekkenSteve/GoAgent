// Package agent provides agent lifecycle management.
package agent

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// ToolBindingOverride allows per-agent customization of a tool's behaviour.
type ToolBindingOverride struct {
	Name        string
	Description string // override tool description for this agent
	Required    bool   // fail if tool is unavailable
}

// AgentDefinition is a fully resolved agent ready for execution.
// It combines the domain AgentSpec with resolved tool references.
type AgentDefinition struct {
	Spec   entity.AgentSpec
	Tools  []entity.ToolDef // resolved tool definitions for LLM
}

// Validate checks that the agent definition is internally consistent.
func (a *AgentDefinition) Validate() error {
	if a.Spec.ID == "" {
		return fmt.Errorf("agent definition: id is required")
	}
	if a.Spec.Name == "" {
		return fmt.Errorf("agent definition: name is required for agent %q", a.Spec.ID)
	}
	if a.Spec.ModelRef == "" {
		return fmt.Errorf("agent definition: model_ref is required for agent %q", a.Spec.ID)
	}
	return nil
}
