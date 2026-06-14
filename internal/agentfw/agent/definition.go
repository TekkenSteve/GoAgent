// Package agent provides agent lifecycle management.
package agent

import (
	"errors"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// ErrInvalidAgent is returned when an agent definition fails validation.
var ErrInvalidAgent = errors.New("invalid agent definition")

// ToolBindingOverride allows per-agent customization of a tool's behavior.
type ToolBindingOverride struct {
	Name        string
	Description string // override tool description for this agent
	Required    bool   // fail if tool is unavailable
}

// Definition is a fully resolved agent ready for execution.
// It combines the domain AgentSpec with resolved tool references.
type Definition struct {
	Spec  entity.AgentSpec
	Tools []entity.ToolDef // resolved tool definitions for LLM
}

// Validate checks that the agent definition is internally consistent.
func (a *Definition) Validate() error {
	if a.Spec.ID == "" {
		return fmt.Errorf("%w: id is required", ErrInvalidAgent)
	}

	if a.Spec.Name == "" {
		return fmt.Errorf("%w: name is required for agent %q", ErrInvalidAgent, a.Spec.ID)
	}

	if a.Spec.ModelRef == "" {
		return fmt.Errorf("%w: model_ref is required for agent %q", ErrInvalidAgent, a.Spec.ID)
	}

	return nil
}
