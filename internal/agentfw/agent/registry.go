package agent

import (
	"fmt"
	"sync"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// AgentRegistry manages agent definitions and supports multiple sources:
// code-defined, config-defined, and runtime-created agents.
type AgentRegistry struct {
	mu      sync.RWMutex
	agents  map[string]*AgentDefinition
}

// NewRegistry creates an empty agent registry.
func NewRegistry() *AgentRegistry {
	return &AgentRegistry{
		agents: make(map[string]*AgentDefinition),
	}
}

// Register adds an agent definition to the registry.
func (r *AgentRegistry) Register(def *AgentDefinition) error {
	if def == nil {
		return fmt.Errorf("agent registry: cannot register nil definition")
	}
	if err := def.Validate(); err != nil {
		return fmt.Errorf("agent registry: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.agents[def.Spec.ID] = def
	return nil
}

// Get retrieves an agent definition by ID.
func (r *AgentRegistry) Get(id string) (*AgentDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.agents[id]
	return def, ok
}

// MustGet retrieves an agent definition by ID, panicking if missing.
func (r *AgentRegistry) MustGet(id string) *AgentDefinition {
	def, ok := r.Get(id)
	if !ok {
		panic(fmt.Sprintf("agent registry: agent %q not found", id))
	}
	return def
}

// List returns all registered agent definitions.
func (r *AgentRegistry) List() []*AgentDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*AgentDefinition, 0, len(r.agents))
	for _, def := range r.agents {
		result = append(result, def)
	}
	return result
}

// RegisterFromSpec registers an agent directly from a domain AgentSpec.
func (r *AgentRegistry) RegisterFromSpec(spec entity.AgentSpec, toolDefs []entity.ToolDef) error {
	return r.Register(&AgentDefinition{
		Spec:  spec,
		Tools: toolDefs,
	})
}

// Remove unregisters an agent by ID.
func (r *AgentRegistry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.agents, id)
}
