package agent

import (
	"errors"
	"fmt"
	"sync"

	"github.com/TekkenSteve/GoAgent/entity"
)

var ErrNilDefinition = errors.New("agent registry: cannot register nil definition")

// Registry manages agent definitions and supports multiple sources:
// code-defined, config-defined, and runtime-created agents.
type Registry struct {
	mu     sync.RWMutex
	agents map[string]*Definition
}

// NewRegistry creates an empty agent registry.
func NewRegistry() *Registry {
	return &Registry{
		agents: make(map[string]*Definition),
	}
}

// Register adds an agent definition to the registry.
func (r *Registry) Register(def *Definition) error {
	if def == nil {
		return ErrNilDefinition
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
func (r *Registry) Get(id string) (*Definition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	def, ok := r.agents[id]

	return def, ok
}

// MustGet retrieves an agent definition by ID, panicking if missing.
func (r *Registry) MustGet(id string) *Definition {
	def, ok := r.Get(id)
	if !ok {
		panic(fmt.Sprintf("agent registry: agent %q not found", id))
	}

	return def
}

// List returns all registered agent definitions.
func (r *Registry) List() []*Definition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*Definition, 0, len(r.agents))
	for _, def := range r.agents {
		result = append(result, def)
	}

	return result
}

// RegisterFromSpec registers an agent directly from a domain AgentSpec.
func (r *Registry) RegisterFromSpec(spec *entity.AgentSpec, toolDefs []entity.ToolDef) error {
	return r.Register(&Definition{
		Spec:  *spec,
		Tools: toolDefs,
	})
}

// Remove unregisters an agent by ID.
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.agents, id)
}
