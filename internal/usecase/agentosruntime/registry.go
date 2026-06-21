package agentosruntime

import (
	"fmt"
	"sync"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// Registry stores named agent backends.
type Registry struct {
	mu       sync.RWMutex
	backends map[agentos.BackendRef]AgentBackend
}

// NewRegistry creates an empty backend registry.
func NewRegistry() *Registry {
	return &Registry{backends: make(map[agentos.BackendRef]AgentBackend)}
}

// Register installs a backend under a stable public reference.
func (r *Registry) Register(ref agentos.BackendRef, backend AgentBackend) error {
	if err := validateBackendRef(ref); err != nil {
		return err
	}
	if backend == nil {
		return fmt.Errorf("%w: nil backend for %s/%s", agentos.ErrBackendNotFound, ref.Kind, ref.Name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.backends[ref] = backend

	return nil
}

// Get returns the backend registered for ref.
func (r *Registry) Get(ref agentos.BackendRef) (AgentBackend, error) {
	if err := validateBackendRef(ref); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	backend, ok := r.backends[ref]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", agentos.ErrBackendNotFound, ref.Kind, ref.Name)
	}

	return backend, nil
}

func validateBackendRef(ref agentos.BackendRef) error {
	if ref.Kind == "" {
		return fmt.Errorf("%w: kind is required", agentos.ErrInvalidBackendRef)
	}
	if ref.Name == "" {
		return fmt.Errorf("%w: name is required", agentos.ErrInvalidBackendRef)
	}

	return nil
}
