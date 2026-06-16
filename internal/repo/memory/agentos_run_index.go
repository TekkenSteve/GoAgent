package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// AgentOSRunIndex is a process-local AgentOS run route index.
type AgentOSRunIndex struct {
	mu     sync.RWMutex
	routes map[string]agentos.BackendRef
}

// NewAgentOSRunIndex creates an empty in-memory AgentOS run route index.
func NewAgentOSRunIndex() *AgentOSRunIndex {
	return &AgentOSRunIndex{routes: make(map[string]agentos.BackendRef)}
}

// Bind stores the backend reference for a run.
func (i *AgentOSRunIndex) Bind(_ context.Context, spec agentos.RunSpec) error {
	if spec.RunID == "" {
		return fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}
	if spec.Backend.Kind == "" {
		return fmt.Errorf("%w: kind is required", agentos.ErrInvalidBackendRef)
	}
	if spec.Backend.Name == "" {
		return fmt.Errorf("%w: name is required", agentos.ErrInvalidBackendRef)
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	i.routes[spec.RunID] = spec.Backend

	return nil
}

// Resolve returns the backend reference that owns a run.
func (i *AgentOSRunIndex) Resolve(_ context.Context, runID string) (agentos.BackendRef, error) {
	if runID == "" {
		return agentos.BackendRef{}, fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	ref, ok := i.routes[runID]
	if !ok {
		return agentos.BackendRef{}, fmt.Errorf("%w: %s", agentos.ErrRunRouteNotFound, runID)
	}

	return ref, nil
}
