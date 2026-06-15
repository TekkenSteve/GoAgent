package backend

import (
	"context"
	"fmt"
	"sync"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// RunBackendIndex records which backend owns each run.
type RunBackendIndex interface {
	Bind(ctx context.Context, runID string, ref agentos.BackendRef) error
	Resolve(ctx context.Context, runID string) (agentos.BackendRef, error)
}

// MemoryRunBackendIndex is a process-local run route index.
type MemoryRunBackendIndex struct {
	mu     sync.RWMutex
	routes map[string]agentos.BackendRef
}

// NewMemoryRunBackendIndex creates an empty in-memory route index.
func NewMemoryRunBackendIndex() *MemoryRunBackendIndex {
	return &MemoryRunBackendIndex{routes: make(map[string]agentos.BackendRef)}
}

// Bind stores the backend reference for a run.
func (i *MemoryRunBackendIndex) Bind(_ context.Context, runID string, ref agentos.BackendRef) error {
	if runID == "" {
		return fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}
	if err := validateBackendRef(ref); err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	i.routes[runID] = ref

	return nil
}

// Resolve returns the backend reference that owns a run.
func (i *MemoryRunBackendIndex) Resolve(_ context.Context, runID string) (agentos.BackendRef, error) {
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

// Router directs AgentOS operations to the backend that owns the run.
type Router struct {
	registry *Registry
	index    RunBackendIndex
}

// NewRouter creates a backend router.
func NewRouter(registry *Registry, index RunBackendIndex) (*Router, error) {
	if registry == nil {
		return nil, fmt.Errorf("%w: nil registry", agentos.ErrBackendNotFound)
	}
	if index == nil {
		return nil, fmt.Errorf("%w: nil run backend index", agentos.ErrRunRouteNotFound)
	}

	return &Router{registry: registry, index: index}, nil
}

// Start routes a new run to the selected backend and records run ownership.
func (r *Router) Start(ctx context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	if err := validateBackendRef(spec.Backend); err != nil {
		return agentos.RunStatus{}, err
	}

	backend, err := r.registry.Get(spec.Backend)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	status, err := backend.Start(ctx, spec)
	if err != nil {
		return agentos.RunStatus{}, err
	}
	if err := r.index.Bind(ctx, spec.RunID, spec.Backend); err != nil {
		return agentos.RunStatus{}, err
	}

	return status, nil
}

// Signal sends a business signal to the backend that owns the run.
func (r *Router) Signal(ctx context.Context, runID string, signal agentos.Signal) error {
	backend, err := r.backendForRun(ctx, runID)
	if err != nil {
		return err
	}

	return backend.Signal(ctx, runID, signal)
}

// Control sends a lifecycle operation to the backend that owns the run.
func (r *Router) Control(ctx context.Context, runID string, op agentos.ControlOperation) error {
	backend, err := r.backendForRun(ctx, runID)
	if err != nil {
		return err
	}

	return backend.Control(ctx, runID, op)
}

// Status queries the backend that owns the run.
func (r *Router) Status(ctx context.Context, runID string) (agentos.RunStatus, error) {
	backend, err := r.backendForRun(ctx, runID)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	return backend.Status(ctx, runID)
}

// Subscribe opens a stream subscription through the backend selected by the scope.
func (r *Router) Subscribe(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error) {
	ref, err := r.refForScope(ctx, scope)
	if err != nil {
		return nil, err
	}

	backend, err := r.registry.Get(ref)
	if err != nil {
		return nil, err
	}

	return backend.Subscribe(ctx, scope)
}

func (r *Router) backendForRun(ctx context.Context, runID string) (AgentBackend, error) {
	ref, err := r.index.Resolve(ctx, runID)
	if err != nil {
		return nil, err
	}

	return r.registry.Get(ref)
}

func (r *Router) refForScope(ctx context.Context, scope agentos.StreamScope) (agentos.BackendRef, error) {
	if scope.RunID == "" {
		return agentos.BackendRef{}, fmt.Errorf("%w: run id is required for backend routing", agentos.ErrInvalidStreamScope)
	}

	return r.index.Resolve(ctx, scope.RunID)
}
