package agentosruntime

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// RunBackendIndex records which backend owns each run.
type RunBackendIndex interface {
	Bind(ctx context.Context, spec agentos.RunSpec) error
	Resolve(ctx context.Context, runID string) (agentos.BackendRef, error)
}

// BackendSelector resolves a backend for runs that do not specify one directly.
type BackendSelector interface {
	Select(ctx context.Context, spec agentos.RunSpec) (agentos.BackendRef, error)
}

// Router directs AgentOS operations to the backend that owns the run.
type Router struct {
	registry        *Registry
	index           RunBackendIndex
	backendSelector BackendSelector
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

// WithBackendSelector installs an optional backend policy resolver.
func (r *Router) WithBackendSelector(selector BackendSelector) *Router {
	r.backendSelector = selector

	return r
}

// Start routes a new run to the selected backend and records run ownership.
func (r *Router) Start(ctx context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	if spec.IdempotencyKey == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run idempotency key is required", agentos.ErrInvalidRunSpec)
	}
	selectedBackend, err := r.selectBackend(ctx, spec)
	if err != nil {
		return agentos.RunStatus{}, err
	}
	spec.Backend = selectedBackend

	backend, err := r.registry.Get(spec.Backend)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	status, err := backend.Start(ctx, spec)
	if err != nil {
		return agentos.RunStatus{}, err
	}
	if err := r.index.Bind(ctx, spec); err != nil {
		return agentos.RunStatus{}, err
	}

	return status, nil
}

func (r *Router) selectBackend(ctx context.Context, spec agentos.RunSpec) (agentos.BackendRef, error) {
	if spec.Backend.Kind != "" || spec.Backend.Name != "" {
		if err := validateBackendRef(spec.Backend); err != nil {
			return agentos.BackendRef{}, err
		}

		return spec.Backend, nil
	}
	if r.backendSelector == nil {
		return agentos.BackendRef{}, fmt.Errorf("%w: backend is required", agentos.ErrInvalidBackendRef)
	}

	ref, err := r.backendSelector.Select(ctx, spec)
	if err != nil {
		return agentos.BackendRef{}, err
	}
	if err := validateBackendRef(ref); err != nil {
		return agentos.BackendRef{}, err
	}

	return ref, nil
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
func (r *Router) Control(ctx context.Context, runID string, control agentos.ControlRequest) error {
	if err := agentos.ValidateControlRequest(control); err != nil {
		return err
	}
	backend, err := r.backendForRun(ctx, runID)
	if err != nil {
		return err
	}

	return backend.Control(ctx, runID, control)
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
