package agentosruntime

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// RunBackendIndex records which backend owns each run.
type RunBackendIndex interface {
	Bind(ctx context.Context, spec agentos.RunSpec, status agentos.RunStatus) error
	BindPlanNode(ctx context.Context, planID, nodeID string, spec agentos.RunSpec, status agentos.RunStatus) error
	GetRunBackend(ctx context.Context, runID string) (agentos.RunBackendOwnership, bool, error)
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
	if spec.RunID == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}
	if spec.IdempotencyKey == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run idempotency key is required", agentos.ErrInvalidRunSpec)
	}
	if err := validateRunSpecScope(spec); err != nil {
		return agentos.RunStatus{}, err
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

	claimStatus := agentos.RunStatus{RunID: spec.RunID, LifecycleState: RunBackendLifecycleClaiming}
	existing, exists, err := r.index.GetRunBackend(ctx, spec.RunID)
	if err != nil {
		return agentos.RunStatus{}, err
	}
	if exists {
		requested := RunBackendIndexRecordFromRunSpec(spec, claimStatus)
		if err := ValidateRunBackendIndexIdempotency(RunBackendIndexRecordFromOwnership(existing), requested); err != nil {
			return agentos.RunStatus{}, err
		}
		if existing.LifecycleState != RunBackendLifecycleClaiming {
			status, err := backend.Status(ctx, spec.RunID)
			if err != nil {
				return agentos.RunStatus{}, err
			}

			return normalizeOwnedRunStatus(spec.RunID, status)
		}
	} else if err := r.index.Bind(ctx, spec, claimStatus); err != nil {
		return agentos.RunStatus{}, err
	}

	status, err := backend.Start(ctx, spec)
	if err != nil {
		return agentos.RunStatus{}, err
	}
	status, err = normalizeOwnedRunStatus(spec.RunID, status)
	if err != nil {
		return agentos.RunStatus{}, err
	}
	if err := r.index.Bind(ctx, spec, status); err != nil {
		return agentos.RunStatus{}, err
	}

	return status, nil
}

// StartPlanNode starts a backend-owned child run and records plan-node
// ownership without first creating a standalone route.
func (r *Router) StartPlanNode(ctx context.Context, planID, nodeID string, spec agentos.RunSpec) (agentos.RunStatus, error) {
	if planID == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if nodeID == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: node id is required", agentos.ErrInvalidRunPlan)
	}
	if spec.RunID == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}
	if spec.IdempotencyKey == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run idempotency key is required", agentos.ErrInvalidRunSpec)
	}
	if err := validateRunSpecScope(spec); err != nil {
		return agentos.RunStatus{}, err
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

	claimStatus := agentos.RunStatus{RunID: spec.RunID, LifecycleState: RunBackendLifecycleClaiming}
	existing, exists, err := r.index.GetRunBackend(ctx, spec.RunID)
	if err != nil {
		return agentos.RunStatus{}, err
	}
	if exists {
		requested := RunBackendIndexRecordFromPlanNode(planID, nodeID, spec, claimStatus)
		if err := ValidateRunBackendIndexIdempotency(RunBackendIndexRecordFromOwnership(existing), requested); err != nil {
			return agentos.RunStatus{}, err
		}
		if existing.LifecycleState != RunBackendLifecycleClaiming {
			status, err := backend.Status(ctx, spec.RunID)
			if err != nil {
				return agentos.RunStatus{}, err
			}

			return normalizeOwnedRunStatus(spec.RunID, status)
		}
	} else if err := r.index.BindPlanNode(ctx, planID, nodeID, spec, claimStatus); err != nil {
		return agentos.RunStatus{}, err
	}

	status, err := backend.Start(ctx, spec)
	if err != nil {
		return agentos.RunStatus{}, err
	}
	status, err = normalizeOwnedRunStatus(spec.RunID, status)
	if err != nil {
		return agentos.RunStatus{}, err
	}
	if err := r.index.BindPlanNode(ctx, planID, nodeID, spec, status); err != nil {
		return agentos.RunStatus{}, err
	}

	return status, nil
}

func validateRunSpecScope(spec agentos.RunSpec) error {
	if spec.AccountID == "" {
		return fmt.Errorf("%w: account id is required", agentos.ErrInvalidRunSpec)
	}
	if spec.ProjectID == "" {
		return fmt.Errorf("%w: project id is required", agentos.ErrInvalidRunSpec)
	}

	return nil
}

func normalizeOwnedRunStatus(runID string, status agentos.RunStatus) (agentos.RunStatus, error) {
	if status.RunID == "" {
		status.RunID = runID
	}
	if status.RunID != runID {
		return agentos.RunStatus{}, fmt.Errorf("%w: backend returned run id %q for requested run %q", agentos.ErrInvalidRunSpec, status.RunID, runID)
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

	status, err := backend.Status(ctx, runID)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	return normalizeOwnedRunStatus(runID, status)
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
