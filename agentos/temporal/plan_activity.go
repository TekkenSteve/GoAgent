package temporal

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

// PlanActivities bridge Temporal PlanWorkflow decisions to AgentOS runtime calls.
type PlanActivities struct {
	Runtime          agentos.Runtime
	Validator        agentosplan.Validator
	PlanStateStore   agentosplan.PlanStateStore
	PlanEventStore   agentosplan.PlanEventStore
	RunBackendBinder PlanRunBackendBinder
	ArtifactStore    agentosplan.ArtifactStore
	Expressions      agentosplan.ValueExpressionCompiler
}

// PlanRunBackendBinder records plan-node ownership into the run route index.
type PlanRunBackendBinder interface {
	BindPlanNode(ctx context.Context, planID, nodeID string, spec agentos.RunSpec, status agentos.RunStatus) error
}

// NewPlanActivities creates plan activities backed by an AgentOS runtime.
func NewPlanActivities(runtime agentos.Runtime) *PlanActivities {
	activities, err := NewPlanActivitiesWithCapabilities(runtime, nil)
	if err != nil {
		panic(err)
	}

	return activities
}

// NewPlanActivitiesWithCapabilities creates plan activities with a static
// capability catalog used by RunPlan validation.
func NewPlanActivitiesWithCapabilities(runtime agentos.Runtime, capabilities []agentos.Capability) (*PlanActivities, error) {
	store := agentosplan.NewMemoryPlanStore()
	artifactStore := agentosplan.NewMemoryArtifactStore()

	return NewPlanActivitiesWithStores(runtime, capabilities, store, store, nil, artifactStore)
}

// NewPlanActivitiesWithStores creates plan activities with explicit durable
// state and event stores.
func NewPlanActivitiesWithStores(
	runtime agentos.Runtime,
	capabilities []agentos.Capability,
	stateStore agentosplan.PlanStateStore,
	eventStore agentosplan.PlanEventStore,
	runBackendBinder PlanRunBackendBinder,
	artifactStore agentosplan.ArtifactStore,
) (*PlanActivities, error) {
	compiler, err := agentosplan.NewCELCompiler()
	if err != nil {
		return nil, err
	}
	catalog, err := agentosplan.NewStaticCapabilityCatalog(capabilities)
	if err != nil {
		return nil, err
	}

	return &PlanActivities{
		Runtime: runtime,
		Validator: agentosplan.Validator{
			Expressions:  compiler,
			Capabilities: catalog,
		},
		PlanStateStore:   stateStore,
		PlanEventStore:   eventStore,
		RunBackendBinder: runBackendBinder,
		ArtifactStore:    artifactStore,
		Expressions:      compiler,
	}, nil
}

type validatePlanInput struct {
	Spec agentos.RunPlanSpec
}

type validatePlanOutput struct {
	Plan           agentosplan.ExecutablePlan
	ControlsByNode map[string][]agentos.ControlOperation
}

// ValidatePlanActivity validates a RunPlan before workflow execution.
func (a *PlanActivities) ValidatePlanActivity(ctx context.Context, input validatePlanInput) (validatePlanOutput, error) {
	plan, err := a.Validator.Validate(ctx, input.Spec)
	if err != nil {
		return validatePlanOutput{}, err
	}

	controlsByNode, err := a.controlsByNode(ctx, plan)
	if err != nil {
		return validatePlanOutput{}, err
	}

	return validatePlanOutput{Plan: plan, ControlsByNode: controlsByNode}, nil
}

func (a *PlanActivities) controlsByNode(ctx context.Context, plan agentosplan.ExecutablePlan) (map[string][]agentos.ControlOperation, error) {
	controlsByNode := make(map[string][]agentos.ControlOperation, len(plan.Spec.Nodes))
	for _, node := range plan.Spec.Nodes {
		if node.Capability == "" || a.Validator.Capabilities == nil {
			controlsByNode[node.NodeID] = nil
			continue
		}
		capability, ok, err := a.Validator.Capabilities.GetCapability(ctx, node.Run.Backend, node.Capability)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("%w: node %q capability %s/%s/%s", agentos.ErrCapabilityNotFound, node.NodeID, node.Run.Backend.Kind, node.Run.Backend.Name, node.Capability)
		}
		controlsByNode[node.NodeID] = append([]agentos.ControlOperation(nil), capability.Controls...)
	}

	return controlsByNode, nil
}

type startPlanNodeInput struct {
	PlanID     string
	PlanInputs map[string]any
	Status     agentos.RunPlanStatus
	Node       agentos.PlanNodeSpec
	Edges      []agentos.PlanEdgeSpec
}

type startPlanNodeOutput struct {
	Status agentos.RunStatus
}

// StartPlanNodeActivity starts one backend-owned child run.
func (a *PlanActivities) StartPlanNodeActivity(ctx context.Context, input startPlanNodeInput) (startPlanNodeOutput, error) {
	if a.Runtime == nil {
		return startPlanNodeOutput{}, fmt.Errorf("%w: plan activity runtime is required", agentos.ErrInvalidRunPlan)
	}
	if input.Node.Run.IdempotencyKey == "" {
		key, err := agentosplan.NodeStartIdempotencyKey(input.PlanID, input.Node.NodeID)
		if err != nil {
			return startPlanNodeOutput{}, err
		}
		input.Node.Run.IdempotencyKey = key
	}
	resolvedInput, err := agentosplan.ResolveRunInput(ctx, a.ArtifactStore, a.Expressions, input.PlanInputs, input.Status, input.Node, input.Edges)
	if err != nil {
		return startPlanNodeOutput{}, err
	}
	input.Node.Run.Input = resolvedInput

	status, err := a.Runtime.Start(ctx, input.Node.Run)
	if err != nil {
		return startPlanNodeOutput{}, err
	}
	if a.RunBackendBinder != nil {
		if err := a.RunBackendBinder.BindPlanNode(ctx, input.PlanID, input.Node.NodeID, input.Node.Run, status); err != nil {
			return startPlanNodeOutput{}, err
		}
	}

	return startPlanNodeOutput{Status: status}, nil
}

type persistPlanStateInput struct {
	Spec           agentos.RunPlanSpec
	Status         agentos.RunPlanStatus
	Event          agentos.PlanEvent
	IdempotencyKey string
}

type persistPlanStateOutput struct {
	Event agentos.PlanEvent
}

// PersistPlanStateActivity writes the latest reducer snapshot and appends the
// corresponding public PlanEvent to the durable event source.
func (a *PlanActivities) PersistPlanStateActivity(ctx context.Context, input persistPlanStateInput) (persistPlanStateOutput, error) {
	if a.PlanStateStore == nil {
		return persistPlanStateOutput{}, fmt.Errorf("%w: plan state store is required", agentos.ErrInvalidRunPlan)
	}
	if a.PlanEventStore == nil {
		return persistPlanStateOutput{}, fmt.Errorf("%w: plan event store is required", agentos.ErrInvalidRunPlan)
	}
	if err := a.PlanStateStore.SavePlanState(ctx, agentosplan.PlanStateSnapshot{
		Spec:           input.Spec,
		Status:         input.Status,
		IdempotencyKey: input.IdempotencyKey,
	}); err != nil {
		return persistPlanStateOutput{}, err
	}
	event, err := a.PlanEventStore.AppendPlanEvent(ctx, input.Event, input.IdempotencyKey)
	if err != nil {
		return persistPlanStateOutput{}, err
	}

	return persistPlanStateOutput{Event: event}, nil
}

type publishPlanArtifactsInput struct {
	PlanID string
	Node   agentos.PlanNodeSpec
	Status agentos.RunStatus
}

type publishPlanArtifactsOutput struct {
	Artifacts []agentos.ArtifactRef
}

// PublishPlanArtifactsActivity persists child-run artifact refs outside workflow history.
func (a *PlanActivities) PublishPlanArtifactsActivity(ctx context.Context, input publishPlanArtifactsInput) (publishPlanArtifactsOutput, error) {
	if a.ArtifactStore == nil {
		return publishPlanArtifactsOutput{}, fmt.Errorf("%w: artifact store is required", agentos.ErrInvalidArtifact)
	}
	refs, err := normalizeRunArtifacts(input.PlanID, input.Node, input.Status)
	if err != nil {
		return publishPlanArtifactsOutput{}, err
	}
	stored := make([]agentos.ArtifactRef, 0, len(refs))
	for _, ref := range refs {
		key, err := agentosplan.ArtifactPublishIdempotencyKey(input.PlanID, input.Node.NodeID, input.Status.RunID, ref.Name)
		if err != nil {
			return publishPlanArtifactsOutput{}, err
		}
		storedRef, err := a.ArtifactStore.Put(ctx, ref, nil, key)
		if err != nil {
			return publishPlanArtifactsOutput{}, err
		}
		stored = append(stored, storedRef)
	}

	return publishPlanArtifactsOutput{Artifacts: stored}, nil
}

type statusPlanNodeInput struct {
	RunID string
}

type statusPlanNodeOutput struct {
	Status agentos.RunStatus
}

// StatusPlanNodeActivity queries one backend-owned child run.
func (a *PlanActivities) StatusPlanNodeActivity(ctx context.Context, input statusPlanNodeInput) (statusPlanNodeOutput, error) {
	if a.Runtime == nil {
		return statusPlanNodeOutput{}, fmt.Errorf("%w: plan activity runtime is required", agentos.ErrInvalidRunPlan)
	}
	status, err := a.Runtime.Status(ctx, input.RunID)
	if err != nil {
		return statusPlanNodeOutput{}, err
	}

	return statusPlanNodeOutput{Status: status}, nil
}

type controlPlanNodeInput struct {
	RunID   string
	Control agentos.ControlRequest
}

// ControlPlanNodeActivity sends lifecycle control to one child run.
func (a *PlanActivities) ControlPlanNodeActivity(ctx context.Context, input controlPlanNodeInput) error {
	if a.Runtime == nil {
		return fmt.Errorf("%w: plan activity runtime is required", agentos.ErrInvalidRunPlan)
	}

	return a.Runtime.Control(ctx, input.RunID, input.Control)
}
