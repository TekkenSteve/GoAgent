package temporal

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

// PlanActivities bridge Temporal PlanWorkflow decisions to AgentOS runtime calls.
type PlanActivities struct {
	Runtime             agentos.Runtime
	PlanNodeStarter     PlanNodeStarter
	Validator           agentosplan.Validator
	PlanTransitionStore agentosplan.PlanTransitionStore
	PlanEventPublisher  agentosplan.PlanEventPublisher
	ArtifactStore       agentosplan.ArtifactStore
	ArtifactSchemas     agentosplan.ArtifactSchemaCatalog
	PlanDeltaProvider   agentosplan.PlanDeltaProvider
	Expressions         agentosplan.ValueExpressionCompiler
}

// PlanNodeStarter starts backend-owned child runs and records plan-node route
// ownership atomically at the AgentOS runtime boundary.
type PlanNodeStarter interface {
	StartPlanNode(ctx context.Context, planID, nodeID string, spec agentos.RunSpec) (agentos.RunStatus, error)
}

// NewPlanActivitiesWithStores creates plan activities with explicit durable
// state and event stores.
func NewPlanActivitiesWithStores(
	runtime agentos.Runtime,
	capabilities []agentos.Capability,
	transitionStore agentosplan.PlanTransitionStore,
	eventPublisher agentosplan.PlanEventPublisher,
	artifactStore agentosplan.ArtifactStore,
) (*PlanActivities, error) {
	catalog, err := agentosplan.NewStaticCapabilityCatalog(capabilities)
	if err != nil {
		return nil, err
	}

	return NewPlanActivitiesWithCatalog(runtime, catalog, transitionStore, eventPublisher, artifactStore)
}

// NewPlanActivitiesWithCatalog creates plan activities with an explicit
// capability catalog. Production wiring should provide a durable catalog.
func NewPlanActivitiesWithCatalog(
	runtime agentos.Runtime,
	catalog agentosplan.CapabilityCatalog,
	transitionStore agentosplan.PlanTransitionStore,
	eventPublisher agentosplan.PlanEventPublisher,
	artifactStore agentosplan.ArtifactStore,
) (*PlanActivities, error) {
	return NewPlanActivitiesWithCatalogAndSchemas(runtime, catalog, nil, transitionStore, eventPublisher, artifactStore)
}

// NewPlanActivitiesWithCatalogAndSchemas creates plan activities with explicit
// capability and artifact schema catalogs.
func NewPlanActivitiesWithCatalogAndSchemas(
	runtime agentos.Runtime,
	catalog agentosplan.CapabilityCatalog,
	artifactSchemas agentosplan.ArtifactSchemaCatalog,
	transitionStore agentosplan.PlanTransitionStore,
	eventPublisher agentosplan.PlanEventPublisher,
	artifactStore agentosplan.ArtifactStore,
) (*PlanActivities, error) {
	if runtime == nil {
		return nil, fmt.Errorf("%w: plan activity runtime is required", agentos.ErrInvalidRunPlan)
	}
	planNodeStarter, ok := runtime.(PlanNodeStarter)
	if !ok {
		return nil, fmt.Errorf("%w: plan activity runtime must support plan-node start", agentos.ErrInvalidRunPlan)
	}
	if transitionStore == nil {
		return nil, fmt.Errorf("%w: plan transition store is required", agentos.ErrInvalidRunPlan)
	}
	if artifactStore == nil {
		return nil, fmt.Errorf("%w: artifact store is required", agentos.ErrInvalidArtifact)
	}
	compiler, err := agentosplan.NewCELCompiler()
	if err != nil {
		return nil, err
	}

	return &PlanActivities{
		Runtime:         runtime,
		PlanNodeStarter: planNodeStarter,
		Validator: agentosplan.Validator{
			Expressions:     compiler,
			Capabilities:    catalog,
			ArtifactSchemas: artifactSchemas,
		},
		PlanTransitionStore: transitionStore,
		PlanEventPublisher:  eventPublisher,
		ArtifactStore:       artifactStore,
		ArtifactSchemas:     artifactSchemas,
		PlanDeltaProvider:   agentosplan.NewArtifactPlanDeltaProvider(artifactStore),
		Expressions:         compiler,
	}, nil
}

type validatePlanInput struct {
	Spec agentos.RunPlanSpec
}

type validatePlanOutput struct {
	Plan               agentosplan.ExecutablePlan
	ControlsByNode     map[string][]agentos.ControlOperation
	CapabilitiesByNode map[string]agentosplan.CapabilitySelectionTrace
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
	capabilitiesByNode, err := a.capabilitiesByNode(ctx, plan)
	if err != nil {
		return validatePlanOutput{}, err
	}

	return validatePlanOutput{
		Plan:               plan,
		ControlsByNode:     controlsByNode,
		CapabilitiesByNode: capabilitiesByNode,
	}, nil
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

func (a *PlanActivities) capabilitiesByNode(ctx context.Context, plan agentosplan.ExecutablePlan) (map[string]agentosplan.CapabilitySelectionTrace, error) {
	capabilitiesByNode := make(map[string]agentosplan.CapabilitySelectionTrace)
	for _, node := range plan.Spec.Nodes {
		if node.Capability == "" || a.Validator.Capabilities == nil {
			continue
		}
		capability, ok, err := a.Validator.Capabilities.GetCapability(ctx, node.Run.Backend, node.Capability)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("%w: node %q capability %s/%s/%s", agentos.ErrCapabilityNotFound, node.NodeID, node.Run.Backend.Kind, node.Run.Backend.Name, node.Capability)
		}
		capabilitiesByNode[node.NodeID] = agentosplan.NewCapabilitySelectionTrace(capability)
	}

	return capabilitiesByNode, nil
}

type resolvePlanNodeInputInput struct {
	Spec   agentos.RunPlanSpec
	Status agentos.RunPlanStatus
	Node   agentos.PlanNodeSpec
	Edges  []agentos.PlanEdgeSpec
}

type resolvePlanNodeInputOutput struct {
	Input map[string]any
	Trace agentosplan.InputResolutionTrace
}

// ResolvePlanNodeInputActivity resolves node input mapping before a child run starts.
func (a *PlanActivities) ResolvePlanNodeInputActivity(ctx context.Context, input resolvePlanNodeInputInput) (resolvePlanNodeInputOutput, error) {
	resolvedInput, err := agentosplan.ResolveRunInput(ctx, a.ArtifactStore, a.Expressions, input.Spec, input.Status, input.Node, input.Edges)
	if err != nil {
		return resolvePlanNodeInputOutput{}, err
	}

	return resolvePlanNodeInputOutput{
		Input: resolvedInput,
		Trace: agentosplan.NewInputResolutionTrace(
			resolvedInput,
			input.Node.Inputs,
			input.Edges,
		),
	}, nil
}

type startPlanNodeInput struct {
	PlanID    string
	AccountID string
	ProjectID string
	Node      agentos.PlanNodeSpec
	Attempt   int32
}

type startPlanNodeOutput struct {
	Status agentos.RunStatus
}

// StartPlanNodeActivity starts one backend-owned child run.
func (a *PlanActivities) StartPlanNodeActivity(ctx context.Context, input startPlanNodeInput) (startPlanNodeOutput, error) {
	if a.Runtime == nil {
		return startPlanNodeOutput{}, fmt.Errorf("%w: plan activity runtime is required", agentos.ErrInvalidRunPlan)
	}
	attempt := input.Attempt
	if attempt <= 0 {
		attempt = 1
	}
	if input.Node.Run.IdempotencyKey == "" {
		key, err := agentosplan.NodeStartIdempotencyKey(input.PlanID, input.Node.NodeID, attempt)
		if err != nil {
			return startPlanNodeOutput{}, err
		}
		input.Node.Run.IdempotencyKey = key
	}
	if input.AccountID == "" {
		return startPlanNodeOutput{}, fmt.Errorf("%w: account id is required", agentos.ErrInvalidRunSpec)
	}
	if input.ProjectID == "" {
		return startPlanNodeOutput{}, fmt.Errorf("%w: project id is required", agentos.ErrInvalidRunSpec)
	}
	input.Node.Run.AccountID = input.AccountID
	input.Node.Run.ProjectID = input.ProjectID
	status, err := a.PlanNodeStarter.StartPlanNode(ctx, input.PlanID, input.Node.NodeID, input.Node.Run)
	if err != nil {
		return startPlanNodeOutput{}, err
	}
	if status.RunID == "" {
		status.RunID = input.Node.Run.RunID
	}
	if status.RunID != input.Node.Run.RunID {
		return startPlanNodeOutput{}, fmt.Errorf("%w: backend returned run id %q for requested run %q", agentos.ErrInvalidRunSpec, status.RunID, input.Node.Run.RunID)
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
	if a.PlanTransitionStore == nil {
		return persistPlanStateOutput{}, fmt.Errorf("%w: plan transition store is required", agentos.ErrInvalidRunPlan)
	}
	event, err := a.PlanTransitionStore.PersistPlanTransition(ctx, agentosplan.PlanStateSnapshot{
		Spec:   input.Spec,
		Status: input.Status,
	}, input.Event, input.IdempotencyKey)
	if err != nil {
		return persistPlanStateOutput{}, err
	}
	if a.PlanEventPublisher != nil {
		// Redis is a live transport only. The durable event source is the
		// PlanTransitionStore above, so live publish failures must not block
		// workflow progress or make Redis part of replay correctness.
		_ = a.PlanEventPublisher.PublishPlanEvent(ctx, event)
	}

	return persistPlanStateOutput{Event: event}, nil
}

type publishPlanArtifactsInput struct {
	Spec   agentos.RunPlanSpec
	Node   agentos.PlanNodeSpec
	Status agentos.RunStatus
}

type publishPlanArtifactsOutput struct {
	Artifacts []agentos.ArtifactRef
}

// PublishPlanArtifactsActivity persists child-run artifact refs outside
// workflow history. Backends that produce payloads must write those payloads to
// the configured AgentOS ArtifactStore before returning refs; this activity
// only claims the ref idempotently and returns metadata to the workflow.
func (a *PlanActivities) PublishPlanArtifactsActivity(ctx context.Context, input publishPlanArtifactsInput) (publishPlanArtifactsOutput, error) {
	if a.ArtifactStore == nil {
		return publishPlanArtifactsOutput{}, fmt.Errorf("%w: artifact store is required", agentos.ErrInvalidArtifact)
	}
	refs, err := normalizeRunArtifacts(input.Spec.PlanID, input.Node, input.Status)
	if err != nil {
		return publishPlanArtifactsOutput{}, err
	}
	if runSucceeded(input.Status.LifecycleState) {
		if err := a.validatePublishedArtifacts(ctx, input.Spec, input.Node, refs); err != nil {
			return publishPlanArtifactsOutput{}, err
		}
	}
	stored := make([]agentos.ArtifactRef, 0, len(refs))
	for _, ref := range refs {
		key, err := agentosplan.ArtifactPublishIdempotencyKey(input.Spec.PlanID, input.Node.NodeID, input.Status.RunID, ref.Name)
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

func (a *PlanActivities) validatePublishedArtifacts(ctx context.Context, spec agentos.RunPlanSpec, node agentos.PlanNodeSpec, refs []agentos.ArtifactRef) error {
	if err := agentosplan.ValidateArtifactsAgainstSpecs(node.NodeID, node.Outputs, refs); err != nil {
		return err
	}
	if err := agentosplan.ValidateArtifactPayloadsAgainstSchemas(ctx, a.ArtifactStore, a.ArtifactSchemas, spec, node, refs); err != nil {
		return err
	}
	if node.Capability == "" || a.Validator.Capabilities == nil {
		return nil
	}
	capability, ok, err := a.Validator.Capabilities.GetCapability(ctx, node.Run.Backend, node.Capability)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: node %q capability %s/%s/%s", agentos.ErrCapabilityNotFound, node.NodeID, node.Run.Backend.Kind, node.Run.Backend.Name, node.Capability)
	}
	if err := agentosplan.ValidateCapabilityOutputArtifacts(node.NodeID, capability.OutputSchema, refs); err != nil {
		return err
	}

	return nil
}

type evaluatePlanExpansionInput struct {
	Spec           agentos.RunPlanSpec
	Status         agentos.RunPlanStatus
	Node           agentos.PlanNodeSpec
	RunStatus      agentos.RunStatus
	Artifacts      []agentos.ArtifactRef
	ExpansionCount int32
}

type evaluatePlanExpansionOutput struct {
	Expanded       bool
	Delta          agentosplan.PlanDelta
	Spec           agentos.RunPlanSpec
	Plan           agentosplan.ExecutablePlan
	ControlsByNode map[string][]agentos.ControlOperation
}

// EvaluatePlanExpansionActivity materializes and validates workflow-owned
// dynamic topology expansion after a child run publishes explicit plan_delta
// artifacts.
func (a *PlanActivities) EvaluatePlanExpansionActivity(ctx context.Context, input evaluatePlanExpansionInput) (evaluatePlanExpansionOutput, error) {
	if a.PlanDeltaProvider == nil {
		return evaluatePlanExpansionOutput{}, fmt.Errorf("%w: plan delta provider is required", agentos.ErrInvalidRunPlan)
	}
	delta, ok, err := a.PlanDeltaProvider.NextPlanDelta(ctx, agentosplan.PlanDeltaInput{
		Spec:           input.Spec,
		Status:         input.Status,
		Node:           input.Node,
		RunStatus:      input.RunStatus,
		Artifacts:      input.Artifacts,
		ExpansionCount: input.ExpansionCount,
	})
	if err != nil || !ok {
		return evaluatePlanExpansionOutput{Expanded: false}, err
	}

	nextSpec, plan, err := agentosplan.ApplyDelta(ctx, a.Validator, input.Spec, delta, input.ExpansionCount)
	if err != nil {
		return evaluatePlanExpansionOutput{}, err
	}
	controlsByNode, err := a.controlsByNode(ctx, plan)
	if err != nil {
		return evaluatePlanExpansionOutput{}, err
	}

	return evaluatePlanExpansionOutput{
		Expanded:       true,
		Delta:          delta,
		Spec:           nextSpec,
		Plan:           plan,
		ControlsByNode: controlsByNode,
	}, nil
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
