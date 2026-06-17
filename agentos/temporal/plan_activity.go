package temporal

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

// PlanActivities bridge Temporal PlanWorkflow decisions to AgentOS runtime calls.
type PlanActivities struct {
	Runtime   agentos.Runtime
	Validator agentosplan.Validator
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
	PlanID string
	Node   agentos.PlanNodeSpec
}

type startPlanNodeOutput struct {
	Status agentos.RunStatus
}

// StartPlanNodeActivity starts one backend-owned child run.
func (a *PlanActivities) StartPlanNodeActivity(ctx context.Context, input startPlanNodeInput) (startPlanNodeOutput, error) {
	if a.Runtime == nil {
		return startPlanNodeOutput{}, fmt.Errorf("%w: plan activity runtime is required", agentos.ErrInvalidRunPlan)
	}
	status, err := a.Runtime.Start(ctx, input.Node.Run)
	if err != nil {
		return startPlanNodeOutput{}, err
	}

	return startPlanNodeOutput{Status: status}, nil
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
	RunID     string
	Operation agentos.ControlOperation
}

// ControlPlanNodeActivity sends lifecycle control to one child run.
func (a *PlanActivities) ControlPlanNodeActivity(ctx context.Context, input controlPlanNodeInput) error {
	if a.Runtime == nil {
		return fmt.Errorf("%w: plan activity runtime is required", agentos.ErrInvalidRunPlan)
	}

	return a.Runtime.Control(ctx, input.RunID, input.Operation)
}
