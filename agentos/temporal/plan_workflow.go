package temporal

import (
	"context"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	PlanWorkflowName            = "AgentOSPlanWorkflow"
	PlanStatusQueryName         = "agentos.plan.status"
	PlanSignalName              = "agentos.plan.signal"
	PlanControlSignalName       = "agentos.plan.control"
	ValidatePlanActivityName    = "AgentOSValidatePlan"
	StartPlanNodeActivityName   = "AgentOSStartPlanNode"
	StatusPlanNodeActivityName  = "AgentOSStatusPlanNode"
	ControlPlanNodeActivityName = "AgentOSControlPlanNode"
)

const planNodePollInterval = 5 * time.Second

// PlanWorkflow executes a cross-backend RunPlan using deterministic plan state
// and activity-backed backend I/O.
func PlanWorkflow(ctx workflow.Context, spec agentos.RunPlanSpec) (agentos.RunPlanStatus, error) {
	state := agentosplan.NewState(spec, workflow.Now(ctx))
	_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanStarted, At: workflow.Now(ctx)})

	if err := workflow.SetQueryHandler(ctx, PlanStatusQueryName, func() (agentos.RunPlanStatus, error) {
		return state.Status, nil
	}); err != nil {
		return state.Status, err
	}

	controlCh := workflow.GetSignalChannel(ctx, PlanControlSignalName)
	signalCh := workflow.GetSignalChannel(ctx, PlanSignalName)

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

	var validation validatePlanOutput
	if err := workflow.ExecuteActivity(activityCtx, ValidatePlanActivityName, validatePlanInput{Spec: spec}).Get(activityCtx, &validation); err != nil {
		_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error(), At: workflow.Now(ctx)})

		return state.Status, err
	}

	compiler, err := agentosplan.NewCELCompiler()
	if err != nil {
		_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error(), At: workflow.Now(ctx)})

		return state.Status, err
	}
	scheduler := agentosplan.Scheduler{Expressions: compiler}
	maxParallel := normalizePlanParallelism(spec.Policy.MaxParallelNodes)
	paused := false

	for {
		canceled, nextPaused, err := drainPlanControl(activityCtx, ctx, controlCh, &state, paused, validation.ControlsByNode)
		paused = nextPaused
		if err != nil {
			_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error(), At: workflow.Now(ctx)})

			return state.Status, err
		}
		if canceled {
			return state.Status, nil
		}
		drainPlanSignals(signalCh)

		if planNodesTerminal(state.Status) {
			applyTerminalPlanState(ctx, &state)

			return state.Status, nil
		}
		if paused {
			if err := workflow.Sleep(ctx, planNodePollInterval); err != nil {
				return state.Status, err
			}
			continue
		}

		progressed := false
		decision, err := scheduler.Decide(context.Background(), validation.Plan, state.Status, agentosplan.Variables(spec, state.Status))
		if err != nil {
			_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error(), At: workflow.Now(ctx)})

			return state.Status, err
		}
		for _, skipped := range decision.Skipped {
			if !nodeSchedulable(state, skipped.NodeID) {
				continue
			}
			_ = state.Apply(agentosplan.StateEvent{
				Kind:   agentosplan.EventNodeSkipped,
				NodeID: skipped.NodeID,
				Reason: skipped.Reason,
				At:     workflow.Now(ctx),
			})
			progressed = true
		}

		capacity := maxParallel - runningNodeCount(state.Status)
		for _, node := range decision.Ready {
			if capacity <= 0 {
				break
			}
			if !nodeSchedulable(state, node.NodeID) {
				continue
			}
			if err := startPlanNode(activityCtx, ctx, &state, spec.PlanID, node); err != nil {
				_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, Reason: err.Error(), At: workflow.Now(ctx)})
			}
			capacity--
			progressed = true
		}

		pollProgressed, err := pollRunningPlanNodes(activityCtx, ctx, &state, spec.PlanID, validation.Plan.NodeByID)
		if err != nil {
			_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error(), At: workflow.Now(ctx)})

			return state.Status, err
		}
		progressed = progressed || pollProgressed

		if planNodesTerminal(state.Status) {
			applyTerminalPlanState(ctx, &state)

			return state.Status, nil
		}
		if !progressed && runningNodeCount(state.Status) == 0 {
			reason := "plan has pending nodes but no runnable or active nodes"
			_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: reason, At: workflow.Now(ctx)})

			return state.Status, fmt.Errorf("%w: %s", agentos.ErrInvalidRunPlan, reason)
		}
		if !progressed {
			if err := workflow.Sleep(ctx, planNodePollInterval); err != nil {
				return state.Status, err
			}
		}
	}
}

func startPlanNode(activityCtx workflow.Context, workflowCtx workflow.Context, state *agentosplan.State, planID string, node agentos.PlanNodeSpec) error {
	now := workflow.Now(workflowCtx)
	_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventNodeReady, NodeID: node.NodeID, At: now})
	_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventNodeStarted, NodeID: node.NodeID, RunID: node.Run.RunID, At: now})

	var started startPlanNodeOutput
	if err := workflow.ExecuteActivity(activityCtx, StartPlanNodeActivityName, startPlanNodeInput{PlanID: planID, Node: node}).Get(activityCtx, &started); err != nil {
		return err
	}
	if started.Status.RunID != "" {
		_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventNodeStarted, NodeID: node.NodeID, RunID: started.Status.RunID, At: workflow.Now(workflowCtx)})
	}
	if runTerminal(started.Status.LifecycleState) {
		applyNodeRunTerminal(workflowCtx, state, planID, node, started.Status)
	}

	return nil
}

func pollRunningPlanNodes(activityCtx workflow.Context, workflowCtx workflow.Context, state *agentosplan.State, planID string, nodes map[string]agentos.PlanNodeSpec) (bool, error) {
	progressed := false
	for _, node := range state.Status.Nodes {
		if node.LifecycleState != agentos.PlanNodeRunning {
			continue
		}
		if node.RunID == "" {
			_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventNodeSucceeded, NodeID: node.NodeID, At: workflow.Now(workflowCtx)})
			progressed = true
			continue
		}

		var current statusPlanNodeOutput
		if err := workflow.ExecuteActivity(activityCtx, StatusPlanNodeActivityName, statusPlanNodeInput{RunID: node.RunID}).Get(activityCtx, &current); err != nil {
			_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, Reason: err.Error(), At: workflow.Now(workflowCtx)})

			return true, nil
		}
		if runTerminal(current.Status.LifecycleState) {
			spec, ok := nodes[node.NodeID]
			if !ok {
				return progressed, fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, node.NodeID)
			}
			applyNodeRunTerminal(workflowCtx, state, planID, spec, current.Status)
			progressed = true
		}
	}

	return progressed, nil
}

func applyNodeRunTerminal(ctx workflow.Context, state *agentosplan.State, planID string, node agentos.PlanNodeSpec, status agentos.RunStatus) {
	artifacts, err := normalizeRunArtifacts(planID, node, status)
	if err != nil {
		_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, Reason: err.Error(), At: workflow.Now(ctx)})

		return
	}
	if len(artifacts) > 0 {
		_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventArtifactsPublished, NodeID: node.NodeID, Artifacts: artifacts, At: workflow.Now(ctx)})
	}
	switch status.LifecycleState {
	case "completed", "succeeded":
		if err := validateRequiredArtifacts(node.Outputs, artifacts); err != nil {
			_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, Reason: err.Error(), At: workflow.Now(ctx)})

			return
		}
		_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventNodeSucceeded, NodeID: node.NodeID, At: workflow.Now(ctx)})
	case "failed":
		_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, Reason: status.Reason, At: workflow.Now(ctx)})
	case "canceled", "cancelled":
		_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventNodeCanceled, NodeID: node.NodeID, Reason: status.Reason, At: workflow.Now(ctx)})
	}
}

func drainPlanControl(activityCtx workflow.Context, workflowCtx workflow.Context, ch workflow.ReceiveChannel, state *agentosplan.State, paused bool, controlsByNode map[string][]agentos.ControlOperation) (bool, bool, error) {
	var op agentos.ControlOperation
	for ch.ReceiveAsync(&op) {
		switch op {
		case agentos.ControlCancel:
			if err := controlActivePlanNodes(activityCtx, state.Status, op, controlsByNode); err != nil {
				return false, paused, err
			}
			for _, node := range state.Status.Nodes {
				if node.LifecycleState == agentos.PlanNodeRunning {
					_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventNodeCanceled, NodeID: node.NodeID, Reason: "cancel requested", At: workflow.Now(workflowCtx)})
				}
			}
			_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanCanceled, Reason: "cancel requested", At: workflow.Now(workflowCtx)})

			return true, paused, nil
		case agentos.ControlPause:
			if err := controlActivePlanNodes(activityCtx, state.Status, op, controlsByNode); err != nil {
				_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanBlocked, Reason: err.Error(), At: workflow.Now(workflowCtx)})

				return false, true, nil
			}
			_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanBlocked, Reason: "pause requested", At: workflow.Now(workflowCtx)})
			paused = true
		case agentos.ControlResume:
			if err := controlActivePlanNodes(activityCtx, state.Status, op, controlsByNode); err != nil {
				_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanBlocked, Reason: err.Error(), At: workflow.Now(workflowCtx)})

				return false, true, nil
			}
			_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanStarted, At: workflow.Now(workflowCtx)})
			paused = false
		default:
			return false, paused, fmt.Errorf("%w: %s", agentos.ErrInvalidControlOperation, op)
		}
	}

	return false, paused, nil
}

func controlActivePlanNodes(activityCtx workflow.Context, status agentos.RunPlanStatus, op agentos.ControlOperation, controlsByNode map[string][]agentos.ControlOperation) error {
	for _, node := range status.Nodes {
		if node.LifecycleState != agentos.PlanNodeRunning || node.RunID == "" {
			continue
		}
		if op != agentos.ControlCancel && !nodeSupportsControl(controlsByNode, node.NodeID, op) {
			return fmt.Errorf("%w: node %q does not declare support for %s", agentos.ErrInvalidControlOperation, node.NodeID, op)
		}
		if err := workflow.ExecuteActivity(activityCtx, ControlPlanNodeActivityName, controlPlanNodeInput{RunID: node.RunID, Operation: op}).Get(activityCtx, nil); err != nil {
			return err
		}
	}

	return nil
}

func nodeSupportsControl(controlsByNode map[string][]agentos.ControlOperation, nodeID string, op agentos.ControlOperation) bool {
	for _, supported := range controlsByNode[nodeID] {
		if supported == op {
			return true
		}
	}

	return false
}

func drainPlanSignals(ch workflow.ReceiveChannel) {
	var signal agentos.Signal
	for ch.ReceiveAsync(&signal) {
	}
}

func normalizePlanParallelism(maxParallel int32) int {
	if maxParallel <= 0 {
		return 32
	}

	return int(maxParallel)
}

func runningNodeCount(status agentos.RunPlanStatus) int {
	count := 0
	for _, node := range status.Nodes {
		if node.LifecycleState == agentos.PlanNodeRunning {
			count++
		}
	}

	return count
}

func nodeSchedulable(state agentosplan.State, nodeID string) bool {
	status, ok := state.NodeStatus(nodeID)
	if !ok {
		return false
	}

	return status.LifecycleState == agentos.PlanNodePending || status.LifecycleState == agentos.PlanNodeReady
}

func planNodesTerminal(status agentos.RunPlanStatus) bool {
	if len(status.Nodes) == 0 {
		return false
	}
	for _, node := range status.Nodes {
		if !planNodeTerminal(node.LifecycleState) {
			return false
		}
	}

	return true
}

func planNodeTerminal(lifecycle string) bool {
	switch lifecycle {
	case agentos.PlanNodeSucceeded, agentos.PlanNodeFailed, agentos.PlanNodeSkipped, agentos.PlanNodeCanceled:
		return true
	default:
		return false
	}
}

func runTerminal(lifecycle string) bool {
	switch lifecycle {
	case "completed", "succeeded", "failed", "canceled", "cancelled":
		return true
	default:
		return false
	}
}

func applyTerminalPlanState(ctx workflow.Context, state *agentosplan.State) {
	for _, node := range state.Status.Nodes {
		switch node.LifecycleState {
		case agentos.PlanNodeFailed:
			reason := node.Reason
			if reason == "" {
				reason = "node failed: " + node.NodeID
			}
			_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: reason, At: workflow.Now(ctx)})

			return
		case agentos.PlanNodeCanceled:
			reason := node.Reason
			if reason == "" {
				reason = "node canceled: " + node.NodeID
			}
			_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanCanceled, Reason: reason, At: workflow.Now(ctx)})

			return
		}
	}
	_ = state.Apply(agentosplan.StateEvent{Kind: agentosplan.EventPlanSucceeded, At: workflow.Now(ctx)})
}
