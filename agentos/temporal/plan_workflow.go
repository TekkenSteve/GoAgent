package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	PlanWorkflowName             = "AgentOSPlanWorkflow"
	PlanStatusQueryName          = "agentos.plan.status"
	PlanSignalName               = "agentos.plan.signal"
	PlanControlSignalName        = "agentos.plan.control"
	ValidatePlanActivityName     = "AgentOSValidatePlan"
	StartPlanNodeActivityName    = "AgentOSStartPlanNode"
	StatusPlanNodeActivityName   = "AgentOSStatusPlanNode"
	ControlPlanNodeActivityName  = "AgentOSControlPlanNode"
	PersistPlanStateActivityName = "AgentOSPersistPlanState"
)

const planNodePollInterval = 5 * time.Second

// PlanWorkflow executes a cross-backend RunPlan using deterministic plan state
// and activity-backed backend I/O.
func PlanWorkflow(ctx workflow.Context, spec agentos.RunPlanSpec) (agentos.RunPlanStatus, error) {
	state := agentosplan.NewState(spec, workflow.Now(ctx))
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
	if err := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{Kind: agentosplan.EventPlanStarted}); err != nil {
		return state.Status, err
	}

	var validation validatePlanOutput
	if err := workflow.ExecuteActivity(activityCtx, ValidatePlanActivityName, validatePlanInput{Spec: spec}).Get(activityCtx, &validation); err != nil {
		persistErr := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error()})

		return state.Status, errors.Join(err, persistErr)
	}

	compiler, err := agentosplan.NewCELCompiler()
	if err != nil {
		persistErr := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error()})

		return state.Status, errors.Join(err, persistErr)
	}
	scheduler := agentosplan.Scheduler{Expressions: compiler}
	maxParallel := normalizePlanParallelism(spec.Policy.MaxParallelNodes)
	paused := false

	for {
		canceled, nextPaused, err := drainPlanControl(activityCtx, ctx, spec, controlCh, &state, paused, validation.ControlsByNode)
		paused = nextPaused
		if err != nil {
			persistErr := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error()})

			return state.Status, errors.Join(err, persistErr)
		}
		if canceled {
			return state.Status, nil
		}
		drainPlanSignals(signalCh)

		if planNodesTerminal(state.Status) {
			if err := applyTerminalPlanState(activityCtx, ctx, spec, &state); err != nil {
				return state.Status, err
			}

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
			persistErr := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error()})

			return state.Status, errors.Join(err, persistErr)
		}
		for _, skipped := range decision.Skipped {
			if !nodeSchedulable(state, skipped.NodeID) {
				continue
			}
			if err := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{
				Kind:   agentosplan.EventNodeSkipped,
				NodeID: skipped.NodeID,
				Reason: skipped.Reason,
			}); err != nil {
				return state.Status, err
			}
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
			if err := startPlanNode(activityCtx, ctx, spec, &state, node); err != nil {
				if persistErr := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, Reason: err.Error()}); persistErr != nil {
					return state.Status, errors.Join(err, persistErr)
				}
			}
			capacity--
			progressed = true
		}

		pollProgressed, err := pollRunningPlanNodes(activityCtx, ctx, spec, &state, validation.Plan.NodeByID)
		if err != nil {
			persistErr := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error()})

			return state.Status, errors.Join(err, persistErr)
		}
		progressed = progressed || pollProgressed

		if planNodesTerminal(state.Status) {
			if err := applyTerminalPlanState(activityCtx, ctx, spec, &state); err != nil {
				return state.Status, err
			}

			return state.Status, nil
		}
		if !progressed && runningNodeCount(state.Status) == 0 {
			reason := "plan has pending nodes but no runnable or active nodes"
			if err := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: reason}); err != nil {
				return state.Status, err
			}

			return state.Status, fmt.Errorf("%w: %s", agentos.ErrInvalidRunPlan, reason)
		}
		if !progressed {
			if err := workflow.Sleep(ctx, planNodePollInterval); err != nil {
				return state.Status, err
			}
		}
	}
}

func applyPlanStateEvent(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, state *agentosplan.State, event agentosplan.StateEvent) error {
	if event.At.IsZero() {
		event.At = workflow.Now(workflowCtx)
	}
	if err := state.Apply(event); err != nil {
		return err
	}
	planEvent, idempotencyKey, err := agentosplan.PlanEventFromStateEvent(spec, state.Status, event)
	if err != nil {
		return err
	}

	var persisted persistPlanStateOutput
	if err := workflow.ExecuteActivity(activityCtx, PersistPlanStateActivityName, persistPlanStateInput{
		Spec:           spec,
		Status:         state.Status,
		Event:          planEvent,
		IdempotencyKey: idempotencyKey,
	}).Get(activityCtx, &persisted); err != nil {
		return err
	}

	return nil
}

func startPlanNode(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, state *agentosplan.State, node agentos.PlanNodeSpec) error {
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeReady, NodeID: node.NodeID}); err != nil {
		return err
	}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeStarted, NodeID: node.NodeID, RunID: node.Run.RunID}); err != nil {
		return err
	}

	var started startPlanNodeOutput
	if err := workflow.ExecuteActivity(activityCtx, StartPlanNodeActivityName, startPlanNodeInput{PlanID: spec.PlanID, Node: node}).Get(activityCtx, &started); err != nil {
		return err
	}
	if started.Status.RunID != "" && started.Status.RunID != node.Run.RunID {
		if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeStarted, NodeID: node.NodeID, RunID: started.Status.RunID}); err != nil {
			return err
		}
	}
	if runTerminal(started.Status.LifecycleState) {
		return applyNodeRunTerminal(activityCtx, workflowCtx, spec, state, node, started.Status)
	}

	return nil
}

func pollRunningPlanNodes(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, state *agentosplan.State, nodes map[string]agentos.PlanNodeSpec) (bool, error) {
	progressed := false
	for _, node := range state.Status.Nodes {
		if node.LifecycleState != agentos.PlanNodeRunning {
			continue
		}
		if node.RunID == "" {
			if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeSucceeded, NodeID: node.NodeID}); err != nil {
				return progressed, err
			}
			progressed = true
			continue
		}

		var current statusPlanNodeOutput
		if err := workflow.ExecuteActivity(activityCtx, StatusPlanNodeActivityName, statusPlanNodeInput{RunID: node.RunID}).Get(activityCtx, &current); err != nil {
			if persistErr := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, Reason: err.Error()}); persistErr != nil {
				return true, persistErr
			}

			return true, nil
		}
		if runTerminal(current.Status.LifecycleState) {
			nodeSpec, ok := nodes[node.NodeID]
			if !ok {
				return progressed, fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, node.NodeID)
			}
			if err := applyNodeRunTerminal(activityCtx, workflowCtx, spec, state, nodeSpec, current.Status); err != nil {
				return progressed, err
			}
			progressed = true
		}
	}

	return progressed, nil
}

func applyNodeRunTerminal(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, state *agentosplan.State, node agentos.PlanNodeSpec, status agentos.RunStatus) error {
	artifacts, err := normalizeRunArtifacts(spec.PlanID, node, status)
	if err != nil {
		return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, RunID: status.RunID, Reason: err.Error()})

	}
	if len(artifacts) > 0 {
		if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventArtifactsPublished, NodeID: node.NodeID, RunID: status.RunID, Artifacts: artifacts}); err != nil {
			return err
		}
	}
	switch status.LifecycleState {
	case "completed", "succeeded":
		if err := validateRequiredArtifacts(node.Outputs, artifacts); err != nil {
			return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, RunID: status.RunID, Reason: err.Error()})

		}
		return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeSucceeded, NodeID: node.NodeID, RunID: status.RunID})
	case "failed":
		return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, RunID: status.RunID, Reason: status.Reason})
	case "canceled", "cancelled":
		return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeCanceled, NodeID: node.NodeID, RunID: status.RunID, Reason: status.Reason})
	}

	return nil
}

func drainPlanControl(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, ch workflow.ReceiveChannel, state *agentosplan.State, paused bool, controlsByNode map[string][]agentos.ControlOperation) (bool, bool, error) {
	var op agentos.ControlOperation
	for ch.ReceiveAsync(&op) {
		switch op {
		case agentos.ControlCancel:
			if err := controlActivePlanNodes(activityCtx, state.Status, op, controlsByNode); err != nil {
				return false, paused, err
			}
			for _, node := range state.Status.Nodes {
				if node.LifecycleState == agentos.PlanNodeRunning {
					if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeCanceled, NodeID: node.NodeID, Reason: "cancel requested"}); err != nil {
						return false, paused, err
					}
				}
			}
			if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanCanceled, Reason: "cancel requested"}); err != nil {
				return false, paused, err
			}

			return true, paused, nil
		case agentos.ControlPause:
			if err := controlActivePlanNodes(activityCtx, state.Status, op, controlsByNode); err != nil {
				if persistErr := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanBlocked, Reason: err.Error()}); persistErr != nil {
					return false, true, persistErr
				}

				return false, true, nil
			}
			if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanBlocked, Reason: "pause requested"}); err != nil {
				return false, paused, err
			}
			paused = true
		case agentos.ControlResume:
			if err := controlActivePlanNodes(activityCtx, state.Status, op, controlsByNode); err != nil {
				if persistErr := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanBlocked, Reason: err.Error()}); persistErr != nil {
					return false, true, persistErr
				}

				return false, true, nil
			}
			if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanStarted}); err != nil {
				return false, paused, err
			}
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

func applyTerminalPlanState(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, state *agentosplan.State) error {
	for _, node := range state.Status.Nodes {
		switch node.LifecycleState {
		case agentos.PlanNodeFailed:
			reason := node.Reason
			if reason == "" {
				reason = "node failed: " + node.NodeID
			}

			return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: reason})
		case agentos.PlanNodeCanceled:
			reason := node.Reason
			if reason == "" {
				reason = "node canceled: " + node.NodeID
			}

			return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanCanceled, Reason: reason})
		}
	}

	return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanSucceeded})
}
