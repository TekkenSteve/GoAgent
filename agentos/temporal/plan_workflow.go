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
	PlanWorkflowName                 = "AgentOSPlanWorkflow"
	PlanStatusQueryName              = "agentos.plan.status"
	PlanSignalName                   = "agentos.plan.signal"
	PlanControlSignalName            = "agentos.plan.control"
	ValidatePlanActivityName         = "AgentOSValidatePlan"
	StartPlanNodeActivityName        = "AgentOSStartPlanNode"
	StatusPlanNodeActivityName       = "AgentOSStatusPlanNode"
	ControlPlanNodeActivityName      = "AgentOSControlPlanNode"
	PublishPlanArtifactsActivityName = "AgentOSPublishPlanArtifacts"
	PersistPlanStateActivityName     = "AgentOSPersistPlanState"
)

const planNodePollInterval = 5 * time.Second

type planWorkflowInput struct {
	Spec              agentos.RunPlanSpec   `json:"spec"`
	Status            agentos.RunPlanStatus `json:"status,omitempty"`
	Continued         bool                  `json:"continued,omitempty"`
	ContinuationCount int32                 `json:"continuation_count,omitempty"`
}

// PlanWorkflow executes a cross-backend RunPlan using deterministic plan state
// and activity-backed backend I/O.
func PlanWorkflow(ctx workflow.Context, input planWorkflowInput) (agentos.RunPlanStatus, error) {
	spec := input.Spec
	state, err := initialPlanWorkflowState(input, workflow.Now(ctx))
	if err != nil {
		return agentos.RunPlanStatus{}, err
	}
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
	if !input.Continued {
		if err := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{Kind: agentosplan.EventPlanStarted}); err != nil {
			return state.Status, err
		}
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
	paused := state.Status.LifecycleState == agentos.PlanLifecycleBlocked
	processedControls := make(map[string]bool)
	processedSignals := make(map[string]bool)

	for {
		canceled, nextPaused, err := drainPlanControl(activityCtx, ctx, spec, controlCh, &state, paused, validation.ControlsByNode, processedControls)
		paused = nextPaused
		if err != nil {
			persistErr := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error()})

			return state.Status, errors.Join(err, persistErr)
		}
		if canceled {
			return state.Status, nil
		}
		rejected, nextPaused, signalProgressed, err := drainPlanSignals(activityCtx, ctx, spec, signalCh, &state, paused, validation.Plan.NodeByID, processedSignals)
		paused = nextPaused
		if err != nil {
			persistErr := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error()})

			return state.Status, errors.Join(err, persistErr)
		}
		if rejected {
			return state.Status, nil
		}

		if planNodesTerminal(state.Status) {
			if err := applyTerminalPlanState(activityCtx, ctx, spec, &state); err != nil {
				return state.Status, err
			}

			return state.Status, nil
		}
		if paused {
			if err := continuePlanWorkflowIfNeeded(ctx, input, state); err != nil {
				return state.Status, err
			}
			if err := workflow.Sleep(ctx, planNodePollInterval); err != nil {
				return state.Status, err
			}
			continue
		}

		progressed := signalProgressed
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
			if err := startPlanNode(activityCtx, ctx, spec, &state, node, validation.Plan.EdgesByTo[node.NodeID]); err != nil {
				if persistErr := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, Reason: err.Error()}); persistErr != nil {
					return state.Status, errors.Join(err, persistErr)
				}
			}
			capacity--
			progressed = true
			budgetExceeded, err := applyPlanBudgetGuard(activityCtx, ctx, spec, &state, validation.ControlsByNode)
			if err != nil {
				return state.Status, err
			}
			if budgetExceeded {
				return state.Status, nil
			}
		}

		pollProgressed, err := pollRunningPlanNodes(activityCtx, ctx, spec, &state, validation.Plan.NodeByID)
		if err != nil {
			persistErr := applyPlanStateEvent(activityCtx, ctx, spec, &state, agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error()})

			return state.Status, errors.Join(err, persistErr)
		}
		progressed = progressed || pollProgressed

		budgetExceeded, err := applyPlanBudgetGuard(activityCtx, ctx, spec, &state, validation.ControlsByNode)
		if err != nil {
			return state.Status, err
		}
		if budgetExceeded {
			return state.Status, nil
		}

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
		if err := continuePlanWorkflowIfNeeded(ctx, input, state); err != nil {
			return state.Status, err
		}
		if !progressed {
			if err := workflow.Sleep(ctx, planNodePollInterval); err != nil {
				return state.Status, err
			}
		}
	}
}

func initialPlanWorkflowState(input planWorkflowInput, now time.Time) (agentosplan.State, error) {
	if input.Continued {
		return agentosplan.NewStateFromStatus(input.Spec, input.Status)
	}

	return agentosplan.NewState(input.Spec, now), nil
}

func continuePlanWorkflowIfNeeded(ctx workflow.Context, input planWorkflowInput, state agentosplan.State) error {
	if planTerminal(state.Status.LifecycleState) {
		return nil
	}
	decision := agentosplan.EvaluateContinuationPolicy(input.Spec.Policy, agentosplan.ContinuationSnapshot{
		AppliedTransitions: state.AppliedTransitions(),
	})
	if !decision.ShouldContinue {
		return nil
	}

	nextInput := input
	nextInput.Status = state.Status
	nextInput.Continued = true
	nextInput.ContinuationCount++

	return workflow.NewContinueAsNewError(ctx, PlanWorkflowName, nextInput)
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

func startPlanNode(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, state *agentosplan.State, node agentos.PlanNodeSpec, incomingEdges []agentos.PlanEdgeSpec) error {
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeReady, NodeID: node.NodeID}); err != nil {
		return err
	}
	attempt := nextNodeAttempt(*state, node.NodeID)
	attemptNode, err := nodeForAttempt(spec.PlanID, node, attempt)
	if err != nil {
		return err
	}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeStarted, NodeID: node.NodeID, RunID: attemptNode.Run.RunID, Attempt: attempt}); err != nil {
		return err
	}

	var started startPlanNodeOutput
	if err := workflow.ExecuteActivity(activityCtx, StartPlanNodeActivityName, startPlanNodeInput{
		PlanID:     spec.PlanID,
		PlanInputs: spec.Inputs,
		Status:     state.Status,
		Node:       attemptNode,
		Edges:      incomingEdges,
		Attempt:    attempt,
	}).Get(activityCtx, &started); err != nil {
		return applyNodeAttemptFailure(activityCtx, workflowCtx, spec, state, node, attemptNode.Run.RunID, fmt.Sprintf("start attempt %d failed: %s", attempt, err))
	}
	if started.Status.RunID != "" && started.Status.RunID != attemptNode.Run.RunID {
		if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeStarted, NodeID: node.NodeID, RunID: started.Status.RunID, Attempt: attempt}); err != nil {
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
	now := workflow.Now(workflowCtx)
	for _, node := range state.Status.Nodes {
		if node.LifecycleState != agentos.PlanNodeRunning {
			continue
		}
		nodeSpec, ok := nodes[node.NodeID]
		if !ok {
			return progressed, fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, node.NodeID)
		}
		if nodeTimedOut(now, node, nodeSpec) {
			if err := cancelTimedOutNode(activityCtx, spec.PlanID, node); err != nil {
				return progressed, err
			}
			reason := fmt.Sprintf("node timed out after %d seconds", nodeSpec.Policy.TimeoutSeconds)
			if err := applyNodeAttemptFailure(activityCtx, workflowCtx, spec, state, nodeSpec, node.RunID, reason); err != nil {
				return true, err
			}

			progressed = true
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
			if err := applyNodeRunTerminal(activityCtx, workflowCtx, spec, state, nodeSpec, current.Status); err != nil {
				return progressed, err
			}
			progressed = true
		}
	}

	return progressed, nil
}

func applyNodeRunTerminal(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, state *agentosplan.State, node agentos.PlanNodeSpec, status agentos.RunStatus) error {
	if err := applyRunBudgetUsage(activityCtx, workflowCtx, spec, state, node, status); err != nil {
		return err
	}

	var published publishPlanArtifactsOutput
	if err := workflow.ExecuteActivity(activityCtx, PublishPlanArtifactsActivityName, publishPlanArtifactsInput{
		PlanID: spec.PlanID,
		Node:   node,
		Status: status,
	}).Get(activityCtx, &published); err != nil {
		return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, RunID: status.RunID, Reason: err.Error()})

	}
	artifacts := published.Artifacts
	if len(artifacts) > 0 {
		if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventArtifactsPublished, NodeID: node.NodeID, RunID: status.RunID, Artifacts: artifacts}); err != nil {
			return err
		}
	}
	switch status.LifecycleState {
	case "completed", "succeeded":
		if err := validateRequiredArtifacts(node.Outputs, artifacts); err != nil {
			return applyNodeAttemptFailure(activityCtx, workflowCtx, spec, state, node, status.RunID, err.Error())

		}
		return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeSucceeded, NodeID: node.NodeID, RunID: status.RunID})
	case "failed":
		return applyNodeAttemptFailure(activityCtx, workflowCtx, spec, state, node, status.RunID, status.Reason)
	case "canceled", "cancelled":
		return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeCanceled, NodeID: node.NodeID, RunID: status.RunID, Reason: status.Reason})
	}

	return nil
}

func applyRunBudgetUsage(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, state *agentosplan.State, node agentos.PlanNodeSpec, status agentos.RunStatus) error {
	if status.BudgetUsage.SpentCents == 0 {
		return nil
	}
	if status.BudgetUsage.SpentCents < 0 {
		return fmt.Errorf("%w: run %q reported negative budget usage", agentos.ErrInvalidRunPlan, status.RunID)
	}

	return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{
		Kind:        agentosplan.EventBudgetReported,
		NodeID:      node.NodeID,
		RunID:       status.RunID,
		BudgetDelta: status.BudgetUsage,
	})
}

func applyNodeAttemptFailure(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, state *agentosplan.State, node agentos.PlanNodeSpec, runID string, reason string) error {
	if reason == "" {
		reason = "node attempt failed"
	}
	current, ok := state.NodeStatus(node.NodeID)
	if !ok {
		return fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, node.NodeID)
	}
	nextAttempt := current.Attempts + 1
	if current.Attempts < maxNodeAttempts(node.Policy) {
		return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{
			Kind:    agentosplan.EventNodeRetryScheduled,
			NodeID:  node.NodeID,
			RunID:   runID,
			Reason:  reason,
			Attempt: nextAttempt,
		})
	}

	return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{
		Kind:   agentosplan.EventNodeFailed,
		NodeID: node.NodeID,
		RunID:  runID,
		Reason: reason,
	})
}

func cancelTimedOutNode(activityCtx workflow.Context, planID string, node agentos.PlanNodeStatus) error {
	if node.RunID == "" {
		return fmt.Errorf("%w: timed out node %q has no active run id", agentos.ErrInvalidRunPlan, node.NodeID)
	}
	key, err := agentosplan.NodeTimeoutControlIdempotencyKey(planID, node.NodeID, node.RunID)
	if err != nil {
		return err
	}
	control := agentos.ControlRequest{
		Operation:      agentos.ControlCancel,
		IdempotencyKey: key,
	}
	if err := workflow.ExecuteActivity(activityCtx, ControlPlanNodeActivityName, controlPlanNodeInput{RunID: node.RunID, Control: control}).Get(activityCtx, nil); err != nil {
		return err
	}

	return nil
}

func applyPlanBudgetGuard(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, state *agentosplan.State, controlsByNode map[string][]agentos.ControlOperation) (bool, error) {
	if !agentosplan.BudgetExceeded(spec.Policy, state.Status.BudgetUsage) {
		return false, nil
	}

	key, err := agentosplan.BudgetExceededControlIdempotencyKey(spec.PlanID, state.Status.BudgetUsage, spec.Policy.BudgetCents)
	if err != nil {
		return false, err
	}
	control := agentos.ControlRequest{
		Operation:      agentos.ControlCancel,
		IdempotencyKey: key,
	}
	if err := controlActivePlanNodes(activityCtx, spec.PlanID, state.Status, control, controlsByNode); err != nil {
		return false, err
	}
	reason := agentosplan.BudgetExceededReason(spec.Policy, state.Status.BudgetUsage)
	for _, node := range state.Status.Nodes {
		if node.LifecycleState != agentos.PlanNodeRunning {
			continue
		}
		if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventNodeCanceled, NodeID: node.NodeID, RunID: node.RunID, Reason: reason}); err != nil {
			return false, err
		}
	}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: reason}); err != nil {
		return false, err
	}

	return true, nil
}

func nodeTimedOut(now time.Time, status agentos.PlanNodeStatus, node agentos.PlanNodeSpec) bool {
	if status.LifecycleState != agentos.PlanNodeRunning || status.StartedAt.IsZero() || node.Policy.TimeoutSeconds <= 0 {
		return false
	}

	return !now.Before(status.StartedAt.Add(time.Duration(node.Policy.TimeoutSeconds) * time.Second))
}

func nextNodeAttempt(state agentosplan.State, nodeID string) int32 {
	status, ok := state.NodeStatus(nodeID)
	if !ok {
		return 1
	}

	return status.Attempts + 1
}

func nodeForAttempt(planID string, node agentos.PlanNodeSpec, attempt int32) (agentos.PlanNodeSpec, error) {
	if attempt <= 0 {
		return agentos.PlanNodeSpec{}, fmt.Errorf("%w: node start attempt must be positive", agentos.ErrInvalidRunPlan)
	}
	node.Run.RunID = nodeRunIDForAttempt(node.Run.RunID, attempt)
	key, err := agentosplan.NodeStartIdempotencyKey(planID, node.NodeID, attempt)
	if err != nil {
		return agentos.PlanNodeSpec{}, err
	}
	node.Run.IdempotencyKey = key

	return node, nil
}

func nodeRunIDForAttempt(baseRunID string, attempt int32) string {
	if attempt <= 1 {
		return baseRunID
	}

	return fmt.Sprintf("%s-attempt-%d", baseRunID, attempt)
}

func maxNodeAttempts(policy agentos.NodePolicy) int32 {
	if policy.MaxAttempts <= 0 {
		return 1
	}

	return policy.MaxAttempts
}

func drainPlanControl(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, ch workflow.ReceiveChannel, state *agentosplan.State, paused bool, controlsByNode map[string][]agentos.ControlOperation, processed map[string]bool) (bool, bool, error) {
	var control agentos.ControlRequest
	for ch.ReceiveAsync(&control) {
		if err := agentos.ValidateControlRequest(control); err != nil {
			return false, paused, err
		}
		if control.IdempotencyKey == "" {
			return false, paused, fmt.Errorf("%w: control idempotency key is required", agentos.ErrInvalidControlOperation)
		}
		if processed[control.IdempotencyKey] {
			continue
		}
		processed[control.IdempotencyKey] = true
		switch control.Operation {
		case agentos.ControlCancel:
			if err := controlActivePlanNodes(activityCtx, spec.PlanID, state.Status, control, controlsByNode); err != nil {
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
			if err := controlActivePlanNodes(activityCtx, spec.PlanID, state.Status, control, controlsByNode); err != nil {
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
			if err := controlActivePlanNodes(activityCtx, spec.PlanID, state.Status, control, controlsByNode); err != nil {
				if persistErr := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanBlocked, Reason: err.Error()}); persistErr != nil {
					return false, true, persistErr
				}

				return false, true, nil
			}
			if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanStarted}); err != nil {
				return false, paused, err
			}
			paused = false
		}
	}

	return false, paused, nil
}

func controlActivePlanNodes(activityCtx workflow.Context, planID string, status agentos.RunPlanStatus, control agentos.ControlRequest, controlsByNode map[string][]agentos.ControlOperation) error {
	for _, node := range status.Nodes {
		if node.LifecycleState != agentos.PlanNodeRunning || node.RunID == "" {
			continue
		}
		if control.Operation != agentos.ControlCancel && !nodeSupportsControl(controlsByNode, node.NodeID, control.Operation) {
			return fmt.Errorf("%w: node %q does not declare support for %s", agentos.ErrInvalidControlOperation, node.NodeID, control.Operation)
		}
		childControl := control
		key, err := agentosplan.NodeControlIdempotencyKey(planID, node.NodeID, control.Operation, control.IdempotencyKey)
		if err != nil {
			return err
		}
		childControl.IdempotencyKey = key
		if err := workflow.ExecuteActivity(activityCtx, ControlPlanNodeActivityName, controlPlanNodeInput{RunID: node.RunID, Control: childControl}).Get(activityCtx, nil); err != nil {
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

func drainPlanSignals(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, ch workflow.ReceiveChannel, state *agentosplan.State, paused bool, nodes map[string]agentos.PlanNodeSpec, processed map[string]bool) (bool, bool, bool, error) {
	var signal agentos.Signal
	progressed := false
	for ch.ReceiveAsync(&signal) {
		if err := agentosplan.ValidatePlanSignal(signal); err != nil {
			return false, paused, progressed, err
		}
		if processed[signal.IdempotencyKey] {
			continue
		}
		processed[signal.IdempotencyKey] = true
		switch signal.Type {
		case agentos.SignalPlanNodeRetry:
			if err := applyManualNodeRetry(activityCtx, workflowCtx, spec, state, nodes, signal); err != nil {
				return false, paused, progressed, err
			}
			paused = false
			progressed = true
		case agentos.SignalPlanApprove:
			if state.Status.LifecycleState != agentos.PlanLifecycleBlocked {
				return false, paused, progressed, fmt.Errorf("%w: approve requires blocked plan state", agentos.ErrInvalidSignal)
			}
			reason := agentosplan.PlanSignalReason(signal)
			if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanApproved, Reason: reason}); err != nil {
				return false, paused, progressed, err
			}
			paused = false
			progressed = true
		case agentos.SignalPlanReject:
			reason := agentosplan.PlanSignalReason(signal)
			if reason == "" {
				reason = "plan rejected"
			}
			if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanRejected, Reason: reason}); err != nil {
				return false, paused, progressed, err
			}

			return true, paused, true, nil
		}
	}

	return false, paused, progressed, nil
}

func applyManualNodeRetry(activityCtx workflow.Context, workflowCtx workflow.Context, spec agentos.RunPlanSpec, state *agentosplan.State, nodes map[string]agentos.PlanNodeSpec, signal agentos.Signal) error {
	nodeID, err := agentosplan.PlanSignalNodeID(signal)
	if err != nil {
		return err
	}
	nodeSpec, ok := nodes[nodeID]
	if !ok {
		return fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, nodeID)
	}
	current, ok := state.NodeStatus(nodeID)
	if !ok {
		return fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, nodeID)
	}
	if current.LifecycleState != agentos.PlanNodeFailed {
		return fmt.Errorf("%w: node %q is %s, not failed", agentos.ErrInvalidSignal, nodeID, current.LifecycleState)
	}
	reason := agentosplan.PlanSignalReason(signal)
	if reason == "" {
		reason = "manual retry requested"
	}
	if state.Status.LifecycleState == agentos.PlanLifecycleBlocked || state.Status.LifecycleState == agentos.PlanLifecycleFailed {
		if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{Kind: agentosplan.EventPlanApproved, Reason: reason}); err != nil {
			return err
		}
	}
	nextAttempt := current.Attempts + 1
	if nextAttempt <= 0 {
		nextAttempt = 1
	}

	return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, agentosplan.StateEvent{
		Kind:    agentosplan.EventNodeRetryScheduled,
		NodeID:  nodeSpec.NodeID,
		RunID:   current.RunID,
		Reason:  reason,
		Attempt: nextAttempt,
	})
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

func planTerminal(lifecycle string) bool {
	switch lifecycle {
	case agentos.PlanLifecycleSucceeded, agentos.PlanLifecycleFailed, agentos.PlanLifecycleCanceled:
		return true
	default:
		return false
	}
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
