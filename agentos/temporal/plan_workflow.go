package temporal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	PlanWorkflowName                  = "AgentOSPlanWorkflow"
	PlanStatusQueryName               = "agentos.plan.status"
	PlanSignalName                    = "agentos.plan.signal"
	PlanControlSignalName             = "agentos.plan.control"
	ValidatePlanActivityName          = "AgentOSValidatePlan"
	ResolvePlanNodeInputActivityName  = "AgentOSResolvePlanNodeInput"
	StartPlanNodeActivityName         = "AgentOSStartPlanNode"
	StatusPlanNodeActivityName        = "AgentOSStatusPlanNode"
	ControlPlanNodeActivityName       = "AgentOSControlPlanNode"
	PublishPlanArtifactsActivityName  = "AgentOSPublishPlanArtifacts"
	EvaluatePlanExpansionActivityName = "AgentOSEvaluatePlanExpansion"
	PersistPlanStateActivityName      = "AgentOSPersistPlanState"
)

const (
	planNodePollInterval       = 5 * time.Second
	defaultActivityTimeout     = 5 * time.Minute
	defaultActivityMaxAttempts = 3
	defaultPlanParallelism     = 32
	FAILED                     = "failed"
	CANCELED                   = "canceled"
)

type planWorkflowInput struct {
	Spec              agentos.RunPlanSpec   `json:"spec"`
	Status            agentos.RunPlanStatus `json:"status"`
	TaskQueues        agentfwTaskQueues     `json:"task_queues"`
	Continued         bool                  `json:"continued,omitempty"`
	ContinuationCount int32                 `json:"continuation_count,omitempty"`
	IterationCount    int32                 `json:"iteration_count,omitempty"`
	ExpansionCount    int32                 `json:"expansion_count,omitempty"`
	ProcessedControls []string              `json:"processed_controls,omitempty"`
	ProcessedSignals  []string              `json:"processed_signals,omitempty"`
}

type agentfwTaskQueues struct {
	PlanActivity string `json:"plan_activity"`
}

// PlanWorkflow executes a cross-backend RunPlan using deterministic plan state
// and activity-backed backend I/O.
func PlanWorkflow(ctx workflow.Context, input *planWorkflowInput) (agentos.RunPlanStatus, error) {
	setup, err := newPlanWorkflowSetup(ctx, input)
	if err != nil {
		return agentos.RunPlanStatus{}, err
	}

	if err := workflow.SetQueryHandler(ctx, PlanStatusQueryName, func() (agentos.RunPlanStatus, error) {
		return setup.State.Status, nil
	}); err != nil {
		return setup.State.Status, err
	}

	controlCh := workflow.GetSignalChannel(ctx, PlanControlSignalName)
	signalCh := workflow.GetSignalChannel(ctx, PlanSignalName)
	activityCtx := newPlanWorkflowActivityContext(ctx, input.TaskQueues.PlanActivity)

	if !input.Continued {
		evt1 := agentosplan.StateEvent{Kind: agentosplan.EventPlanStarted}
		if err := applyPlanStateEvent(activityCtx, ctx, setup.Spec, &setup.State, &evt1); err != nil {
			return setup.State.Status, err
		}
	}

	var validation ValidatePlanOutput
	if err := workflow.ExecuteActivity(activityCtx, ValidatePlanActivityName, validatePlanInput{Spec: *setup.Spec}).Get(activityCtx, &validation); err != nil {
		evt2 := agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error()}

		return setup.State.Status, errors.Join(err, applyPlanStateEvent(activityCtx, ctx, setup.Spec, &setup.State, &evt2))
	}

	compiler, err := agentosplan.NewCELCompiler()
	if err != nil {
		evt3 := agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: err.Error()}

		return setup.State.Status, errors.Join(err, applyPlanStateEvent(activityCtx, ctx, setup.Spec, &setup.State, &evt3))
	}

	expansionCount := input.ExpansionCount
	iterationCount := input.IterationCount
	paused := setup.State.Status.LifecycleState == agentos.PlanLifecycleBlocked
	runtime := newPlanWorkflowRuntime(activityCtx, ctx, controlCh, signalCh, setup.Spec, input, compiler)

	for {
		iterationCount++
		result, err := runPlanWorkflowIteration(&runtime, input, setup.Spec, &setup.State, &validation, &expansionCount, iterationCount, paused)
		paused = result.Paused

		if err != nil {
			return setup.State.Status, err
		}

		if result.Done {
			return setup.State.Status, nil
		}
	}
}

type planWorkflowSetup struct {
	Spec  *agentos.RunPlanSpec
	State agentosplan.State
}

func newPlanWorkflowSetup(ctx workflow.Context, input *planWorkflowInput) (planWorkflowSetup, error) {
	if input == nil {
		return planWorkflowSetup{}, temporal.NewNonRetryableApplicationError("PlanWorkflow input is required", "validation", nil)
	}

	if input.TaskQueues.PlanActivity == "" {
		return planWorkflowSetup{}, temporal.NewNonRetryableApplicationError("PlanWorkflow plan activity task queue is required", "validation", nil)
	}

	state, err := initialPlanWorkflowState(input, workflow.Now(ctx))
	if err != nil {
		return planWorkflowSetup{}, err
	}

	return planWorkflowSetup{
		Spec:  &input.Spec,
		State: state,
	}, nil
}

func newPlanWorkflowActivityContext(ctx workflow.Context, taskQueue string) workflow.Context {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: defaultActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: defaultActivityMaxAttempts,
		},
	})

	return workflow.WithTaskQueue(activityCtx, taskQueue)
}

type planWorkflowRuntime struct {
	ActivityCtx       workflow.Context
	WorkflowCtx       workflow.Context
	ControlCh         workflow.ReceiveChannel
	SignalCh          workflow.ReceiveChannel
	Scheduler         agentosplan.Scheduler
	MaxParallel       int
	ProcessedControls map[string]bool
	ProcessedSignals  map[string]bool
}

type planWorkflowIterationResult struct {
	Done   bool
	Paused bool
}

type planWorkflowPreScheduleResult struct {
	Done         bool
	SkipSchedule bool
}

func newPlanWorkflowRuntime(activityCtx, workflowCtx workflow.Context, controlCh, signalCh workflow.ReceiveChannel, spec *agentos.RunPlanSpec, input *planWorkflowInput, compiler agentosplan.ExpressionCompiler) planWorkflowRuntime {
	return planWorkflowRuntime{
		ActivityCtx:       activityCtx,
		WorkflowCtx:       workflowCtx,
		ControlCh:         controlCh,
		SignalCh:          signalCh,
		Scheduler:         agentosplan.Scheduler{Expressions: compiler},
		MaxParallel:       normalizePlanParallelism(spec.Policy.MaxParallelNodes),
		ProcessedControls: processedPlanWorkflowKeys(input.ProcessedControls),
		ProcessedSignals:  processedPlanWorkflowKeys(input.ProcessedSignals),
	}
}

func runPlanWorkflowIteration(
	runtime *planWorkflowRuntime,
	input *planWorkflowInput,
	spec *agentos.RunPlanSpec,
	state *agentosplan.State,
	validation *ValidatePlanOutput,
	expansionCount *int32,
	iterationCount int32,
	paused bool,
) (planWorkflowIterationResult, error) {
	result := planWorkflowIterationResult{Paused: paused}

	if err := applyPlanIterationGuard(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, iterationCount); err != nil {
		return result, err
	}

	if done, err := applyPlanWorkflowControls(runtime, spec, state, validation, &result); done {
		return result, err
	}

	signalProgressed, err := applyPlanWorkflowSignals(runtime, spec, state, validation, &result)
	if workflowIterationDone(&result, err) {
		return result, err
	}

	if stop, err := applyPlanWorkflowPreSchedule(runtime, input, spec, state, validation, expansionCount, iterationCount, &result); stop {
		return result, err
	}

	progressed, err := applyPlanWorkflowSchedule(runtime, spec, state, validation, expansionCount, signalProgressed, &result)
	if workflowIterationDone(&result, err) {
		return result, err
	}

	return finishPlanWorkflowIteration(runtime, input, spec, state, expansionCount, iterationCount, progressed, &result)
}

func workflowIterationDone(result *planWorkflowIterationResult, err error) bool {
	return err != nil || result.Done
}

func applyPlanWorkflowControls(
	runtime *planWorkflowRuntime,
	spec *agentos.RunPlanSpec,
	state *agentosplan.State,
	validation *ValidatePlanOutput,
	result *planWorkflowIterationResult,
) (bool, error) {
	canceled, paused, err := drainPlanControl(runtime.ActivityCtx, runtime.WorkflowCtx, spec, runtime.ControlCh, state, result.Paused, validation.ControlsByNode, runtime.ProcessedControls)
	result.Paused = paused

	if err != nil {
		return true, failPlanWorkflowIteration(runtime, spec, state, err)
	}

	result.Done = canceled

	return canceled, nil
}

func applyPlanWorkflowSignals(
	runtime *planWorkflowRuntime,
	spec *agentos.RunPlanSpec,
	state *agentosplan.State,
	validation *ValidatePlanOutput,
	result *planWorkflowIterationResult,
) (bool, error) {
	rejected, paused, signalProgressed, err := drainPlanSignals(runtime.ActivityCtx, runtime.WorkflowCtx, spec, runtime.SignalCh, state, result.Paused, validation.Plan.NodeByID, validation.ControlsByNode, runtime.ProcessedSignals)
	result.Paused = paused

	if err != nil {
		return false, failPlanWorkflowIteration(runtime, spec, state, err)
	}

	result.Done = rejected

	return signalProgressed, nil
}

func applyPlanWorkflowPreSchedule(
	runtime *planWorkflowRuntime,
	input *planWorkflowInput,
	spec *agentos.RunPlanSpec,
	state *agentosplan.State,
	validation *ValidatePlanOutput,
	expansionCount *int32,
	iterationCount int32,
	result *planWorkflowIterationResult,
) (bool, error) {
	guard, err := applyPlanWorkflowPreScheduleGuards(runtime, input, spec, state, validation, expansionCount, iterationCount, result.Paused)
	result.Done = guard.Done

	return err != nil || guard.Done || guard.SkipSchedule, err
}

func applyPlanWorkflowSchedule(
	runtime *planWorkflowRuntime,
	spec *agentos.RunPlanSpec,
	state *agentosplan.State,
	validation *ValidatePlanOutput,
	expansionCount *int32,
	progressed bool,
	result *planWorkflowIterationResult,
) (bool, error) {
	progressed, done, err := schedulePlanWorkflowNodes(runtime, spec, state, validation, expansionCount, progressed)
	result.Done = done

	return progressed, err
}

func failPlanWorkflowIteration(runtime *planWorkflowRuntime, spec *agentos.RunPlanSpec, state *agentosplan.State, cause error) error {
	evt4 := agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: cause.Error()}
	persistErr := applyPlanStateEvent(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, &evt4)

	return errors.Join(cause, persistErr)
}

func applyPlanWorkflowPreScheduleGuards(
	runtime *planWorkflowRuntime,
	input *planWorkflowInput,
	spec *agentos.RunPlanSpec,
	state *agentosplan.State,
	validation *ValidatePlanOutput,
	expansionCount *int32,
	iterationCount int32,
	paused bool,
) (planWorkflowPreScheduleResult, error) {
	timedOut, err := applyPlanTimeoutGuard(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, validation.ControlsByNode)
	if err != nil || timedOut {
		return planWorkflowPreScheduleResult{Done: timedOut}, err
	}

	if planNodesTerminal(&state.Status) {
		return planWorkflowPreScheduleResult{Done: true}, applyTerminalPlanState(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state)
	}

	if !paused {
		return planWorkflowPreScheduleResult{}, nil
	}

	if err := continuePlanWorkflowIfNeeded(runtime.WorkflowCtx, input, spec, state, *expansionCount, iterationCount, runtime.ProcessedControls, runtime.ProcessedSignals); err != nil {
		return planWorkflowPreScheduleResult{}, err
	}

	return planWorkflowPreScheduleResult{SkipSchedule: true}, workflow.Sleep(runtime.WorkflowCtx, planNodePollInterval)
}

func schedulePlanWorkflowNodes(
	runtime *planWorkflowRuntime,
	spec *agentos.RunPlanSpec,
	state *agentosplan.State,
	validation *ValidatePlanOutput,
	expansionCount *int32,
	progressed bool,
) (progressedOut, done bool, err error) {
	decision, err := runtime.Scheduler.Decide(context.Background(), &validation.Plan, &state.Status, agentosplan.Variables(spec, &state.Status))
	if err != nil {
		return progressed, false, failPlanWorkflowIteration(runtime, spec, state, err)
	}

	if err := applyPlanSchedulerDecision(runtime, spec, state, validation, expansionCount, decision, &progressed); err != nil {
		return progressed, false, err
	}

	pollProgressed, err := pollRunningPlanNodes(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, validation, expansionCount)
	if err != nil {
		return progressed || pollProgressed, false, failPlanWorkflowIteration(runtime, spec, state, err)
	}

	progressed = progressed || pollProgressed

	budgetExceeded, err := applyPlanBudgetGuard(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, validation.ControlsByNode)
	if err != nil || budgetExceeded {
		return progressed, budgetExceeded, err
	}

	if planNodesTerminal(&state.Status) {
		return progressed, true, applyTerminalPlanState(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state)
	}

	return progressed, false, nil
}

func applyPlanSchedulerDecision(
	runtime *planWorkflowRuntime,
	spec *agentos.RunPlanSpec,
	state *agentosplan.State,
	validation *ValidatePlanOutput,
	expansionCount *int32,
	decision agentosplan.SchedulerDecision,
	progressed *bool,
) error {
	if traces := conditionTracesForDecision(decision); len(traces) > 0 {
		evt5 := agentosplan.StateEvent{Kind: agentosplan.EventConditionsEvaluated, ConditionTraces: traces}
		if err := applyPlanStateEvent(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, &evt5); err != nil {
			return err
		}
	}

	if err := applySkippedPlanNodes(runtime, spec, state, decision, progressed); err != nil {
		return err
	}

	return startReadyPlanNodes(runtime, spec, state, validation, expansionCount, decision, progressed)
}

func applySkippedPlanNodes(runtime *planWorkflowRuntime, spec *agentos.RunPlanSpec, state *agentosplan.State, decision agentosplan.SchedulerDecision, progressed *bool) error {
	for _, skipped := range decision.Skipped {
		if !nodeSchedulable(state, skipped.NodeID) {
			continue
		}

		evt6 := agentosplan.StateEvent{
			Kind:   agentosplan.EventNodeSkipped,
			NodeID: skipped.NodeID,
			Reason: skipped.Reason,
		}
		if err := applyPlanStateEvent(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, &evt6); err != nil {
			return err
		}

		*progressed = true
	}

	return nil
}

func startReadyPlanNodes(
	runtime *planWorkflowRuntime,
	spec *agentos.RunPlanSpec,
	state *agentosplan.State,
	validation *ValidatePlanOutput,
	expansionCount *int32,
	decision agentosplan.SchedulerDecision,
	progressed *bool,
) error {
	capacity := runtime.MaxParallel - runningNodeCount(&state.Status)
	for i := range decision.Ready {
		if capacity <= 0 {
			return nil
		}

		if !nodeSchedulable(state, decision.Ready[i].NodeID) {
			continue
		}

		if err := startPlanNode(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, validation, expansionCount, &decision.Ready[i], validation.Plan.EdgesByTo[decision.Ready[i].NodeID]); err != nil {
			evt7 := agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: decision.Ready[i].NodeID, Reason: err.Error()}
			if persistErr := applyPlanStateEvent(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, &evt7); persistErr != nil {
				return errors.Join(err, persistErr)
			}
		}

		capacity--
		*progressed = true

		budgetExceeded, err := applyPlanBudgetGuard(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, validation.ControlsByNode)
		if err != nil || budgetExceeded {
			return err
		}
	}

	return nil
}

func finishPlanWorkflowIteration(
	runtime *planWorkflowRuntime,
	input *planWorkflowInput,
	spec *agentos.RunPlanSpec,
	state *agentosplan.State,
	expansionCount *int32,
	iterationCount int32,
	progressed bool,
	result *planWorkflowIterationResult,
) (planWorkflowIterationResult, error) {
	if !progressed && runningNodeCount(&state.Status) == 0 {
		reason := "plan has pending nodes but no runnable or active nodes"
		evt8 := agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: reason}

		if err := applyPlanStateEvent(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, &evt8); err != nil {
			return *result, err
		}

		return *result, fmt.Errorf("%w: %s", agentos.ErrInvalidRunPlan, reason)
	}

	if err := continuePlanWorkflowIfNeeded(runtime.WorkflowCtx, input, spec, state, *expansionCount, iterationCount, runtime.ProcessedControls, runtime.ProcessedSignals); err != nil {
		return *result, err
	}

	if progressed {
		return *result, nil
	}

	return *result, workflow.Sleep(runtime.WorkflowCtx, planNodePollInterval)
}

func initialPlanWorkflowState(input *planWorkflowInput, now time.Time) (agentosplan.State, error) {
	if input.Continued {
		return agentosplan.NewStateFromStatus(&input.Spec, &input.Status)
	}

	return agentosplan.NewState(&input.Spec, now), nil
}

func continuePlanWorkflowIfNeeded(ctx workflow.Context, input *planWorkflowInput, spec *agentos.RunPlanSpec, state *agentosplan.State, expansionCount, iterationCount int32, processedControls, processedSignals map[string]bool) error {
	if planTerminal(state.Status.LifecycleState) {
		return nil
	}

	decision := agentosplan.EvaluateContinuationPolicy(spec.Policy, agentosplan.ContinuationSnapshot{
		AppliedTransitions: state.AppliedTransitions(),
		HistoryEvents:      workflow.GetInfo(ctx).GetCurrentHistoryLength(),
	})
	if !decision.ShouldContinue {
		return nil
	}

	nextInput := continuedPlanWorkflowInput(input, spec, state, expansionCount, iterationCount, processedControls, processedSignals)

	return workflow.NewContinueAsNewError(ctx, PlanWorkflowName, nextInput)
}

func continuedPlanWorkflowInput(input *planWorkflowInput, spec *agentos.RunPlanSpec, state *agentosplan.State, expansionCount, iterationCount int32, processedControls, processedSignals map[string]bool) planWorkflowInput {
	nextInput := *input
	nextInput.Spec = *spec
	nextInput.Status = state.Status
	nextInput.Continued = true
	nextInput.ContinuationCount++
	nextInput.IterationCount = iterationCount
	nextInput.ExpansionCount = expansionCount
	nextInput.ProcessedControls = sortedPlanWorkflowKeys(processedControls)
	nextInput.ProcessedSignals = sortedPlanWorkflowKeys(processedSignals)

	return nextInput
}

func processedPlanWorkflowKeys(keys []string) map[string]bool {
	processed := make(map[string]bool, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}

		processed[key] = true
	}

	return processed
}

func sortedPlanWorkflowKeys(processed map[string]bool) []string {
	if len(processed) == 0 {
		return nil
	}

	keys := make([]string, 0, len(processed))
	for key, ok := range processed {
		if key == "" || !ok {
			continue
		}

		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

func applyPlanIterationGuard(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, iterationCount int32) error {
	decision := agentosplan.EvaluateIterationPolicy(spec.Policy, agentosplan.IterationSnapshot{
		Iterations: iterationCount,
	})
	if !decision.ShouldFail {
		return nil
	}

	reason := fmt.Sprintf("plan exceeded max iterations: %d exceeds max %d", iterationCount, spec.Policy.MaxIterations)
	evt9 := agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: reason}
	persistErr := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt9)

	return errors.Join(fmt.Errorf("%w: %s", agentos.ErrInvalidRunPlan, reason), persistErr)
}

func applyPlanStateEvent(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, event *agentosplan.StateEvent) error {
	if event.At.IsZero() {
		event.At = workflow.Now(workflowCtx)
	}

	event.PreviousLifecycleState = lifecycleForEvent(&state.Status, event)
	if err := state.Apply(event); err != nil {
		return err
	}

	event.NextLifecycleState = lifecycleForEvent(&state.Status, event)

	planEvent, idempotencyKey, err := agentosplan.PlanEventFromStateEvent(spec, &state.Status, event)
	if err != nil {
		return err
	}

	var persisted PersistPlanStateOutput
	if err := workflow.ExecuteActivity(activityCtx, PersistPlanStateActivityName, persistPlanStateInput{
		Spec:           *spec,
		Status:         state.Status,
		Event:          planEvent,
		IdempotencyKey: idempotencyKey,
	}).Get(activityCtx, &persisted); err != nil {
		return err
	}

	return nil
}

func lifecycleForEvent(status *agentos.RunPlanStatus, event *agentosplan.StateEvent) string {
	if event.NodeID == "" {
		return status.LifecycleState
	}

	for i := range status.Nodes {
		if status.Nodes[i].NodeID == event.NodeID {
			return status.Nodes[i].LifecycleState
		}
	}

	return ""
}

func startPlanNode(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32, node *agentos.PlanNodeSpec, incomingEdges []agentos.PlanEdgeSpec) error {
	if capability, ok := validation.CapabilitiesByNode[node.NodeID]; ok {
		evt10 := agentosplan.StateEvent{Kind: agentosplan.EventCapabilitySelected, NodeID: node.NodeID, Capability: capability}
		if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt10); err != nil {
			return err
		}
	}

	evt11 := agentosplan.StateEvent{Kind: agentosplan.EventNodeReady, NodeID: node.NodeID}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt11); err != nil {
		return err
	}

	attempt := nextNodeAttempt(state, node.NodeID)

	attemptNode, err := nodeForAttempt(spec.PlanID, node, attempt)
	if err != nil {
		return err
	}

	var resolved ResolvePlanNodeInputOutput
	if err := workflow.ExecuteActivity(activityCtx, ResolvePlanNodeInputActivityName, resolvePlanNodeInputInput{
		Spec:   *spec,
		Status: state.Status,
		Node:   attemptNode,
		Edges:  incomingEdges,
	}).Get(activityCtx, &resolved); err != nil {
		return applyNodeAttemptFailure(activityCtx, workflowCtx, spec, state, node, attemptNode.Run.RunID, fmt.Sprintf("resolve input for attempt %d failed: %s", attempt, err))
	}

	attemptNode.Run.Input = resolved.Input
	evt12 := agentosplan.StateEvent{Kind: agentosplan.EventNodeInputResolved, NodeID: node.NodeID, RunID: attemptNode.Run.RunID, InputTrace: resolved.Trace}

	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt12); err != nil {
		return err
	}

	evt13 := agentosplan.StateEvent{Kind: agentosplan.EventNodeStarted, NodeID: node.NodeID, RunID: attemptNode.Run.RunID, Attempt: attempt}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt13); err != nil {
		return err
	}

	var started StartPlanNodeOutput
	if err := workflow.ExecuteActivity(activityCtx, StartPlanNodeActivityName, startPlanNodeInput{
		PlanID:    spec.PlanID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		Node:      attemptNode,
		Attempt:   attempt,
	}).Get(activityCtx, &started); err != nil {
		return applyNodeAttemptFailure(activityCtx, workflowCtx, spec, state, node, attemptNode.Run.RunID, fmt.Sprintf("start attempt %d failed: %s", attempt, err))
	}

	if runTerminal(started.Status.LifecycleState) {
		return applyNodeRunTerminal(activityCtx, workflowCtx, spec, state, validation, expansionCount, node, &started.Status)
	}

	return nil
}

func conditionTracesForDecision(decision agentosplan.SchedulerDecision) []agentosplan.ConditionEvaluationTrace {
	if len(decision.ConditionTraces) == 0 {
		return nil
	}

	nodeIDs := make(map[string]struct{}, len(decision.Ready)+len(decision.Skipped))
	for i := range decision.Ready {
		nodeIDs[decision.Ready[i].NodeID] = struct{}{}
	}

	for i := range decision.Skipped {
		nodeIDs[decision.Skipped[i].NodeID] = struct{}{}
	}

	if len(nodeIDs) == 0 {
		return nil
	}

	traces := make([]agentosplan.ConditionEvaluationTrace, 0, len(decision.ConditionTraces))
	for i := range decision.ConditionTraces {
		if _, ok := nodeIDs[decision.ConditionTraces[i].NodeID]; ok {
			traces = append(traces, decision.ConditionTraces[i])
		}
	}

	return traces
}

func pollRunningPlanNodes(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32) (bool, error) {
	progressed := false
	now := workflow.Now(workflowCtx)

	for i := range state.Status.Nodes {
		node := &state.Status.Nodes[i]
		if node.LifecycleState != agentos.PlanNodeRunning {
			continue
		}

		nodeProgressed, err := pollRunningPlanNode(activityCtx, workflowCtx, spec, state, validation, expansionCount, now, node)
		if err != nil {
			return progressed || nodeProgressed, err
		}

		progressed = progressed || nodeProgressed
	}

	return progressed, nil
}

func pollRunningPlanNode(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32, now time.Time, node *agentos.PlanNodeStatus) (bool, error) {
	nodeSpec, ok := validation.Plan.NodeByID[node.NodeID]
	if !ok {
		return false, fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, node.NodeID)
	}

	if nodeTimedOut(now, node, &nodeSpec) {
		return handleTimedOutPlanNode(activityCtx, workflowCtx, spec, state, node, &nodeSpec)
	}

	if node.RunID == "" {
		return markRunningPlanNodeSucceeded(activityCtx, workflowCtx, spec, state, node)
	}

	return refreshRunningPlanNode(activityCtx, workflowCtx, spec, state, validation, expansionCount, node, &nodeSpec)
}

func handleTimedOutPlanNode(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, node *agentos.PlanNodeStatus, nodeSpec *agentos.PlanNodeSpec) (bool, error) {
	if err := cancelTimedOutNode(activityCtx, spec.PlanID, node); err != nil {
		return false, err
	}

	reason := fmt.Sprintf("node timed out after %d seconds", nodeSpec.Policy.TimeoutSeconds)
	if err := applyNodeAttemptFailure(activityCtx, workflowCtx, spec, state, nodeSpec, node.RunID, reason); err != nil {
		return true, err
	}

	return true, nil
}

func markRunningPlanNodeSucceeded(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, node *agentos.PlanNodeStatus) (bool, error) {
	evt14 := agentosplan.StateEvent{Kind: agentosplan.EventNodeSucceeded, NodeID: node.NodeID}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt14); err != nil {
		return false, err
	}

	return true, nil
}

func refreshRunningPlanNode(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32, node *agentos.PlanNodeStatus, nodeSpec *agentos.PlanNodeSpec) (bool, error) {
	var current StatusPlanNodeOutput
	if err := workflow.ExecuteActivity(activityCtx, StatusPlanNodeActivityName, statusPlanNodeInput{RunID: node.RunID}).Get(activityCtx, &current); err != nil {
		evt15 := agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, Reason: err.Error()}
		if persistErr := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt15); persistErr != nil {
			return true, persistErr
		}

		return true, nil
	}

	if !runTerminal(current.Status.LifecycleState) {
		return false, nil
	}

	if err := applyNodeRunTerminal(activityCtx, workflowCtx, spec, state, validation, expansionCount, nodeSpec, &current.Status); err != nil {
		return false, err
	}

	return true, nil
}

func applyNodeRunTerminal(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32, node *agentos.PlanNodeSpec, status *agentos.RunStatus) error {
	if err := applyRunBudgetUsage(activityCtx, workflowCtx, spec, state, node, status); err != nil {
		return err
	}

	var published PublishPlanArtifactsOutput
	if err := workflow.ExecuteActivity(activityCtx, PublishPlanArtifactsActivityName, publishPlanArtifactsInput{
		Spec:   *spec,
		Node:   *node,
		Status: *status,
	}).Get(activityCtx, &published); err != nil {
		evt16 := agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, RunID: status.RunID, Reason: err.Error()}

		return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt16)
	}

	artifacts := published.Artifacts
	if len(artifacts) > 0 {
		evt17 := agentosplan.StateEvent{Kind: agentosplan.EventArtifactsPublished, NodeID: node.NodeID, RunID: status.RunID, Artifacts: artifacts}
		if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt17); err != nil {
			return err
		}
	}

	switch status.LifecycleState {
	case COMPLETED, SUCCEEDED:
		if err := validateRequiredArtifacts(node.Outputs, artifacts); err != nil {
			return applyNodeAttemptFailure(activityCtx, workflowCtx, spec, state, node, status.RunID, err.Error())
		}

		if err := applyPlanExpansion(activityCtx, workflowCtx, spec, state, validation, expansionCount, node, status, artifacts); err != nil {
			return err
		}

		evt18 := agentosplan.StateEvent{Kind: agentosplan.EventNodeSucceeded, NodeID: node.NodeID, RunID: status.RunID}

		return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt18)
	case FAILED:
		return applyNodeAttemptFailure(activityCtx, workflowCtx, spec, state, node, status.RunID, status.Reason)
	case CANCELED:
		evt19 := agentosplan.StateEvent{Kind: agentosplan.EventNodeCanceled, NodeID: node.NodeID, RunID: status.RunID, Reason: status.Reason}

		return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt19)
	}

	return nil
}

func applyPlanExpansion(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32, node *agentos.PlanNodeSpec, status *agentos.RunStatus, artifacts []agentos.ArtifactRef) error {
	if !hasPlanDeltaArtifact(artifacts) {
		return nil
	}

	var expanded EvaluatePlanExpansionOutput
	if err := workflow.ExecuteActivity(activityCtx, EvaluatePlanExpansionActivityName, evaluatePlanExpansionInput{
		Spec:           *spec,
		Status:         state.Status,
		Node:           *node,
		RunStatus:      *status,
		Artifacts:      artifacts,
		ExpansionCount: *expansionCount,
	}).Get(activityCtx, &expanded); err != nil {
		return err
	}

	if !expanded.Expanded {
		return nil
	}

	nextSpec := expanded.Spec
	evt20 := agentosplan.StateEvent{Kind: agentosplan.EventPlanExpanded, Expansion: expanded.Delta}

	if err := applyPlanStateEvent(activityCtx, workflowCtx, &nextSpec, state, &evt20); err != nil {
		return err
	}

	*spec = nextSpec
	validation.Plan = expanded.Plan
	validation.ControlsByNode = expanded.ControlsByNode
	validation.CapabilitiesByNode = expanded.CapabilitiesByNode
	(*expansionCount)++

	return nil
}

func hasPlanDeltaArtifact(artifacts []agentos.ArtifactRef) bool {
	for i := range artifacts {
		if artifacts[i].Kind == agentos.ArtifactKindPlanDelta {
			return true
		}
	}

	return false
}

func applyRunBudgetUsage(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, node *agentos.PlanNodeSpec, status *agentos.RunStatus) error {
	if status.BudgetUsage.SpentCents == 0 {
		return nil
	}

	if status.BudgetUsage.SpentCents < 0 {
		return fmt.Errorf("%w: run %q reported negative budget usage", agentos.ErrInvalidRunPlan, status.RunID)
	}

	evt21 := agentosplan.StateEvent{
		Kind:        agentosplan.EventBudgetReported,
		NodeID:      node.NodeID,
		RunID:       status.RunID,
		BudgetDelta: status.BudgetUsage,
	}

	return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt21)
}

func applyNodeAttemptFailure(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, node *agentos.PlanNodeSpec, runID, reason string) error {
	if reason == "" {
		reason = "node attempt failed"
	}

	current, ok := state.NodeStatus(node.NodeID)
	if !ok {
		return fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, node.NodeID)
	}

	failedAttempt := current.Attempts
	if failedAttempt <= 0 {
		failedAttempt = 1
	}

	nextAttempt := failedAttempt + 1
	if failedAttempt < maxNodeAttempts(node.Policy) {
		evt22 := agentosplan.StateEvent{
			Kind:    agentosplan.EventNodeRetryScheduled,
			NodeID:  node.NodeID,
			RunID:   runID,
			Reason:  reason,
			Attempt: nextAttempt,
		}

		return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt22)
	}

	evt23 := agentosplan.StateEvent{
		Kind:   agentosplan.EventNodeFailed,
		NodeID: node.NodeID,
		RunID:  runID,
		Reason: reason,
	}

	return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt23)
}

func cancelTimedOutNode(activityCtx workflow.Context, planID string, node *agentos.PlanNodeStatus) error {
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

func applyPlanBudgetGuard(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, controlsByNode map[string][]agentos.ControlOperation) (bool, error) {
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
	if err := controlActivePlanNodes(activityCtx, spec.PlanID, &state.Status, &control, controlsByNode); err != nil {
		return false, err
	}

	reason := agentosplan.BudgetExceededReason(spec.Policy, state.Status.BudgetUsage)
	if err := markActivePlanNodesCanceled(activityCtx, workflowCtx, spec, state, reason); err != nil {
		return false, err
	}

	evt24 := agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: reason}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt24); err != nil {
		return false, err
	}

	return true, nil
}

func applyPlanTimeoutGuard(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, controlsByNode map[string][]agentos.ControlOperation) (bool, error) {
	if !agentosplan.PlanTimedOut(spec.Policy, state.Status.StartedAt, workflow.Now(workflowCtx)) {
		return false, nil
	}

	key, err := agentosplan.PlanTimeoutControlIdempotencyKey(spec.PlanID, state.Status.StartedAt, spec.Policy.TimeoutSeconds)
	if err != nil {
		return false, err
	}

	control := agentos.ControlRequest{
		Operation:      agentos.ControlCancel,
		IdempotencyKey: key,
	}
	if err := controlActivePlanNodes(activityCtx, spec.PlanID, &state.Status, &control, controlsByNode); err != nil {
		return false, err
	}

	reason := agentosplan.PlanTimeoutReason(spec.Policy)
	if err := markActivePlanNodesCanceled(activityCtx, workflowCtx, spec, state, reason); err != nil {
		return false, err
	}

	evt25 := agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: reason}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt25); err != nil {
		return false, err
	}

	return true, nil
}

func nodeTimedOut(now time.Time, status *agentos.PlanNodeStatus, node *agentos.PlanNodeSpec) bool {
	if status.LifecycleState != agentos.PlanNodeRunning || status.StartedAt.IsZero() || node.Policy.TimeoutSeconds <= 0 {
		return false
	}

	return !now.Before(status.StartedAt.Add(time.Duration(node.Policy.TimeoutSeconds) * time.Second))
}

func nextNodeAttempt(state *agentosplan.State, nodeID string) int32 {
	status, ok := state.NodeStatus(nodeID)
	if !ok {
		return 1
	}

	return status.Attempts + 1
}

func nodeForAttempt(planID string, node *agentos.PlanNodeSpec, attempt int32) (agentos.PlanNodeSpec, error) {
	if attempt <= 0 {
		return agentos.PlanNodeSpec{}, fmt.Errorf("%w: node start attempt must be positive", agentos.ErrInvalidRunPlan)
	}

	node.Run.RunID = nodeRunIDForAttempt(node.Run.RunID, attempt)

	key, err := agentosplan.NodeStartIdempotencyKey(planID, node.NodeID, attempt)
	if err != nil {
		return agentos.PlanNodeSpec{}, err
	}

	node.Run.IdempotencyKey = key

	return *node, nil
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

type planControlDrainResult struct {
	Canceled bool
	Paused   bool
}

func drainPlanControl(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, ch workflow.ReceiveChannel, state *agentosplan.State, paused bool, controlsByNode map[string][]agentos.ControlOperation, processed map[string]bool) (canceled, pausedOut bool, err error) {
	var control agentos.ControlRequest
	for ch.ReceiveAsync(&control) {
		if err := agentos.ValidateControlRequest(&control); err != nil {
			return false, paused, err
		}

		if control.IdempotencyKey == "" {
			return false, paused, fmt.Errorf("%w: control idempotency key is required", agentos.ErrInvalidControlOperation)
		}

		if processed[control.IdempotencyKey] {
			continue
		}

		processed[control.IdempotencyKey] = true

		result, err := applyPlanControl(activityCtx, workflowCtx, spec, state, paused, &control, controlsByNode)
		if err != nil {
			return false, result.Paused, err
		}

		paused = result.Paused
		if result.Canceled {
			return true, paused, nil
		}
	}

	return false, paused, nil
}

func applyPlanControl(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, paused bool, control *agentos.ControlRequest, controlsByNode map[string][]agentos.ControlOperation) (planControlDrainResult, error) {
	result := planControlDrainResult{Paused: paused}

	switch control.Operation {
	case agentos.ControlCancel:
		return cancelPlanFromControl(activityCtx, workflowCtx, spec, state, control, controlsByNode, result)
	case agentos.ControlPause:
		return pausePlanFromControl(activityCtx, workflowCtx, spec, state, control, controlsByNode, result)
	case agentos.ControlResume:
		return resumePlanFromControl(activityCtx, workflowCtx, spec, state, control, controlsByNode, result)
	default:
		return result, nil
	}
}

func cancelPlanFromControl(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, control *agentos.ControlRequest, controlsByNode map[string][]agentos.ControlOperation, result planControlDrainResult) (planControlDrainResult, error) {
	if err := controlActivePlanNodes(activityCtx, spec.PlanID, &state.Status, control, controlsByNode); err != nil {
		return result, err
	}

	if err := markActivePlanNodesCanceled(activityCtx, workflowCtx, spec, state, "cancel requested"); err != nil {
		return result, err
	}

	evt26 := agentosplan.StateEvent{Kind: agentosplan.EventPlanCanceled, Reason: "cancel requested"}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt26); err != nil {
		return result, err
	}

	result.Canceled = true

	return result, nil
}

func pausePlanFromControl(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, control *agentos.ControlRequest, controlsByNode map[string][]agentos.ControlOperation, result planControlDrainResult) (planControlDrainResult, error) {
	if err := controlActivePlanNodes(activityCtx, spec.PlanID, &state.Status, control, controlsByNode); err != nil {
		return blockPlanAfterControlFailure(activityCtx, workflowCtx, spec, state, err)
	}

	evt27 := agentosplan.StateEvent{Kind: agentosplan.EventPlanBlocked, Reason: "pause requested"}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt27); err != nil {
		return result, err
	}

	result.Paused = true

	return result, nil
}

func resumePlanFromControl(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, control *agentos.ControlRequest, controlsByNode map[string][]agentos.ControlOperation, result planControlDrainResult) (planControlDrainResult, error) {
	if err := controlActivePlanNodes(activityCtx, spec.PlanID, &state.Status, control, controlsByNode); err != nil {
		return blockPlanAfterControlFailure(activityCtx, workflowCtx, spec, state, err)
	}

	evt28 := agentosplan.StateEvent{Kind: agentosplan.EventPlanStarted}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt28); err != nil {
		return result, err
	}

	result.Paused = false

	return result, nil
}

func blockPlanAfterControlFailure(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, controlErr error) (planControlDrainResult, error) {
	result := planControlDrainResult{Paused: true}
	evt29 := agentosplan.StateEvent{Kind: agentosplan.EventPlanBlocked, Reason: controlErr.Error()}

	if persistErr := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt29); persistErr != nil {
		return result, persistErr
	}

	return result, nil
}

func markActivePlanNodesCanceled(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, reason string) error {
	if reason == "" {
		reason = "cancel requested"
	}

	running := make([]agentos.PlanNodeStatus, 0, len(state.Status.Nodes))
	for i := range state.Status.Nodes {
		if state.Status.Nodes[i].LifecycleState == agentos.PlanNodeRunning {
			running = append(running, state.Status.Nodes[i])
		}
	}

	for i := range running {
		evt30 := agentosplan.StateEvent{
			Kind:   agentosplan.EventNodeCanceled,
			NodeID: running[i].NodeID,
			RunID:  running[i].RunID,
			Reason: reason,
		}
		if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt30); err != nil {
			return err
		}
	}

	return nil
}

func controlActivePlanNodes(activityCtx workflow.Context, planID string, status *agentos.RunPlanStatus, control *agentos.ControlRequest, controlsByNode map[string][]agentos.ControlOperation) error {
	inputs, err := activePlanNodeControlInputs(planID, status, control, controlsByNode)
	if err != nil {
		return err
	}

	for _, input := range inputs {
		if err := workflow.ExecuteActivity(activityCtx, ControlPlanNodeActivityName, input).Get(activityCtx, nil); err != nil {
			return err
		}
	}

	return nil
}

func activePlanNodeControlInputs(planID string, status *agentos.RunPlanStatus, control *agentos.ControlRequest, controlsByNode map[string][]agentos.ControlOperation) ([]controlPlanNodeInput, error) {
	inputs := make([]controlPlanNodeInput, 0, len(status.Nodes))
	for i := range status.Nodes {
		node := &status.Nodes[i]
		if node.LifecycleState != agentos.PlanNodeRunning || node.RunID == "" {
			continue
		}

		if control.Operation != agentos.ControlCancel && !nodeSupportsControl(controlsByNode, node.NodeID, control.Operation) {
			return nil, fmt.Errorf("%w: node %q does not declare support for %s", agentos.ErrInvalidControlOperation, node.NodeID, control.Operation)
		}

		childControl := *control

		key, err := agentosplan.NodeControlIdempotencyKey(planID, node.NodeID, control.Operation, control.IdempotencyKey)
		if err != nil {
			return nil, err
		}

		childControl.IdempotencyKey = key
		inputs = append(inputs, controlPlanNodeInput{RunID: node.RunID, Control: childControl})
	}

	return inputs, nil
}

func nodeSupportsControl(controlsByNode map[string][]agentos.ControlOperation, nodeID string, op agentos.ControlOperation) bool {
	return slices.Contains(controlsByNode[nodeID], op)
}

type planSignalDrainResult struct {
	Rejected   bool
	Paused     bool
	Progressed bool
}

func drainPlanSignals(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, ch workflow.ReceiveChannel, state *agentosplan.State, paused bool, nodes map[string]agentos.PlanNodeSpec, controlsByNode map[string][]agentos.ControlOperation, processed map[string]bool) (rejected, pausedOut, progressedOut bool, err error) {
	var signal agentos.Signal

	progressed := false

	for ch.ReceiveAsync(&signal) {
		if err := agentosplan.ValidatePlanSignal(&signal); err != nil {
			return false, paused, progressed, err
		}

		if processed[signal.IdempotencyKey] {
			continue
		}

		processed[signal.IdempotencyKey] = true

		result, err := applyPlanSignal(activityCtx, workflowCtx, spec, state, paused, nodes, controlsByNode, &signal)
		if err != nil {
			return false, result.Paused, progressed || result.Progressed, err
		}

		paused = result.Paused
		progressed = progressed || result.Progressed

		if result.Rejected {
			return true, paused, progressed, nil
		}
	}

	return false, paused, progressed, nil
}

func applyPlanSignal(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, paused bool, nodes map[string]agentos.PlanNodeSpec, controlsByNode map[string][]agentos.ControlOperation, signal *agentos.Signal) (planSignalDrainResult, error) {
	result := planSignalDrainResult{Paused: paused}

	switch signal.Type {
	case agentos.SignalPlanNodeRetry:
		return retryPlanNodeFromSignal(activityCtx, workflowCtx, spec, state, nodes, signal, result)
	case agentos.SignalPlanApprove:
		return approvePlanFromSignal(activityCtx, workflowCtx, spec, state, signal, result)
	case agentos.SignalPlanReject:
		return rejectPlanFromSignal(activityCtx, workflowCtx, spec, state, controlsByNode, signal, result)
	case agentos.SignalControlPause, agentos.SignalControlResume, agentos.SignalControlCancel,
		agentos.SignalUserMessage, agentos.SignalUserApproval, agentos.SignalUserReject,
		agentos.SignalToolResult, agentos.SignalHumanFeedback, agentos.SignalConfigPatch,
		agentos.SignalMemoryPatch:
		return result, nil
	default:
		return result, nil
	}
}

func retryPlanNodeFromSignal(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, nodes map[string]agentos.PlanNodeSpec, signal *agentos.Signal, result planSignalDrainResult) (planSignalDrainResult, error) {
	if err := applyManualNodeRetry(activityCtx, workflowCtx, spec, state, nodes, signal); err != nil {
		return result, err
	}

	result.Paused = false
	result.Progressed = true

	return result, nil
}

func approvePlanFromSignal(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, signal *agentos.Signal, result planSignalDrainResult) (planSignalDrainResult, error) {
	if state.Status.LifecycleState != agentos.PlanLifecycleBlocked {
		return result, fmt.Errorf("%w: approve requires blocked plan state", agentos.ErrInvalidSignal)
	}

	reason := agentosplan.PlanSignalReason(signal)
	evt31 := agentosplan.StateEvent{Kind: agentosplan.EventPlanApproved, Reason: reason}

	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt31); err != nil {
		return result, err
	}

	result.Paused = false
	result.Progressed = true

	return result, nil
}

func rejectPlanFromSignal(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, controlsByNode map[string][]agentos.ControlOperation, signal *agentos.Signal, result planSignalDrainResult) (planSignalDrainResult, error) {
	reason := agentosplan.PlanSignalReason(signal)
	if reason == "" {
		reason = "plan rejected"
	}

	key, err := agentosplan.PlanSignalCancelControlIdempotencyKey(spec.PlanID, signal)
	if err != nil {
		return result, err
	}

	control := agentos.ControlRequest{
		Operation:      agentos.ControlCancel,
		IdempotencyKey: key,
	}
	if err := controlActivePlanNodes(activityCtx, spec.PlanID, &state.Status, &control, controlsByNode); err != nil {
		return result, err
	}

	if err := markActivePlanNodesCanceled(activityCtx, workflowCtx, spec, state, reason); err != nil {
		return result, err
	}

	evt32 := agentosplan.StateEvent{Kind: agentosplan.EventPlanRejected, Reason: reason}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt32); err != nil {
		return result, err
	}

	result.Rejected = true
	result.Progressed = true

	return result, nil
}

func applyManualNodeRetry(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, nodes map[string]agentos.PlanNodeSpec, signal *agentos.Signal) error {
	retry, err := manualNodeRetry(signal, state, nodes)
	if err != nil {
		return err
	}

	if state.Status.LifecycleState == agentos.PlanLifecycleBlocked || state.Status.LifecycleState == agentos.PlanLifecycleFailed {
		evt33 := agentosplan.StateEvent{Kind: agentosplan.EventPlanApproved, Reason: retry.reason}
		if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt33); err != nil {
			return err
		}
	}

	evt34 := agentosplan.StateEvent{
		Kind:    agentosplan.EventNodeRetryScheduled,
		NodeID:  retry.node.NodeID,
		RunID:   retry.current.RunID,
		Reason:  retry.reason,
		Attempt: retry.nextAttempt,
	}

	return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt34)
}

type manualRetryRequest struct {
	node        agentos.PlanNodeSpec
	current     agentos.PlanNodeStatus
	reason      string
	nextAttempt int32
}

func manualNodeRetry(signal *agentos.Signal, state *agentosplan.State, nodes map[string]agentos.PlanNodeSpec) (manualRetryRequest, error) {
	nodeID, err := agentosplan.PlanSignalNodeID(signal)
	if err != nil {
		return manualRetryRequest{}, err
	}

	nodeSpec, ok := nodes[nodeID]
	if !ok {
		return manualRetryRequest{}, fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, nodeID)
	}

	current, ok := state.NodeStatus(nodeID)
	if !ok {
		return manualRetryRequest{}, fmt.Errorf("%w: unknown node %q", agentos.ErrInvalidRunPlan, nodeID)
	}

	reason := agentosplan.PlanSignalReason(signal)
	if reason == "" {
		reason = "manual retry requested"
	}

	nextAttempt := nextManualRetryAttempt(&current)
	if err := validateManualRetryAttempt(nodeID, nodeSpec.Policy, &current, nextAttempt); err != nil {
		return manualRetryRequest{}, err
	}

	return manualRetryRequest{node: nodeSpec, current: current, reason: reason, nextAttempt: nextAttempt}, nil
}

func nextManualRetryAttempt(current *agentos.PlanNodeStatus) int32 {
	failedAttempts := current.Attempts
	if failedAttempts <= 0 {
		failedAttempts = 1
	}

	return failedAttempts + 1
}

func validateManualRetryAttempt(nodeID string, policy agentos.NodePolicy, current *agentos.PlanNodeStatus, nextAttempt int32) error {
	if current.LifecycleState != agentos.PlanNodeFailed {
		return fmt.Errorf("%w: node %q is %s, not failed", agentos.ErrInvalidSignal, nodeID, current.LifecycleState)
	}

	maxAttempts := maxNodeAttempts(policy)
	if nextAttempt > maxAttempts {
		return fmt.Errorf("%w: node %q retry attempt %d exceeds max attempts %d", agentos.ErrInvalidSignal, nodeID, nextAttempt, maxAttempts)
	}

	return nil
}

func normalizePlanParallelism(maxParallel int32) int {
	if maxParallel <= 0 {
		return defaultPlanParallelism
	}

	return int(maxParallel)
}

func runningNodeCount(status *agentos.RunPlanStatus) int {
	count := 0

	for i := range status.Nodes {
		if status.Nodes[i].LifecycleState == agentos.PlanNodeRunning {
			count++
		}
	}

	return count
}

func nodeSchedulable(state *agentosplan.State, nodeID string) bool {
	status, ok := state.NodeStatus(nodeID)
	if !ok {
		return false
	}

	return status.LifecycleState == agentos.PlanNodePending || status.LifecycleState == agentos.PlanNodeReady
}

func planNodesTerminal(status *agentos.RunPlanStatus) bool {
	if len(status.Nodes) == 0 {
		return false
	}

	for i := range status.Nodes {
		if !planNodeTerminal(status.Nodes[i].LifecycleState) {
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
	case COMPLETED, SUCCEEDED, FAILED, CANCELED:
		return true
	default:
		return false
	}
}

func applyTerminalPlanState(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State) error {
	for i := range state.Status.Nodes {
		node := &state.Status.Nodes[i]
		switch node.LifecycleState {
		case agentos.PlanNodeFailed:
			reason := node.Reason
			if reason == "" {
				reason = "node failed: " + node.NodeID
			}

			evt35 := agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: reason}

			return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt35)
		case agentos.PlanNodeCanceled:
			reason := node.Reason
			if reason == "" {
				reason = "node canceled: " + node.NodeID
			}

			evt36 := agentosplan.StateEvent{Kind: agentosplan.EventPlanCanceled, Reason: reason}

			return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt36)
		}
	}

	evt37 := agentosplan.StateEvent{Kind: agentosplan.EventPlanSucceeded}

	return applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt37)
}
