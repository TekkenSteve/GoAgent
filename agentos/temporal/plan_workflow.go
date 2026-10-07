package temporal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/agentos/nexusapi"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	// PlanWorkflowName is the Temporal workflow type that executes AgentOS plans.
	PlanWorkflowName = "AgentOSPlanWorkflow"
	// PlanStatusQueryName is the Temporal query name that returns the plan's aggregate status.
	PlanStatusQueryName = "agentos.plan.status"
	// PlanSignalName is the Temporal signal name that delivers plan-level signals.
	PlanSignalName = "agentos.plan.signal"
	// PlanControlSignalName is the Temporal signal name that delivers plan control requests.
	PlanControlSignalName = "agentos.plan.control"
	// ValidatePlanActivityName is the Temporal activity name that validates a plan.
	ValidatePlanActivityName = "AgentOSValidatePlan"
	// ResolvePlanNodeInputActivityName is the Temporal activity name that resolves a plan node input.
	ResolvePlanNodeInputActivityName = "AgentOSResolvePlanNodeInput"
	// StartPlanNodeActivityName is the Temporal activity name that starts a plan node.
	StartPlanNodeActivityName = "AgentOSStartPlanNode"
	// StatusPlanNodeActivityName is the Temporal activity name that reads a plan node status.
	StatusPlanNodeActivityName = "AgentOSStatusPlanNode"
	// SignalPlanNodeActivityName is the Temporal activity name that signals a plan node.
	SignalPlanNodeActivityName = "AgentOSSignalPlanNode"
	// ControlPlanNodeActivityName is the Temporal activity name that sends control to a plan node.
	ControlPlanNodeActivityName = "AgentOSControlPlanNode"
	// PublishPlanArtifactsActivityName is the Temporal activity name that publishes plan artifacts.
	PublishPlanArtifactsActivityName = "AgentOSPublishPlanArtifacts"
	// EvaluatePlanExpansionActivityName is the Temporal activity name that evaluates a plan expansion.
	EvaluatePlanExpansionActivityName = "AgentOSEvaluatePlanExpansion"
	// PersistPlanStateActivityName is the Temporal activity name that persists plan state.
	PersistPlanStateActivityName = "AgentOSPersistPlanState"
)

const (
	planNodePollInterval       = 5 * time.Second
	defaultActivityTimeout     = 5 * time.Minute
	defaultActivityMaxAttempts = 3
	defaultPlanParallelism     = 32
	// planSearchAttributesVersionMarker is the workflow.GetVersion marker that
	// gates lifecycle search-attribute upserts. Upserts emit history events, so
	// executions started before the marker replay without them; new executions
	// keep goagent.lifecycle_state fresh on every lifecycle transition.
	planSearchAttributesVersionMarker = "agentos-plan-search-attributes"
	// planNexusNodeRunsVersionMarker is the workflow.GetVersion marker that
	// gates Nexus-backed node execution. Executions started before the marker
	// replay with the activity start + status-poll commands already in their
	// histories and keep that path; new executions start nodes through the
	// Nexus run operation and observe completion as a durable promise instead
	// of per-tick status polling. Nodes without a live promise (started before
	// a continue-as-new, or by pre-marker executions) still poll, so the two
	// paths coexist safely while old histories drain.
	planNexusNodeRunsVersionMarker = "agentos-plan-nexus-node-runs"
	// planNodeNexusDefaultTimeout bounds Nexus-backed node runs whose policy
	// sets no timeout: Nexus operations require a schedule-to-close deadline.
	planNodeNexusDefaultTimeout = 24 * time.Hour
	// planNodePromiseWaitSlack widens the promise-settle wait beyond one poll
	// interval: the operation's own status poll timer is created after the
	// settle timer, so at an identical deadline the settle timer would fire
	// first and miss the completion it is waiting for.
	planNodePromiseWaitSlack = time.Second
	// planNodeNexusDrainTimeout bounds how long the plan Workflow waits for its
	// canceled Nexus operations to resolve before returning.
	planNodeNexusDrainTimeout = 30 * time.Second
	// planNodeNexusCancelGrace keeps the operation's schedule-to-close deadline
	// beyond the node timeout the plan enforces itself. If the operation
	// deadline fired first, the caller would abandon the operation and Nexus
	// would stop delivering the cancelation, leaving the handler running.
	planNodeNexusCancelGrace = 30 * time.Second
	// FAILED marks a run lifecycle state in which the run failed.
	FAILED = "failed"
	// CANCELED marks a run lifecycle state in which the run was canceled.
	CANCELED = "canceled"
)

type planWorkflowInput struct {
	Spec          agentos.RunPlanSpec   `json:"spec"`
	Status        agentos.RunPlanStatus `json:"status"`
	TaskQueues    agentfwTaskQueues     `json:"task_queues"`
	NexusEndpoint string                `json:"nexus_endpoint,omitempty"`
	// NexusPeers maps a peer town to the endpoint that reaches it, so a node
	// naming a peer runs in that town. Part of the input rather than a global:
	// a workflow may only read what it was given, and the mapping is deployment
	// policy that must stay fixed for the life of the execution.
	NexusPeers        map[string]string `json:"nexus_peers,omitempty"`
	WorkflowVersion   int               `json:"workflow_version"`
	Continued         bool              `json:"continued,omitempty"`
	ContinuationCount int32             `json:"continuation_count,omitempty"`
	IterationCount    int32             `json:"iteration_count,omitempty"`
	ExpansionCount    int32             `json:"expansion_count,omitempty"`
	ProcessedControls []string          `json:"processed_controls,omitempty"`
	ProcessedSignals  []string          `json:"processed_signals,omitempty"`
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

	// Executions started before the search-attributes change replay without the
	// upserts, so their history stays identical; new executions keep the
	// lifecycle visible to operators on every transition.
	searchAttributesEnabled := workflow.GetVersion(ctx, planSearchAttributesVersionMarker, workflow.DefaultVersion, 1)
	lifecycleSync := newLifecycleSearchAttributeSync(searchAttributesEnabled, setup.Spec.PlanID)

	// Executions started before the Nexus change keep polling; new executions
	// start nodes through the Nexus run operation (durable completion promise).
	nexusNodeRuns := workflow.GetVersion(ctx, planNexusNodeRunsVersionMarker, workflow.DefaultVersion, 1) == 1

	if err := registerPlanStatusQuery(ctx, &setup.State); err != nil {
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

	validation, err := validatePlanAtStart(activityCtx, ctx, &setup, lifecycleSync, input.NexusPeers)
	if err != nil {
		return setup.State.Status, err
	}

	compiler, err := agentosplan.NewCELCompiler()
	if err != nil {
		return failPlanWorkflowSetup(activityCtx, ctx, &setup, lifecycleSync, err.Error(), err)
	}

	expansionCount := input.ExpansionCount
	iterationCount := input.IterationCount
	paused := setup.State.Status.LifecycleState == agentos.PlanLifecycleBlocked
	runtime := newPlanWorkflowRuntime(activityCtx, ctx, controlCh, signalCh, setup.Spec, input, compiler, nexusNodeRuns)

	for {
		iterationCount++
		result, err := runPlanWorkflowIteration(&runtime, input, setup.Spec, &setup.State, &validation, &expansionCount, iterationCount, paused)
		paused = result.Paused

		// Sync on every lifecycle transition so a blocked/failed plan is
		// immediately queryable; change-detection avoids per-iteration no-op
		// upserts bloating history.
		lifecycleSync.sync(ctx, setup.State.Status.LifecycleState)

		if err != nil {
			drainNexusNodeRuns(&runtime)

			return setup.State.Status, err
		}

		if result.Done {
			drainNexusNodeRuns(&runtime)

			return setup.State.Status, nil
		}
	}
}

// failPlanWorkflowSetup records a setup failure (plan validation or CEL
// compiler) as the terminal plan.failed event, syncs the visible lifecycle so
// the failure is queryable, and returns the combined error.
func failPlanWorkflowSetup(activityCtx, ctx workflow.Context, setup *planWorkflowSetup, lifecycleSync *lifecycleSearchAttributeSync, reason string, cause error) (agentos.RunPlanStatus, error) {
	evt := agentosplan.StateEvent{Kind: agentosplan.EventPlanFailed, Reason: reason}

	applyErr := applyPlanStateEvent(activityCtx, ctx, setup.Spec, &setup.State, &evt)

	lifecycleSync.sync(ctx, setup.State.Status.LifecycleState)

	return setup.State.Status, errors.Join(cause, applyErr)
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

	if err := validatePlanWorkflowVersion(input.WorkflowVersion); err != nil {
		return planWorkflowSetup{}, temporal.NewNonRetryableApplicationError(err.Error(), "validation", err)
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
	// NexusPeers resolves a node's peer to the endpoint that reaches that town.
	NexusPeers map[string]string
	// NexusEndpoint names the deployment's Nexus endpoint; callers reference it
	// to reach the AgentOS run service.
	NexusEndpoint string
	// NexusNodeRuns reports whether this execution starts nodes through the
	// Nexus run operation (gated by the planNexusNodeRunsVersionMarker).
	NexusNodeRuns bool
	// NodeRuns tracks in-flight Nexus run promises for this execution. It is
	// workflow-local: promises created before a continue-as-new do not survive
	// it, and nodes without a promise fall back to status polling.
	NodeRuns *planNodeRunRegistry
}

// planNodeRunRegistry maps run IDs to their in-flight Nexus operation
// promises.
type planNodeRunRegistry struct {
	runs map[string]*planNodeRun
}

type planNodeRun struct {
	future workflow.NexusOperationFuture
	cancel workflow.CancelFunc
}

func newPlanNodeRunRegistry() *planNodeRunRegistry {
	return &planNodeRunRegistry{runs: map[string]*planNodeRun{}}
}

func (r *planNodeRunRegistry) add(runID string, future workflow.NexusOperationFuture, cancel workflow.CancelFunc) {
	r.runs[runID] = &planNodeRun{future: future, cancel: cancel}
}

func (r *planNodeRunRegistry) get(runID string) (*planNodeRun, bool) {
	run, ok := r.runs[runID]

	return run, ok
}

func (r *planNodeRunRegistry) remove(runID string) {
	delete(r.runs, runID)
}

// cancelRun cancels one node's Nexus operation; the operation workflow then
// best-effort cancels the underlying run. The registry entry is kept so the
// exit drain can wait for the cancelation to actually be delivered — Nexus
// only guarantees delivery while the caller Workflow is still running.
func (r *planNodeRunRegistry) cancelRun(runID string) {
	if run, ok := r.runs[runID]; ok && run.cancel != nil {
		run.cancel()
	}
}

// pendingRunIDs returns the run IDs with unsettled Nexus promises.
func (r *planNodeRunRegistry) pendingRunIDs() []string {
	ids := make([]string, 0, len(r.runs))
	for runID, run := range r.runs {
		if !run.future.IsReady() {
			ids = append(ids, runID)
		}
	}

	return ids
}

func (r *planNodeRunRegistry) len() int {
	return len(r.runs)
}

func (r *planNodeRunRegistry) runIDs() []string {
	ids := make([]string, 0, len(r.runs))
	for runID := range r.runs {
		ids = append(ids, runID)
	}

	return ids
}

// drainResolved removes every promise that has settled, discarding its result.
// It is used on the way out of the Workflow, where results no longer matter
// but pending operations must not block completion.
func (r *planNodeRunRegistry) drainResolved(ctx workflow.Context) {
	for runID, run := range r.runs {
		if !run.future.IsReady() {
			continue
		}

		var out nexusapi.RunOutput
		if err := run.future.Get(ctx, &out); err != nil {
			workflow.GetLogger(ctx).Debug("plan node nexus operation resolved on drain", "run_id", runID, "error", err)
		}

		delete(r.runs, runID)
	}
}

// hasRunningPendingPromise reports whether any node still running in plan state
// has an unresolved Nexus promise.
func (r *planNodeRunRegistry) hasRunningPendingPromise(state *agentosplan.State) bool {
	for i := range state.Status.Nodes {
		node := &state.Status.Nodes[i]
		if node.LifecycleState != agentos.PlanNodeRunning || node.RunID == "" {
			continue
		}

		if run, ok := r.runs[node.RunID]; ok && !run.future.IsReady() {
			return true
		}
	}

	return false
}

// settleNexusNodePromises waits (bounded by one poll interval plus slack) for
// any pending Nexus node promise to resolve, then applies every resolved
// promise to plan state. It returns whether any node progressed and whether
// the wait paced this iteration (pending promises existed, so the trailing
// poll sleep is redundant). For executions without Nexus node runs it is a
// no-op.
func settleNexusNodePromises(runtime *planWorkflowRuntime, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32) (settled, paced bool, err error) {
	if !runtime.NexusNodeRuns || !runtime.NodeRuns.hasRunningPendingPromise(state) {
		return false, false, nil
	}

	if _, err := workflow.AwaitWithTimeout(runtime.WorkflowCtx, planNodePollInterval+planNodePromiseWaitSlack, func() bool {
		return !runtime.NodeRuns.hasRunningPendingPromise(state)
	}); err != nil {
		return false, true, err
	}

	progressed, err := applySettledNexusNodePromises(runtime, spec, state, validation, expansionCount)

	return progressed, true, err
}

// drainNexusNodeRuns cancels every operation this execution still has pending
// and waits, bounded, for those cancelations to be delivered before the
// Workflow returns. Nexus does not attempt cancelation once the caller
// Workflow has completed, so skipping this would leave backend runs orphaned.
func drainNexusNodeRuns(runtime *planWorkflowRuntime) {
	if !runtime.NexusNodeRuns || runtime.NodeRuns.len() == 0 {
		return
	}

	for _, runID := range runtime.NodeRuns.runIDs() {
		runtime.NodeRuns.cancelRun(runID)
	}

	if _, err := workflow.AwaitWithTimeout(runtime.WorkflowCtx, planNodeNexusDrainTimeout, func() bool {
		return len(runtime.NodeRuns.pendingRunIDs()) == 0
	}); err != nil {
		workflow.GetLogger(runtime.WorkflowCtx).Warn("plan nexus node runs - drain wait interrupted", "error", err)
	}

	runtime.NodeRuns.drainResolved(runtime.WorkflowCtx)
}

// applySettledNexusNodePromises applies every resolved promise to plan state.
func applySettledNexusNodePromises(runtime *planWorkflowRuntime, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32) (bool, error) {
	progressed := false

	for i := range state.Status.Nodes {
		node := &state.Status.Nodes[i]
		if node.LifecycleState != agentos.PlanNodeRunning || node.RunID == "" {
			continue
		}

		run, ok := runtime.NodeRuns.get(node.RunID)
		if !ok || !run.future.IsReady() {
			continue
		}

		nodeSpec, ok := validation.Plan.NodeByID[node.NodeID]
		if !ok {
			return progressed, fmt.Errorf("%w: unknown node %q", agentoscore.ErrInvalidRunPlan, node.NodeID)
		}

		nodeProgressed, err := applyNexusNodePromise(runtime, spec, state, validation, expansionCount, node, &nodeSpec, run)
		if err != nil {
			return progressed, err
		}

		progressed = progressed || nodeProgressed
	}

	return progressed, nil
}

type planWorkflowIterationResult struct {
	Done   bool
	Paused bool
}

type planWorkflowPreScheduleResult struct {
	Done         bool
	SkipSchedule bool
}

func newPlanWorkflowRuntime(activityCtx, workflowCtx workflow.Context, controlCh, signalCh workflow.ReceiveChannel, spec *agentos.RunPlanSpec, input *planWorkflowInput, compiler agentosplan.ExpressionCompiler, nexusNodeRuns bool) planWorkflowRuntime {
	return planWorkflowRuntime{
		ActivityCtx:       activityCtx,
		WorkflowCtx:       workflowCtx,
		ControlCh:         controlCh,
		SignalCh:          signalCh,
		Scheduler:         agentosplan.Scheduler{Expressions: compiler},
		MaxParallel:       normalizePlanParallelism(spec.Policy.MaxParallelNodes),
		ProcessedControls: processedPlanWorkflowKeys(input.ProcessedControls),
		ProcessedSignals:  processedPlanWorkflowKeys(input.ProcessedSignals),
		NexusEndpoint:     input.NexusEndpoint,
		NexusPeers:        input.NexusPeers,
		NexusNodeRuns:     nexusNodeRuns,
		NodeRuns:          newPlanNodeRunRegistry(),
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

	// Nexus-backed nodes complete asynchronously. Settle any resolved promise
	// before controls and signals are validated against node state, and let
	// the bounded wait double as this iteration's poll interval: signals see
	// state at least as fresh as the legacy status-poll path provided.
	settled, paced, err := settleNexusNodePromises(runtime, spec, state, validation, expansionCount)
	if err != nil {
		return result, err
	}

	if done, err := applyPlanWorkflowControls(runtime, spec, state, validation, &result); done {
		return result, err
	}

	signalProgressed, err := applyPlanWorkflowSignals(runtime, spec, state, validation, &result)
	if workflowIterationDone(&result, err) {
		return result, err
	}

	signalProgressed = signalProgressed || settled

	if stop, err := applyPlanWorkflowPreSchedule(runtime, input, spec, state, validation, expansionCount, iterationCount, &result); stop {
		return result, err
	}

	progressed, err := applyPlanWorkflowSchedule(runtime, spec, state, validation, expansionCount, signalProgressed, &result)
	if workflowIterationDone(&result, err) {
		return result, err
	}

	// When the settle wait paced this iteration, skip the trailing sleep:
	// the poll interval was already consumed by the bounded promise wait.
	if paced {
		return result, nil
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
	timedOut, err := applyPlanDeadlineGuard(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, validation.ControlsByNode, planDeadlineGuardParams{
		expired:        agentosplan.PlanTimedOut,
		anchor:         func(s agentos.RunPlanStatus) time.Time { return s.StartedAt },
		idempotencyKey: agentosplan.PlanTimeoutControlIdempotencyKey,
		seconds:        func(p agentos.PlanPolicy) int64 { return p.TimeoutSeconds },
		reason:         agentosplan.PlanTimeoutReason,
		eventKind:      agentosplan.EventPlanFailed,
	})
	if err != nil || timedOut {
		return planWorkflowPreScheduleResult{Done: timedOut}, err
	}

	if planNodesTerminal(&state.Status) {
		return planWorkflowPreScheduleResult{Done: true}, applyTerminalPlanState(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state)
	}

	if !paused {
		return planWorkflowPreScheduleResult{}, nil
	}

	if timedOut, err := applyPlanDeadlineGuard(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, validation.ControlsByNode, planDeadlineGuardParams{
		expired:        agentosplan.PlanBlockedTimedOut,
		anchor:         func(s agentos.RunPlanStatus) time.Time { return s.BlockedAt },
		idempotencyKey: agentosplan.PlanBlockedTimeoutControlIdempotencyKey,
		seconds:        func(p agentos.PlanPolicy) int64 { return p.ApprovalTimeoutSeconds },
		reason:         agentosplan.PlanBlockedTimeoutReason,
		eventKind:      agentosplan.EventPlanRejected,
	}); err != nil {
		return planWorkflowPreScheduleResult{}, err
	} else if timedOut {
		return planWorkflowPreScheduleResult{Done: true}, nil
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

	pollProgressed, err := pollRunningPlanNodes(runtime, spec, state, validation, expansionCount)
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

		if err := startPlanNode(runtime, spec, state, validation, expansionCount, &decision.Ready[i], validation.Plan.EdgesByTo[decision.Ready[i].NodeID]); err != nil {
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

		return *result, fmt.Errorf("%w: %s", agentoscore.ErrInvalidRunPlan, reason)
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

	return errors.Join(fmt.Errorf("%w: %s", agentoscore.ErrInvalidRunPlan, reason), persistErr)
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

func startPlanNode(runtime *planWorkflowRuntime, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32, node *agentos.PlanNodeSpec, incomingEdges []agentos.PlanEdgeSpec) error {
	activityCtx, workflowCtx := runtime.ActivityCtx, runtime.WorkflowCtx

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

	attemptNode, proceed, err := resolvePlanNodeAttempt(runtime, spec, state, node, incomingEdges, attempt)
	if err != nil || !proceed {
		return err
	}

	if runtime.NexusNodeRuns {
		return startPlanNodeViaNexus(runtime, spec, node, &attemptNode, attempt)
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

// resolvePlanNodeAttempt builds the attempt's run spec, resolves its input,
// and records the input-resolved and started state events. proceed is false
// when the attempt failure was already recorded and the caller must not start
// the run.
func resolvePlanNodeAttempt(runtime *planWorkflowRuntime, spec *agentos.RunPlanSpec, state *agentosplan.State, node *agentos.PlanNodeSpec, incomingEdges []agentos.PlanEdgeSpec, attempt int32) (attemptNode agentos.PlanNodeSpec, proceed bool, err error) {
	activityCtx, workflowCtx := runtime.ActivityCtx, runtime.WorkflowCtx

	attemptNode, err = nodeForAttempt(spec.PlanID, node, attempt)
	if err != nil {
		return agentos.PlanNodeSpec{}, false, err
	}

	var resolved ResolvePlanNodeInputOutput
	if err := workflow.ExecuteActivity(activityCtx, ResolvePlanNodeInputActivityName, resolvePlanNodeInputInput{
		Spec:   *spec,
		Status: state.Status,
		Node:   attemptNode,
		Edges:  incomingEdges,
	}).Get(activityCtx, &resolved); err != nil {
		failureErr := applyNodeAttemptFailure(activityCtx, workflowCtx, spec, state, node, attemptNode.Run.RunID, fmt.Sprintf("resolve input for attempt %d failed: %s", attempt, err))

		return agentos.PlanNodeSpec{}, false, failureErr
	}

	attemptNode.Run.Input = resolved.Input
	evt12 := agentosplan.StateEvent{Kind: agentosplan.EventNodeInputResolved, NodeID: node.NodeID, RunID: attemptNode.Run.RunID, InputTrace: resolved.Trace}

	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt12); err != nil {
		return agentos.PlanNodeSpec{}, false, err
	}

	evt13 := agentosplan.StateEvent{Kind: agentosplan.EventNodeStarted, NodeID: node.NodeID, RunID: attemptNode.Run.RunID, Attempt: attempt}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt13); err != nil {
		return agentos.PlanNodeSpec{}, false, err
	}

	return attemptNode, true, nil
}

// failPlanAtStart records a refused plan on the plan itself and returns the
// error, so the caller propagates it without unpacking a status it discards.
func failPlanAtStart(
	activityCtx workflow.Context,
	ctx workflow.Context,
	setup *planWorkflowSetup,
	lifecycleSync *lifecycleSearchAttributeSync,
	cause error,
) error {
	_, err := failPlanWorkflowSetup(activityCtx, ctx, setup, lifecycleSync, cause.Error(), cause)

	return err
}

// validatePlanAtStart runs everything that can refuse a plan before its first
// node starts: the plan's own validation activity, and the check that every
// node naming a peer has an endpoint to reach it. A mapping mistake found here
// is the difference between a plan that fails fast and one that fails halfway
// through its work.
func validatePlanAtStart(
	activityCtx workflow.Context,
	ctx workflow.Context,
	setup *planWorkflowSetup,
	lifecycleSync *lifecycleSearchAttributeSync,
	peers map[string]string,
) (ValidatePlanOutput, error) {
	var validation ValidatePlanOutput

	if err := workflow.ExecuteActivity(activityCtx, ValidatePlanActivityName, validatePlanInput{Spec: *setup.Spec}).Get(activityCtx, &validation); err != nil {
		return ValidatePlanOutput{}, failPlanAtStart(activityCtx, ctx, setup, lifecycleSync, err)
	}

	if err := validatePlanNodePeers(setup.Spec, peers); err != nil {
		return ValidatePlanOutput{}, failPlanAtStart(activityCtx, ctx, setup, lifecycleSync, err)
	}

	return validation, nil
}

// registerPlanStatusQuery publishes the plan's status query, so an operator can
// read the running state without loading the workflow history.
func registerPlanStatusQuery(ctx workflow.Context, state *agentosplan.State) error {
	return workflow.SetQueryHandler(ctx, PlanStatusQueryName, func() (agentos.RunPlanStatus, error) {
		return state.Status, nil
	})
}

// validatePlanNodePeers rejects a plan whose node names a peer this deployment
// has no endpoint for, before any node starts. Nodes added later by a plan
// expansion are caught at dispatch instead, because the expansion happens after
// this check.
func validatePlanNodePeers(spec *agentos.RunPlanSpec, peers map[string]string) error {
	for i := range spec.Nodes {
		node := &spec.Nodes[i]

		if _, err := nexusEndpointFor(peers, "", node.Peer); err != nil {
			return fmt.Errorf("%w: node %q: %w", agentoscore.ErrInvalidRunPlan, node.NodeID, err)
		}
	}

	return nil
}

// nexusEndpointFor resolves the endpoint a node's run operation goes to: the
// endpoint of the town the node names, or the deployment's own endpoint when it
// names none. A peer without a mapping is refused rather than silently run
// locally: running a node in the wrong town is worse than not running it.
func nexusEndpointFor(peers map[string]string, localEndpoint, peer string) (string, error) {
	if peer == "" {
		return localEndpoint, nil
	}

	endpoint, ok := peers[peer]
	if !ok {
		return "", fmt.Errorf("%w: peer %q has no Nexus endpoint in this deployment", agentoscore.ErrInvalidRunPlan, peer)
	}

	return endpoint, nil
}

// startPlanNodeViaNexus starts the node run through the AgentOS Nexus run
// operation. The operation's activity routes the start through the plan node
// starter (ownership bookkeeping stays atomic with the start), and the
// returned promise resolves when the run reaches a terminal state — no status
// polling. Start failures surface on the promise, not here.
func startPlanNodeViaNexus(runtime *planWorkflowRuntime, spec *agentos.RunPlanSpec, node, attemptNode *agentos.PlanNodeSpec, attempt int32) error {
	runSpec := attemptNode.Run
	runSpec.AccountID = spec.AccountID
	runSpec.ProjectID = spec.ProjectID

	if runSpec.IdempotencyKey == "" {
		key, err := agentosplan.NodeStartIdempotencyKey(spec.PlanID, node.NodeID, attempt)
		if err != nil {
			return err
		}

		runSpec.IdempotencyKey = key
	}

	runCtx, cancel := workflow.WithCancel(runtime.WorkflowCtx)

	// All three Nexus timeouts are set deliberately: schedule-to-start fails
	// fast when no handler is polling, start-to-close bounds the run once the
	// handler owns it, and schedule-to-close stays beyond the plan's own node
	// timeout so the plan cancels the operation itself (and Nexus keeps
	// delivering that cancelation) instead of the deadline abandoning it.
	timeout := planNodeNexusDefaultTimeout
	if node.Policy.TimeoutSeconds > 0 {
		timeout = time.Duration(node.Policy.TimeoutSeconds)*time.Second + planNodeNexusCancelGrace
	}

	options := workflow.NexusOperationOptions{
		ScheduleToStartTimeout: nexusRunScheduleToStartTimeout,
		StartToCloseTimeout:    timeout,
		ScheduleToCloseTimeout: timeout + nexusRunScheduleToStartTimeout,
	}

	endpoint, err := nexusEndpointFor(runtime.NexusPeers, runtime.NexusEndpoint, node.Peer)
	if err != nil {
		return err
	}

	future := workflow.NewNexusClient(endpoint, nexusapi.ServiceName).ExecuteOperation(
		runCtx,
		nexusapi.RunOperationName,
		nexusapi.RunRequest{
			RequestID: runSpec.IdempotencyKey,
			Spec:      &runSpec,
			PlanScope: &nexusapi.RunPlanScope{PlanID: spec.PlanID, NodeID: node.NodeID},
		},
		options,
	)
	runtime.NodeRuns.add(runSpec.RunID, future, cancel)

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

func pollRunningPlanNodes(runtime *planWorkflowRuntime, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32) (bool, error) {
	progressed := false
	now := workflow.Now(runtime.WorkflowCtx)

	for i := range state.Status.Nodes {
		node := &state.Status.Nodes[i]
		if node.LifecycleState != agentos.PlanNodeRunning {
			continue
		}

		nodeProgressed, err := pollRunningPlanNode(runtime, spec, state, validation, expansionCount, now, node)
		if err != nil {
			return progressed || nodeProgressed, err
		}

		progressed = progressed || nodeProgressed
	}

	return progressed, nil
}

func pollRunningPlanNode(runtime *planWorkflowRuntime, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32, now time.Time, node *agentos.PlanNodeStatus) (bool, error) {
	nodeSpec, ok := validation.Plan.NodeByID[node.NodeID]
	if !ok {
		return false, fmt.Errorf("%w: unknown node %q", agentoscore.ErrInvalidRunPlan, node.NodeID)
	}

	if nodeTimedOut(now, node, &nodeSpec) {
		return handleTimedOutPlanNode(runtime, spec, state, node, &nodeSpec)
	}

	if node.RunID == "" {
		return markRunningPlanNodeSucceeded(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, node)
	}

	return refreshRunningPlanNode(runtime, spec, state, validation, expansionCount, node, &nodeSpec)
}

func handleTimedOutPlanNode(runtime *planWorkflowRuntime, spec *agentos.RunPlanSpec, state *agentosplan.State, node *agentos.PlanNodeStatus, nodeSpec *agentos.PlanNodeSpec) (bool, error) {
	// The plan owns run cancelation and sends it with the node-timeout
	// idempotency key. For Nexus-backed nodes the operation is canceled too so
	// the handler stops promptly instead of polling an abandoned run.
	if err := cancelTimedOutNode(runtime.ActivityCtx, spec, node); err != nil {
		return false, err
	}

	runtime.NodeRuns.cancelRun(node.RunID)

	reason := fmt.Sprintf("node timed out after %d seconds", nodeSpec.Policy.TimeoutSeconds)
	if err := applyNodeAttemptFailure(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, nodeSpec, node.RunID, reason); err != nil {
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

// planNodeRunRef pairs the plan's tenant with one of its node runs. The plan
// may only reach child runs it started, so its scope travels with every
// node-addressed operation.
func planNodeRunRef(spec *agentos.RunPlanSpec, runID string) agentos.RunRef {
	return agentos.RunRef{RunID: runID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}
}

func refreshRunningPlanNode(runtime *planWorkflowRuntime, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32, node *agentos.PlanNodeStatus, nodeSpec *agentos.PlanNodeSpec) (bool, error) {
	if run, ok := runtime.NodeRuns.get(node.RunID); ok {
		return refreshNexusBackedPlanNode(runtime, spec, state, validation, expansionCount, node, nodeSpec, run)
	}

	activityCtx, workflowCtx := runtime.ActivityCtx, runtime.WorkflowCtx

	var current StatusPlanNodeOutput

	nodeRef := planNodeRunRef(spec, node.RunID)

	if err := workflow.ExecuteActivity(activityCtx, StatusPlanNodeActivityName, statusPlanNodeInput{
		RunID:     nodeRef.RunID,
		AccountID: nodeRef.AccountID,
		ProjectID: nodeRef.ProjectID,
	}).Get(activityCtx, &current); err != nil {
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

// refreshNexusBackedPlanNode applies the node's Nexus run promise: a pending
// promise is zero-cost (no backend roundtrip), a resolved one carries the
// terminal run status directly. The promise is dropped after resolution;
// anything non-terminal that still slips through falls back to status polling
// on the next tick.
func refreshNexusBackedPlanNode(runtime *planWorkflowRuntime, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32, node *agentos.PlanNodeStatus, nodeSpec *agentos.PlanNodeSpec, run *planNodeRun) (bool, error) {
	if !run.future.IsReady() {
		return false, nil
	}

	return applyNexusNodePromise(runtime, spec, state, validation, expansionCount, node, nodeSpec, run)
}

// applyNexusNodePromise drains one resolved Nexus run promise into plan
// state: an operation failure fails the node, a terminal status flows through
// the standard terminal handling (budget, artifacts, expansion).
func applyNexusNodePromise(runtime *planWorkflowRuntime, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32, node *agentos.PlanNodeStatus, nodeSpec *agentos.PlanNodeSpec, run *planNodeRun) (bool, error) {
	runtime.NodeRuns.remove(node.RunID)

	var out nexusapi.RunOutput
	if err := run.future.Get(runtime.WorkflowCtx, &out); err != nil {
		evt15 := agentosplan.StateEvent{Kind: agentosplan.EventNodeFailed, NodeID: node.NodeID, RunID: node.RunID, Reason: err.Error()}
		if persistErr := applyPlanStateEvent(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, &evt15); persistErr != nil {
			return true, persistErr
		}

		return true, nil
	}

	if !runTerminal(out.Status.LifecycleState) {
		return false, nil
	}

	if err := applyNodeRunTerminal(runtime.ActivityCtx, runtime.WorkflowCtx, spec, state, validation, expansionCount, nodeSpec, &out.Status); err != nil {
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

func applyPlanExpansion(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, validation *ValidatePlanOutput, expansionCount *int32, node *agentos.PlanNodeSpec, status *agentos.RunStatus, artifacts []agentoscore.ArtifactRef) error {
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

func hasPlanDeltaArtifact(artifacts []agentoscore.ArtifactRef) bool {
	for i := range artifacts {
		if artifacts[i].Kind == agentoscore.ArtifactKindPlanDelta {
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
		return fmt.Errorf("%w: run %q reported negative budget usage", agentoscore.ErrInvalidRunPlan, status.RunID)
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
		return fmt.Errorf("%w: unknown node %q", agentoscore.ErrInvalidRunPlan, node.NodeID)
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

func cancelTimedOutNode(activityCtx workflow.Context, spec *agentos.RunPlanSpec, node *agentos.PlanNodeStatus) error {
	planID := spec.PlanID

	if node.RunID == "" {
		return fmt.Errorf("%w: timed out node %q has no active run id", agentoscore.ErrInvalidRunPlan, node.NodeID)
	}

	key, err := agentosplan.NodeTimeoutControlIdempotencyKey(planID, node.NodeID, node.RunID)
	if err != nil {
		return err
	}

	control := agentoscore.ControlRequest{
		Operation:      agentoscore.ControlCancel,
		IdempotencyKey: key,
	}
	nodeRef := planNodeRunRef(spec, node.RunID)

	if err := workflow.ExecuteActivity(activityCtx, ControlPlanNodeActivityName, controlPlanNodeInput{
		RunID:     nodeRef.RunID,
		AccountID: nodeRef.AccountID,
		ProjectID: nodeRef.ProjectID,
		Control:   control,
	}).Get(activityCtx, nil); err != nil {
		return err
	}

	return nil
}

// cancelActivePlanNodesAndFail cancels every active node run and then fails
// the plan with the given event kind and reason. Shared by the budget, timeout
// and blocked-approval guards, so each guard differs only in its deadline
// predicate, idempotency key and failure kind.
func cancelActivePlanNodesAndFail(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, controlsByNode map[string][]agentoscore.ControlOperation, kind agentosplan.EventKind, idempotencyKey, reason string) error {
	control := agentoscore.ControlRequest{
		Operation:      agentoscore.ControlCancel,
		IdempotencyKey: idempotencyKey,
	}
	if err := controlActivePlanNodes(activityCtx, spec, &state.Status, &control, controlsByNode); err != nil {
		return err
	}

	if err := markActivePlanNodesCanceled(activityCtx, workflowCtx, spec, state, reason); err != nil {
		return err
	}

	evt := agentosplan.StateEvent{Kind: kind, Reason: reason}
	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt); err != nil {
		return err
	}

	return nil
}

func applyPlanBudgetGuard(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, controlsByNode map[string][]agentoscore.ControlOperation) (bool, error) {
	if !agentosplan.BudgetExceeded(spec.Policy, state.Status.BudgetUsage) {
		return false, nil
	}

	key, err := agentosplan.BudgetExceededControlIdempotencyKey(spec.PlanID, state.Status.BudgetUsage, spec.Policy.BudgetCents)
	if err != nil {
		return false, err
	}

	reason := agentosplan.BudgetExceededReason(spec.Policy, state.Status.BudgetUsage)
	if err := cancelActivePlanNodesAndFail(activityCtx, workflowCtx, spec, state, controlsByNode, agentosplan.EventPlanFailed, key, reason); err != nil {
		return false, err
	}

	return true, nil
}

// planDeadlineGuardParams supplies what differs between the wall-clock and
// approval-timeout plan guards: which deadline predicate fires, which status
// field anchors it, and which failure event and reason the plan receives.
type planDeadlineGuardParams struct {
	expired        func(agentos.PlanPolicy, time.Time, time.Time) bool
	anchor         func(agentos.RunPlanStatus) time.Time
	idempotencyKey func(string, time.Time, int64) (string, error)
	seconds        func(agentos.PlanPolicy) int64
	reason         func(agentos.PlanPolicy) string
	eventKind      agentosplan.EventKind
}

// applyPlanDeadlineGuard trips the plan failure pipeline when a plan-level
// deadline passes: cancel active nodes, then fail the plan under a stable
// idempotency key so replays and retries never emit duplicate controls.
func applyPlanDeadlineGuard(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, controlsByNode map[string][]agentoscore.ControlOperation, params planDeadlineGuardParams) (bool, error) {
	anchor := params.anchor(state.Status)
	if !params.expired(spec.Policy, anchor, workflow.Now(workflowCtx)) {
		return false, nil
	}

	key, err := params.idempotencyKey(spec.PlanID, anchor, params.seconds(spec.Policy))
	if err != nil {
		return false, err
	}

	reason := params.reason(spec.Policy)
	if err := cancelActivePlanNodesAndFail(activityCtx, workflowCtx, spec, state, controlsByNode, params.eventKind, key, reason); err != nil {
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
		return agentos.PlanNodeSpec{}, fmt.Errorf("%w: node start attempt must be positive", agentoscore.ErrInvalidRunPlan)
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

func drainPlanControl(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, ch workflow.ReceiveChannel, state *agentosplan.State, paused bool, controlsByNode map[string][]agentoscore.ControlOperation, processed map[string]bool) (canceled, pausedOut bool, err error) {
	var control agentoscore.ControlRequest
	for ch.ReceiveAsync(&control) {
		if err := agentoscore.ValidateControlRequest(&control); err != nil {
			return false, paused, err
		}

		if control.IdempotencyKey == "" {
			return false, paused, fmt.Errorf("%w: control idempotency key is required", agentoscore.ErrInvalidControlOperation)
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

func applyPlanControl(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, paused bool, control *agentoscore.ControlRequest, controlsByNode map[string][]agentoscore.ControlOperation) (planControlDrainResult, error) {
	result := planControlDrainResult{Paused: paused}

	switch control.Operation {
	case agentoscore.ControlCancel:
		return cancelPlanFromControl(activityCtx, workflowCtx, spec, state, control, controlsByNode, result)
	case agentoscore.ControlPause:
		return pausePlanFromControl(activityCtx, workflowCtx, spec, state, control, controlsByNode, result)
	case agentoscore.ControlResume:
		return resumePlanFromControl(activityCtx, workflowCtx, spec, state, control, controlsByNode, result)
	default:
		return result, nil
	}
}

func cancelPlanFromControl(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, control *agentoscore.ControlRequest, controlsByNode map[string][]agentoscore.ControlOperation, result planControlDrainResult) (planControlDrainResult, error) {
	if err := controlActivePlanNodes(activityCtx, spec, &state.Status, control, controlsByNode); err != nil {
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

func pausePlanFromControl(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, control *agentoscore.ControlRequest, controlsByNode map[string][]agentoscore.ControlOperation, result planControlDrainResult) (planControlDrainResult, error) {
	if err := controlActivePlanNodes(activityCtx, spec, &state.Status, control, controlsByNode); err != nil {
		return blockPlanAfterControlFailure(activityCtx, workflowCtx, spec, state, err)
	}

	gate, err := agentosplan.NewPlanApprovalGate(spec, &state.Status, "pause requested", workflow.Now(workflowCtx))
	if err != nil {
		return result, err
	}

	evt27 := agentosplan.StateEvent{
		Kind:     agentosplan.EventPlanBlocked,
		Reason:   "pause requested",
		Approval: &agentosplan.StateEventApproval{Gate: gate},
	}

	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt27); err != nil {
		return result, err
	}

	result.Paused = true

	return result, nil
}

func resumePlanFromControl(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, control *agentoscore.ControlRequest, controlsByNode map[string][]agentoscore.ControlOperation, result planControlDrainResult) (planControlDrainResult, error) {
	if err := controlActivePlanNodes(activityCtx, spec, &state.Status, control, controlsByNode); err != nil {
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

	gate, err := agentosplan.NewPlanApprovalGate(spec, &state.Status, controlErr.Error(), workflow.Now(workflowCtx))
	if err != nil {
		return result, err
	}

	evt29 := agentosplan.StateEvent{
		Kind:     agentosplan.EventPlanBlocked,
		Reason:   controlErr.Error(),
		Approval: &agentosplan.StateEventApproval{Gate: gate},
	}

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

func controlActivePlanNodes(activityCtx workflow.Context, spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, control *agentoscore.ControlRequest, controlsByNode map[string][]agentoscore.ControlOperation) error {
	inputs, err := activePlanNodeControlInputs(spec, status, control, controlsByNode)
	if err != nil {
		return err
	}

	for i := range inputs {
		if err := workflow.ExecuteActivity(activityCtx, ControlPlanNodeActivityName, inputs[i]).Get(activityCtx, nil); err != nil {
			return err
		}
	}

	return nil
}

func activePlanNodeControlInputs(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, control *agentoscore.ControlRequest, controlsByNode map[string][]agentoscore.ControlOperation) ([]controlPlanNodeInput, error) {
	planID := spec.PlanID

	inputs := make([]controlPlanNodeInput, 0, len(status.Nodes))

	for i := range status.Nodes {
		node := &status.Nodes[i]
		if node.LifecycleState != agentos.PlanNodeRunning || node.RunID == "" {
			continue
		}

		if control.Operation != agentoscore.ControlCancel && !nodeSupportsControl(controlsByNode, node.NodeID, control.Operation) {
			return nil, fmt.Errorf("%w: node %q does not declare support for %s", agentoscore.ErrInvalidControlOperation, node.NodeID, control.Operation)
		}

		childControl := *control

		key, err := agentosplan.NodeControlIdempotencyKey(planID, node.NodeID, control.Operation, control.IdempotencyKey)
		if err != nil {
			return nil, err
		}

		childControl.IdempotencyKey = key
		nodeRef := planNodeRunRef(spec, node.RunID)

		inputs = append(inputs, controlPlanNodeInput{
			RunID:     nodeRef.RunID,
			AccountID: nodeRef.AccountID,
			ProjectID: nodeRef.ProjectID,
			Control:   childControl,
		})
	}

	return inputs, nil
}

func nodeSupportsControl(controlsByNode map[string][]agentoscore.ControlOperation, nodeID string, op agentoscore.ControlOperation) bool {
	return slices.Contains(controlsByNode[nodeID], op)
}

type planSignalDrainResult struct {
	Rejected   bool
	Paused     bool
	Progressed bool
}

func drainPlanSignals(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, ch workflow.ReceiveChannel, state *agentosplan.State, paused bool, nodes map[string]agentos.PlanNodeSpec, controlsByNode map[string][]agentoscore.ControlOperation, processed map[string]bool) (rejected, pausedOut, progressedOut bool, err error) {
	var signal agentoscore.Signal

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

func applyPlanSignal(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, paused bool, nodes map[string]agentos.PlanNodeSpec, controlsByNode map[string][]agentoscore.ControlOperation, signal *agentoscore.Signal) (planSignalDrainResult, error) {
	result := planSignalDrainResult{Paused: paused}

	switch signal.Type {
	case agentoscore.SignalPlanNodeRetry:
		return retryPlanNodeFromSignal(activityCtx, workflowCtx, spec, state, nodes, signal, result)
	case agentoscore.SignalPlanApprove:
		return approvePlanFromSignal(activityCtx, workflowCtx, spec, state, signal, result)
	case agentoscore.SignalPlanReject:
		return rejectPlanFromSignal(activityCtx, workflowCtx, spec, state, controlsByNode, signal, result)
	case agentoscore.SignalControlPause, agentoscore.SignalControlResume, agentoscore.SignalControlCancel:
		return result, nil
	// Signals addressed to whatever is running inside a node are forwarded to
	// it: the node's run may be a native step queue, which is what a queue
	// modification or an outside event is for.
	case agentoscore.SignalUserMessage, agentoscore.SignalUserApproval, agentoscore.SignalUserReject,
		agentoscore.SignalToolResult, agentoscore.SignalHumanFeedback, agentoscore.SignalConfigPatch,
		agentoscore.SignalStepModify, agentoscore.SignalExternalEvent,
		agentoscore.SignalMemoryPatch:
		if err := signalActivePlanNodes(activityCtx, spec, state, signal); err != nil {
			return result, err
		}

		return result, nil
	default:
		return result, nil
	}
}

func signalActivePlanNodes(activityCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, signal *agentoscore.Signal) error {
	for i := range state.Status.Nodes {
		node := &state.Status.Nodes[i]
		if node.LifecycleState != agentos.PlanNodeRunning || node.RunID == "" {
			continue
		}

		nodeRef := planNodeRunRef(spec, node.RunID)

		if err := workflow.ExecuteActivity(activityCtx, SignalPlanNodeActivityName, signalPlanNodeInput{
			RunID:     nodeRef.RunID,
			AccountID: nodeRef.AccountID,
			ProjectID: nodeRef.ProjectID,
			Signal:    *signal,
		}).Get(activityCtx, nil); err != nil {
			return err
		}
	}

	return nil
}

func retryPlanNodeFromSignal(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, nodes map[string]agentos.PlanNodeSpec, signal *agentoscore.Signal, result planSignalDrainResult) (planSignalDrainResult, error) {
	if err := applyManualNodeRetry(activityCtx, workflowCtx, spec, state, nodes, signal); err != nil {
		return result, err
	}

	result.Paused = false
	result.Progressed = true

	return result, nil
}

func approvePlanFromSignal(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, signal *agentoscore.Signal, result planSignalDrainResult) (planSignalDrainResult, error) {
	if state.Status.LifecycleState != agentos.PlanLifecycleBlocked {
		return result, fmt.Errorf("%w: approve requires blocked plan state", agentoscore.ErrInvalidSignal)
	}

	decision, err := agentosplan.PlanApprovalDecisionFromSignal(spec, &state.Status, signal, true, workflow.Now(workflowCtx))
	if err != nil {
		return result, err
	}

	approval, err := approvalEventForSignal(spec, &state.Status, decision.Reason, workflow.Now(workflowCtx), &decision)
	if err != nil {
		return result, err
	}

	evt31 := agentosplan.StateEvent{
		Kind:     agentosplan.EventPlanApproved,
		Reason:   decision.Reason,
		Approval: approval,
	}

	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt31); err != nil {
		return result, err
	}

	result.Paused = false
	result.Progressed = true

	return result, nil
}

func rejectPlanFromSignal(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, controlsByNode map[string][]agentoscore.ControlOperation, signal *agentoscore.Signal, result planSignalDrainResult) (planSignalDrainResult, error) {
	decision, err := agentosplan.PlanApprovalDecisionFromSignal(spec, &state.Status, signal, false, workflow.Now(workflowCtx))
	if err != nil {
		return result, err
	}

	reason := decision.Reason
	if reason == "" {
		reason = "plan rejected"
	}

	key, err := agentosplan.PlanSignalCancelControlIdempotencyKey(spec.PlanID, signal)
	if err != nil {
		return result, err
	}

	control := agentoscore.ControlRequest{
		Operation:      agentoscore.ControlCancel,
		IdempotencyKey: key,
	}
	if err := controlActivePlanNodes(activityCtx, spec, &state.Status, &control, controlsByNode); err != nil {
		return result, err
	}

	if err := markActivePlanNodesCanceled(activityCtx, workflowCtx, spec, state, reason); err != nil {
		return result, err
	}

	approval, err := approvalEventForSignal(spec, &state.Status, reason, workflow.Now(workflowCtx), &decision)
	if err != nil {
		return result, err
	}

	evt32 := agentosplan.StateEvent{
		Kind:     agentosplan.EventPlanRejected,
		Reason:   reason,
		Approval: approval,
	}

	if err := applyPlanStateEvent(activityCtx, workflowCtx, spec, state, &evt32); err != nil {
		return result, err
	}

	result.Rejected = true
	result.Progressed = true

	return result, nil
}

// approvalEventForSignal builds the StateEvent approval payload for an
// approve/reject decision. A blocked plan persisted before the
// auditable-approval feature has no gate snapshot; a fresh gate is derived from
// the live topology so the decision is still auditable. A reject of a
// never-blocked plan records no gate (the decision stays in the durable event
// ledger, where it belongs, without implying a gate existed).
func approvalEventForSignal(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, reason string, now time.Time, decision *agentos.PlanApprovalDecision) (*agentosplan.StateEventApproval, error) {
	approval := &agentosplan.StateEventApproval{Decision: decision}

	if status.Approval == nil && status.LifecycleState == agentos.PlanLifecycleBlocked {
		gate, err := agentosplan.NewPlanApprovalGate(spec, status, reason, now)
		if err != nil {
			return nil, err
		}

		approval.Gate = gate
	}

	return approval, nil
}

func applyManualNodeRetry(activityCtx, workflowCtx workflow.Context, spec *agentos.RunPlanSpec, state *agentosplan.State, nodes map[string]agentos.PlanNodeSpec, signal *agentoscore.Signal) error {
	// A retry signal for a node whose automatic retry is already scheduled
	// (ready after a failed attempt) is satisfied by that pending retry.
	// Acknowledge it instead of failing the plan over an operator/UI race —
	// with asynchronous run completion the signal routinely lands between
	// the attempt failure and the scheduled retry's start.
	if manualRetryAlreadyScheduled(signal, state) {
		return nil
	}

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

// manualRetryAlreadyScheduled reports whether the retry signal targets a node
// whose retry is already scheduled: ready after at least one failed attempt.
func manualRetryAlreadyScheduled(signal *agentoscore.Signal, state *agentosplan.State) bool {
	nodeID, err := agentosplan.PlanSignalNodeID(signal)
	if err != nil {
		return false
	}

	current, ok := state.NodeStatus(nodeID)

	return ok && current.LifecycleState == agentos.PlanNodeReady && current.Attempts > 0
}

func manualNodeRetry(signal *agentoscore.Signal, state *agentosplan.State, nodes map[string]agentos.PlanNodeSpec) (manualRetryRequest, error) {
	nodeID, err := agentosplan.PlanSignalNodeID(signal)
	if err != nil {
		return manualRetryRequest{}, err
	}

	nodeSpec, ok := nodes[nodeID]
	if !ok {
		return manualRetryRequest{}, fmt.Errorf("%w: unknown node %q", agentoscore.ErrInvalidRunPlan, nodeID)
	}

	current, ok := state.NodeStatus(nodeID)
	if !ok {
		return manualRetryRequest{}, fmt.Errorf("%w: unknown node %q", agentoscore.ErrInvalidRunPlan, nodeID)
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
		return fmt.Errorf("%w: node %q is %s, not failed", agentoscore.ErrInvalidSignal, nodeID, current.LifecycleState)
	}

	maxAttempts := maxNodeAttempts(policy)
	if nextAttempt > maxAttempts {
		return fmt.Errorf("%w: node %q retry attempt %d exceeds max attempts %d", agentoscore.ErrInvalidSignal, nodeID, nextAttempt, maxAttempts)
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
