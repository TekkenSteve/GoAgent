package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/agentos/nexusapi"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/workflow"
)

// The AgentOS Nexus bridge exposes the run-control boundary as Nexus
// operations so any Temporal workflow — in this namespace, another namespace,
// or an external cluster — can start an AgentOS run and await its terminal
// state as a durable promise, or steer a running one. This is the inbound
// half of the "durable bridge": GoAgent is the handler.
//
// Service "AgentOS" (see nexusapi.ServiceName) on the endpoint named by
// NexusEndpointName provides four operations:
//
//	run          async  — starts a backend-owned run and completes with its
//	                     terminal RunStatus; canceling the operation cancels
//	                     the run (durable promise semantics).
//	run.signal   sync   — delivers a control-plane signal to a running run.
//	run.control  sync   — delivers pause/resume/cancel to a running run.
//	run.status   sync   — reads the current RunStatus of a run.
//
// The async run operation is backed by NexusRunOperationWorkflow (via
// temporalnexus.NewWorkflowRunOperationWithOptions): operation start maps to a
// deterministic workflow ID derived from the caller's RequestID, so retried
// starts bind to the same execution; operation completion maps to workflow
// completion; operation cancellation cancels that workflow, which then
// best-effort cancels the underlying run before exiting.

const (
	// NexusRunOperationWorkflowName backs the async run operation.
	NexusRunOperationWorkflowName = "AgentOSNexusRunOperationWorkflow"

	// NexusStartRunActivityName starts a run through the AgentOS runtime.
	NexusStartRunActivityName = "AgentOSNexusStartRun"

	// NexusStatusRunActivityName reads run status through the AgentOS runtime.
	NexusStatusRunActivityName = "AgentOSNexusStatusRun"

	// NexusCancelRunActivityName cancels a run through the AgentOS runtime.
	NexusCancelRunActivityName = "AgentOSNexusCancelRun"
)

const (
	nexusRunPollInterval = 5 * time.Second

	nexusRunStartActivityTimeout  = time.Minute
	nexusRunStatusActivityTimeout = 30 * time.Second
	nexusRunCancelActivityTimeout = 30 * time.Second

	nexusRunActivityInitialInterval = time.Second
	nexusRunActivityMaxInterval     = 10 * time.Second

	// nexusRetryBackoffCoefficient is stated explicitly because the retry
	// windows below are computed from it: a policy whose coefficient is left to
	// the SDK default cannot be reasoned about here.
	nexusRetryBackoffCoefficient = 2.0

	// nexusRunStartMaxAttempts is not a latency budget: a run's ownership row
	// (run_backend_index) is written by the plan's own state persist, which can
	// still be committing that row when the node's run start is already
	// scheduled. Every bind attempt inside that window fails the plan-node
	// foreign key, so the start has to keep retrying at least as long as the
	// persist can legitimately hold it — otherwise a healthy run dies on a
	// bookkeeping race. nexusRunStartRetryWindow states that budget as a
	// duration and TestNexusRunStartRetryCoversPlanStateDurability holds it
	// against the persist activity's timeout.
	nexusRunStartInitialInterval = time.Second
	nexusRunStartMaxInterval     = 30 * time.Second
	nexusRunStartMaxAttempts     = 16
	nexusRunStatusMaxAttempts    = 5
	nexusRunCancelMaxAttempts    = 2

	// nexusRunScheduleToStartTimeout bounds how long an operation may wait for a
	// handler to start it. Without it a handler fleet that is not polling would
	// silently consume the caller's whole schedule-to-close budget.
	nexusRunScheduleToStartTimeout = 5 * time.Minute

	nexusRunWorkflowIDPrefix = "agentos-nexus-run-"
	nexusRunIDPrefix         = "nexus-run-"
)

var (
	// ErrNexusRunNilRuntime reports a Nexus run bridge without an AgentOS runtime.
	ErrNexusRunNilRuntime = errors.New("agentos nexus run: runtime is required")

	// Caller-facing handler error messages. Static so the mapping stays free of
	// dynamically constructed errors; the underlying failure is wrapped too, so
	// it still reaches server-side logs.
	errNexusRunNotFound        = errors.New("run not found")
	errNexusBadRequest         = errors.New("invalid request")
	errNexusBackendTimedOut    = errors.New("backend timed out")
	errNexusBackendUnavailable = errors.New("backend unavailable")

	// ErrNexusRunActivityQueueRequired reports a missing activity task queue.
	ErrNexusRunActivityQueueRequired = errors.New("agentos nexus run: activity task queue is required")
)

// nexusRunWorkflowInput is the run-operation workflow input: the caller's
// request plus the queue its bridge activities run on.
type nexusRunWorkflowInput struct {
	Request           nexusapi.RunRequest `json:"request"`
	ActivityTaskQueue string              `json:"activity_task_queue"`
}

type nexusRunActivityInput struct {
	Spec      *agentos.RunSpec       `json:"spec"`
	PlanScope *nexusapi.RunPlanScope `json:"plan_scope,omitempty"`
}

type nexusRunStatusInput struct {
	RunID     string `json:"run_id"`
	AccountID string `json:"account_id,omitempty"`
	ProjectID string `json:"project_id,omitempty"`
}

type nexusRunControlInput struct {
	RunID          string `json:"run_id"`
	AccountID      string `json:"account_id,omitempty"`
	ProjectID      string `json:"project_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// NexusRunWorkflowID returns the deterministic workflow ID for one request.
func NexusRunWorkflowID(requestID string) string {
	return nexusRunWorkflowIDPrefix + requestID
}

// NexusRunOperationWorkflow backs the async run operation: it starts the run
// through the AgentOS runtime, waits for its terminal state, and returns that
// state as the operation result. Cancellation of the workflow (which the
// WorkflowRunOperation maps from operation cancellation) triggers a
// best-effort cancel of the underlying run.
func NexusRunOperationWorkflow(ctx workflow.Context, in nexusRunWorkflowInput) (nexusapi.RunOutput, error) {
	spec := withDeterministicRunID(in.Request)

	started, err := startRunViaActivity(ctx, in.ActivityTaskQueue, spec, in.Request.PlanScope)
	if err != nil {
		return nexusapi.RunOutput{}, err
	}

	terminal, err := awaitRunTerminal(ctx, in.ActivityTaskQueue, &started, in.Request.PlanScope, runTenantRef(in.Request.Spec), nexusOperationCancelKey(in.Request.RequestID))
	if err != nil {
		return nexusapi.RunOutput{}, err
	}

	return nexusapi.RunOutput{Status: terminal}, nil
}

// withDeterministicRunID derives the RunID from the request's idempotency key
// when the caller did not pin one, so activity retries and workflow replays
// converge on the same backend-owned run.
func withDeterministicRunID(req nexusapi.RunRequest) *agentos.RunSpec {
	spec := req.Spec
	if spec == nil || spec.RunID != "" {
		return spec
	}

	next := *spec
	next.RunID = nexusRunIDPrefix + req.RequestID

	return &next
}

// nexusRunStartRetryPolicy retries long enough to outlast the plan-state
// persist that makes the run's ownership row visible. Both the policy and the
// window it computes come from here so they cannot drift apart.
func nexusRunStartRetryPolicy() *temporal.RetryPolicy {
	return &temporal.RetryPolicy{
		InitialInterval:    nexusRunStartInitialInterval,
		BackoffCoefficient: nexusRetryBackoffCoefficient,
		MaximumInterval:    nexusRunStartMaxInterval,
		MaximumAttempts:    nexusRunStartMaxAttempts,
	}
}

// nexusRunStartRetryWindow is the total time the start activity keeps retrying
// under nexusRunStartRetryPolicy.
func nexusRunStartRetryWindow() time.Duration {
	return retryWindow(
		nexusRunStartInitialInterval,
		nexusRunStartMaxInterval,
		nexusRetryBackoffCoefficient,
		nexusRunStartMaxAttempts,
	)
}

// retryWindow sums Temporal's interval backoff for a retry policy: each
// retry waits min(initial*coefficient^(attempt-1), maximum), and the policy
// makes totalAttempts attempts.
func retryWindow(initial, maximum time.Duration, coefficient float64, totalAttempts int) time.Duration {
	var window time.Duration

	interval := initial

	for attempt := 1; attempt < totalAttempts; attempt++ {
		window += interval

		next := time.Duration(float64(interval) * coefficient)
		if next > maximum {
			next = maximum
		}

		interval = next
	}

	return window
}

// runTenant is the tenant a Nexus run belongs to, carried from the request
// spec into the activities that address the run by id.
type runTenant struct {
	AccountID string
	ProjectID string
}

func runTenantRef(spec *agentos.RunSpec) runTenant {
	if spec == nil {
		return runTenant{}
	}

	return runTenant{AccountID: spec.AccountID, ProjectID: spec.ProjectID}
}

func startRunViaActivity(ctx workflow.Context, taskQueue string, spec *agentos.RunSpec, planScope *nexusapi.RunPlanScope) (agentos.RunStatus, error) {
	actCtx := workflow.WithTaskQueue(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: nexusRunStartActivityTimeout,
		RetryPolicy:         nexusRunStartRetryPolicy(),
	}), taskQueue)

	var started agentos.RunStatus
	if err := workflow.ExecuteActivity(actCtx, NexusStartRunActivityName, nexusRunActivityInput{
		Spec:      spec,
		PlanScope: planScope,
	}).Get(actCtx, &started); err != nil {
		return agentos.RunStatus{}, fmt.Errorf("nexus run operation - start run: %w", err)
	}

	return started, nil
}

func awaitRunTerminal(
	ctx workflow.Context,
	taskQueue string,
	started *agentos.RunStatus,
	planScope *nexusapi.RunPlanScope,
	tenant runTenant,
	operationCancelKey string,
) (agentos.RunStatus, error) {
	statusCtx := workflow.WithTaskQueue(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: nexusRunStatusActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    nexusRunActivityInitialInterval,
			BackoffCoefficient: nexusRetryBackoffCoefficient,
			MaximumInterval:    nexusRunActivityMaxInterval,
			MaximumAttempts:    nexusRunStatusMaxAttempts,
		},
	}), taskQueue)

	status := *started
	for !runTerminal(status.LifecycleState) {
		if err := workflow.Sleep(ctx, nexusRunPollInterval); err != nil {
			if temporal.IsCanceledError(err) {
				// A canceled operation must stop the run it started. The run
				// is a separate workflow on a separate queue: it does not stop
				// because its caller's operation did, and a caller that is not
				// a plan has nobody else to cancel it. Without this, canceling
				// the operation leaves an agent running and spending while the
				// caller has been told the work is over.
				//
				// A plan-scoped run is the exception: the plan cancels its own
				// nodes under its own key, and this operation being canceled is
				// how the plan asks for it. Canceling again here would be a
				// second control for a run the plan already stopped.
				if planScope == nil {
					return agentos.RunStatus{}, cancelRunBestEffort(statusCtx, status.RunID, operationCancelKey, tenant, err)
				}

				return agentos.RunStatus{}, err
			}

			return agentos.RunStatus{}, cancelRunBestEffort(statusCtx, status.RunID, nexusCancelIdempotencyKey(planScope, status.RunID), tenant, err)
		}

		var current agentos.RunStatus
		if err := workflow.ExecuteActivity(statusCtx, NexusStatusRunActivityName, nexusRunStatusInput{
			RunID:     status.RunID,
			AccountID: tenant.AccountID,
			ProjectID: tenant.ProjectID,
		}).Get(statusCtx, &current); err != nil {
			return agentos.RunStatus{}, fmt.Errorf("nexus run operation - status run: %w", err)
		}

		status = current
	}

	return status, nil
}

// cancelRunBestEffort requests run cancellation on a context disconnected
// from the canceled parent (the workflow context is already canceled at this
// point) and returns the original cancellation cause regardless of the
// control outcome.
func cancelRunBestEffort(statusCtx workflow.Context, runID, idempotencyKey string, tenant runTenant, cause error) error {
	if runID == "" {
		return cause
	}

	detached, _ := workflow.NewDisconnectedContext(workflow.WithActivityOptions(statusCtx, workflow.ActivityOptions{
		StartToCloseTimeout: nexusRunCancelActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: nexusRunActivityInitialInterval,
			MaximumInterval: nexusRunActivityMaxInterval,
			MaximumAttempts: nexusRunCancelMaxAttempts,
		},
	}))

	if err := workflow.ExecuteActivity(detached, NexusCancelRunActivityName, nexusRunControlInput{
		RunID:          runID,
		AccountID:      tenant.AccountID,
		ProjectID:      tenant.ProjectID,
		IdempotencyKey: idempotencyKey,
	}).Get(detached, nil); err != nil {
		workflow.GetLogger(statusCtx).Warn("nexus run operation - best-effort cancel failed", "error", err)
	}

	return cause
}

// nexusOperationCancelKeyPrefix names the key family for canceling the run a
// Nexus operation started, as opposed to any other control aimed at the same
// run.
const nexusOperationCancelKeyPrefix = "nexus-cancel:"

// nexusOperationCancelKey derives the deterministic cancelation key for the run
// an operation started: the operation's own idempotency key, so a retried
// operation cancels once rather than repeatedly.
func nexusOperationCancelKey(requestID string) string {
	if requestID == "" {
		return ""
	}

	return nexusOperationCancelKeyPrefix + requestID
}

// nexusCancelIdempotencyKey derives the deterministic cancelation key for a
// plan-scoped run so repeated best-effort cancels stay idempotent. Runs
// without a plan scope carry no key.
func nexusCancelIdempotencyKey(planScope *nexusapi.RunPlanScope, runID string) string {
	if planScope == nil || runID == "" {
		return ""
	}

	key, err := agentosplan.NodeTimeoutControlIdempotencyKey(planScope.PlanID, planScope.NodeID, runID)
	if err != nil {
		return ""
	}

	return key
}

// NexusRunActivities bridge Nexus operations to the AgentOS runtime.
type NexusRunActivities struct {
	Runtime agentos.Runtime

	// PlanNodeStarter routes plan-scoped run starts through plan node
	// ownership bookkeeping. Optional: nil means the bridge only serves
	// external callers and plan-scoped requests fall back to Runtime.Start.
	PlanNodeStarter PlanNodeStarter
}

// NewNexusRunActivities creates the Nexus run activities. planStarter may be
// nil for a bridge-only deployment.
func NewNexusRunActivities(runtime agentos.Runtime, planStarter PlanNodeStarter) (*NexusRunActivities, error) {
	if runtime == nil {
		return nil, ErrNexusRunNilRuntime
	}

	return &NexusRunActivities{Runtime: runtime, PlanNodeStarter: planStarter}, nil
}

// StartRunActivity starts a backend-owned run via the AgentOS runtime.
// Plan-scoped requests go through the plan node starter so ownership is
// recorded atomically with the start; everything else starts directly.
func (a *NexusRunActivities) StartRunActivity(ctx context.Context, in nexusRunActivityInput) (agentos.RunStatus, error) {
	if in.PlanScope != nil && a.PlanNodeStarter != nil {
		return a.startPlanScopedRun(ctx, in)
	}

	status, err := a.Runtime.Start(ctx, in.Spec)
	if err != nil {
		return agentos.RunStatus{}, fmt.Errorf("NexusRunActivities - StartRunActivity: %w", err)
	}

	return status, nil
}

func (a *NexusRunActivities) startPlanScopedRun(ctx context.Context, in nexusRunActivityInput) (agentos.RunStatus, error) {
	status, err := a.PlanNodeStarter.StartPlanNode(ctx, in.PlanScope.PlanID, in.PlanScope.NodeID, in.Spec)
	if err != nil {
		return agentos.RunStatus{}, fmt.Errorf("NexusRunActivities - StartRunActivity - plan node %s: %w", in.PlanScope.NodeID, err)
	}

	if status.RunID == "" {
		status.RunID = in.Spec.RunID
	}

	if status.RunID != in.Spec.RunID {
		return agentos.RunStatus{}, fmt.Errorf("%w: backend returned run id %q for requested run %q",
			agentoscore.ErrInvalidRunSpec, status.RunID, in.Spec.RunID)
	}

	return status, nil
}

// StatusRunActivity reads run status via the AgentOS runtime.
func (a *NexusRunActivities) StatusRunActivity(ctx context.Context, in nexusRunStatusInput) (agentos.RunStatus, error) {
	status, err := a.Runtime.Status(ctx, agentos.RunRef{
		RunID:     in.RunID,
		AccountID: in.AccountID,
		ProjectID: in.ProjectID,
	})
	if err != nil {
		return agentos.RunStatus{}, fmt.Errorf("NexusRunActivities - StatusRunActivity: %w", err)
	}

	return status, nil
}

// CancelRunActivity requests run cancellation via the AgentOS runtime.
func (a *NexusRunActivities) CancelRunActivity(ctx context.Context, in nexusRunControlInput) error {
	if err := a.Runtime.Control(ctx, agentos.RunRef{
		RunID:     in.RunID,
		AccountID: in.AccountID,
		ProjectID: in.ProjectID,
	}, &agentoscore.ControlRequest{
		Operation:      agentoscore.ControlCancel,
		IdempotencyKey: in.IdempotencyKey,
		RequestedAt:    time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("NexusRunActivities - CancelRunActivity: %w", err)
	}

	return nil
}

// NewNexusRunService assembles the AgentOS Nexus service against one AgentOS
// runtime. The async run operation dispatches its activities to
// activityTaskQueue (the plan-activity queue in the default deployment).
func NewNexusRunService(runtime agentos.Runtime, activityTaskQueue string) (*nexus.Service, error) {
	if runtime == nil {
		return nil, ErrNexusRunNilRuntime
	}

	if strings.TrimSpace(activityTaskQueue) == "" {
		return nil, ErrNexusRunActivityQueueRequired
	}

	runOp, err := newNexusRunOperation(activityTaskQueue)
	if err != nil {
		return nil, err
	}

	signalOp := newNexusRunSignalOperation(runtime)
	controlOp := newNexusRunControlOperation(runtime)
	statusOp := newNexusRunStatusOperation(runtime)

	service := nexus.NewService(nexusapi.ServiceName)
	if err := service.Register(runOp, signalOp, controlOp, statusOp); err != nil {
		return nil, fmt.Errorf("agentos nexus run service - register operations: %w", err)
	}

	return service, nil
}

// newNexusRunOperation builds the async run operation: operation start maps to
// the run-operation workflow with a deterministic ID derived from the
// caller's RequestID.
func newNexusRunOperation(activityTaskQueue string) (nexus.Operation[nexusapi.RunRequest, nexusapi.RunOutput], error) {
	runOp, err := temporalnexus.NewWorkflowRunOperationWithOptions(
		temporalnexus.WorkflowRunOperationOptions[nexusapi.RunRequest, nexusapi.RunOutput]{
			Name: nexusapi.RunOperationName,
			Handler: func(ctx context.Context, req nexusapi.RunRequest, opts nexus.StartOperationOptions) (temporalnexus.WorkflowHandle[nexusapi.RunOutput], error) {
				if req.RequestID == "" {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "request_id is required")
				}

				return temporalnexus.ExecuteWorkflow(ctx, opts, client.StartWorkflowOptions{
					ID: NexusRunWorkflowID(req.RequestID),
				}, NexusRunOperationWorkflow, nexusRunWorkflowInput{
					Request:           req,
					ActivityTaskQueue: activityTaskQueue,
				})
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("agentos nexus run service - run operation: %w", err)
	}

	return runOp, nil
}

// nexusRunRef is the reference used by the tenant-less Nexus operations.
//
// The Nexus API is an inter-cell service contract: it carries a run id and no
// tenant, because the trust boundary there is the cell deployment, not a user
// credential (see agentos-panorama.md, "多城是对等部署互发事实"). The empty
// account states exactly that — these callers were never told a tenant, so
// they enforce none — rather than inventing one from the request.
func nexusRunRef(runID string) agentos.RunRef {
	return agentos.RunRef{RunID: runID}
}

func newNexusRunSignalOperation(runtime agentos.Runtime) nexus.Operation[nexusapi.RunSignalRequest, struct{}] {
	return nexus.NewSyncOperation(
		nexusapi.RunSignalOperationName,
		func(ctx context.Context, req nexusapi.RunSignalRequest, _ nexus.StartOperationOptions) (struct{}, error) {
			if err := runtime.Signal(ctx, nexusRunRef(req.RunID), req.Signal); err != nil {
				return struct{}{}, mapNexusRunError(err, "run.signal")
			}

			return struct{}{}, nil
		},
	)
}

func newNexusRunControlOperation(runtime agentos.Runtime) nexus.Operation[nexusapi.RunControlRequest, struct{}] {
	return nexus.NewSyncOperation(
		nexusapi.RunControlOperationName,
		func(ctx context.Context, req nexusapi.RunControlRequest, _ nexus.StartOperationOptions) (struct{}, error) {
			if err := runtime.Control(ctx, nexusRunRef(req.RunID), req.Control); err != nil {
				return struct{}{}, mapNexusRunError(err, "run.control")
			}

			return struct{}{}, nil
		},
	)
}

func newNexusRunStatusOperation(runtime agentos.Runtime) nexus.Operation[nexusapi.RunStatusRequest, nexusapi.RunStatusOutput] {
	return nexus.NewSyncOperation(
		nexusapi.RunStatusOperationName,
		func(ctx context.Context, req nexusapi.RunStatusRequest, _ nexus.StartOperationOptions) (nexusapi.RunStatusOutput, error) {
			status, err := runtime.Status(ctx, nexusRunRef(req.RunID))
			if err != nil {
				return nexusapi.RunStatusOutput{}, mapNexusRunError(err, "run.status")
			}

			return nexusapi.RunStatusOutput{Status: status}, nil
		},
	)
}

// mapNexusRunError translates AgentOS failures into typed Nexus handler
// errors. Every branch has to classify deliberately: Nexus retries handler
// errors by default, and five consecutive retryable errors trip the Endpoint's
// circuit breaker — which would block every operation from the caller to this
// Endpoint, not just the failing one. Caller-fixable failures therefore map to
// non-retryable types, and only genuine backend unavailability stays
// retryable. The underlying cause is attached for server-side logging while
// callers receive a generic message.
func mapNexusRunError(err error, operation string) error {
	switch {
	case errors.Is(err, agentoscore.ErrRunRouteNotFound), errors.Is(err, agentoscore.ErrBackendNotFound):
		return &nexus.HandlerError{Type: nexus.HandlerErrorTypeNotFound, Cause: fmt.Errorf("%s: %w: %w", operation, errNexusRunNotFound, err)}
	case errors.Is(err, agentoscore.ErrInvalidRunSpec),
		errors.Is(err, agentoscore.ErrInvalidBackendRef),
		errors.Is(err, agentoscore.ErrInvalidControlOperation),
		errors.Is(err, agentoscore.ErrInvalidSignal):
		return &nexus.HandlerError{Type: nexus.HandlerErrorTypeBadRequest, Cause: fmt.Errorf("%s: %w: %w", operation, errNexusBadRequest, err)}
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return &nexus.HandlerError{Type: nexus.HandlerErrorTypeUpstreamTimeout, Cause: fmt.Errorf("%s: %w: %w", operation, errNexusBackendTimedOut, err)}
	default:
		return &nexus.HandlerError{Type: nexus.HandlerErrorTypeUnavailable, Cause: fmt.Errorf("%s: %w: %w", operation, errNexusBackendUnavailable, err)}
	}
}
