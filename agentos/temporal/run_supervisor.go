package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosruntime "github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// This file gives every external-backend run a Temporal workflow owner.
//
// Backends that are not themselves Temporal workflows (HTTP, gRPC, DSH) used to
// answer signal/control/status by calling the backend inline. That put external
// network I/O on the caller's path — including inside Nexus synchronous
// handlers, which must stay reliable, low latency and well inside the handler
// deadline. The supervisor moves that I/O into durable activities and answers
// the control path with Temporal primitives (Query, Signal) only:
//
//	run.status  -> Query   the supervisor's cached status
//	run.signal  -> Signal  the supervisor, which applies it as an activity
//	run.control -> Signal  the supervisor, which applies it as an activity
//
// The start stays direct: it already runs inside an activity (the run operation
// and the plan bridge both start runs from activities), not inside a handler.

const (
	// RunSupervisorWorkflowName backs an external run's control-plane owner.
	RunSupervisorWorkflowName = "AgentOSRunSupervisorWorkflow"

	// RunSupervisorStatusQueryName reports the supervisor's cached run status.
	RunSupervisorStatusQueryName = "AgentOSRunSupervisorStatus"
	// RunSupervisorSignalName delivers a signal to the supervised run.
	RunSupervisorSignalName = "AgentOSRunSupervisorSignal"
	// RunSupervisorControlName delivers a control request to the supervised run.
	RunSupervisorControlName = "AgentOSRunSupervisorControl"

	// RunSupervisorStatusActivityName reads backend status for the supervisor.
	RunSupervisorStatusActivityName = "AgentOSRunSupervisorReadStatus"
	// RunSupervisorSignalActivityName applies a signal to the backend run.
	RunSupervisorSignalActivityName = "AgentOSRunSupervisorApplySignal"
	// RunSupervisorControlActivityName applies a control request to the backend run.
	RunSupervisorControlActivityName = "AgentOSRunSupervisorApplyControl"

	runSupervisorIDPrefix        = "agentos-run-supervisor-"
	runSupervisorPollInterval    = 5 * time.Second
	runSupervisorActivityTimeout = time.Minute
	runSupervisorMaxAttempts     = 5
	runSupervisorMaxInterval     = 30 * time.Second
)

var (
	// ErrRunSupervisorBackendResolverRequired reports a supervisor without a
	// way to reach the backends it supervises.
	ErrRunSupervisorBackendResolverRequired = errors.New("agentos temporal run supervisor: backend resolver is required")
	// ErrRunSupervisorClientRequired reports a supervisor scheduler without a
	// Temporal client.
	ErrRunSupervisorClientRequired = errors.New("agentos temporal run supervisor: temporal client is required")
	// ErrRunSupervisorTaskQueueRequired reports a supervisor scheduler without
	// a task queue to run supervisors on.
	ErrRunSupervisorTaskQueueRequired = errors.New("agentos temporal run supervisor: task queue is required")
)

// RunSupervisorInput starts one run's supervisor. The run is already started:
// Status is its status as observed by the direct start.
type RunSupervisorInput struct {
	Ref    agentos.BackendRef `json:"ref"`
	Spec   agentos.RunSpec    `json:"spec"`
	Status agentos.RunStatus  `json:"status"`
}

// RunSupervisorID returns the deterministic workflow ID owning one run.
func RunSupervisorID(runID string) string {
	return runSupervisorIDPrefix + runID
}

// RunSupervisorWorkflow owns one external run's control plane: it answers
// signal/control/status from its own durable state and applies changes to the
// backend through activities.
func RunSupervisorWorkflow(ctx workflow.Context, in *RunSupervisorInput) (agentos.RunStatus, error) {
	status := in.Status

	if err := workflow.SetQueryHandler(ctx, RunSupervisorStatusQueryName, func() (agentos.RunStatus, error) {
		return status, nil
	}); err != nil {
		return status, err
	}

	signalCh := workflow.GetSignalChannel(ctx, RunSupervisorSignalName)
	controlCh := workflow.GetSignalChannel(ctx, RunSupervisorControlName)
	actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: runSupervisorActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumInterval: runSupervisorMaxInterval,
			MaximumAttempts: runSupervisorMaxAttempts,
		},
	})

	for !runTerminal(status.LifecycleState) {
		if err := drainSupervisorSignals(actCtx, in.Ref, status.RunID, signalCh); err != nil {
			return status, err
		}

		controlled, err := drainSupervisorControls(actCtx, in.Ref, status.RunID, controlCh)
		if err != nil {
			return status, err
		}

		if controlled {
			refreshed, err := readSupervisedStatus(actCtx, in.Ref, status.RunID)
			if err != nil {
				return status, err
			}

			status = refreshed

			continue
		}

		if err := workflow.Sleep(ctx, runSupervisorPollInterval); err != nil {
			return status, cancelSupervisedRunOnExit(actCtx, ctx, in.Ref, status.RunID, err)
		}

		refreshed, err := readSupervisedStatus(actCtx, in.Ref, status.RunID)
		if err != nil {
			return status, err
		}

		status = refreshed
	}

	return status, nil
}

func drainSupervisorSignals(actCtx workflow.Context, ref agentos.BackendRef, runID string, ch workflow.ReceiveChannel) error {
	var signal agentoscore.Signal

	for ch.ReceiveAsync(&signal) {
		if err := workflow.ExecuteActivity(actCtx, RunSupervisorSignalActivityName, runSupervisorSignalInput{
			Ref:    ref,
			RunID:  runID,
			Signal: &signal,
		}).Get(actCtx, nil); err != nil {
			return fmt.Errorf("run supervisor - apply signal: %w", err)
		}
	}

	return nil
}

// drainSupervisorControls applies every pending control request and reports
// whether any was applied, so the caller can refresh the cached status.
func drainSupervisorControls(actCtx workflow.Context, ref agentos.BackendRef, runID string, ch workflow.ReceiveChannel) (bool, error) {
	applied := false

	var control agentoscore.ControlRequest

	for ch.ReceiveAsync(&control) {
		if err := workflow.ExecuteActivity(actCtx, RunSupervisorControlActivityName, runSupervisorControlInput{
			Ref:     ref,
			RunID:   runID,
			Control: &control,
		}).Get(actCtx, nil); err != nil {
			return applied, fmt.Errorf("run supervisor - apply control: %w", err)
		}

		applied = true
	}

	return applied, nil
}

func readSupervisedStatus(actCtx workflow.Context, ref agentos.BackendRef, runID string) (agentos.RunStatus, error) {
	var status agentos.RunStatus
	if err := workflow.ExecuteActivity(actCtx, RunSupervisorStatusActivityName, runSupervisorStatusInput{
		Ref:   ref,
		RunID: runID,
	}).Get(actCtx, &status); err != nil {
		return agentos.RunStatus{}, fmt.Errorf("run supervisor - read status: %w", err)
	}

	return status, nil
}

// cancelSupervisedRunOnExit best-effort cancels a run whose supervisor is being
// canceled, on a context detached from the canceled parent.
func cancelSupervisedRunOnExit(actCtx, ctx workflow.Context, ref agentos.BackendRef, runID string, cause error) error {
	if runID == "" {
		return cause
	}

	detached, _ := workflow.NewDisconnectedContext(actCtx)

	if err := workflow.ExecuteActivity(detached, RunSupervisorControlActivityName, runSupervisorControlInput{
		Ref:   ref,
		RunID: runID,
		Control: &agentoscore.ControlRequest{
			Operation: agentoscore.ControlCancel,
		},
	}).Get(detached, nil); err != nil {
		workflow.GetLogger(ctx).Warn("run supervisor - best-effort cancel failed", "run_id", runID, "error", err)
	}

	return cause
}

type runSupervisorStatusInput struct {
	Ref   agentos.BackendRef `json:"ref"`
	RunID string             `json:"run_id"`
}

type runSupervisorSignalInput struct {
	Ref    agentos.BackendRef  `json:"ref"`
	RunID  string              `json:"run_id"`
	Signal *agentoscore.Signal `json:"signal"`
}

type runSupervisorControlInput struct {
	Ref     agentos.BackendRef          `json:"ref"`
	RunID   string                      `json:"run_id"`
	Control *agentoscore.ControlRequest `json:"control"`
}

// RunSupervisorActivities apply supervisor decisions to the backend a run
// belongs to. They deliberately bypass the supervised runtime so applying a
// control cannot route back into the supervisor that issued it.
type RunSupervisorActivities struct {
	Resolve func(ref agentos.BackendRef) (agentosruntime.AgentBackend, error)
}

// NewRunSupervisorActivities creates the supervisor activities.
func NewRunSupervisorActivities(resolve func(ref agentos.BackendRef) (agentosruntime.AgentBackend, error)) (*RunSupervisorActivities, error) {
	if resolve == nil {
		return nil, ErrRunSupervisorBackendResolverRequired
	}

	return &RunSupervisorActivities{Resolve: resolve}, nil
}

// ReadStatusActivity reads backend run status.
func (a *RunSupervisorActivities) ReadStatusActivity(ctx context.Context, in runSupervisorStatusInput) (agentos.RunStatus, error) {
	backend, err := a.Resolve(in.Ref)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	status, err := backend.Status(ctx, in.RunID)
	if err != nil {
		return agentos.RunStatus{}, fmt.Errorf("RunSupervisorActivities - ReadStatusActivity: %w", err)
	}

	return status, nil
}

// ApplySignalActivity applies one signal to the backend run.
func (a *RunSupervisorActivities) ApplySignalActivity(ctx context.Context, in runSupervisorSignalInput) error {
	if in.Signal == nil {
		return agentoscore.ErrInvalidSignal
	}

	backend, err := a.Resolve(in.Ref)
	if err != nil {
		return err
	}

	if err := backend.Signal(ctx, in.RunID, in.Signal); err != nil {
		return fmt.Errorf("RunSupervisorActivities - ApplySignalActivity: %w", err)
	}

	return nil
}

// ApplyControlActivity applies one control request to the backend run.
func (a *RunSupervisorActivities) ApplyControlActivity(ctx context.Context, in runSupervisorControlInput) error {
	if in.Control == nil {
		return agentoscore.ErrInvalidControlOperation
	}

	backend, err := a.Resolve(in.Ref)
	if err != nil {
		return err
	}

	if err := backend.Control(ctx, in.RunID, in.Control); err != nil {
		return fmt.Errorf("RunSupervisorActivities - ApplyControlActivity: %w", err)
	}

	return nil
}

// RunSupervisor is the control-plane handle a supervised backend uses to reach
// the workflow owning its runs.
type RunSupervisor interface {
	// Ensure makes sure a supervisor owns the run described by in.
	Ensure(ctx context.Context, in *RunSupervisorInput) error
	// Status returns the supervised run status; found is false when no
	// supervisor owns the run.
	Status(ctx context.Context, runID string) (agentos.RunStatus, bool, error)
	// Signal forwards a signal; forwarded is false when no supervisor owns the run.
	Signal(ctx context.Context, runID string, signal *agentoscore.Signal) (bool, error)
	// Control forwards a control request; forwarded is false when no supervisor owns the run.
	Control(ctx context.Context, runID string, control *agentoscore.ControlRequest) (bool, error)
}

// SupervisorClient reaches supervisors through the Temporal client.
type SupervisorClient struct {
	Client    client.Client
	TaskQueue string
}

// NewSupervisorClient creates the client-backed supervisor handle.
func NewSupervisorClient(c client.Client, taskQueue string) (*SupervisorClient, error) {
	if strings.TrimSpace(taskQueue) == "" {
		return nil, ErrRunSupervisorTaskQueueRequired
	}

	if c == nil {
		return nil, ErrRunSupervisorClientRequired
	}

	return &SupervisorClient{Client: c, TaskQueue: taskQueue}, nil
}

// Ensure starts the run's supervisor if none is running. An existing supervisor
// wins: the run is already owned, and the input is only a starting snapshot.
func (s *SupervisorClient) Ensure(ctx context.Context, in *RunSupervisorInput) error {
	if in.Status.RunID == "" {
		return fmt.Errorf("%w: supervised run id is required", agentoscore.ErrInvalidRunSpec)
	}

	options := client.StartWorkflowOptions{
		ID:        RunSupervisorID(in.Status.RunID),
		TaskQueue: s.TaskQueue,
		// A run is supervised exactly once: reuse the running supervisor and
		// allow a fresh one if the previous supervisor already completed.
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}

	_, err := s.Client.ExecuteWorkflow(ctx, options, RunSupervisorWorkflowName, in)
	if err != nil {
		return fmt.Errorf("run supervisor - ensure: %w", err)
	}

	return nil
}

// Status queries the run's supervisor.
func (s *SupervisorClient) Status(ctx context.Context, runID string) (agentos.RunStatus, bool, error) {
	value, err := s.Client.QueryWorkflow(ctx, RunSupervisorID(runID), "", RunSupervisorStatusQueryName)
	if err != nil {
		if isSupervisorMissing(err) {
			return agentos.RunStatus{}, false, nil
		}

		return agentos.RunStatus{}, false, fmt.Errorf("run supervisor - query status: %w", err)
	}

	var status agentos.RunStatus
	if err := value.Get(&status); err != nil {
		return agentos.RunStatus{}, false, fmt.Errorf("run supervisor - decode status: %w", err)
	}

	return status, true, nil
}

// Signal forwards a signal to the run's supervisor.
func (s *SupervisorClient) Signal(ctx context.Context, runID string, signal *agentoscore.Signal) (bool, error) {
	if signal == nil {
		return false, agentoscore.ErrInvalidSignal
	}

	if err := s.Client.SignalWorkflow(ctx, RunSupervisorID(runID), "", RunSupervisorSignalName, signal); err != nil {
		if isSupervisorMissing(err) {
			return false, nil
		}

		return false, fmt.Errorf("run supervisor - signal: %w", err)
	}

	return true, nil
}

// Control forwards a control request to the run's supervisor.
func (s *SupervisorClient) Control(ctx context.Context, runID string, control *agentoscore.ControlRequest) (bool, error) {
	if control == nil {
		return false, agentoscore.ErrInvalidControlOperation
	}

	err := s.Client.SignalWorkflow(ctx, RunSupervisorID(runID), "", RunSupervisorControlName, control)
	if err != nil {
		if isSupervisorMissing(err) {
			return false, nil
		}

		return false, fmt.Errorf("run supervisor - control: %w", err)
	}

	return true, nil
}

// isSupervisorMissing reports whether err means no supervisor workflow exists
// for the run, which lets the caller fall back to the backend directly.
func isSupervisorMissing(err error) bool {
	var notFound *serviceerror.NotFound

	return errors.As(err, &notFound)
}

// SupervisedBackend decorates a backend whose runs are not Temporal workflows
// so their control plane is served by a supervisor workflow.
type SupervisedBackend struct {
	inner      agentosruntime.AgentBackend
	ref        agentos.BackendRef
	supervisor RunSupervisor
	// Logger reports supervisor start failures. Nil drops those diagnostics.
	Logger logger.Interface
}

// NewSupervisedBackend wraps one backend with run supervision.
func NewSupervisedBackend(ref agentos.BackendRef, inner agentosruntime.AgentBackend, supervisor RunSupervisor) (*SupervisedBackend, error) {
	if inner == nil {
		return nil, fmt.Errorf("%w: supervised backend is required", agentoscore.ErrInvalidBackendRef)
	}

	if supervisor == nil {
		return nil, fmt.Errorf("%w: run supervisor is required", agentoscore.ErrInvalidBackendRef)
	}

	return &SupervisedBackend{inner: inner, ref: ref, supervisor: supervisor}, nil
}

// Start starts the run as before and hands its control plane to a supervisor.
// Failing to start the supervisor does not fail the run: an unsupervised run
// still works, it just answers control directly.
func (b *SupervisedBackend) Start(ctx context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	status, err := b.inner.Start(ctx, spec)
	if err != nil {
		return status, err
	}

	if err := b.supervisor.Ensure(ctx, &RunSupervisorInput{Ref: b.ref, Spec: *spec, Status: status}); err != nil && b.Logger != nil {
		// The run is already started: failing the call would misreport a
		// successful start. An unsupervised run still works, it just answers
		// control directly.
		b.Logger.Warn("agentos temporal supervised backend - start run supervisor for run %s: %v", status.RunID, err)
	}

	return status, nil
}

// Signal forwards to the supervisor, falling back to the backend when no
// supervisor owns the run.
func (b *SupervisedBackend) Signal(ctx context.Context, runID string, signal *agentoscore.Signal) error {
	forwarded, err := b.supervisor.Signal(ctx, runID, signal)
	if err != nil {
		return err
	}

	if forwarded {
		return nil
	}

	return b.inner.Signal(ctx, runID, signal)
}

// Control forwards to the supervisor, falling back to the backend when no
// supervisor owns the run.
func (b *SupervisedBackend) Control(ctx context.Context, runID string, control *agentoscore.ControlRequest) error {
	forwarded, err := b.supervisor.Control(ctx, runID, control)
	if err != nil {
		return err
	}

	if forwarded {
		return nil
	}

	return b.inner.Control(ctx, runID, control)
}

// Status reads the supervisor's cached status, falling back to the backend when
// no supervisor owns the run.
func (b *SupervisedBackend) Status(ctx context.Context, runID string) (agentos.RunStatus, error) {
	status, supervised, err := b.supervisor.Status(ctx, runID)
	if err != nil {
		return agentos.RunStatus{}, err
	}

	if supervised {
		return status, nil
	}

	return b.inner.Status(ctx, runID)
}

// Subscribe stays on the backend: the supervisor mirrors status, not the run's
// byte stream.
func (b *SupervisedBackend) Subscribe(ctx context.Context, scope agentoscore.StreamScope) (agentoscore.Subscription, error) {
	return b.inner.Subscribe(ctx, scope)
}
