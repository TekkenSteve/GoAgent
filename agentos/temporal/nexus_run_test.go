package temporal

import (
	"context"
	"errors"
	"sync"
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/agentos/nexusapi"
	agentosplan "github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

var (
	errNexusTestBackendUnavailable = errors.New("nexus test: backend unavailable")
	errNexusTestConnectionReset    = errors.New("nexus test: connection reset")
)

// fakeNexusRuntime records every interaction and replays scripted statuses.
// When terminalState is set, Start appends it after the initial running
// status so the workflow's poll loop observes the transition.
type fakeNexusRuntime struct {
	mu sync.Mutex

	terminalState string
	startedSpecs  []*agentos.RunSpec
	signals       map[string][]agentoscore.SignalType
	controls      map[string][]agentoscore.ControlOperation
	statuses      map[string][]agentos.RunStatus
	startErr      error
}

func newFakeNexusRuntime() *fakeNexusRuntime {
	return &fakeNexusRuntime{
		signals:  map[string][]agentoscore.SignalType{},
		controls: map[string][]agentoscore.ControlOperation{},
		statuses: map[string][]agentos.RunStatus{},
	}
}

func (f *fakeNexusRuntime) Start(_ context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.startErr != nil {
		return agentos.RunStatus{}, f.startErr
	}

	f.startedSpecs = append(f.startedSpecs, spec)

	status := agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running"}
	f.statuses[spec.RunID] = append(f.statuses[spec.RunID], status)

	if f.terminalState != "" {
		f.statuses[spec.RunID] = append(f.statuses[spec.RunID], agentos.RunStatus{
			RunID:          spec.RunID,
			LifecycleState: f.terminalState,
		})
	}

	return status, nil
}

func (f *fakeNexusRuntime) Signal(_ context.Context, ref agentos.RunRef, signal *agentoscore.Signal) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if signal == nil {
		return agentoscore.ErrInvalidSignal
	}

	f.signals[ref.RunID] = append(f.signals[ref.RunID], signal.Type)

	return nil
}

func (f *fakeNexusRuntime) Status(_ context.Context, ref agentos.RunRef) (agentos.RunStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	seq := f.statuses[ref.RunID]
	if len(seq) == 0 {
		return agentos.RunStatus{}, agentoscore.ErrRunRouteNotFound
	}

	if len(seq) == 1 {
		return seq[0], nil
	}

	next := seq[0]
	f.statuses[ref.RunID] = seq[1:]

	return next, nil
}

func (f *fakeNexusRuntime) Control(_ context.Context, ref agentos.RunRef, control *agentoscore.ControlRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if control == nil {
		return agentoscore.ErrInvalidControlOperation
	}

	f.controls[ref.RunID] = append(f.controls[ref.RunID], control.Operation)

	return nil
}

func (f *fakeNexusRuntime) Subscribe(context.Context, agentoscore.StreamScope) (agentoscore.Subscription, error) {
	return nil, agentoscore.ErrInvalidStreamScope
}

func (f *fakeNexusRuntime) Close() error {
	return nil
}

func (f *fakeNexusRuntime) startedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.startedSpecs)
}

func newNexusRunTestEnv(rt *fakeNexusRuntime) *testsuite.TestWorkflowEnvironment {
	runActivities, err := NewNexusRunActivities(rt, nil)
	if err != nil {
		panic(err)
	}

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(runActivities.StartRunActivity, activity.RegisterOptions{
		Name: NexusStartRunActivityName,
	})
	env.RegisterActivityWithOptions(runActivities.StatusRunActivity, activity.RegisterOptions{
		Name: NexusStatusRunActivityName,
	})
	env.RegisterActivityWithOptions(runActivities.CancelRunActivity, activity.RegisterOptions{
		Name: NexusCancelRunActivityName,
	})

	return env
}

func nexusRunTestSpec() *agentos.RunSpec {
	return &agentos.RunSpec{
		Backend: agentos.BackendRef{
			Kind: agentos.BackendKindNative,
			Name: agentos.BackendNameGoAgentNative,
		},
		UserMessage: "summarize the city of systems",
	}
}

func TestNexusRunOperationWorkflow_CompletesAtTerminalState(t *testing.T) {
	t.Parallel()

	rt := newFakeNexusRuntime()
	rt.terminalState = SUCCEEDED
	env := newNexusRunTestEnv(rt)

	env.ExecuteWorkflow(NexusRunOperationWorkflow, nexusRunWorkflowInput{
		Request:           nexusapi.RunRequest{RequestID: "req-1", Spec: nexusRunTestSpec()},
		ActivityTaskQueue: "agentos-plan-activity",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var out nexusapi.RunOutput
	require.NoError(t, env.GetWorkflowResult(&out))

	assert.Equal(t, SUCCEEDED, out.Status.LifecycleState)
	require.Len(t, rt.startedSpecs, 1)
}

func TestNexusRunOperationWorkflow_DerivesDeterministicRunID(t *testing.T) {
	t.Parallel()

	rt := newFakeNexusRuntime()
	rt.terminalState = SUCCEEDED
	env := newNexusRunTestEnv(rt)

	env.ExecuteWorkflow(NexusRunOperationWorkflow, nexusRunWorkflowInput{
		Request:           nexusapi.RunRequest{RequestID: "req-42", Spec: nexusRunTestSpec()},
		ActivityTaskQueue: "agentos-plan-activity",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.Len(t, rt.startedSpecs, 1)
	assert.Equal(t, "nexus-run-req-42", rt.startedSpecs[0].RunID)
}

func TestNexusRunOperationWorkflow_PreservesCallerRunID(t *testing.T) {
	t.Parallel()

	rt := newFakeNexusRuntime()
	rt.terminalState = SUCCEEDED
	env := newNexusRunTestEnv(rt)

	spec := nexusRunTestSpec()
	spec.RunID = "caller-run-id"

	env.ExecuteWorkflow(NexusRunOperationWorkflow, nexusRunWorkflowInput{
		Request:           nexusapi.RunRequest{RequestID: "req-7", Spec: spec},
		ActivityTaskQueue: "agentos-plan-activity",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.Len(t, rt.startedSpecs, 1)
	assert.Equal(t, "caller-run-id", rt.startedSpecs[0].RunID)
}

func TestNexusRunOperationWorkflow_FailedRunFailsOperation(t *testing.T) {
	t.Parallel()

	rt := newFakeNexusRuntime()
	rt.terminalState = FAILED
	env := newNexusRunTestEnv(rt)

	env.ExecuteWorkflow(NexusRunOperationWorkflow, nexusRunWorkflowInput{
		Request:           nexusapi.RunRequest{RequestID: "req-9", Spec: nexusRunTestSpec()},
		ActivityTaskQueue: "agentos-plan-activity",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var out nexusapi.RunOutput
	require.NoError(t, env.GetWorkflowResult(&out))

	assert.Equal(t, FAILED, out.Status.LifecycleState)
}

func TestNexusRunOperationWorkflow_StartFailureFailsOperation(t *testing.T) {
	t.Parallel()

	rt := newFakeNexusRuntime()
	rt.startErr = errNexusTestBackendUnavailable
	env := newNexusRunTestEnv(rt)

	env.ExecuteWorkflow(NexusRunOperationWorkflow, nexusRunWorkflowInput{
		Request:           nexusapi.RunRequest{RequestID: "req-fail", Spec: nexusRunTestSpec()},
		ActivityTaskQueue: "agentos-plan-activity",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	assert.Equal(t, 0, rt.startedCount())
}

func TestNewNexusRunService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		runtime   agentos.Runtime
		taskQueue string
		wantErr   error
	}{
		{
			name:      "nil runtime rejected",
			runtime:   nil,
			taskQueue: "agentos-plan-activity",
			wantErr:   ErrNexusRunNilRuntime,
		},
		{
			name:      "empty activity queue rejected",
			runtime:   newFakeNexusRuntime(),
			taskQueue: "  ",
			wantErr:   ErrNexusRunActivityQueueRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewNexusRunService(tt.runtime, tt.taskQueue)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestNewNexusRunService_RegistersAllOperations(t *testing.T) {
	t.Parallel()

	service, err := NewNexusRunService(newFakeNexusRuntime(), "agentos-plan-activity")
	require.NoError(t, err)
	require.NotNil(t, service)

	for _, opName := range []string{
		nexusapi.RunOperationName,
		nexusapi.RunSignalOperationName,
		nexusapi.RunControlOperationName,
		nexusapi.RunStatusOperationName,
	} {
		assert.NotNil(t, service.Operation(opName), "operation %q should be registered", opName)
	}
}

func TestMapNexusRunError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		wantType nexus.HandlerErrorType
	}{
		{
			name:     "route not found maps to nexus not found",
			err:      agentoscore.ErrRunRouteNotFound,
			wantType: nexus.HandlerErrorTypeNotFound,
		},
		{
			name:     "backend not found maps to nexus not found",
			err:      agentoscore.ErrBackendNotFound,
			wantType: nexus.HandlerErrorTypeNotFound,
		},
		{
			name:     "invalid run spec maps to bad request",
			err:      agentoscore.ErrInvalidRunSpec,
			wantType: nexus.HandlerErrorTypeBadRequest,
		},
		{
			name:     "invalid control operation maps to bad request",
			err:      agentoscore.ErrInvalidControlOperation,
			wantType: nexus.HandlerErrorTypeBadRequest,
		},
		{
			name:     "invalid signal maps to bad request",
			err:      agentoscore.ErrInvalidSignal,
			wantType: nexus.HandlerErrorTypeBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mapped := mapNexusRunError(tt.err, "run.status")

			var handlerErr *nexus.HandlerError
			require.ErrorAs(t, mapped, &handlerErr)
			assert.Equal(t, tt.wantType, handlerErr.Type)
		})
	}
}

func TestMapNexusRunError_UnknownErrorsBecomeRetryableUnavailable(t *testing.T) {
	t.Parallel()

	cause := errNexusTestConnectionReset

	mapped := mapNexusRunError(cause, "run.signal")

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, mapped, &handlerErr)
	assert.Equal(t, nexus.HandlerErrorTypeUnavailable, handlerErr.Type)
	assert.ErrorIs(t, handlerErr.Cause, cause, "the cause must stay attached for server-side logging")
}

func TestRegisterNexusRunService(t *testing.T) {
	t.Parallel()

	control := &fakeWorker{}

	err := RegisterNexusRunService(control, newFakeNexusRuntime(), "agentos-plan-activity")
	require.NoError(t, err)

	assert.Equal(t, 1, control.nexusServiceCount())
	assert.True(t, control.workflowRegistered(NexusRunOperationWorkflowName))
}

func TestRegisterNexusRunService_NilWorkerRejected(t *testing.T) {
	t.Parallel()

	require.ErrorIs(t, RegisterNexusRunService(nil, newFakeNexusRuntime(), "agentos-plan-activity"), errWorkerKitNilWorker)
}

func TestRegisterNexusRunActivities(t *testing.T) {
	t.Parallel()

	activityWorker := &fakeWorker{}

	err := RegisterNexusRunActivities(activityWorker, newFakeNexusRuntime(), nil)
	require.NoError(t, err)
	assert.True(t, activityWorker.activityRegistered(NexusStartRunActivityName))
	assert.True(t, activityWorker.activityRegistered(NexusStatusRunActivityName))
	assert.True(t, activityWorker.activityRegistered(NexusCancelRunActivityName))
}

func TestRegisterNexusRunActivities_NilWorkerRejected(t *testing.T) {
	t.Parallel()

	require.ErrorIs(t, RegisterNexusRunActivities(nil, newFakeNexusRuntime(), nil), errWorkerKitNilWorker)
}

func TestWithDeterministicRunID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		request   nexusapi.RunRequest
		wantRunID string
		wantNil   bool
	}{
		{
			name:    "nil spec stays nil",
			request: nexusapi.RunRequest{RequestID: "req-1"},
			wantNil: true,
		},
		{
			name:      "caller run id preserved",
			request:   nexusapi.RunRequest{RequestID: "req-2", Spec: &agentos.RunSpec{RunID: "pinned"}},
			wantRunID: "pinned",
		},
		{
			name:      "missing run id derived from request id",
			request:   nexusapi.RunRequest{RequestID: "req-3", Spec: &agentos.RunSpec{}},
			wantRunID: "nexus-run-req-3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := withDeterministicRunID(tt.request)

			if tt.wantNil {
				assert.Nil(t, spec)

				return
			}

			require.NotNil(t, spec)
			assert.Equal(t, tt.wantRunID, spec.RunID)
		})
	}
}

type fakePlanNodeStarter struct {
	mu       sync.Mutex
	planIDs  []string
	nodeIDs  []string
	specs    []*agentos.RunSpec
	status   agentos.RunStatus
	startErr error
}

func (f *fakePlanNodeStarter) StartPlanNode(_ context.Context, planID, nodeID string, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.startErr != nil {
		return agentos.RunStatus{}, f.startErr
	}

	f.planIDs = append(f.planIDs, planID)
	f.nodeIDs = append(f.nodeIDs, nodeID)
	f.specs = append(f.specs, spec)

	if f.status.RunID == "" {
		return agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running"}, nil
	}

	return f.status, nil
}

func TestStartRunActivity_PlanScopeRoutesThroughStarter(t *testing.T) {
	t.Parallel()

	rt := newFakeNexusRuntime()
	starter := &fakePlanNodeStarter{}
	activities, err := NewNexusRunActivities(rt, starter)
	require.NoError(t, err)

	spec := nexusRunTestSpec()
	spec.RunID = "run-plan-1"

	status, err := activities.StartRunActivity(context.Background(), nexusRunActivityInput{
		Spec:      spec,
		PlanScope: &nexusapi.RunPlanScope{PlanID: "plan-1", NodeID: "node-7"},
	})
	require.NoError(t, err)

	assert.Equal(t, "run-plan-1", status.RunID)
	require.Len(t, starter.specs, 1)
	assert.Equal(t, "plan-1", starter.planIDs[0])
	assert.Equal(t, "node-7", starter.nodeIDs[0])
	assert.Equal(t, 0, rt.startedCount(), "plan-scoped start must bypass the raw runtime")
}

func TestStartRunActivity_WithoutScopeStartsViaRuntime(t *testing.T) {
	t.Parallel()

	rt := newFakeNexusRuntime()
	activities, err := NewNexusRunActivities(rt, &fakePlanNodeStarter{})
	require.NoError(t, err)

	spec := nexusRunTestSpec()
	spec.RunID = "run-external-1"

	status, err := activities.StartRunActivity(context.Background(), nexusRunActivityInput{Spec: spec})
	require.NoError(t, err)

	assert.Equal(t, "run-external-1", status.RunID)
	assert.Equal(t, 1, rt.startedCount())
}

func TestStartRunActivity_PlanScopeRunIDMismatchRejected(t *testing.T) {
	t.Parallel()

	starter := &fakePlanNodeStarter{status: agentos.RunStatus{RunID: "other-run", LifecycleState: "running"}}
	activities, err := NewNexusRunActivities(newFakeNexusRuntime(), starter)
	require.NoError(t, err)

	spec := nexusRunTestSpec()
	spec.RunID = "requested-run"

	_, err = activities.StartRunActivity(context.Background(), nexusRunActivityInput{
		Spec:      spec,
		PlanScope: &nexusapi.RunPlanScope{PlanID: "plan-1", NodeID: "node-7"},
	})

	require.ErrorIs(t, err, agentoscore.ErrInvalidRunSpec)
}

// TestNexusRunStartRetryCoversPlanStateDurability holds the start activity's
// retry budget against the plan-state persist it races with. A run's ownership
// row is written by that persist, so every bind attempt before it commits fails
// the plan-node foreign key; a budget shorter than the persist's own timeout
// lets a healthy run die on a bookkeeping race.
func TestNexusRunStartRetryCoversPlanStateDurability(t *testing.T) {
	t.Parallel()

	window := nexusRunStartRetryWindow()
	if window < defaultActivityTimeout {
		t.Fatalf(
			"nexus run start retries for %s, which does not cover the plan-state persist timeout %s",
			window, defaultActivityTimeout,
		)
	}

	policy := nexusRunStartRetryPolicy()

	if policy.MaximumAttempts != nexusRunStartMaxAttempts {
		t.Fatalf("retry policy attempts = %d, want %d", policy.MaximumAttempts, nexusRunStartMaxAttempts)
	}

	if policy.InitialInterval != nexusRunStartInitialInterval || policy.MaximumInterval != nexusRunStartMaxInterval {
		t.Fatalf("retry policy intervals = %s..%s, want %s..%s",
			policy.InitialInterval, policy.MaximumInterval, nexusRunStartInitialInterval, nexusRunStartMaxInterval)
	}

	if policy.BackoffCoefficient != nexusRetryBackoffCoefficient {
		t.Fatalf("retry policy coefficient = %v, want %v", policy.BackoffCoefficient, nexusRetryBackoffCoefficient)
	}

	// The window has to be the sum of the policy's own backoff, not a guess:
	// a policy whose coefficient changed would otherwise silently shrink it.
	if expected := retryWindow(policy.InitialInterval, policy.MaximumInterval, policy.BackoffCoefficient, int(policy.MaximumAttempts)); window != expected {
		t.Fatalf("retry window = %s, want %s from the policy", window, expected)
	}
}

// Canceling the operation must stop the run it started. The run is a separate
// workflow on a separate queue: if this operation only stopped itself, the
// caller would be told the work is over while the agent kept running and
// spending. This is the case a direct Nexus caller hits, and no plan is
// involved to cancel the run on its behalf.
func TestNexusRunOperationWorkflow_CancelStopsTheUnderlyingRun(t *testing.T) {
	t.Parallel()

	rt := newFakeNexusRuntime() // no terminal state: the run polls as running forever
	env := newNexusRunTestEnv(rt)

	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, nexusRunPollInterval/2)

	env.ExecuteWorkflow(NexusRunOperationWorkflow, nexusRunWorkflowInput{
		Request:           nexusapi.RunRequest{RequestID: "req-cancel", Spec: nexusRunTestSpec()},
		ActivityTaskQueue: "agentos-plan-activity",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(), "a canceled operation reports cancellation")

	rt.mu.Lock()
	defer rt.mu.Unlock()

	require.Len(t, rt.controls, 1, "the run the operation started must be controlled")

	for runID, operations := range rt.controls {
		require.Contains(t, operations, agentoscore.ControlCancel, "run %s was left running", runID)
	}
}

// A plan-scoped run is canceled by the same operation cancel, under the
// operation's own key: the plan's timeout path keeps its own key, so the two
// cannot become two different controls for one run.
func TestNexusOperationCancelKeyIsDerivedFromTheOperation(t *testing.T) {
	t.Parallel()

	require.Equal(t, "nexus-cancel:req-1", nexusOperationCancelKey("req-1"))
	require.Empty(t, nexusOperationCancelKey(""), "an operation without an id has no key to derive")
	require.NotEqual(t, nexusOperationCancelKey("req-1"), nexusOperationCancelKey("req-2"))

	// The plan's timeout cancel and an operation cancel are distinct keys for
	// distinct causes.
	planKey, err := agentosplan.NodeTimeoutControlIdempotencyKey("plan-1", "node-1", "run-1")
	require.NoError(t, err)
	require.NotEqual(t, planKey, nexusOperationCancelKey("req-1"))
}
