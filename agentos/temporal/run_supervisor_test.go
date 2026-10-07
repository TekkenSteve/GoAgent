package temporal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosruntime "github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

var errSupervisorTestBackend = errors.New("run supervisor test: backend failed")

// supervisorTestBackend is a stand-in external backend: it answers status from
// scripted values and records the signals and controls it receives.
type supervisorTestBackend struct {
	mu sync.Mutex

	statuses []agentos.RunStatus
	signals  []agentoscore.SignalType
	controls []agentoscore.ControlOperation
	canceled bool
	readErr  error
}

func (b *supervisorTestBackend) Start(_ context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running"}, nil
}

func (b *supervisorTestBackend) Signal(_ context.Context, _ string, signal *agentoscore.Signal) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.signals = append(b.signals, signal.Type)

	return nil
}

func (b *supervisorTestBackend) Control(_ context.Context, _ string, control *agentoscore.ControlRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.controls = append(b.controls, control.Operation)

	if control.Operation == agentoscore.ControlCancel {
		b.canceled = true
	}

	return nil
}

func (b *supervisorTestBackend) Status(context.Context, string) (agentos.RunStatus, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.readErr != nil {
		return agentos.RunStatus{}, b.readErr
	}

	if b.canceled {
		return agentos.RunStatus{LifecycleState: CANCELED}, nil
	}

	if len(b.statuses) == 0 {
		return agentos.RunStatus{LifecycleState: "running"}, nil
	}

	next := b.statuses[0]
	if len(b.statuses) > 1 {
		b.statuses = b.statuses[1:]
	}

	return next, nil
}

func (b *supervisorTestBackend) Subscribe(context.Context, agentoscore.StreamScope) (agentoscore.Subscription, error) {
	return nil, nil
}

func (b *supervisorTestBackend) recordedSignals() []agentoscore.SignalType {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]agentoscore.SignalType(nil), b.signals...)
}

func (b *supervisorTestBackend) recordedControls() []agentoscore.ControlOperation {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]agentoscore.ControlOperation(nil), b.controls...)
}

type supervisorTestResolver struct {
	mu       sync.Mutex
	backend  agentosruntime.AgentBackend
	err      error
	resolved int
}

func (r *supervisorTestResolver) resolve(agentos.BackendRef) (agentosruntime.AgentBackend, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.resolved++

	if r.err != nil {
		return nil, r.err
	}

	return r.backend, nil
}

func newSupervisorTestEnv(t *testing.T, resolver *supervisorTestResolver) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	acts, err := NewRunSupervisorActivities(resolver.resolve)
	require.NoError(t, err)

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(RunSupervisorWorkflow, workflow.RegisterOptions{Name: RunSupervisorWorkflowName})
	env.RegisterActivityWithOptions(acts.ReadStatusActivity, activity.RegisterOptions{Name: RunSupervisorStatusActivityName})
	env.RegisterActivityWithOptions(acts.ApplySignalActivity, activity.RegisterOptions{Name: RunSupervisorSignalActivityName})
	env.RegisterActivityWithOptions(acts.ApplyControlActivity, activity.RegisterOptions{Name: RunSupervisorControlActivityName})

	return env
}

func supervisorTestInput(status *agentos.RunStatus) *RunSupervisorInput {
	return &RunSupervisorInput{
		Ref:    agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "mock-http"},
		Spec:   agentos.RunSpec{RunID: status.RunID, Backend: agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "mock-http"}},
		Status: *status,
	}
}

func TestRunSupervisorWorkflow_PollsUntilTerminal(t *testing.T) {
	t.Parallel()

	backend := &supervisorTestBackend{statuses: []agentos.RunStatus{
		{RunID: "run-1", LifecycleState: "running"},
		{RunID: "run-1", LifecycleState: SUCCEEDED},
	}}
	env := newSupervisorTestEnv(t, &supervisorTestResolver{backend: backend})

	env.ExecuteWorkflow(RunSupervisorWorkflow, supervisorTestInput(&agentos.RunStatus{RunID: "run-1", LifecycleState: "running"}))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var status agentos.RunStatus
	require.NoError(t, env.GetWorkflowResult(&status))
	assert.Equal(t, SUCCEEDED, status.LifecycleState)
}

func TestRunSupervisorWorkflow_AppliesSignalsAndControls(t *testing.T) {
	t.Parallel()

	backend := &supervisorTestBackend{}
	env := newSupervisorTestEnv(t, &supervisorTestResolver{backend: backend})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(RunSupervisorSignalName, &agentoscore.Signal{
			Type:           agentoscore.SignalUserMessage,
			IdempotencyKey: "sig-1",
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(RunSupervisorControlName, &agentoscore.ControlRequest{
			Operation:      agentoscore.ControlPause,
			IdempotencyKey: "ctl-1",
		})
	}, 2*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(RunSupervisorControlName, &agentoscore.ControlRequest{
			Operation:      agentoscore.ControlCancel,
			IdempotencyKey: "ctl-2",
		})
	}, 3*time.Second)

	env.ExecuteWorkflow(RunSupervisorWorkflow, supervisorTestInput(&agentos.RunStatus{RunID: "run-2", LifecycleState: "running"}))

	require.True(t, env.IsWorkflowCompleted())
	assert.Equal(t, []agentoscore.SignalType{agentoscore.SignalUserMessage}, backend.recordedSignals())
	assert.Equal(t, []agentoscore.ControlOperation{agentoscore.ControlPause, agentoscore.ControlCancel}, backend.recordedControls())
}

func TestNewRunSupervisorActivities_RequiresResolver(t *testing.T) {
	t.Parallel()

	_, err := NewRunSupervisorActivities(nil)
	require.ErrorIs(t, err, ErrRunSupervisorBackendResolverRequired)
}

func TestNewSupervisorClient_ValidatesConfig(t *testing.T) {
	t.Parallel()

	_, err := NewSupervisorClient(nil, "  ")
	require.ErrorIs(t, err, ErrRunSupervisorTaskQueueRequired)

	_, err = NewSupervisorClient(nil, "queue")
	require.ErrorIs(t, err, ErrRunSupervisorClientRequired)
}

// fakeRunSupervisor stands in for the Temporal-backed handle.
type fakeRunSupervisor struct {
	ensureErr error
	ensured   []*RunSupervisorInput
	status    agentos.RunStatus
	statusOK  bool
	forward   bool
	signals   int
	controls  int
}

func (f *fakeRunSupervisor) Ensure(_ context.Context, in *RunSupervisorInput) error {
	f.ensured = append(f.ensured, in)

	return f.ensureErr
}

func (f *fakeRunSupervisor) Status(context.Context, string) (agentos.RunStatus, bool, error) {
	return f.status, f.statusOK, nil
}

func (f *fakeRunSupervisor) Signal(context.Context, string, *agentoscore.Signal) (bool, error) {
	f.signals++

	return f.forward, nil
}

func (f *fakeRunSupervisor) Control(context.Context, string, *agentoscore.ControlRequest) (bool, error) {
	f.controls++

	return f.forward, nil
}

func newSupervisedTestBackend(t *testing.T, sup RunSupervisor) (*SupervisedBackend, *supervisorTestBackend) {
	t.Helper()

	inner := &supervisorTestBackend{statuses: []agentos.RunStatus{{RunID: "run-3", LifecycleState: "failed"}}}
	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "mock-http"}

	backend, err := NewSupervisedBackend(ref, inner, sup)
	require.NoError(t, err)

	return backend, inner
}

func TestSupervisedBackend_StatusPrefersSupervisor(t *testing.T) {
	t.Parallel()

	sup := &fakeRunSupervisor{status: agentos.RunStatus{RunID: "run-3", LifecycleState: SUCCEEDED}, statusOK: true}
	backend, _ := newSupervisedTestBackend(t, sup)

	status, err := backend.Status(context.Background(), "run-3")
	require.NoError(t, err)
	assert.Equal(t, SUCCEEDED, status.LifecycleState)
}

func TestSupervisedBackend_StatusFallsBackWhenUnsupervised(t *testing.T) {
	t.Parallel()

	backend, _ := newSupervisedTestBackend(t, &fakeRunSupervisor{})

	status, err := backend.Status(context.Background(), "run-3")
	require.NoError(t, err)
	assert.Equal(t, "failed", status.LifecycleState, "the backend answers when no supervisor owns the run")
}

func TestSupervisedBackend_SignalAndControlForward(t *testing.T) {
	t.Parallel()

	sup := &fakeRunSupervisor{forward: true}
	backend, inner := newSupervisedTestBackend(t, sup)

	require.NoError(t, backend.Signal(context.Background(), "run-3", &agentoscore.Signal{Type: agentoscore.SignalUserMessage}))
	require.NoError(t, backend.Control(context.Background(), "run-3", &agentoscore.ControlRequest{Operation: agentoscore.ControlPause}))

	assert.Equal(t, 1, sup.signals)
	assert.Equal(t, 1, sup.controls)
	assert.Empty(t, inner.recordedControls(), "the supervisor owns the control path")
}

func TestSupervisedBackend_SignalFallsBackWhenUnsupervised(t *testing.T) {
	t.Parallel()

	backend, inner := newSupervisedTestBackend(t, &fakeRunSupervisor{forward: false})

	require.NoError(t, backend.Signal(context.Background(), "run-3", &agentoscore.Signal{Type: agentoscore.SignalUserMessage}))
	assert.Equal(t, []agentoscore.SignalType{agentoscore.SignalUserMessage}, inner.recordedSignals())
}

func TestSupervisedBackend_StartHandsControlToSupervisor(t *testing.T) {
	t.Parallel()

	sup := &fakeRunSupervisor{}
	backend, inner := newSupervisedTestBackend(t, sup)

	status, err := backend.Start(context.Background(), &agentos.RunSpec{RunID: "run-4"})
	require.NoError(t, err)
	assert.Equal(t, "running", status.LifecycleState)
	require.Len(t, sup.ensured, 1)
	assert.Equal(t, "run-4", sup.ensured[0].Status.RunID)
	assert.Empty(t, inner.recordedControls())
}

func TestSupervisedBackend_StartSurvivesSupervisorFailure(t *testing.T) {
	t.Parallel()

	sup := &fakeRunSupervisor{ensureErr: errSupervisorTestBackend}
	backend, _ := newSupervisedTestBackend(t, sup)

	status, err := backend.Start(context.Background(), &agentos.RunSpec{RunID: "run-5"})
	require.NoError(t, err, "an already-started run must not fail because supervision could not start")
	assert.Equal(t, "running", status.LifecycleState)
}

func TestRunSupervisorID(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "agentos-run-supervisor-run-9", RunSupervisorID("run-9"))
}
