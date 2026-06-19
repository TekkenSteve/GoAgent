package temporal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestPlanWorkflowExecutesSuccessEdgeAndPublishesArtifacts(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-success",
		Policy: agentos.PlanPolicy{
			MaxParallelNodes: 2,
		},
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "research",
				Run:    agentos.RunSpec{RunID: "run-research", Backend: ref},
				Outputs: []agentos.ArtifactSpec{
					{Name: "summary", Kind: agentos.ArtifactKindObject, Required: true},
				},
			},
			{NodeID: "verify", Run: agentos.RunSpec{RunID: "run-verify", Backend: ref}},
		},
		Edges: []agentos.PlanEdgeSpec{
			{EdgeID: "research-verify", From: "research", To: "verify", On: agentos.EdgeOnSuccess},
		},
	}
	mocks := &planWorkflowMocks{
		statuses: map[string]agentos.RunStatus{
			"run-research": {
				RunID:          "run-research",
				LifecycleState: "completed",
				Artifacts: []agentos.ArtifactRef{
					{ArtifactID: "artifact-summary", Name: "summary", Kind: agentos.ArtifactKindObject},
				},
			},
			"run-verify": {RunID: "run-verify", LifecycleState: "completed"},
		},
	}
	env := newPlanWorkflowTestEnv(mocks)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInput{Spec: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleSucceeded, result.LifecycleState)
	require.Equal(t, []string{"run-research", "run-verify"}, mocks.started)
	require.Len(t, result.Artifacts, 1)
	require.Equal(t, "artifact-summary", result.Artifacts[0].ArtifactID)
	require.Equal(t, "plan-success", result.Artifacts[0].PlanID)
	require.Equal(t, "research", result.Artifacts[0].NodeID)
}

func TestPlanWorkflowErrorEdgeRunsRecoveryButPlanRemainsFailed(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-error",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "attempt", Run: agentos.RunSpec{RunID: "run-attempt", Backend: ref}},
			{NodeID: "recover", Run: agentos.RunSpec{RunID: "run-recover", Backend: ref}},
		},
		Edges: []agentos.PlanEdgeSpec{
			{EdgeID: "attempt-recover", From: "attempt", To: "recover", On: agentos.EdgeOnError},
		},
	}
	mocks := &planWorkflowMocks{
		statuses: map[string]agentos.RunStatus{
			"run-attempt": {RunID: "run-attempt", LifecycleState: "failed", Reason: "backend failed"},
			"run-recover": {RunID: "run-recover", LifecycleState: "completed"},
		},
	}
	env := newPlanWorkflowTestEnv(mocks)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInput{Spec: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleFailed, result.LifecycleState)
	require.Equal(t, []string{"run-attempt", "run-recover"}, mocks.started)
}

func TestPlanWorkflowRetriesFailedNodeAttempt(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-retry",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "flaky",
				Run:    agentos.RunSpec{RunID: "run-flaky", Backend: ref},
				Policy: agentos.NodePolicy{
					MaxAttempts: 2,
				},
			},
		},
	}
	mocks := &planWorkflowMocks{
		statuses: map[string]agentos.RunStatus{
			"run-flaky":           {RunID: "run-flaky", LifecycleState: "failed", Reason: "first attempt failed"},
			"run-flaky-attempt-2": {RunID: "run-flaky-attempt-2", LifecycleState: "completed"},
		},
	}
	env := newPlanWorkflowTestEnv(mocks)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInput{Spec: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleSucceeded, result.LifecycleState)
	require.Equal(t, []string{"run-flaky", "run-flaky-attempt-2"}, mocks.started)
	require.Len(t, result.Nodes, 1)
	require.Equal(t, int32(2), result.Nodes[0].Attempts)
	require.Equal(t, "run-flaky-attempt-2", result.Nodes[0].RunID)
}

func TestPlanWorkflowCancelsTimedOutNode(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-timeout",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "slow",
				Run:    agentos.RunSpec{RunID: "run-slow", Backend: ref},
				Policy: agentos.NodePolicy{
					TimeoutSeconds: 1,
				},
			},
		},
	}
	mocks := &planWorkflowMocks{
		statuses: map[string]agentos.RunStatus{
			"run-slow": {RunID: "run-slow", LifecycleState: "running"},
		},
	}
	env := newPlanWorkflowTestEnv(mocks)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInput{Spec: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleFailed, result.LifecycleState)
	require.Contains(t, result.Reason, "timed out")
	require.Equal(t, []string{"run-slow"}, mocks.started)
	require.Len(t, mocks.controls, 1)
	require.Equal(t, "run-slow", mocks.controls[0].RunID)
	require.Equal(t, agentos.ControlCancel, mocks.controls[0].Control.Operation)
	require.NotEmpty(t, mocks.controls[0].Control.IdempotencyKey)
}

func TestPlanWorkflowCancelsActiveNodesWhenBudgetExceeded(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-budget",
		Policy: agentos.PlanPolicy{
			BudgetCents:      50,
			MaxParallelNodes: 2,
		},
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "expensive", Run: agentos.RunSpec{RunID: "run-expensive", Backend: ref}},
			{NodeID: "slow", Run: agentos.RunSpec{RunID: "run-slow", Backend: ref}},
		},
	}
	mocks := &planWorkflowMocks{
		statuses: map[string]agentos.RunStatus{
			"run-expensive": {
				RunID:          "run-expensive",
				LifecycleState: "completed",
				BudgetUsage:    agentos.PlanBudgetUsage{SpentCents: 60},
			},
			"run-slow": {RunID: "run-slow", LifecycleState: "running"},
		},
	}
	env := newPlanWorkflowTestEnv(mocks)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInput{Spec: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleFailed, result.LifecycleState)
	require.Equal(t, int64(60), result.BudgetUsage.SpentCents)
	require.Contains(t, result.Reason, "budget exceeded")
	require.Equal(t, []string{"run-expensive", "run-slow"}, mocks.started)
	require.Len(t, mocks.controls, 1)
	require.Equal(t, "run-slow", mocks.controls[0].RunID)
	require.Equal(t, agentos.ControlCancel, mocks.controls[0].Control.Operation)
}

func TestPlanWorkflowOrchestratesMixedBackendsThroughRuntime(t *testing.T) {
	t.Parallel()

	refs := []agentos.BackendRef{
		{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
		{Kind: agentos.BackendKindTemporalExternal, Name: "langgraph"},
		{Kind: agentos.BackendKindHTTP, Name: "claude-code"},
		{Kind: agentos.BackendKindGRPC, Name: "grpc-agent"},
	}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-mixed-backends",
		Policy: agentos.PlanPolicy{
			MaxParallelNodes: 2,
		},
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "native", Capability: "run", Run: agentos.RunSpec{RunID: "run-native", Backend: refs[0]}},
			{NodeID: "external", Capability: "run", Run: agentos.RunSpec{RunID: "run-external", Backend: refs[1]}},
			{NodeID: "http", Capability: "run", Run: agentos.RunSpec{RunID: "run-http", Backend: refs[2]}},
			{NodeID: "grpc", Capability: "run", Run: agentos.RunSpec{RunID: "run-grpc", Backend: refs[3]}},
		},
		Edges: []agentos.PlanEdgeSpec{
			{EdgeID: "native-external", From: "native", To: "external", On: agentos.EdgeOnSuccess},
			{EdgeID: "external-http", From: "external", To: "http", On: agentos.EdgeOnSuccess},
			{EdgeID: "http-grpc", From: "http", To: "grpc", On: agentos.EdgeOnSuccess},
		},
	}
	runtime := newMixedBackendRuntime()
	store := agentosplan.NewMemoryPlanStore()
	binder := &mixedBackendBinder{}
	activities, err := NewPlanActivitiesWithStores(
		runtime,
		mixedBackendCapabilities(refs),
		store,
		store,
		nil,
		binder,
		agentosplan.NewMemoryArtifactStore(),
	)
	require.NoError(t, err)

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(PlanWorkflow, workflow.RegisterOptions{Name: PlanWorkflowName})
	env.RegisterActivityWithOptions(activities.ValidatePlanActivity, activity.RegisterOptions{Name: ValidatePlanActivityName})
	env.RegisterActivityWithOptions(activities.PersistPlanStateActivity, activity.RegisterOptions{Name: PersistPlanStateActivityName})
	env.RegisterActivityWithOptions(activities.StartPlanNodeActivity, activity.RegisterOptions{Name: StartPlanNodeActivityName})
	env.RegisterActivityWithOptions(activities.StatusPlanNodeActivity, activity.RegisterOptions{Name: StatusPlanNodeActivityName})
	env.RegisterActivityWithOptions(activities.ControlPlanNodeActivity, activity.RegisterOptions{Name: ControlPlanNodeActivityName})
	env.RegisterActivityWithOptions(activities.PublishPlanArtifactsActivity, activity.RegisterOptions{Name: PublishPlanArtifactsActivityName})

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInput{Spec: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleSucceeded, result.LifecycleState)
	require.Equal(t, []agentos.BackendRef{refs[0], refs[1], refs[2], refs[3]}, runtime.startedBackends())
	require.Equal(t, []agentos.BackendRef{refs[0], refs[1], refs[2], refs[3]}, binder.boundBackends())

	events, err := store.ListPlanEvents(context.Background(), agentos.PlanStreamScope{PlanID: spec.PlanID}, 0)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	require.Equal(t, agentos.EventPlanStarted, events[0].EventType)
	require.Equal(t, agentos.EventPlanSucceeded, events[len(events)-1].EventType)
}

func TestPlanWorkflowRetriesFailedNodeFromSignal(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-manual-retry",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "flaky", Run: agentos.RunSpec{RunID: "run-flaky", Backend: ref}},
			{NodeID: "slow", Run: agentos.RunSpec{RunID: "run-slow", Backend: ref}},
		},
	}
	mocks := &planWorkflowMocks{
		statuses: map[string]agentos.RunStatus{
			"run-flaky":           {RunID: "run-flaky", LifecycleState: "failed", Reason: "needs manual retry"},
			"run-flaky-attempt-2": {RunID: "run-flaky-attempt-2", LifecycleState: "completed"},
		},
		statusSequences: map[string][]agentos.RunStatus{
			"run-slow": {
				{RunID: "run-slow", LifecycleState: "running"},
				{RunID: "run-slow", LifecycleState: "running"},
				{RunID: "run-slow", LifecycleState: "completed"},
			},
		},
	}
	env := newPlanWorkflowTestEnv(mocks)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanSignalName, agentos.Signal{
			Type:           agentos.SignalPlanNodeRetry,
			IdempotencyKey: "retry-flaky",
			Payload: map[string]any{
				agentosplan.SignalPayloadNodeID: "flaky",
			},
		})
	}, time.Second)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInput{Spec: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleSucceeded, result.LifecycleState)
	require.Equal(t, []string{"run-flaky", "run-slow", "run-flaky-attempt-2"}, mocks.started)
	require.Len(t, result.Nodes, 2)
	require.Equal(t, int32(2), result.Nodes[0].Attempts)
	require.Equal(t, "run-flaky-attempt-2", result.Nodes[0].RunID)
}

func TestPlanWorkflowRejectSignalFailsRunningPlan(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-reject",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "slow", Run: agentos.RunSpec{RunID: "run-slow", Backend: ref}},
		},
	}
	mocks := &planWorkflowMocks{
		statusSequences: map[string][]agentos.RunStatus{
			"run-slow": {
				{RunID: "run-slow", LifecycleState: "running"},
				{RunID: "run-slow", LifecycleState: "running"},
			},
		},
	}
	env := newPlanWorkflowTestEnv(mocks)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanSignalName, agentos.Signal{
			Type:           agentos.SignalPlanReject,
			IdempotencyKey: "reject-plan",
			Payload: map[string]any{
				agentosplan.SignalPayloadReason: "operator rejected",
			},
		})
	}, time.Second)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInput{Spec: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleFailed, result.LifecycleState)
	require.Equal(t, "operator rejected", result.Reason)
	require.Equal(t, []string{"run-slow"}, mocks.started)
}

func TestPlanWorkflowApproveSignalUnblocksPausedPlan(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-approve",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "slow", Capability: "pausable", Run: agentos.RunSpec{RunID: "run-slow", Backend: ref}},
		},
	}
	mocks := &planWorkflowMocks{
		statusSequences: map[string][]agentos.RunStatus{
			"run-slow": {
				{RunID: "run-slow", LifecycleState: "running"},
				{RunID: "run-slow", LifecycleState: "running"},
				{RunID: "run-slow", LifecycleState: "completed"},
			},
		},
	}
	env := newPlanWorkflowTestEnvWithCapabilities(mocks, []agentos.Capability{
		{Backend: ref, Name: "pausable", Controls: []agentos.ControlOperation{agentos.ControlPause}},
	})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanControlSignalName, agentos.ControlRequest{
			Operation:      agentos.ControlPause,
			IdempotencyKey: "pause-plan",
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanSignalName, agentos.Signal{
			Type:           agentos.SignalPlanApprove,
			IdempotencyKey: "approve-plan",
		})
	}, 2*time.Second)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInput{Spec: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleSucceeded, result.LifecycleState)
	require.Equal(t, []string{"run-slow"}, mocks.started)
	require.Len(t, mocks.controls, 1)
	require.Equal(t, agentos.ControlPause, mocks.controls[0].Control.Operation)
}

func TestPlanWorkflowContinuedInputRestoresSnapshotStatus(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-continued",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "running", Run: agentos.RunSpec{RunID: "run-running", Backend: ref}},
		},
	}
	status := agentos.RunPlanStatus{
		PlanID:         "plan-continued",
		LifecycleState: agentos.PlanLifecycleRunning,
		Nodes: []agentos.PlanNodeStatus{
			{NodeID: "running", RunID: "run-running", Backend: ref, LifecycleState: agentos.PlanNodeRunning},
		},
		BudgetUsage: agentos.PlanBudgetUsage{SpentCents: 25},
	}

	state, err := initialPlanWorkflowState(planWorkflowInput{
		Spec:      spec,
		Status:    status,
		Continued: true,
	}, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, agentos.PlanLifecycleRunning, state.Status.LifecycleState)
	require.Equal(t, int64(25), state.Status.BudgetUsage.SpentCents)
	require.Equal(t, []string{"run-running"}, state.Status.ActiveRunIDs)
}

type planWorkflowMocks struct {
	started         []string
	statuses        map[string]agentos.RunStatus
	statusSequences map[string][]agentos.RunStatus
	controls        []controlPlanNodeInput
}

func newPlanWorkflowTestEnv(mocks *planWorkflowMocks) *testsuite.TestWorkflowEnvironment {
	return newPlanWorkflowTestEnvWithCapabilities(mocks, nil)
}

func newPlanWorkflowTestEnvWithCapabilities(mocks *planWorkflowMocks, capabilities []agentos.Capability) *testsuite.TestWorkflowEnvironment {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(PlanWorkflow, workflow.RegisterOptions{Name: PlanWorkflowName})

	activities, err := NewPlanActivitiesWithCapabilities(&fakePlanRuntime{}, capabilities)
	if err != nil {
		panic(err)
	}
	env.RegisterActivityWithOptions(activities.ValidatePlanActivity, activity.RegisterOptions{Name: ValidatePlanActivityName})
	env.RegisterActivityWithOptions(activities.PersistPlanStateActivity, activity.RegisterOptions{Name: PersistPlanStateActivityName})
	env.RegisterActivityWithOptions(mocks.start, activity.RegisterOptions{Name: StartPlanNodeActivityName})
	env.RegisterActivityWithOptions(mocks.status, activity.RegisterOptions{Name: StatusPlanNodeActivityName})
	env.RegisterActivityWithOptions(mocks.control, activity.RegisterOptions{Name: ControlPlanNodeActivityName})
	env.RegisterActivityWithOptions(mocks.publishArtifacts, activity.RegisterOptions{Name: PublishPlanArtifactsActivityName})

	return env
}

func (m *planWorkflowMocks) start(_ context.Context, input startPlanNodeInput) (startPlanNodeOutput, error) {
	m.started = append(m.started, input.Node.Run.RunID)

	return startPlanNodeOutput{Status: agentos.RunStatus{RunID: input.Node.Run.RunID, LifecycleState: "running"}}, nil
}

func (m *planWorkflowMocks) status(_ context.Context, input statusPlanNodeInput) (statusPlanNodeOutput, error) {
	if sequence := m.statusSequences[input.RunID]; len(sequence) > 0 {
		status := sequence[0]
		if len(sequence) > 1 {
			m.statusSequences[input.RunID] = sequence[1:]
		}
		if status.RunID == "" {
			status.RunID = input.RunID
		}

		return statusPlanNodeOutput{Status: status}, nil
	}
	status := m.statuses[input.RunID]
	if status.RunID == "" {
		status.RunID = input.RunID
	}
	if status.LifecycleState == "" {
		status.LifecycleState = "running"
	}

	return statusPlanNodeOutput{Status: status}, nil
}

func (m *planWorkflowMocks) control(_ context.Context, input controlPlanNodeInput) error {
	m.controls = append(m.controls, input)

	return nil
}

func (m *planWorkflowMocks) publishArtifacts(_ context.Context, input publishPlanArtifactsInput) (publishPlanArtifactsOutput, error) {
	refs, err := normalizeRunArtifacts(input.PlanID, input.Node, input.Status)
	if err != nil {
		return publishPlanArtifactsOutput{}, err
	}

	return publishPlanArtifactsOutput{Artifacts: refs}, nil
}

type mixedBackendRuntime struct {
	mu       sync.Mutex
	started  []agentos.RunSpec
	statuses map[string]agentos.RunStatus
}

func newMixedBackendRuntime() *mixedBackendRuntime {
	return &mixedBackendRuntime{
		statuses: map[string]agentos.RunStatus{
			"run-native":   {RunID: "run-native", LifecycleState: "completed"},
			"run-external": {RunID: "run-external", LifecycleState: "completed"},
			"run-http":     {RunID: "run-http", LifecycleState: "completed"},
			"run-grpc":     {RunID: "run-grpc", LifecycleState: "completed"},
		},
	}
}

func (r *mixedBackendRuntime) Start(_ context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started = append(r.started, spec)

	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running"}, nil
}

func (r *mixedBackendRuntime) Signal(context.Context, string, agentos.Signal) error {
	return nil
}

func (r *mixedBackendRuntime) Status(_ context.Context, runID string) (agentos.RunStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	status := r.statuses[runID]
	if status.RunID == "" {
		status.RunID = runID
	}
	if status.LifecycleState == "" {
		status.LifecycleState = "running"
	}

	return status, nil
}

func (r *mixedBackendRuntime) Control(context.Context, string, agentos.ControlRequest) error {
	return nil
}

func (r *mixedBackendRuntime) Subscribe(context.Context, agentos.StreamScope) (agentos.Subscription, error) {
	return nil, nil
}

func (r *mixedBackendRuntime) Close() error {
	return nil
}

func (r *mixedBackendRuntime) startedBackends() []agentos.BackendRef {
	r.mu.Lock()
	defer r.mu.Unlock()
	backends := make([]agentos.BackendRef, 0, len(r.started))
	for _, spec := range r.started {
		backends = append(backends, spec.Backend)
	}

	return backends
}

type mixedBackendBinder struct {
	mu    sync.Mutex
	bound []agentos.RunSpec
}

func (b *mixedBackendBinder) BindPlanNode(_ context.Context, _ string, _ string, spec agentos.RunSpec, _ agentos.RunStatus) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bound = append(b.bound, spec)

	return nil
}

func (b *mixedBackendBinder) boundBackends() []agentos.BackendRef {
	b.mu.Lock()
	defer b.mu.Unlock()
	backends := make([]agentos.BackendRef, 0, len(b.bound))
	for _, spec := range b.bound {
		backends = append(backends, spec.Backend)
	}

	return backends
}

func mixedBackendCapabilities(refs []agentos.BackendRef) []agentos.Capability {
	capabilities := make([]agentos.Capability, 0, len(refs))
	for _, ref := range refs {
		capabilities = append(capabilities, agentos.Capability{
			Backend: ref,
			Name:    "run",
		})
	}

	return capabilities
}
