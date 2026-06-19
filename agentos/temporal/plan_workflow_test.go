package temporal

import (
	"context"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
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

	env.ExecuteWorkflow(PlanWorkflow, spec)

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

	env.ExecuteWorkflow(PlanWorkflow, spec)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleFailed, result.LifecycleState)
	require.Equal(t, []string{"run-attempt", "run-recover"}, mocks.started)
}

type planWorkflowMocks struct {
	started  []string
	statuses map[string]agentos.RunStatus
}

func newPlanWorkflowTestEnv(mocks *planWorkflowMocks) *testsuite.TestWorkflowEnvironment {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(PlanWorkflow, workflow.RegisterOptions{Name: PlanWorkflowName})

	activities := NewPlanActivities(&fakePlanRuntime{})
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
	status := m.statuses[input.RunID]
	if status.RunID == "" {
		status.RunID = input.RunID
	}
	if status.LifecycleState == "" {
		status.LifecycleState = "running"
	}

	return statusPlanNodeOutput{Status: status}, nil
}

func (m *planWorkflowMocks) control(context.Context, controlPlanNodeInput) error {
	return nil
}

func (m *planWorkflowMocks) publishArtifacts(_ context.Context, input publishPlanArtifactsInput) (publishPlanArtifactsOutput, error) {
	refs, err := normalizeRunArtifacts(input.PlanID, input.Node, input.Status)
	if err != nil {
		return publishPlanArtifactsOutput{}, err
	}

	return publishPlanArtifactsOutput{Artifacts: refs}, nil
}
