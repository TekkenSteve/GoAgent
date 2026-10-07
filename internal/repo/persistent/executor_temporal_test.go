package persistent

import (
	"context"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

// startedWorkflow is one ExecuteWorkflow call: which workflow, under which id,
// with which input.
type startedWorkflow struct {
	workflow string
	id       string
	input    any
}

// fakeTemporalClient records the workflows a run starts. It embeds the SDK
// interface so the methods this test does not exercise stay unimplemented.
type fakeTemporalClient struct {
	client.Client

	started []startedWorkflow
	signals []sentSignal
	err     error
}

// sentSignal is one SignalWorkflow call.
type sentSignal struct {
	workflowID string
	name       string
	payload    any
}

func (c *fakeTemporalClient) SignalWorkflow(_ context.Context, workflowID, _, signalName string, arg any) error {
	c.signals = append(c.signals, sentSignal{workflowID: workflowID, name: signalName, payload: arg})

	return nil
}

//nolint:gocritic // the Temporal client interface takes the options by value
func (c *fakeTemporalClient) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error) {
	if c.err != nil {
		return nil, c.err
	}

	var input any
	if len(args) > 0 {
		input = args[0]
	}

	c.started = append(c.started, startedWorkflow{workflow: workflowName(workflow), id: options.ID, input: input})

	return nil, nil
}

func workflowName(workflow any) string {
	if name, ok := workflow.(string); ok {
		return name
	}

	return ""
}

func newTestExecutor(t *testing.T, c client.Client) *ExecutorTemporal {
	t.Helper()

	executor, err := NewExecutorTemporal(c, &config.Temporal{
		TaskQueues:         config.DefaultTaskQueues(),
		RunWorkflowTimeout: time.Hour,
	})
	require.NoError(t, err)

	return executor
}

// The native backend has two execution modes behind one request. A queue is one
// of them, and it runs on the step-queue interpreter — under the same workflow
// id as the single-agent loop, so a run is addressed the same way either way.
func TestStartExecutionRunsAStepQueueOnTheInterpreter(t *testing.T) {
	t.Parallel()

	fake := &fakeTemporalClient{}
	executor := newTestExecutor(t, fake)

	status, err := executor.StartExecution(t.Context(), &entity.ExecuteRequest{
		RunID:     "run-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
		Steps: []entity.Step{
			{ID: "research", Type: entity.StepAgent, Input: map[string]any{"message": "go"}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "run-1", status.RunID)

	require.Len(t, fake.started, 1)
	require.Equal(t, orchestration.OrchestrationWorkflowName, fake.started[0].workflow)
	require.Equal(t, "agentfw-run-run-1", fake.started[0].id)

	input, ok := fake.started[0].input.(*orchestration.WorkflowInput)
	require.True(t, ok)
	require.Equal(t, "acct-1", input.Input.AccountID)
	require.Equal(t, "proj-1", input.Input.ProjectID)
	require.Len(t, input.Input.Steps, 1)
}

// A team is expanded by the backend before the workflow starts: the queue the
// workflow executes is a value in its input, which is what keeps the expansion
// out of workflow code.
func TestStartExecutionExpandsATeamBeforeStarting(t *testing.T) {
	t.Parallel()

	fake := &fakeTemporalClient{}
	executor := newTestExecutor(t, fake)

	status, err := executor.StartExecution(t.Context(), &entity.ExecuteRequest{
		RunID:     "run-2",
		AccountID: "acct-1",
		ProjectID: "proj-1",
		TeamSpec: &entity.TeamSpec{
			ID:     "team-1",
			Name:   "Team",
			Agents: []entity.AgentSpec{{ID: "worker", Name: "Worker", ModelRef: "gpt-4.1-mini"}},
			Steps: []entity.StepTemplate{
				{ID: "work", Type: entity.StepAgent, AgentRef: "worker", Input: map[string]any{"message": "hello"}},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "run-2", status.RunID)

	require.Len(t, fake.started, 1)
	require.Equal(t, orchestration.OrchestrationWorkflowName, fake.started[0].workflow)

	input, ok := fake.started[0].input.(*orchestration.WorkflowInput)
	require.True(t, ok)
	require.Len(t, input.Input.Steps, 1, "the team must arrive expanded")
	require.Equal(t, "work", input.Input.Steps[0].ID)
	require.Equal(t, entity.StepPending, input.Input.Steps[0].Status)
}

// Without a queue the request is the single-agent loop, unchanged.
func TestStartExecutionRunsTheAgentLoopWithoutAQueue(t *testing.T) {
	t.Parallel()

	fake := &fakeTemporalClient{}
	executor := newTestExecutor(t, fake)

	_, err := executor.StartExecution(t.Context(), &entity.ExecuteRequest{
		RunID:       "run-3",
		AccountID:   "acct-1",
		ProjectID:   "proj-1",
		UserMessage: "hello",
	})
	require.NoError(t, err)

	require.Len(t, fake.started, 1)
	require.Equal(t, orchestration.AgentWorkflowName, fake.started[0].workflow)

	input, ok := fake.started[0].input.(*orchestration.AgentWorkflowInput)
	require.True(t, ok)
	require.Equal(t, "proj-1", input.ProjectID)
}

// The two step-queue signals are what makes the mode controllable: a queue that
// runs but cannot be changed, and a wait step nothing can wake, would be a mode
// the control plane can start and then not talk to.
func TestStepQueueSignalsAddressTheRunWorkflow(t *testing.T) {
	t.Parallel()

	fake := &fakeTemporalClient{}
	executor := newTestExecutor(t, fake)

	err := executor.SignalStepModify(t.Context(), "run-1", &entity.StepMutation{
		AppendAfter: "research",
		InsertSteps: []entity.Step{{ID: "follow-up", Type: entity.StepAgent}},
	})
	require.NoError(t, err)

	err = executor.SignalExternalEvent(t.Context(), "run-1", map[string]any{"ticket": "TCK-1"})
	require.NoError(t, err)

	require.Len(t, fake.signals, 2)
	require.Equal(t, "agentfw-run-run-1", fake.signals[0].workflowID)
	require.Equal(t, orchestration.StepModifySignal, fake.signals[0].name)
	require.Equal(t, orchestration.ExternalEventSignal, fake.signals[1].name)
}
