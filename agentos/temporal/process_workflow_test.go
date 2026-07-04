package temporal

import (
	"context"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosprocess"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestProcessWorkflowSignalResumesWaitingProcess(t *testing.T) {
	t.Parallel()

	spec := processWorkflowTestSpec("process-signal")
	env := newProcessWorkflowTestEnv(t)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ProcessControlSignalName, agentoscore.ControlRequest{
			Operation:      agentoscore.ControlPause,
			IdempotencyKey: "pause-1",
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ProcessSignalName, agentoscore.Signal{
			Type:           "external.update",
			IdempotencyKey: "signal-1",
		})
	}, 2*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ProcessControlSignalName, agentoscore.ControlRequest{
			Operation:      agentoscore.ControlCancel,
			IdempotencyKey: "cancel-1",
		})
	}, 3*time.Second)

	env.ExecuteWorkflow(ProcessWorkflow, processWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentosproc.Status
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentosproc.ProcessCanceled, result.LifecycleState)
}

func TestProcessWorkflowRequiresActivityTaskQueue(t *testing.T) {
	t.Parallel()

	spec := processWorkflowTestSpec("process-invalid")
	env := newProcessWorkflowTestEnv(t)

	env.ExecuteWorkflow(ProcessWorkflow, &processWorkflowInput{Spec: spec})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestProcessWorkflowRequiresCurrentWorkflowVersion(t *testing.T) {
	t.Parallel()

	spec := processWorkflowTestSpec("process-version-required")
	env := newProcessWorkflowTestEnv(t)
	input := processWorkflowInputForTest(&spec)
	input.WorkflowVersion = currentProcessWorkflowVersion + 1

	env.ExecuteWorkflow(ProcessWorkflow, input)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func newProcessWorkflowTestEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	env := newAgentOSTemporalWorkflowTestEnv()
	env.RegisterWorkflowWithOptions(ProcessWorkflow, workflow.RegisterOptions{Name: ProcessWorkflowName})

	activities := newTestProcessActivities(t)
	env.RegisterActivityWithOptions(activities.StartProcessActivity, activity.RegisterOptions{Name: StartProcessActivityName})
	env.RegisterActivityWithOptions(activities.SignalProcessActivity, activity.RegisterOptions{Name: SignalProcessActivityName})
	env.RegisterActivityWithOptions(activities.ControlProcessActivity, activity.RegisterOptions{Name: ControlProcessActivityName})
	env.RegisterActivityWithOptions(activities.FireProcessTimerActivity, activity.RegisterOptions{Name: FireProcessTimerActivityName})

	return env
}

func processWorkflowTestSpec(processID string) agentosproc.Spec {
	return agentosproc.Spec{
		ProcessID:      processID,
		Kind:           "resource-lifecycle",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "start-" + processID,
		Resource: agentosproc.ResourceRef{
			Kind:       "resource-kind",
			ResourceID: "resource-" + processID,
			AccountID:  "acct-1",
			ProjectID:  "proj-1",
		},
		RequestedAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
	}
}

func processWorkflowInputForTest(spec *agentosproc.Spec) *processWorkflowInput {
	return &processWorkflowInput{
		Spec:            *spec,
		WorkflowVersion: currentProcessWorkflowVersion,
		TaskQueues: processTaskQueues{
			ProcessActivity: DefaultTaskQueues().ProcessActivity,
		},
	}
}

func TestProcessActivitiesUseDurableRuntime(t *testing.T) {
	t.Parallel()

	store := agentosprocess.NewMemoryStore()
	activities, err := NewProcessActivities(store, store)
	require.NoError(t, err)

	spec := processWorkflowTestSpec("process-activity")
	status, err := activities.StartProcessActivity(context.Background(), &startProcessActivityInput{Spec: spec})
	require.NoError(t, err)
	require.Equal(t, agentosproc.ProcessRunning, status.LifecycleState)
}
