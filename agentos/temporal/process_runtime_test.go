package temporal

import (
	"testing"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosprocess"
	"github.com/stretchr/testify/require"
)

func TestProcessRuntimeStartsWorkflowOnProcessControlQueue(t *testing.T) {
	t.Parallel()

	store := agentosprocess.NewMemoryStore()
	usecaseRuntime, err := agentosprocess.NewRuntime(store, store)
	require.NoError(t, err)

	temporalClient := &fakePlanTemporalClient{}
	taskQueues := DefaultTaskQueues()
	runtime := newProcessRuntimeWithStores(
		temporalClient,
		false,
		nil,
		usecaseRuntime,
		taskQueues.ProcessControl,
		&taskQueues,
	)
	spec := processWorkflowTestSpec("process-runtime-start")

	status, err := runtime.StartProcess(t.Context(), &spec)
	require.NoError(t, err)
	require.Equal(t, agentosproc.ProcessRunning, status.LifecycleState)
	require.Equal(t, processWorkflowID(spec.ProcessID), temporalClient.executeWorkflowID)
	require.Equal(t, taskQueues.ProcessControl, temporalClient.executeTaskQueue)
	require.Equal(t, 1, temporalClient.executeCount)
}

func TestProcessRuntimeSignalsWorkflowAfterTenantAuthorization(t *testing.T) {
	t.Parallel()

	store := agentosprocess.NewMemoryStore()
	usecaseRuntime, err := agentosprocess.NewRuntime(store, store)
	require.NoError(t, err)

	temporalClient := &fakePlanTemporalClient{}
	taskQueues := DefaultTaskQueues()
	runtime := newProcessRuntimeWithStores(
		temporalClient,
		false,
		nil,
		usecaseRuntime,
		taskQueues.ProcessControl,
		&taskQueues,
	)

	spec := processWorkflowTestSpec("process-runtime-signal")
	if _, err := usecaseRuntime.StartProcess(t.Context(), &spec); err != nil {
		t.Fatalf("StartProcess fixture: %v", err)
	}

	ref := agentosproc.Ref{ProcessID: spec.ProcessID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}
	err = runtime.SignalProcess(t.Context(), ref, &agentoscore.Signal{
		Type:           "external.update",
		IdempotencyKey: "signal-1",
	})
	require.NoError(t, err)
	require.Equal(t, processWorkflowID(spec.ProcessID), temporalClient.signalWorkflowID)
	require.Equal(t, ProcessSignalName, temporalClient.signalName)
}
