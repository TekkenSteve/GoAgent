package temporal

import (
	"context"
	"sync"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	RUNNING                           = "running"
	workflowTestDeadlockDetectionTime = 10 * time.Second
)

func TestPlanWorkflowExecutesSuccessEdgeAndPublishesArtifacts(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-success",
		IdempotencyKey: "plan-start-success",
		Policy: agentos.PlanPolicy{
			MaxParallelNodes: 2,
		},
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "research",
				Run:    agentos.RunSpec{RunID: "run-research", Backend: ref},
				Outputs: []agentos.ArtifactSpec{
					{Name: "summary", Kind: agentoscore.ArtifactKindObject, Required: true},
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
				Artifacts: []agentoscore.ArtifactRef{
					{ArtifactID: "artifact-summary", Name: "summary", Kind: agentoscore.ArtifactKindObject},
				},
			},
			"run-verify": {RunID: "run-verify", LifecycleState: "completed"},
		},
	}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnv(t, mocks, &spec)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

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

func TestPlanWorkflowFailsNodeWhenRequiredInputArtifactIsMissing(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-missing-input-artifact",
		IdempotencyKey: "plan-start-missing-input-artifact",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "research",
				Run:    agentos.RunSpec{RunID: "run-research", Backend: ref},
				Outputs: []agentos.ArtifactSpec{
					{Name: "summary", Kind: agentoscore.ArtifactKindObject},
				},
			},
			{NodeID: "verify", Run: agentos.RunSpec{RunID: "run-verify", Backend: ref}},
		},
		Edges: []agentos.PlanEdgeSpec{
			{
				EdgeID: "research-verify",
				From:   "research",
				To:     "verify",
				On:     agentos.EdgeOnSuccess,
				InputMapping: []agentos.InputMapping{
					{
						Target:         "summary",
						SourceNodeID:   "research",
						SourceArtifact: "summary",
						Required:       true,
					},
				},
			},
		},
	}
	mocks := &planWorkflowMocks{
		statuses: map[string]agentos.RunStatus{
			"run-research": {RunID: "run-research", LifecycleState: "completed"},
		},
	}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnv(t, mocks, &spec)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleFailed, result.LifecycleState)
	require.Contains(t, result.Reason, agentoscore.ErrArtifactNotFound.Error())
	require.Equal(t, []string{"run-research"}, mocks.started)

	verify := findPlanNodeStatus(result.Nodes, "verify")
	require.NotNil(t, verify)
	require.Equal(t, agentos.PlanNodeFailed, verify.LifecycleState)
	require.Contains(t, verify.Reason, "required artifact")
}

func TestPlanWorkflowRequiresCurrentWorkflowVersion(t *testing.T) {
	t.Parallel()

	spec := agentos.RunPlanSpec{
		PlanID:         "plan-version-required",
		IdempotencyKey: "plan-version-required-key",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "node",
				Run: agentos.RunSpec{
					RunID:   "run-version-required",
					Backend: agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
				},
			},
		},
	}
	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnv(t, &planWorkflowMocks{}, &spec)

	input := planWorkflowInputForTest(&spec)
	input.WorkflowVersion = currentPlanWorkflowVersion + 1
	env.ExecuteWorkflow(PlanWorkflow, input)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestPlanWorkflowPublishesDebugTraceEvents(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-debug-trace",
		AccountID:      "acct-debug-trace",
		ProjectID:      "proj-debug-trace",
		IdempotencyKey: "plan-start-debug-trace",
		Inputs: map[string]any{
			"task": map[string]any{"topic": "durable trace"},
		},
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID:     "research",
				Capability: "run",
				Run: agentos.RunSpec{
					RunID:   "run-research",
					Backend: ref,
					Input:   map[string]any{"existing": true},
				},
				Inputs: []agentos.InputMapping{
					{Target: "topic", SourcePath: "task.topic", Required: true},
				},
				Conditions: []string{"inputs.task.topic == 'durable trace'"},
			},
		},
	}
	store := agentosplan.NewMemoryPlanStore()
	mocks := &planWorkflowMocks{statuses: map[string]agentos.RunStatus{"run-research": {RunID: "run-research", LifecycleState: "completed"}}}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnvWithStores(t, mocks, []agentos.Capability{{Backend: ref, Name: "run", Controls: []agentoscore.ControlOperation{agentoscore.ControlCancel}}}, store, agentosplan.NewMemoryArtifactStore(), &spec)
	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, mocks.startedInputs, 1)
	require.Equal(t, "durable trace", mocks.startedInputs[0]["topic"])

	events, err := store.ListPlanEvents(context.Background(), planWorkflowEventScopePtr(&spec), 0)
	require.NoError(t, err)

	capabilityEvent := findPlanEventByType(events, agentoscore.EventCapabilitySelected)
	require.NotNil(t, capabilityEvent)
	require.NotNil(t, capabilityEvent.Payload["capability"])
	require.NotNil(t, capabilityEvent.Payload["transition"])

	inputEvent := findPlanEventByType(events, agentoscore.EventNodeInputResolved)
	require.NotNil(t, inputEvent)
	require.NotNil(t, inputEvent.Payload["input_resolution"])
	require.NotNil(t, inputEvent.Payload["transition"])

	conditionEvent := findPlanEventByType(events, agentoscore.EventConditionEvaluated)
	require.NotNil(t, conditionEvent)
	require.NotNil(t, conditionEvent.Payload["conditions"])
}

func TestPlanWorkflowErrorEdgeRunsRecoveryButPlanRemainsFailed(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-error",
		IdempotencyKey: "plan-start-error",
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

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnv(t, mocks, &spec)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

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
		PlanID:         "plan-retry",
		IdempotencyKey: "plan-start-retry",
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

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnv(t, mocks, &spec)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

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

func TestPlanWorkflowAppliesPlanDeltaArtifact(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-expand", AccountID: "acct-expand", ProjectID: "proj-expand", IdempotencyKey: "plan-start-expand",
		Policy: agentos.PlanPolicy{MaxNodes: 2},
		Nodes:  []agentos.PlanNodeSpec{{NodeID: "seed", Run: agentos.RunSpec{RunID: "run-seed", Backend: ref}}},
	}
	store := agentosplan.NewMemoryPlanStore()
	artifactStore := agentosplan.NewMemoryArtifactStore()
	deltaRef, err := putArtifact(context.Background(), artifactStore, &agentoscore.ArtifactRef{ArtifactID: "delta-1", PlanID: spec.PlanID, NodeID: "seed", RunID: "run-seed", Name: "expand", Kind: agentoscore.ArtifactKindPlanDelta}, agentosplan.PlanDelta{
		Nodes: []agentos.PlanNodeSpec{{NodeID: "expanded", Capability: "expand", Run: agentos.RunSpec{RunID: "run-expanded", Backend: ref}}},
		Edges: []agentos.PlanEdgeSpec{{EdgeID: "seed-expanded", From: "seed", To: "expanded", On: agentos.EdgeOnSuccess}},
	}, "delta-key")
	require.NoError(t, err)

	mocks := &planWorkflowMocks{statuses: map[string]agentos.RunStatus{
		"run-seed":     {RunID: "run-seed", LifecycleState: "completed", Artifacts: []agentoscore.ArtifactRef{deltaRef}},
		"run-expanded": {RunID: "run-expanded", LifecycleState: "completed"},
	}}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnvWithStores(t, mocks, []agentos.Capability{{Backend: ref, Name: "expand"}}, store, artifactStore, &spec)
	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleSucceeded, result.LifecycleState)
	require.Equal(t, []string{"run-seed", "run-expanded"}, mocks.started)
	require.Len(t, result.Nodes, 2)
	require.Equal(t, "expanded", result.Nodes[0].NodeID)
	require.Equal(t, "seed", result.Nodes[1].NodeID)

	events, err := store.ListPlanEvents(context.Background(), planWorkflowEventScopePtr(&spec), 0)
	require.NoError(t, err)

	eventTypes := make([]agentoscore.EventType, 0, len(events))
	expandedCapabilitySelected := false

	for _, event := range events {
		eventTypes = append(eventTypes, event.EventType)
		if event.EventType == agentoscore.EventCapabilitySelected && event.NodeID == "expanded" {
			expandedCapabilitySelected = true
		}
	}

	require.Contains(t, eventTypes, agentoscore.EventPlanExpanded)
	require.True(t, expandedCapabilitySelected)
}

func TestPlanWorkflowCancelsTimedOutNode(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-timeout",
		IdempotencyKey: "plan-start-timeout",
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
			"run-slow": {RunID: "run-slow", LifecycleState: RUNNING},
		},
	}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnv(t, mocks, &spec)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleFailed, result.LifecycleState)
	require.Contains(t, result.Reason, "timed out")
	require.Equal(t, []string{"run-slow"}, mocks.started)
	require.Len(t, mocks.controls, 1)
	require.Equal(t, "run-slow", mocks.controls[0].RunID)
	require.Equal(t, agentoscore.ControlCancel, mocks.controls[0].Control.Operation)
	require.NotEmpty(t, mocks.controls[0].Control.IdempotencyKey)
}

func TestPlanWorkflowCancelsActiveNodesWhenPlanTimesOut(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-timeout-global",
		IdempotencyKey: "plan-start-timeout-global",
		Policy: agentos.PlanPolicy{
			TimeoutSeconds: 1,
		},
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "slow", Run: agentos.RunSpec{RunID: "run-slow", Backend: ref}},
		},
	}
	mocks := &planWorkflowMocks{
		statuses: map[string]agentos.RunStatus{
			"run-slow": {RunID: "run-slow", LifecycleState: RUNNING},
		},
	}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnv(t, mocks, &spec)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleFailed, result.LifecycleState)
	require.Contains(t, result.Reason, "plan timed out")
	require.False(t, result.StartedAt.IsZero())
	require.Equal(t, []string{"run-slow"}, mocks.started)
	require.Len(t, mocks.controls, 1)
	require.Equal(t, "run-slow", mocks.controls[0].RunID)
	require.Equal(t, agentoscore.ControlCancel, mocks.controls[0].Control.Operation)
	require.NotEmpty(t, mocks.controls[0].Control.IdempotencyKey)
}

func TestPlanWorkflowCancelsActiveNodesWhenBudgetExceeded(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-budget",
		IdempotencyKey: "plan-start-budget",
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
			"run-slow": {RunID: "run-slow", LifecycleState: RUNNING},
		},
	}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnv(t, mocks, &spec)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

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
	require.Equal(t, agentoscore.ControlCancel, mocks.controls[0].Control.Operation)
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
		PlanID: "plan-mixed-backends", AccountID: "acct-mixed-backends", ProjectID: "proj-mixed-backends", IdempotencyKey: "plan-start-mixed-backends",
		Policy: agentos.PlanPolicy{MaxParallelNodes: 2},
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
	activities, err := NewPlanActivitiesWithStores(runtime, mixedBackendCapabilities(refs), store, nil, agentosplan.NewMemoryArtifactStore())
	require.NoError(t, err)

	env := newAgentOSTemporalWorkflowTestEnv()
	env.RegisterWorkflowWithOptions(PlanWorkflow, workflow.RegisterOptions{Name: PlanWorkflowName})

	for _, a := range []struct {
		fn   any
		name string
	}{
		{activities.ValidatePlanActivity, ValidatePlanActivityName},
		{activities.PersistPlanStateActivity, PersistPlanStateActivityName},
		{activities.ResolvePlanNodeInputActivity, ResolvePlanNodeInputActivityName},
		{activities.StartPlanNodeActivity, StartPlanNodeActivityName},
		{activities.StatusPlanNodeActivity, StatusPlanNodeActivityName},
		{activities.ControlPlanNodeActivity, ControlPlanNodeActivityName},
		{activities.PublishPlanArtifactsActivity, PublishPlanArtifactsActivityName},
		{activities.EvaluatePlanExpansionActivity, EvaluatePlanExpansionActivityName},
	} {
		env.RegisterActivityWithOptions(a.fn, activity.RegisterOptions{Name: a.name})
	}

	createPlanForWorkflowTest(t, store, &spec)
	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleSucceeded, result.LifecycleState)
	require.Equal(t, []agentos.BackendRef{refs[0], refs[1], refs[2], refs[3]}, runtime.startedBackends())

	events, err := store.ListPlanEvents(context.Background(), planWorkflowEventScopePtr(&spec), 0)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	require.Equal(t, agentoscore.EventPlanStarted, events[0].EventType)
	require.Equal(t, agentoscore.EventPlanSucceeded, events[len(events)-1].EventType)
}

func newAgentOSTemporalWorkflowTestEnv() *testsuite.TestWorkflowEnvironment {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: workflowTestDeadlockDetectionTime})

	return env
}

func TestPlanWorkflowRetriesFailedNodeFromSignal(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-manual-retry",
		IdempotencyKey: "plan-start-manual-retry",
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID: "flaky",
				Run:    agentos.RunSpec{RunID: "run-flaky", Backend: ref},
				Policy: agentos.NodePolicy{
					MaxAttempts: 2,
				},
			},
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
				{RunID: "run-slow", LifecycleState: RUNNING},
				{RunID: "run-slow", LifecycleState: RUNNING},
				{RunID: "run-slow", LifecycleState: "completed"},
			},
		},
	}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnv(t, mocks, &spec)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanSignalName, agentoscore.Signal{
			Type:           agentoscore.SignalPlanNodeRetry,
			IdempotencyKey: "retry-flaky",
			ActorID:        "operator-1",
			Payload: map[string]any{
				agentosplan.SignalPayloadNodeID: "flaky",
			},
		})
	}, time.Second)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

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

func TestPlanWorkflowRejectsManualRetryBeyondMaxAttempts(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-manual-retry-max",
		IdempotencyKey: "plan-start-manual-retry-max",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "flaky", Run: agentos.RunSpec{RunID: "run-flaky", Backend: ref}},
			{NodeID: "slow", Run: agentos.RunSpec{RunID: "run-slow", Backend: ref}},
		},
	}
	store := agentosplan.NewMemoryPlanStore()
	mocks := &planWorkflowMocks{
		statuses: map[string]agentos.RunStatus{
			"run-flaky": {RunID: "run-flaky", LifecycleState: "failed", Reason: "needs manual retry"},
		},
		statusSequences: map[string][]agentos.RunStatus{
			"run-slow": {
				{RunID: "run-slow", LifecycleState: RUNNING},
				{RunID: "run-slow", LifecycleState: RUNNING},
			},
		},
	}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnvWithStores(t, mocks, nil, store, agentosplan.NewMemoryArtifactStore(), &spec)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanSignalName, agentoscore.Signal{
			Type:           agentoscore.SignalPlanNodeRetry,
			IdempotencyKey: "retry-flaky",
			ActorID:        "operator-1",
			Payload: map[string]any{
				agentosplan.SignalPayloadNodeID: "flaky",
			},
		})
	}, time.Second)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Equal(t, []string{"run-flaky", "run-slow"}, mocks.started)

	snapshot, ok, err := store.LoadPlanState(context.Background(), spec.PlanID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, agentos.PlanLifecycleFailed, snapshot.Status.LifecycleState)
	require.Contains(t, snapshot.Status.Reason, "exceeds max attempts")
	require.Len(t, snapshot.Status.Nodes, 2)
	flaky := findPlanNodeStatus(snapshot.Status.Nodes, "flaky")
	require.NotNil(t, flaky)
	require.Equal(t, int32(1), flaky.Attempts)
}

func TestPlanWorkflowRejectSignalFailsRunningPlan(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-reject",
		IdempotencyKey: "plan-start-reject",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "slow", Run: agentos.RunSpec{RunID: "run-slow", Backend: ref}},
		},
	}
	mocks := &planWorkflowMocks{
		statusSequences: map[string][]agentos.RunStatus{
			"run-slow": {
				{RunID: "run-slow", LifecycleState: RUNNING},
				{RunID: "run-slow", LifecycleState: RUNNING},
			},
		},
	}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnv(t, mocks, &spec)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanSignalName, agentoscore.Signal{
			Type:           agentoscore.SignalPlanReject,
			IdempotencyKey: "reject-plan",
			ActorID:        "operator-1",
			Payload: map[string]any{
				agentosplan.SignalPayloadReason: "operator rejected",
			},
		})
	}, time.Second)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleFailed, result.LifecycleState)
	require.Equal(t, "operator rejected", result.Reason)
	require.Equal(t, []string{"run-slow"}, mocks.started)
	require.Empty(t, result.ActiveRunIDs)
	slow := findPlanNodeStatus(result.Nodes, "slow")
	require.NotNil(t, slow)
	require.Equal(t, agentos.PlanNodeCanceled, slow.LifecycleState)
	require.Equal(t, "run-slow", slow.RunID)
	require.Len(t, mocks.controls, 1)
	require.Equal(t, "run-slow", mocks.controls[0].RunID)
	require.Equal(t, agentoscore.ControlCancel, mocks.controls[0].Control.Operation)
	require.NotEmpty(t, mocks.controls[0].Control.IdempotencyKey)
}

func TestPlanWorkflowApproveSignalUnblocksPausedPlan(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-approve",
		IdempotencyKey: "plan-start-approve",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "slow", Capability: "pausable", Run: agentos.RunSpec{RunID: "run-slow", Backend: ref}},
		},
	}
	mocks := &planWorkflowMocks{
		statusSequences: map[string][]agentos.RunStatus{
			"run-slow": {
				{RunID: "run-slow", LifecycleState: RUNNING},
				{RunID: "run-slow", LifecycleState: RUNNING},
				{RunID: "run-slow", LifecycleState: "completed"},
			},
		},
	}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnvWithCapabilities(t, mocks, []agentos.Capability{
		{Backend: ref, Name: "pausable", Controls: []agentoscore.ControlOperation{agentoscore.ControlPause}},
	}, &spec)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanControlSignalName, agentoscore.ControlRequest{
			Operation:      agentoscore.ControlPause,
			IdempotencyKey: "pause-plan",
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanSignalName, agentoscore.Signal{
			Type:           agentoscore.SignalPlanApprove,
			IdempotencyKey: "approve-plan",
			ActorID:        "operator-1",
		})
	}, 2*time.Second)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleSucceeded, result.LifecycleState)
	require.Equal(t, []string{"run-slow"}, mocks.started)
	require.Len(t, mocks.controls, 1)
	require.Equal(t, agentoscore.ControlPause, mocks.controls[0].Control.Operation)
}

func TestPlanWorkflowPauseControlPreflightsUnsupportedRunningNodes(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: "plan-pause-preflight", IdempotencyKey: "plan-start-pause-preflight",
		Policy: agentos.PlanPolicy{MaxParallelNodes: 2},
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "pausable", Capability: "pausable", Run: agentos.RunSpec{RunID: "run-pausable", Backend: ref}},
			{NodeID: "plain", Capability: "plain", Run: agentos.RunSpec{RunID: "run-plain", Backend: ref}},
		},
	}
	store := agentosplan.NewMemoryPlanStore()
	mocks := &planWorkflowMocks{
		statusSequences: map[string][]agentos.RunStatus{
			"run-pausable": {{RunID: "run-pausable", LifecycleState: RUNNING}, {RunID: "run-pausable", LifecycleState: RUNNING}},
			"run-plain":    {{RunID: "run-plain", LifecycleState: RUNNING}, {RunID: "run-plain", LifecycleState: RUNNING}},
		},
	}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnvWithStores(t, mocks, []agentos.Capability{
		{Backend: ref, Name: "pausable", Controls: []agentoscore.ControlOperation{agentoscore.ControlPause}},
		{Backend: ref, Name: "plain"},
	}, store, agentosplan.NewMemoryArtifactStore(), &spec)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanControlSignalName, agentoscore.ControlRequest{Operation: agentoscore.ControlPause, IdempotencyKey: "pause-plan"})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanSignalName, agentoscore.Signal{Type: agentoscore.SignalPlanReject, IdempotencyKey: "reject-plan", ActorID: "operator-1", Payload: map[string]any{agentosplan.SignalPayloadReason: "end preflight test"}})
	}, 2*time.Second)
	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleFailed, result.LifecycleState)
	require.Equal(t, []string{"run-pausable", "run-plain"}, mocks.started)

	for _, control := range mocks.controls {
		require.NotEqual(t, agentoscore.ControlPause, control.Control.Operation)
	}

	require.Len(t, mocks.controls, 2)
	require.Equal(t, agentoscore.ControlCancel, mocks.controls[0].Control.Operation)
	require.Equal(t, agentoscore.ControlCancel, mocks.controls[1].Control.Operation)

	events, err := store.ListPlanEvents(context.Background(), planWorkflowEventScopePtr(&spec), 0)
	require.NoError(t, err)

	blocked := findPlanEventByType(events, agentoscore.EventPlanBlocked)
	require.NotNil(t, blocked)
	reason, ok := blocked.Payload["reason"].(string)
	require.True(t, ok)
	require.Contains(t, reason, `node "plain" does not declare support for pause`)
}

func TestPlanWorkflowPauseControlPropagatesToAllSupportedRunningNodes(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-pause-all-supported",
		IdempotencyKey: "plan-start-pause-all-supported",
		Policy: agentos.PlanPolicy{
			MaxParallelNodes: 2,
		},
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "left", Capability: "pausable", Run: agentos.RunSpec{RunID: "run-left", Backend: ref}},
			{NodeID: "right", Capability: "pausable", Run: agentos.RunSpec{RunID: "run-right", Backend: ref}},
		},
	}
	mocks := &planWorkflowMocks{
		statusSequences: map[string][]agentos.RunStatus{
			"run-left": {
				{RunID: "run-left", LifecycleState: RUNNING},
				{RunID: "run-left", LifecycleState: RUNNING},
				{RunID: "run-left", LifecycleState: "completed"},
			},
			"run-right": {
				{RunID: "run-right", LifecycleState: RUNNING},
				{RunID: "run-right", LifecycleState: RUNNING},
				{RunID: "run-right", LifecycleState: "completed"},
			},
		},
	}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnvWithCapabilities(t, mocks, []agentos.Capability{
		{Backend: ref, Name: "pausable", Controls: []agentoscore.ControlOperation{agentoscore.ControlPause}},
	}, &spec)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanControlSignalName, agentoscore.ControlRequest{
			Operation:      agentoscore.ControlPause,
			IdempotencyKey: "pause-plan",
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanSignalName, agentoscore.Signal{
			Type:           agentoscore.SignalPlanApprove,
			IdempotencyKey: "approve-plan",
			ActorID:        "operator-1",
		})
	}, 2*time.Second)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleSucceeded, result.LifecycleState)
	require.Equal(t, []string{"run-left", "run-right"}, mocks.started)
	require.Len(t, mocks.controls, 2)
	require.Equal(t, "run-left", mocks.controls[0].RunID)
	require.Equal(t, agentoscore.ControlPause, mocks.controls[0].Control.Operation)
	require.Equal(t, "run-right", mocks.controls[1].RunID)
	require.Equal(t, agentoscore.ControlPause, mocks.controls[1].Control.Operation)
}

func TestPlanWorkflowContinuedInputRestoresSnapshotStatus(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-continued",
		IdempotencyKey: "plan-start-continued",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: RUNNING, Run: agentos.RunSpec{RunID: "run-running", Backend: ref}},
		},
	}
	status := agentos.RunPlanStatus{
		PlanID:         "plan-continued",
		LifecycleState: agentos.PlanLifecycleRunning,
		Nodes: []agentos.PlanNodeStatus{
			{NodeID: RUNNING, RunID: "run-running", Backend: ref, LifecycleState: agentos.PlanNodeRunning},
		},
		BudgetUsage: agentos.PlanBudgetUsage{SpentCents: 25},
	}

	input := planWorkflowInput{
		Spec:            spec,
		Status:          status,
		WorkflowVersion: currentPlanWorkflowVersion,
		Continued:       true,
	}
	state, err := initialPlanWorkflowState(&input, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, agentos.PlanLifecycleRunning, state.Status.LifecycleState)
	require.Equal(t, int64(25), state.Status.BudgetUsage.SpentCents)
	require.Equal(t, []string{"run-running"}, state.Status.ActiveRunIDs)
}

func TestPlanWorkflowContinuationCarriesProcessedSignalAndControlKeys(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-continued-keys",
		IdempotencyKey: "plan-start-continued-keys",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: RUNNING, Run: agentos.RunSpec{RunID: "run-running", Backend: ref}},
		},
	}
	status := agentos.RunPlanStatus{
		PlanID:         "plan-continued-keys",
		LifecycleState: agentos.PlanLifecycleRunning,
		Nodes: []agentos.PlanNodeStatus{
			{NodeID: RUNNING, RunID: "run-running", Backend: ref, LifecycleState: agentos.PlanNodeRunning},
		},
	}
	input := planWorkflowInput{
		Spec:              spec,
		Status:            status,
		WorkflowVersion:   currentPlanWorkflowVersion,
		Continued:         true,
		ContinuationCount: 2,
	}
	state, err := initialPlanWorkflowState(&input, time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	processedControls := processedPlanWorkflowKeys([]string{"control-2", "control-1"})
	processedControls["control-3"] = true
	processedSignals := processedPlanWorkflowKeys([]string{"signal-2", "signal-1"})
	processedSignals["signal-3"] = true

	next := continuedPlanWorkflowInput(
		&input,
		&spec,
		&state,
		4,
		9,
		processedControls,
		processedSignals,
	)

	require.True(t, next.Continued)
	require.Equal(t, int32(3), next.ContinuationCount)
	require.Equal(t, currentPlanWorkflowVersion, next.WorkflowVersion)
	require.Equal(t, int32(4), next.ExpansionCount)
	require.Equal(t, int32(9), next.IterationCount)
	require.Equal(t, []string{"control-1", "control-2", "control-3"}, next.ProcessedControls)
	require.Equal(t, []string{"signal-1", "signal-2", "signal-3"}, next.ProcessedSignals)
}

func TestPlanWorkflowContinuesAsNewWhenHistoryLimitReached(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-history-continue",
		IdempotencyKey: "plan-start-history-continue",
		Policy: agentos.PlanPolicy{
			MaxHistoryEvents: 10,
		},
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: RUNNING, Run: agentos.RunSpec{RunID: "run-running", Backend: ref}},
		},
	}
	mocks := &planWorkflowMocks{}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnv(t, mocks, &spec)
	env.SetCurrentHistoryLength(10)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.True(t, workflow.IsContinueAsNewError(env.GetWorkflowError()))
}

func TestPlanWorkflowFailsWhenMaxIterationsExceeded(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-max-iterations",
		IdempotencyKey: "plan-start-max-iterations",
		Policy: agentos.PlanPolicy{
			MaxIterations: 1,
		},
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: RUNNING, Run: agentos.RunSpec{RunID: "run-running", Backend: ref}},
		},
	}
	store := agentosplan.NewMemoryPlanStore()
	mocks := &planWorkflowMocks{}

	planWorkflowTestSpec(&spec)
	env := newPlanWorkflowTestEnvWithStores(t, mocks, nil, store, agentosplan.NewMemoryArtifactStore(), &spec)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())

	snapshot, ok, err := store.LoadPlanState(context.Background(), spec.PlanID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, agentos.PlanLifecycleFailed, snapshot.Status.LifecycleState)
	require.Equal(t, "plan exceeded max iterations: 2 exceeds max 1", snapshot.Status.Reason)
}

type planWorkflowMocks struct {
	started         []string
	startedInputs   []map[string]any
	statuses        map[string]agentos.RunStatus
	statusSequences map[string][]agentos.RunStatus
	controls        []controlPlanNodeInput
}

func newPlanWorkflowTestEnv(t *testing.T, mocks *planWorkflowMocks, spec *agentos.RunPlanSpec) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	return newPlanWorkflowTestEnvWithCapabilities(t, mocks, nil, spec)
}

func newPlanWorkflowTestEnvWithCapabilities(t *testing.T, mocks *planWorkflowMocks, capabilities []agentos.Capability, spec *agentos.RunPlanSpec) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	return newPlanWorkflowTestEnvWithStores(t, mocks, capabilities, agentosplan.NewMemoryPlanStore(), agentosplan.NewMemoryArtifactStore(), spec)
}

func planWorkflowInputForTest(spec *agentos.RunPlanSpec) *planWorkflowInput {
	return &planWorkflowInput{
		Spec:            *spec,
		WorkflowVersion: currentPlanWorkflowVersion,
		TaskQueues: agentfwTaskQueues{
			PlanActivity: DefaultTaskQueues().PlanActivity,
		},
	}
}

func newPlanWorkflowTestEnvWithStores(t *testing.T, mocks *planWorkflowMocks, capabilities []agentos.Capability, store agentosplan.PlanTransitionStore, artifactStore agentosplan.ArtifactStore, spec *agentos.RunPlanSpec) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	planIndex, ok := store.(agentosplan.PlanIndex)
	if !ok {
		t.Fatalf("workflow test store %T does not implement PlanIndex", store)
	}

	createPlanForWorkflowTest(t, planIndex, spec)

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(PlanWorkflow, workflow.RegisterOptions{Name: PlanWorkflowName})

	activities, err := NewPlanActivitiesWithStores(
		&fakePlanRuntime{},
		capabilities,
		store,
		nil,
		artifactStore,
	)
	if err != nil {
		panic(err)
	}

	env.RegisterActivityWithOptions(activities.ValidatePlanActivity, activity.RegisterOptions{Name: ValidatePlanActivityName})
	env.RegisterActivityWithOptions(activities.PersistPlanStateActivity, activity.RegisterOptions{Name: PersistPlanStateActivityName})
	env.RegisterActivityWithOptions(activities.ResolvePlanNodeInputActivity, activity.RegisterOptions{Name: ResolvePlanNodeInputActivityName})
	env.RegisterActivityWithOptions(mocks.start, activity.RegisterOptions{Name: StartPlanNodeActivityName})
	env.RegisterActivityWithOptions(mocks.status, activity.RegisterOptions{Name: StatusPlanNodeActivityName})
	env.RegisterActivityWithOptions(mocks.control, activity.RegisterOptions{Name: ControlPlanNodeActivityName})
	env.RegisterActivityWithOptions(mocks.publishArtifacts, activity.RegisterOptions{Name: PublishPlanArtifactsActivityName})
	env.RegisterActivityWithOptions(activities.EvaluatePlanExpansionActivity, activity.RegisterOptions{Name: EvaluatePlanExpansionActivityName})

	return env
}

func createPlanForWorkflowTest(t *testing.T, index agentosplan.PlanIndex, spec *agentos.RunPlanSpec) {
	t.Helper()

	status := agentosplan.NewState(spec, time.Now().UTC()).Status
	if _, _, err := index.CreatePlan(context.Background(), spec, &status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}
}

func planWorkflowTestSpec(spec *agentos.RunPlanSpec) {
	if spec.AccountID == "" {
		spec.AccountID = "acct-" + spec.PlanID
	}

	if spec.ProjectID == "" {
		spec.ProjectID = "proj-" + spec.PlanID
	}
}

func (m *planWorkflowMocks) start(_ context.Context, input *startPlanNodeInput) (StartPlanNodeOutput, error) {
	m.started = append(m.started, input.Node.Run.RunID)
	m.startedInputs = append(m.startedInputs, input.Node.Run.Input)

	return StartPlanNodeOutput{Status: agentos.RunStatus{RunID: input.Node.Run.RunID, LifecycleState: RUNNING}}, nil
}

func (m *planWorkflowMocks) status(_ context.Context, input *statusPlanNodeInput) (StatusPlanNodeOutput, error) {
	if sequence := m.statusSequences[input.RunID]; len(sequence) > 0 {
		status := sequence[0]
		if len(sequence) > 1 {
			m.statusSequences[input.RunID] = sequence[1:]
		}

		if status.RunID == "" {
			status.RunID = input.RunID
		}

		return StatusPlanNodeOutput{Status: status}, nil
	}

	status := m.statuses[input.RunID]
	if status.RunID == "" {
		status.RunID = input.RunID
	}

	if status.LifecycleState == "" {
		status.LifecycleState = RUNNING
	}

	return StatusPlanNodeOutput{Status: status}, nil
}

func (m *planWorkflowMocks) control(_ context.Context, input *controlPlanNodeInput) error {
	m.controls = append(m.controls, *input)

	return nil
}

func (m *planWorkflowMocks) publishArtifacts(_ context.Context, input *publishPlanArtifactsInput) (PublishPlanArtifactsOutput, error) {
	refs, err := normalizeRunArtifacts(input.Spec.PlanID, &input.Node, &input.Status)
	if err != nil {
		return PublishPlanArtifactsOutput{}, err
	}

	return PublishPlanArtifactsOutput{Artifacts: refs}, nil
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

func (r *mixedBackendRuntime) Start(_ context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.started = append(r.started, *spec)

	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: RUNNING}, nil
}

func (r *mixedBackendRuntime) StartPlanNode(ctx context.Context, _, _ string, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	return r.Start(ctx, spec)
}

func (r *mixedBackendRuntime) Signal(context.Context, string, *agentoscore.Signal) error {
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
		status.LifecycleState = RUNNING
	}

	return status, nil
}

func (r *mixedBackendRuntime) Control(context.Context, string, *agentoscore.ControlRequest) error {
	return nil
}

func (r *mixedBackendRuntime) Subscribe(context.Context, agentoscore.StreamScope) (agentoscore.Subscription, error) {
	return nil, nil
}

func (r *mixedBackendRuntime) Close() error {
	return nil
}

func (r *mixedBackendRuntime) startedBackends() []agentos.BackendRef {
	r.mu.Lock()
	defer r.mu.Unlock()

	backends := make([]agentos.BackendRef, 0, len(r.started))
	for i := range r.started {
		backends = append(backends, r.started[i].Backend)
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

func findPlanEventByType(events []agentos.PlanEvent, eventType agentoscore.EventType) *agentos.PlanEvent {
	for i := range events {
		if events[i].EventType == eventType {
			return &events[i]
		}
	}

	return nil
}

func planWorkflowEventScope(spec *agentos.RunPlanSpec) agentos.PlanStreamScope {
	return agentos.PlanStreamScope{
		PlanID:    spec.PlanID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	}
}

func planWorkflowEventScopePtr(spec *agentos.RunPlanSpec) *agentos.PlanStreamScope {
	scope := planWorkflowEventScope(spec)

	return &scope
}

func findPlanNodeStatus(nodes []agentos.PlanNodeStatus, nodeID string) *agentos.PlanNodeStatus {
	for i := range nodes {
		if nodes[i].NodeID == nodeID {
			return &nodes[i]
		}
	}

	return nil
}
