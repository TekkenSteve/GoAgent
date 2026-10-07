package temporal

import (
	"errors"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// searchAttributeLifecycleCapture records every goagent.lifecycle_state value the
// workflow upserts, in call order. The SDK test environment replays the history
// after the initial run, so a transition is recorded on both passes; callers
// collapse consecutive duplicates with distinctLifecycleSequence.
type searchAttributeLifecycleCapture struct {
	values []string
}

func newSearchAttributeLifecycleCapture(env *testsuite.TestWorkflowEnvironment) *searchAttributeLifecycleCapture {
	c := &searchAttributeLifecycleCapture{}

	env.OnUpsertTypedSearchAttributes(mock.Anything).
		Run(func(args mock.Arguments) {
			if lifecycle := lifecycleSearchAttributeValue(args.Get(0)); lifecycle != "" {
				c.values = append(c.values, lifecycle)
			}
		}).
		Return(nil)

	return c
}

// lifecycleSearchAttributeValue extracts the goagent.lifecycle_state value from
// the typed SearchAttributes the test environment passes to the upsert mock.
// SearchAttributes is a public type with a GetKeyword accessor, so the test
// reads the value through the stable public API instead of SDK internals.
func lifecycleSearchAttributeValue(attrs any) string {
	searchAttributes, ok := attrs.(temporal.SearchAttributes)
	if !ok {
		return ""
	}

	value, _ := searchAttributes.GetKeyword(temporal.NewSearchAttributeKeyKeyword(orchestrationSearchAttrLifecycleState))

	return value
}

// distinctLifecycleSequence collapses consecutive equal values so the replay
// pass does not duplicate transitions.
func (c *searchAttributeLifecycleCapture) distinctLifecycleSequence() []string {
	if len(c.values) == 0 {
		return nil
	}

	sequence := []string{c.values[0]}
	for _, value := range c.values[1:] {
		if value != sequence[len(sequence)-1] {
			sequence = append(sequence, value)
		}
	}

	return sequence
}

// orchestrationSearchAttrLifecycleState mirrors the registered lifecycle key so
// the test does not need to import the agent framework layer.
const orchestrationSearchAttrLifecycleState = "goagent.lifecycle_state"

// errTestSearchAttributeNotDefined simulates a cluster that has not registered
// the visibility keys: the upsert fails but must not fail the run.
var errTestSearchAttributeNotDefined = errors.New("search attribute not defined")

func TestPlanWorkflowLifecycleSearchAttributes(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-search-attrs",
		IdempotencyKey: "plan-start-search-attrs",
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
	capture := newSearchAttributeLifecycleCapture(env)
	// The plan sleeps planNodePollInterval (5s) between iterations. The pause is
	// delivered before the first poll wake and the approval after the plan has
	// slept while blocked, so the blocked lifecycle is observable as its own
	// transition rather than being drained together with the approval in one
	// wake.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanControlSignalName, agentoscore.ControlRequest{
			Operation:      agentoscore.ControlPause,
			IdempotencyKey: "pause-plan",
		})
	}, 3*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PlanSignalName, agentoscore.Signal{
			Type:           agentoscore.SignalPlanApprove,
			IdempotencyKey: "approve-plan",
			ActorID:        "operator-1",
		})
	}, 9*time.Second)

	env.ExecuteWorkflow(PlanWorkflow, planWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentos.RunPlanStatus
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentos.PlanLifecycleSucceeded, result.LifecycleState)

	// The lifecycle stays queryable through every transition: running while the
	// plan executes, blocked while it waits for approval, and succeeded on exit.
	sequence := capture.distinctLifecycleSequence()
	require.Equal(t, agentos.PlanLifecycleRunning, sequence[0], "initial lifecycle must be synced")
	require.Contains(t, sequence, agentos.PlanLifecycleBlocked, "blocked plan must be queryable while awaiting approval")
	require.Equal(t, agentos.PlanLifecycleSucceeded, sequence[len(sequence)-1], "final lifecycle must be synced")
}

func TestProcessWorkflowLifecycleSearchAttributes(t *testing.T) {
	t.Parallel()

	spec := processWorkflowTestSpec("process-search-attrs")
	env := newProcessWorkflowTestEnv(t)
	capture := newSearchAttributeLifecycleCapture(env)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ProcessControlSignalName, agentoscore.ControlRequest{
			Operation:      agentoscore.ControlPause,
			IdempotencyKey: "pause-1",
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ProcessControlSignalName, agentoscore.ControlRequest{
			Operation:      agentoscore.ControlCancel,
			IdempotencyKey: "cancel-1",
		})
	}, 2*time.Second)

	env.ExecuteWorkflow(ProcessWorkflow, processWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentosproc.Status
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentosproc.ProcessCanceled, result.LifecycleState)

	// Paused and terminal lifecycles are visible to operators: a waiting process
	// is queryable mid-run, and the final lifecycle is synced on exit.
	sequence := capture.distinctLifecycleSequence()
	require.Contains(t, sequence, agentosproc.ProcessWaiting, "paused process must be queryable")
	require.Equal(t, agentosproc.ProcessCanceled, sequence[len(sequence)-1], "final lifecycle must be synced")
}

func TestProcessWorkflowSearchAttributesUpsertErrorDoesNotFailWorkflow(t *testing.T) {
	t.Parallel()

	spec := processWorkflowTestSpec("process-sa-error")
	env := newProcessWorkflowTestEnv(t)
	// Visibility is best-effort: a failed upsert (e.g. an unregistered key on a
	// cluster that has not been provisioned) must be logged, not fatal.
	env.OnUpsertTypedSearchAttributes(mock.Anything).Return(errTestSearchAttributeNotDefined).Once()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ProcessControlSignalName, agentoscore.ControlRequest{
			Operation:      agentoscore.ControlCancel,
			IdempotencyKey: "cancel-1",
		})
	}, time.Second)

	env.ExecuteWorkflow(ProcessWorkflow, processWorkflowInputForTest(&spec))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result agentosproc.Status
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, agentosproc.ProcessCanceled, result.LifecycleState)
}
