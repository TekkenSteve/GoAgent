package temporal

import (
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateWorkflowVersion(t *testing.T) {
	t.Parallel()

	require.NoError(t, validatePlanWorkflowVersion(currentPlanWorkflowVersion))
	require.NoError(t, validateTriggerDispatchWorkflowVersion(currentTriggerDispatchWorkflowVersion))

	err := validatePlanWorkflowVersion(currentPlanWorkflowVersion + 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errTemporalWorkflowVersionInvalid))

	err = validateTriggerDispatchWorkflowVersion(currentTriggerDispatchWorkflowVersion + 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errTemporalWorkflowVersionInvalid))
}

func TestAgentOSWorkflowVersionPinsCoverOwnedWorkflows(t *testing.T) {
	t.Parallel()

	pins := agentOSWorkflowVersionPins()

	names := make([]string, 0, len(pins))
	for _, pin := range pins {
		require.NotEmpty(t, pin.Name)
		require.Positive(t, pin.Version)
		require.False(t, slices.Contains(names, pin.Name), "duplicate workflow version pin %q", pin.Name)
		names = append(names, pin.Name)
	}

	require.Contains(t, names, PlanWorkflowName)
	require.Contains(t, names, ProcessWorkflowName)
	require.Contains(t, names, TriggerDispatchWorkflowName)
}
