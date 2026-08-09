package temporal

import (
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateWorkflowVersion(t *testing.T) {
	t.Parallel()

	// Current version is always accepted.
	require.NoError(t, validatePlanWorkflowVersion(currentPlanWorkflowVersion))
	require.NoError(t, validateProcessWorkflowVersion(currentProcessWorkflowVersion))
	require.NoError(t, validateTriggerDispatchWorkflowVersion(currentTriggerDispatchWorkflowVersion))

	// The previous version stays replayable (compatibility window) so a
	// version-bump deploy does not kill in-flight runs on replay.
	require.NoError(t, validateTriggerDispatchWorkflowVersion(currentTriggerDispatchWorkflowVersion-1))

	// Versions newer than the worker are rejected (would replay with
	// unavailable structure).
	err := validatePlanWorkflowVersion(currentPlanWorkflowVersion + 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errTemporalWorkflowVersionInvalid))

	err = validateTriggerDispatchWorkflowVersion(currentTriggerDispatchWorkflowVersion + 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errTemporalWorkflowVersionInvalid))

	// Versions outside the compatibility window are rejected.
	err = validateTriggerDispatchWorkflowVersion(currentTriggerDispatchWorkflowVersion - 2)
	require.Error(t, err)
	require.True(t, errors.Is(err, errTemporalWorkflowVersionInvalid))

	// Version 0 (unpinned runs) is always rejected.
	err = validatePlanWorkflowVersion(0)
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
