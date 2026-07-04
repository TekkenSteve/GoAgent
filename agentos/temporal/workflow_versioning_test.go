package temporal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateWorkflowVersion(t *testing.T) {
	t.Parallel()

	require.NoError(t, validatePlanWorkflowVersion(currentPlanWorkflowVersion))

	err := validatePlanWorkflowVersion(currentPlanWorkflowVersion + 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errTemporalWorkflowVersionInvalid))
}
