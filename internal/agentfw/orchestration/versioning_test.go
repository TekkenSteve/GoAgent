package orchestration

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateVersionPins(t *testing.T) {
	compat := VersionCompatibility{
		WorkflowVersions: []int{1, 2},
		ToolSchemas:      []string{"v1", "v2"},
		EventSchemas:     []string{"v1", "v2"},
	}

	err := ValidateVersionPins(VersionPins{
		WorkflowVersion:    2,
		ToolSchemaVersion:  "v2",
		EventSchemaVersion: "v1",
	}, compat)
	require.NoError(t, err)

	err = ValidateVersionPins(VersionPins{
		WorkflowVersion:    3,
		ToolSchemaVersion:  "v2",
		EventSchemaVersion: "v1",
	}, compat)
	require.Error(t, err)
}
