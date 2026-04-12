package state

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkflowHotStateLayerTags(t *testing.T) {
	tp := reflect.TypeOf(WorkflowHotState{})

	expected := map[string]string{
		"RunID":             "hot",
		"ThreadID":          "hot",
		"ProjectID":         "hot",
		"AccountID":         "hot",
		"ModelRef":          "hot",
		"AgentID":           "hot",
		"AgentVersionID":    "hot",
		"ToolSchemaVersion": "hot",
		"AgentConfigVer":    "hot",
		"Step":              "hot",
		"Lifecycle":         "hot",
		"TerminationReason": "hot",
		"PendingToolCalls":  "hot",
		"AutoContinue":      "hot",
		"Control":           "hot",
		"Continuation":      "hot",
	}

	for i := 0; i < tp.NumField(); i++ {
		field := tp.Field(i)
		tag := field.Tag.Get("layer")
		require.Equal(t, expected[field.Name], tag, "field %s must stay in expected layer", field.Name)
	}
}

func TestWarmAndColdRefsLayerTags(t *testing.T) {
	warm := reflect.TypeOf(WarmRefs{})
	for i := 0; i < warm.NumField(); i++ {
		require.Equal(t, "warm", warm.Field(i).Tag.Get("layer"))
	}

	cold := reflect.TypeOf(ColdRefs{})
	for i := 0; i < cold.NumField(); i++ {
		require.Equal(t, "cold", cold.Field(i).Tag.Get("layer"))
	}
}
