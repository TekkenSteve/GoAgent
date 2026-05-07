package state

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkflowHotStateLayerTags(t *testing.T) {
	tp := reflect.TypeFor[WorkflowHotState]()

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

	for field := range tp.Fields() {
		tag := field.Tag.Get("layer")
		require.Equal(t, expected[field.Name], tag, "field %s must stay in expected layer", field.Name)
	}
}

func TestWarmAndColdRefsLayerTags(t *testing.T) {
	warm := reflect.TypeFor[WarmRefs]()
	for field := range warm.Fields() {
		require.Equal(t, "warm", field.Tag.Get("layer"))
	}

	cold := reflect.TypeFor[ColdRefs]()
	for field := range cold.Fields() {
		require.Equal(t, "cold", field.Tag.Get("layer"))
	}
}
