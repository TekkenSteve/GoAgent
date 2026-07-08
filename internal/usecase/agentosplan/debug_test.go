package agentosplan

import (
	"errors"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestBuildPlanDebugTracesProjectsTypedDebugFields(t *testing.T) {
	t.Parallel()

	traces, err := BuildPlanDebugTraces(debugTraceProjectionEvents())
	if err != nil {
		t.Fatalf("BuildPlanDebugTraces: %v", err)
	}

	if len(traces) != 2 {
		t.Fatalf("traces = %#v", traces)
	}

	requireCapabilityDebugTrace(t, &traces[0])
	requireInputConditionDebugTrace(t, &traces[1])
}

func debugTraceProjectionEvents() []agentos.PlanEvent {
	return []agentos.PlanEvent{
		capabilityDebugProjectionEvent(),
		conditionDebugProjectionEvent(),
		nonDebugProjectionEvent(),
	}
}

func capabilityDebugProjectionEvent() agentos.PlanEvent {
	return agentos.PlanEvent{
		Event: agentoscore.Event{
			EventID:   "evt-1",
			EventType: agentoscore.EventCapabilitySelected,
			Sequence:  1,
			Timestamp: time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
			Payload: map[string]any{
				planEventPayloadTransition: map[string]any{
					"previous_lifecycle_state": agentos.PlanNodePending,
					"next_lifecycle_state":     agentos.PlanNodePending,
				},
				planEventPayloadCapability: CapabilitySelectionTrace{
					Backend:         agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research-http"},
					Capability:      "run",
					Controls:        []agentoscore.ControlOperation{agentoscore.ControlCancel},
					HasOutputSchema: true,
				},
			},
		},
		PlanID: "plan-1",
		NodeID: "research",
	}
}

func conditionDebugProjectionEvent() agentos.PlanEvent {
	return agentos.PlanEvent{
		Event: agentoscore.Event{
			EventID:   "evt-2",
			EventType: agentoscore.EventConditionEvaluated,
			Sequence:  2,
			Payload: map[string]any{
				planEventPayloadInputResolution: debugInputResolutionPayload(),
				planEventPayloadConditions:      debugConditionTracePayload(),
			},
		},
		PlanID: "plan-1",
		NodeID: "write",
	}
}

func debugInputResolutionPayload() map[string]any {
	return map[string]any{
		"input_digest":  "digest-1",
		"input_keys":    []any{"topic"},
		"mapping_count": float64(1),
		"mappings": []any{
			map[string]any{"target": "topic", "source_artifact": "summary", "required": true},
		},
	}
}

func debugConditionTracePayload() []ConditionEvaluationTrace {
	return []ConditionEvaluationTrace{
		{
			Scope:      "edge",
			NodeID:     "write",
			EdgeID:     "research-write",
			From:       "research",
			To:         "write",
			Expression: "status.nodes.research == 'succeeded'",
			Result:     true,
			On:         agentos.EdgeOnSuccess,
		},
	}
}

func nonDebugProjectionEvent() agentos.PlanEvent {
	return agentos.PlanEvent{
		Event:  agentoscore.Event{EventID: "evt-3", EventType: agentoscore.EventPlanStarted, Sequence: 3},
		PlanID: "plan-1",
	}
}

func requireCapabilityDebugTrace(t *testing.T, trace *agentos.PlanDebugTrace) {
	t.Helper()

	if trace.Transition == nil || trace.Capability == nil {
		t.Fatalf("capability trace = %#v", trace)
	}

	if trace.Transition.PreviousLifecycleState != agentos.PlanNodePending ||
		trace.Capability.Backend.Name != "research-http" ||
		len(trace.Capability.Controls) != 1 {
		t.Fatalf("capability trace = %#v", trace)
	}
}

func requireInputConditionDebugTrace(t *testing.T, trace *agentos.PlanDebugTrace) {
	t.Helper()

	if trace.InputResolution == nil {
		t.Fatalf("input trace = %#v", trace)
	}

	if trace.InputResolution.InputDigest != "digest-1" ||
		len(trace.InputResolution.Mappings) != 1 ||
		len(trace.Conditions) != 1 ||
		!trace.Conditions[0].Result {
		t.Fatalf("input/condition trace = %#v", trace)
	}
}

func TestPlanDebugTraceFromEventRejectsInvalidDebugPayload(t *testing.T) {
	t.Parallel()

	_, _, err := PlanDebugTraceFromEvent(&agentos.PlanEvent{
		Event: agentoscore.Event{
			EventID:   "evt-1",
			EventType: agentoscore.EventConditionEvaluated,
			Payload: map[string]any{
				planEventPayloadConditions: map[string]any{"not": "a condition list"},
			},
		},
		PlanID: "plan-1",
	})
	if !errors.Is(err, agentoscore.ErrInvalidPlanEvent) {
		t.Fatalf("error = %v, want ErrInvalidPlanEvent", err)
	}
}
