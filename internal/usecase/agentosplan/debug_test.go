package agentosplan

import (
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestBuildPlanDebugTracesProjectsTypedDebugFields(t *testing.T) {
	events := []agentos.PlanEvent{
		{
			Event: agentos.Event{
				EventID:   "evt-1",
				EventType: agentos.EventCapabilitySelected,
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
						Controls:        []agentos.ControlOperation{agentos.ControlCancel},
						HasOutputSchema: true,
					},
				},
			},
			PlanID: "plan-1",
			NodeID: "research",
		},
		{
			Event: agentos.Event{
				EventID:   "evt-2",
				EventType: agentos.EventConditionEvaluated,
				Sequence:  2,
				Payload: map[string]any{
					planEventPayloadInputResolution: map[string]any{
						"input_digest":  "digest-1",
						"input_keys":    []any{"topic"},
						"mapping_count": float64(1),
						"mappings": []any{
							map[string]any{
								"target":          "topic",
								"source_artifact": "summary",
								"required":        true,
							},
						},
					},
					planEventPayloadConditions: []ConditionEvaluationTrace{
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
					},
				},
			},
			PlanID: "plan-1",
			NodeID: "write",
		},
		{
			Event: agentos.Event{
				EventID:   "evt-3",
				EventType: agentos.EventPlanStarted,
				Sequence:  3,
			},
			PlanID: "plan-1",
		},
	}

	traces, err := BuildPlanDebugTraces(events)
	if err != nil {
		t.Fatalf("BuildPlanDebugTraces: %v", err)
	}
	if len(traces) != 2 {
		t.Fatalf("traces = %#v", traces)
	}
	if traces[0].Transition == nil ||
		traces[0].Transition.PreviousLifecycleState != agentos.PlanNodePending ||
		traces[0].Capability == nil ||
		traces[0].Capability.Backend.Name != "research-http" ||
		len(traces[0].Capability.Controls) != 1 {
		t.Fatalf("capability trace = %#v", traces[0])
	}
	if traces[1].InputResolution == nil ||
		traces[1].InputResolution.InputDigest != "digest-1" ||
		len(traces[1].InputResolution.Mappings) != 1 ||
		len(traces[1].Conditions) != 1 ||
		!traces[1].Conditions[0].Result {
		t.Fatalf("input/condition trace = %#v", traces[1])
	}
}

func TestPlanDebugTraceFromEventRejectsInvalidDebugPayload(t *testing.T) {
	_, _, err := PlanDebugTraceFromEvent(agentos.PlanEvent{
		Event: agentos.Event{
			EventID:   "evt-1",
			EventType: agentos.EventConditionEvaluated,
			Payload: map[string]any{
				planEventPayloadConditions: map[string]any{"not": "a condition list"},
			},
		},
		PlanID: "plan-1",
	})
	if !errors.Is(err, agentos.ErrInvalidPlanEvent) {
		t.Fatalf("error = %v, want ErrInvalidPlanEvent", err)
	}
}
