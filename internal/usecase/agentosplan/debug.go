package agentosplan

import (
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// BuildPlanDebugTraces projects typed debug records from durable PlanEvents.
func BuildPlanDebugTraces(events []agentos.PlanEvent) ([]agentos.PlanDebugTrace, error) {
	traces := make([]agentos.PlanDebugTrace, 0, len(events))
	for i := range events {
		trace, ok, err := PlanDebugTraceFromEvent(&events[i])
		if err != nil {
			return nil, err
		}

		if ok {
			traces = append(traces, trace)
		}
	}

	return traces, nil
}

// PlanDebugTraceFromEvent extracts typed debug details from one durable event.
func PlanDebugTraceFromEvent(event *agentos.PlanEvent) (agentos.PlanDebugTrace, bool, error) {
	trace := agentos.PlanDebugTrace{
		EventID:   event.EventID,
		EventType: event.EventType,
		PlanID:    event.PlanID,
		NodeID:    event.NodeID,
		RunID:     event.RunID,
		ThreadID:  event.ThreadID,
		Sequence:  event.Sequence,
		Timestamp: event.Timestamp,
	}

	var hasDebug bool

	if value, ok := event.Payload[planEventPayloadTransition]; ok {
		transition, err := decodePlanDebugPayload[agentos.PlanStateTransition](value, planEventPayloadTransition)
		if err != nil {
			return agentos.PlanDebugTrace{}, false, err
		}

		trace.Transition = &transition
		hasDebug = true
	}

	if value, ok := event.Payload[planEventPayloadCapability]; ok {
		capability, err := decodePlanDebugPayload[agentos.PlanCapabilityTrace](value, planEventPayloadCapability)
		if err != nil {
			return agentos.PlanDebugTrace{}, false, err
		}

		trace.Capability = &capability
		hasDebug = true
	}

	if value, ok := event.Payload[planEventPayloadInputResolution]; ok {
		input, err := decodePlanDebugPayload[agentos.PlanInputResolutionTrace](value, planEventPayloadInputResolution)
		if err != nil {
			return agentos.PlanDebugTrace{}, false, err
		}

		trace.InputResolution = &input
		hasDebug = true
	}

	if value, ok := event.Payload[planEventPayloadConditions]; ok {
		conditions, err := decodePlanDebugPayload[[]agentos.PlanConditionTrace](value, planEventPayloadConditions)
		if err != nil {
			return agentos.PlanDebugTrace{}, false, err
		}

		trace.Conditions = conditions
		hasDebug = true
	}

	return trace, hasDebug, nil
}

func decodePlanDebugPayload[T any](value any, field string) (T, error) {
	var decoded T

	data, err := json.Marshal(value)
	if err != nil {
		return decoded, fmt.Errorf("%w: marshal debug field %q: %w", agentos.ErrInvalidPlanEvent, field, err)
	}

	if err := json.Unmarshal(data, &decoded); err != nil {
		return decoded, fmt.Errorf("%w: decode debug field %q: %w", agentos.ErrInvalidPlanEvent, field, err)
	}

	return decoded, nil
}
