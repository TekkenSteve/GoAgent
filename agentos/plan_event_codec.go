package agentos

import (
	"encoding/json"
	"fmt"
)

// MarshalPlanEvent serializes the public RunPlan event envelope.
func MarshalPlanEvent(event PlanEvent) ([]byte, error) {
	if err := validatePlanEvent(event); err != nil {
		return nil, err
	}

	return json.Marshal(event)
}

// UnmarshalPlanEvent deserializes the public RunPlan event envelope.
func UnmarshalPlanEvent(data []byte) (PlanEvent, error) {
	var event PlanEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return PlanEvent{}, fmt.Errorf("%w: decode plan event: %s", ErrInvalidPlanEvent, err)
	}
	if err := validatePlanEvent(event); err != nil {
		return PlanEvent{}, err
	}

	return event, nil
}

func validatePlanEvent(event PlanEvent) error {
	if event.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", ErrInvalidPlanEvent)
	}
	if event.EventType == "" {
		return fmt.Errorf("%w: event type is required", ErrInvalidPlanEvent)
	}

	return nil
}
