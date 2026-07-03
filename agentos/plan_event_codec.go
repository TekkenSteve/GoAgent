package agentos

import (
	"encoding/json"
	"fmt"
	"maps"
)

const planEventExtraFields = 4

// MarshalPlanEvent serializes the public RunPlan event envelope.
func MarshalPlanEvent(event *PlanEvent) ([]byte, error) {
	if err := validatePlanEvent(event); err != nil {
		return nil, err
	}

	return json.Marshal(event)
}

// UnmarshalPlanEvent deserializes the public RunPlan event envelope.
func UnmarshalPlanEvent(data []byte) (PlanEvent, error) {
	var event PlanEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return PlanEvent{}, fmt.Errorf("%w: decode plan event: %w", ErrInvalidPlanEvent, err)
	}

	if err := validatePlanEvent(&event); err != nil {
		return PlanEvent{}, err
	}

	return event, nil
}

func validatePlanEvent(event *PlanEvent) error {
	if event == nil {
		return fmt.Errorf("%w: plan event is required", ErrInvalidPlanEvent)
	}

	if event.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", ErrInvalidPlanEvent)
	}

	if event.AccountID == "" {
		return fmt.Errorf("%w: account id is required", ErrInvalidPlanEvent)
	}

	if event.ProjectID == "" {
		return fmt.Errorf("%w: project id is required", ErrInvalidPlanEvent)
	}

	if event.EventID == "" {
		return fmt.Errorf("%w: event id is required", ErrInvalidPlanEvent)
	}

	if event.EventType == "" {
		return fmt.Errorf("%w: event type is required", ErrInvalidPlanEvent)
	}

	if event.Sequence <= 0 {
		return fmt.Errorf("%w: sequence must be positive", ErrInvalidPlanEvent)
	}

	if event.Timestamp.IsZero() {
		return fmt.Errorf("%w: timestamp is required", ErrInvalidPlanEvent)
	}

	return nil
}

// ToEvent projects the plan-scoped event into the generic stream envelope used
// by Subscription. The plan scope remains available in Payload so subscribers
// do not need an out-of-band lookup to identify the event boundary.
func (event *PlanEvent) ToEvent() Event {
	if event == nil {
		return Event{}
	}

	generic := event.Event

	payload := make(map[string]any, len(generic.Payload)+planEventExtraFields)
	maps.Copy(payload, generic.Payload)

	payload["plan_id"] = event.PlanID
	payload["account_id"] = event.AccountID

	payload["project_id"] = event.ProjectID
	if event.NodeID != "" {
		payload["node_id"] = event.NodeID
	}

	generic.Payload = payload

	return generic
}
