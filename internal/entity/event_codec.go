package entity

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

var (
	// ErrMissingEventType is returned when the event payload has no event_type discriminator.
	ErrMissingEventType = errors.New("event_codec: missing event_type discriminator")
	// ErrUnknownEventType is returned when event_type is not registered with the codec.
	ErrUnknownEventType = errors.New("event_codec: unknown event type")
	// ErrUnexpectedEventType is returned when the type registered for event_type does not implement StreamEvent.
	ErrUnexpectedEventType = errors.New("event_codec: unexpected event type")
)

// EventRegistry manages the mapping of event_type to Go types.
type EventRegistry struct {
	mu       sync.RWMutex
	registry map[string]reflect.Type
}

// EventCodec serializes and deserializes stream events through an explicit
// registry. Use a codec with a custom registry at process boundaries that need
// extension event types.
type EventCodec struct {
	registry *EventRegistry
}

// NewEventRegistry creates a registry populated with built-in event types.
func NewEventRegistry(customTypes ...StreamEvent) *EventRegistry {
	r := &EventRegistry{
		registry: make(map[string]reflect.Type),
	}

	for _, typ := range []StreamEvent{
		&TextDeltaEvent{},
		&ReasoningDeltaEvent{},
		&ToolCallStartEvent{},
		&ToolCallDeltaEvent{},
		&ToolCallFinishEvent{},
		&UsageFinishEvent{},
		&ToolExecStartEvent{},
		&ToolExecStdoutEvent{},
		&ToolExecStderrEvent{},
		&ToolExecFinishEvent{},
		&AgentRunStartEvent{},
		&AgentRunFinishEvent{},
		&PrepStageEvent{},
		&ContextUsageEvent{},
		&StateDeltaEvent{},
		&InterruptEvent{},
		&AgentErrorEvent{},
		&UserCommandEvent{},
		&UserFeedbackEvent{},
	} {
		t := reflect.TypeOf(typ).Elem()
		r.registry[typ.EventType()] = t
	}

	for _, eventType := range AgentOSStandardEventTypes() {
		et := &AgentOSEvent{BaseEvent: BaseEvent{EventType: string(eventType)}}
		t := reflect.TypeFor[AgentOSEvent]()
		r.registry[et.EventType()] = t
	}

	for _, typ := range customTypes {
		r.RegisterEventType(typ)
	}

	return r
}

// RegisterEventType registers an event type to this registry.
// typ must be a pointer to a specific event type (for example, &TextDeltaEvent{}).
func (r *EventRegistry) RegisterEventType(typ StreamEvent) {
	t := reflect.TypeOf(typ).Elem()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.registry[typ.EventType()] = t
}

// NewEventCodec creates a codec backed by registry. A nil registry means the
// built-in event registry.
func NewEventCodec(registry *EventRegistry) EventCodec {
	if registry == nil {
		registry = NewEventRegistry()
	}

	return EventCodec{registry: registry}
}

// MarshalEvent serializes StreamEvent.
// event_type is carried by the BaseEvent.EventType field embedded in the specific type.
func MarshalEvent(e StreamEvent) ([]byte, error) {
	return NewEventCodec(nil).MarshalEvent(e)
}

// MarshalEvent serializes StreamEvent.
// event_type is carried by the BaseEvent.EventType field embedded in the specific type.
func (c EventCodec) MarshalEvent(e StreamEvent) ([]byte, error) {
	return json.Marshal(e)
}

// UnmarshalEvent deserializes StreamEvent (dispatched by event_type).
// Unregistered event_type returns an error to prevent unknown type injection.
func UnmarshalEvent(data []byte) (StreamEvent, error) {
	return NewEventCodec(nil).UnmarshalEvent(data)
}

// UnmarshalEvent deserializes StreamEvent using this codec's registry.
func (c EventCodec) UnmarshalEvent(data []byte) (StreamEvent, error) {
	return c.registry.UnmarshalEvent(data)
}

// UnmarshalEvent deserializes StreamEvent using this registry.
func (r *EventRegistry) UnmarshalEvent(data []byte) (StreamEvent, error) {
	var d struct {
		EventType string `json:"event_type"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("event_codec: extract event_type: %w", err)
	}

	if d.EventType == "" {
		return nil, ErrMissingEventType
	}

	r.mu.RLock()
	typ, ok := r.registry[d.EventType]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownEventType, d.EventType)
	}

	rawEv := reflect.New(typ).Interface()

	ev, ok := rawEv.(StreamEvent)
	if !ok {
		return nil, fmt.Errorf("%w: unexpected type %T for event_type %s", ErrUnexpectedEventType, rawEv, d.EventType)
	}

	if err := json.Unmarshal(data, ev); err != nil {
		return nil, fmt.Errorf("event_codec: unmarshal %s: %w", d.EventType, err)
	}

	return ev, nil
}
