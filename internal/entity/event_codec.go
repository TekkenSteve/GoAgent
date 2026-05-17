package entity

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

var (
	ErrMissingEventType    = errors.New("event_codec: missing event_type discriminator")
	ErrUnknownEventType    = errors.New("event_codec: unknown event type")
	ErrUnexpectedEventType = errors.New("event_codec: unexpected event type")
)

// EventRegistry manages the mapping of event_type → Go types.
// To add a new event type, just register it in init(), which does not affect existing code.
// External packages can register new event types through RegisterEventType (supports pluginization)。
type EventRegistry struct {
	mu       sync.RWMutex
	registry map[string]reflect.Type
}

var DefaultRegistry = &EventRegistry{ //nolint:gochecknoglobals // global singleton registry for event codec
	registry: make(map[string]reflect.Type),
}

// RegisterEventType registers an event type to the default registry.
// typ must be a pointer to a specific event type (e.g., &TextDeltaEvent{}).
func RegisterEventType(typ StreamEvent) {
	t := reflect.TypeOf(typ).Elem()

	DefaultRegistry.mu.Lock()
	defer DefaultRegistry.mu.Unlock()

	DefaultRegistry.registry[typ.EventType()] = t
}

func init() { //nolint:gochecknoinits // init registers default event types
	RegisterEventType(&TextDeltaEvent{})
	RegisterEventType(&ReasoningDeltaEvent{})
	RegisterEventType(&ToolCallStartEvent{})
	RegisterEventType(&ToolCallDeltaEvent{})
	RegisterEventType(&ToolCallFinishEvent{})
	RegisterEventType(&UsageFinishEvent{})
	RegisterEventType(&ToolExecStartEvent{})
	RegisterEventType(&ToolExecStdoutEvent{})
	RegisterEventType(&ToolExecStderrEvent{})
	RegisterEventType(&ToolExecFinishEvent{})
	RegisterEventType(&AgentRunStartEvent{})
	RegisterEventType(&AgentRunFinishEvent{})
	RegisterEventType(&PrepStageEvent{})
	RegisterEventType(&ContextUsageEvent{})
	RegisterEventType(&StateDeltaEvent{})
	RegisterEventType(&InterruptEvent{})
	RegisterEventType(&AgentErrorEvent{})
	RegisterEventType(&UserCommandEvent{})
	RegisterEventType(&UserFeedbackEvent{})
}

// MarshalEvent serializes StreamEvent.
// event_type is carried by the BaseEvent.EventType field embedded in the specific type.
func MarshalEvent(e StreamEvent) ([]byte, error) {
	return json.Marshal(e)
}

// UnmarshalEvent deserializes StreamEvent (dispatched by event_type).
// Unregistered event_type returns an error to prevent unknown type injection.
func UnmarshalEvent(data []byte) (StreamEvent, error) {
	var d struct {
		EventType string `json:"event_type"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("event_codec: extract event_type: %w", err)
	}

	if d.EventType == "" {
		return nil, ErrMissingEventType
	}

	DefaultRegistry.mu.RLock()
	typ, ok := DefaultRegistry.registry[d.EventType]
	DefaultRegistry.mu.RUnlock()

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
