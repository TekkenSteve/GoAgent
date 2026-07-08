package entity

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type customCodecEvent struct {
	BaseEvent
	Value string `json:"value"`
}

func (e *customCodecEvent) Base() BaseEvent   { return e.BaseEvent }
func (e *customCodecEvent) EventType() string { return "custom.codec.event" }

func TestEventCodecUsesExplicitRegistry(t *testing.T) {
	t.Parallel()

	event := customCodecEvent{
		BaseEvent: BaseEvent{
			EventType:   "custom.codec.event",
			Source:      SourceSystem,
			Phase:       PhaseDelta,
			ContentType: ContentState,
			Timestamp:   time.Date(2026, 7, 4, 10, 0, 0, 0, time.UTC),
		},
		Value: "ok",
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal custom event: %v", err)
	}

	if _, err := UnmarshalEvent(data); !errors.Is(err, ErrUnknownEventType) {
		t.Fatalf("default UnmarshalEvent error = %v, want ErrUnknownEventType", err)
	}

	registry := NewEventRegistry(&customCodecEvent{})
	codec := NewEventCodec(registry)

	decoded, err := codec.UnmarshalEvent(data)
	if err != nil {
		t.Fatalf("custom codec UnmarshalEvent: %v", err)
	}

	got, ok := decoded.(*customCodecEvent)
	if !ok {
		t.Fatalf("decoded type = %T, want *customCodecEvent", decoded)
	}

	if got.Value != event.Value || got.EventType() != event.EventType() {
		t.Fatalf("decoded event = %#v, want value %q event type %q", got, event.Value, event.EventType())
	}
}
