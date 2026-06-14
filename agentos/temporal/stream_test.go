package temporal

import (
	"testing"
	"time"

	agentfwstream "github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

func TestEventFromStored(t *testing.T) {
	ts := time.Date(2026, 6, 14, 13, 0, 0, 0, time.UTC)

	event := entity.TextDeltaEvent{
		BaseEvent: entity.BaseEvent{
			EventID:   "evt-1",
			RunID:     "run-1",
			SessionID: "thread-1",
			Timestamp: ts,
		},
		Content: "hello",
		Index:   1,
	}

	got := eventFromStored(agentfwstream.StoredEvent{
		Event:    event,
		Sequence: 42,
		StoredAt: ts,
	})

	if got.EventID != "evt-1" ||
		got.EventType != "llm.text.delta" ||
		got.RunID != "run-1" ||
		got.ThreadID != "thread-1" ||
		got.Sequence != 42 ||
		!got.Timestamp.Equal(ts) {
		t.Fatalf("unexpected event mapping: %#v", got)
	}

	if got.Payload["content"] != "hello" {
		t.Fatalf("payload content = %#v", got.Payload["content"])
	}
}
