package stream

import (
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestProjectToCore(t *testing.T) {
	t.Parallel()

	handle := NewHandle("$agentos:run:acme:run-1")
	storedAt := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		ev       *Event
		wantType agentoscore.EventType
		wantOK   bool
	}{
		{"run started", NewRunStarted("t-1", "run-1"), agentoscore.EventRunStarted, true},
		{"run finished", NewRunFinished("t-1", "run-1"), agentoscore.EventRunCompleted, true},
		{"run error", NewRunError("t-1", "run-1", nil), agentoscore.EventRunFailed, true},
		{"step started", NewStepStarted("t-1", "run-1", 1, 1), agentoscore.EventAgentStepStarted, true},
		{"step finished", NewStepFinished("t-1", "run-1", 1, 1), agentoscore.EventAgentStepCompleted, true},
		{"message end", NewTextMessageEnd("t-1", "run-1", "m-1"), agentoscore.EventAgentMessageCompleted, true},
		{"tool start", NewToolCallStart("t-1", "run-1", "c-1", "read_file"), agentoscore.EventToolCallStarted, true},
		{"tool result", NewToolCallResult("t-1", "run-1", "c-1", "ok"), agentoscore.EventToolCallCompleted, true},
		{"tool error", NewToolCallError("t-1", "run-1", "c-1", nil), agentoscore.EventToolCallFailed, true},
		{"text delta is transient", NewTextMessageContent("t-1", "run-1", "m-1", "x"), "", false},
		{"reasoning is transient", NewReasoningMessageContent("t-1", "run-1", "m-1", "x"), "", false},
		{"tool args transient", NewToolCallArgs("t-1", "run-1", "c-1", "{}"), "", false},
		{"custom not re-projected", NewCustom("t-1", "run-1", "agentos.approval.requested"), "", false},
		{"state delta transient", NewEvent(EventStateDelta), "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stored := &StoredEvent{Event: *tt.ev, Sequence: 7, StoredAt: storedAt}
			assertProjectedEvent(t, handle, stored, tt.wantType, tt.wantOK, storedAt)
		})
	}
}

func assertProjectedEvent(t *testing.T, handle *Handle, stored *StoredEvent, wantType agentoscore.EventType, wantOK bool, storedAt time.Time) {
	t.Helper()

	got, ok := ProjectToCore(handle, stored)
	if ok != wantOK {
		t.Fatalf("ok = %v, want %v", ok, wantOK)
	}

	if !wantOK {
		return
	}

	if got == nil {
		t.Fatal("projected event is nil")
	}

	if got.EventType != wantType {
		t.Fatalf("event type = %q, want %q", got.EventType, wantType)
	}

	if got.RunID != "run-1" || got.ThreadID != "t-1" {
		t.Fatalf("scope = %s/%s, want run-1/t-1", got.RunID, got.ThreadID)
	}

	if got.Source != handle.Channel {
		t.Fatalf("source = %q, want %q", got.Source, handle.Channel)
	}

	if !got.Timestamp.Equal(storedAt) {
		t.Fatalf("timestamp = %v, want %v", got.Timestamp, storedAt)
	}
}

func TestProjectToCoreCarriesPayload(t *testing.T) {
	t.Parallel()

	handle := NewHandle("$agentos:run:acme:run-1")
	ev := NewToolCallResult("t-1", "run-1", "c-1", "ok")
	stored := &StoredEvent{Event: *ev, Sequence: 3}

	got, ok := ProjectToCore(handle, stored)
	if !ok {
		t.Fatal("tool result should project")
	}

	if got.Payload == nil {
		t.Fatal("payload not carried")
	}

	if got.Payload[FieldCallID] != "c-1" || got.Payload[FieldResult] != "ok" {
		t.Fatalf("payload = %#v, want callId c-1 / result ok", got.Payload)
	}
}
