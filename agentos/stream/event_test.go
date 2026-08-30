package stream

import (
	"errors"
	"testing"
)

var errTestToolFailure = errors.New("tool failure")

func TestEventValidate(t *testing.T) {
	t.Parallel()

	t.Run("valid text content", func(t *testing.T) {
		t.Parallel()

		ev := NewTextMessageContent("thread-1", "run-1", "msg-1", "hello")
		if err := ev.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	})

	t.Run("type required", func(t *testing.T) {
		t.Parallel()

		ev := NewEvent("")
		if err := ev.Validate(); !errors.Is(err, ErrInvalidEvent) {
			t.Fatalf("Validate error = %v, want %v", err, ErrInvalidEvent)
		}
	})

	t.Run("custom requires namespaced name", func(t *testing.T) {
		t.Parallel()

		ev := NewEvent(EventCustom)
		if err := ev.Validate(); !errors.Is(err, ErrInvalidEvent) {
			t.Fatalf("Validate error = %v, want %v", err, ErrInvalidEvent)
		}
	})

	t.Run("custom with name validates", func(t *testing.T) {
		t.Parallel()

		ev := NewCustom("thread-1", "run-1", "agentos.approval.requested")
		if err := ev.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}

		if got := ev.Payload[FieldName]; got != "agentos.approval.requested" {
			t.Fatalf("name = %v", got)
		}
	})

	t.Run("nil event rejected", func(t *testing.T) {
		t.Parallel()

		if err := (*Event)(nil).Validate(); !errors.Is(err, ErrInvalidEvent) {
			t.Fatalf("Validate error = %v, want %v", err, ErrInvalidEvent)
		}
	})
}

func TestEventHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		build func() *Event
		typ   EventType
	}{
		{"run started", func() *Event { return NewRunStarted("t", "r") }, EventRunStarted},
		{"run finished", func() *Event { return NewRunFinished("t", "r") }, EventRunFinished},
		{"run canceled", func() *Event { return NewRunCanceled("t", "r") }, EventRunCanceled},
		{"step started", func() *Event { return NewStepStarted("t", "r", 2, 1) }, EventStepStarted},
		{"tool call args", func() *Event { return NewToolCallArgs("t", "r", "c1", `{"q":"`) }, EventToolCallArgs},
		{"tool call error", func() *Event { return NewToolCallError("t", "r", "c1", errTestToolFailure) }, EventToolCallError},
		{"text message end", func() *Event { return NewTextMessageEnd("t", "r", "m1") }, EventTextMessageEnd},
		{"custom", func() *Event { return NewCustom("t", "r", "agentos.*") }, EventCustom},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ev := tt.build()
			if ev.Type != tt.typ {
				t.Fatalf("type = %q, want %q", ev.Type, tt.typ)
			}

			if ev.ThreadID != "t" || ev.RunID != "r" {
				t.Fatalf("scope = %s/%s, want t/r", ev.ThreadID, ev.RunID)
			}

			if err := ev.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

func TestToolCallCorrelation(t *testing.T) {
	t.Parallel()

	start := NewToolCallStart("t", "r", "call-42", "read_file")
	result := NewToolCallResult("t", "r", "call-42", map[string]any{"path": "/tmp/x"})

	for _, ev := range []*Event{start, result} {
		if got := ev.Payload[FieldCallID]; got != "call-42" {
			t.Fatalf("callId = %v, want call-42", got)
		}
	}
}

func TestIsMilestone(t *testing.T) {
	t.Parallel()

	milestones := []EventType{
		EventRunStarted, EventRunFinished, EventRunError, EventRunCanceled,
		EventStepStarted, EventStepFinished,
		EventTextMessageEnd,
		EventToolCallStart, EventToolCallResult, EventToolCallError,
	}
	transient := []EventType{
		EventTextMessageStart, EventTextMessageContent,
		EventReasoningStart, EventReasoningMessageStart, EventReasoningMessageContent, EventReasoningMessageEnd,
		EventToolCallArgs, EventToolCallEnd,
		EventStateSnapshot, EventStateDelta, EventMessagesSnapshot,
		EventActivitySnapshot, EventActivityDelta,
		EventRaw, EventCustom,
	}

	for _, typ := range milestones {
		if !IsMilestone(typ) {
			t.Errorf("%s: want milestone", typ)
		}

		if IsTransient(typ) {
			t.Errorf("%s: want not transient", typ)
		}
	}

	for _, typ := range transient {
		if IsMilestone(typ) {
			t.Errorf("%s: want not milestone", typ)
		}

		if !IsTransient(typ) {
			t.Errorf("%s: want transient", typ)
		}
	}
}
