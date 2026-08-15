package streamadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/memstream"
	"github.com/stretchr/testify/require"
)

// TestPublishWriterSynthesizesMessageLifecycle drives the writer through a
// full run shape — text, reasoning, tool call, tool execution, run end — and
// asserts the bus carried the synthesized START/END pairs in order: every text
// delta opens its message once and a non-content event closes whatever is open
// before it lands.
func TestPublishWriterSynthesizesMessageLifecycle(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	handle := HandleForRun("acme", "run-1")
	w := NewPublishWriter(bus, handle, "sess-1", "run-1", nil)

	seq := []entity.StreamEvent{
		&entity.AgentRunStartEvent{BaseEvent: base(), AgentName: "coder"},
		&entity.TextDeltaEvent{BaseEvent: base(), Content: "Hel"},
		&entity.TextDeltaEvent{BaseEvent: base(), Content: "lo"},
		&entity.ReasoningDeltaEvent{BaseEvent: base(), Reasoning: "think"},
		&entity.ToolCallStartEvent{BaseEvent: base(), ToolCallID: "call-1", ToolName: "bash"},
		&entity.ToolExecFinishEvent{BaseEvent: base(), ToolCallID: "call-1", Output: "done"},
		&entity.AgentRunFinishEvent{BaseEvent: base(), FinishReason: "stop"},
	}

	ctx := context.Background()
	for _, ev := range seq {
		require.NoError(t, w.WriteEvent(ctx, ev), "write %s", ev.EventType())
	}

	wantTypes := []stream.EventType{
		stream.EventRunStarted,
		stream.EventTextMessageStart, stream.EventTextMessageContent, stream.EventTextMessageContent,
		stream.EventReasoningMessageStart, stream.EventReasoningMessageContent,
		stream.EventTextMessageEnd, stream.EventReasoningMessageEnd,
		stream.EventToolCallStart,
		stream.EventToolCallResult,
		stream.EventRunFinished,
	}

	got := subscribeAll(t, bus, handle, len(wantTypes))
	require.Len(t, got, len(wantTypes), "synthesized sequence length")

	for i, want := range wantTypes {
		require.Equal(t, want, got[i].Type, "event %d", i)
	}

	// Scope is stamped on every synthesized event, not just the mapped ones.
	for _, ev := range got {
		require.Equal(t, "run-1", ev.RunID, "run scope on %s", ev.Type)
		require.Equal(t, "sess-1", ev.ThreadID, "thread scope on %s", ev.Type)
	}

	// The two deltas share one synthesized text message; reasoning has its own.
	require.Equal(t, "m-1", got[1].MessageID, "text start")
	require.Equal(t, "m-1", got[2].MessageID, "text delta 1")
	require.Equal(t, "m-1", got[3].MessageID, "text delta 2")
	require.Equal(t, "m-1", got[6].MessageID, "text end")
	require.Equal(t, "m-2", got[4].MessageID, "reasoning start")
	require.Equal(t, "m-2", got[5].MessageID, "reasoning delta")
	require.Equal(t, "m-2", got[7].MessageID, "reasoning end")

	require.Equal(t, "Hel", got[2].Payload[stream.FieldDelta])
	require.Equal(t, "lo", got[3].Payload[stream.FieldDelta])
	require.Equal(t, "think", got[5].Payload[stream.FieldDelta])
}

// errBusDown stands in for a data-plane transport that is down (err113: the
// stub must fail with a static sentinel, not a per-test dynamic error).
var errBusDown = errors.New("bus down")

// failingPublisher records every publish attempt and always fails, standing in
// for a data-plane transport that is down.
type failingPublisher struct {
	err   error
	calls int
}

func (f *failingPublisher) Publish(context.Context, *stream.Handle, *stream.Event) error {
	f.calls++

	return f.err
}

// TestPublishWriterFailOpen locks the fail-open contract: a dead bus must not
// surface an error to the run and must not short-circuit message synthesis.
// Unmappable events are dropped silently too.
func TestPublishWriterFailOpen(t *testing.T) {
	t.Parallel()

	pub := &failingPublisher{err: errBusDown}
	handle := HandleForRun("acme", "run-1")
	w := NewPublishWriter(pub, handle, "sess-1", "run-1", nil)

	ctx := context.Background()

	// Content opens + publishes the START, then publishes CONTENT.
	require.NoError(t, w.WriteEvent(ctx, &entity.TextDeltaEvent{BaseEvent: base(), Content: "a"}))

	// Non-content closes the open text message, then publishes the mapped event.
	require.NoError(t, w.WriteEvent(ctx, &entity.ToolCallFinishEvent{BaseEvent: base(), ToolCallID: "call-1", Arguments: `{}`}))

	// Unmappable: dropped, never published, no error.
	require.NoError(t, w.WriteEvent(ctx, &unknownEvent{BaseEvent: base()}))

	require.Equal(t, 4, pub.calls, "START + CONTENT + END + TOOL_CALL_END")
}

// TestPublishWriterFlushClosesOpenMessages locks the stream-end close: a
// text-only round (no following non-content event) must still terminate its
// message on the timeline, and a second flush is a no-op.
func TestPublishWriterFlushClosesOpenMessages(t *testing.T) {
	t.Parallel()

	pub := &failingPublisher{err: nil}
	handle := HandleForRun("acme", "run-1")
	w := NewPublishWriter(pub, handle, "sess-1", "run-1", nil)

	require.NoError(t, w.WriteEvent(t.Context(), &entity.TextDeltaEvent{BaseEvent: base(), Content: "a"}))
	require.Equal(t, 2, pub.calls, "START + CONTENT")

	w.Flush(t.Context())
	require.Equal(t, 3, pub.calls, "flush closes the open text message")

	w.Flush(t.Context())
	require.Equal(t, 3, pub.calls, "second flush is a no-op")
}

// subscribeAll replays the bus history for the run channel, since the run is
// done by the time the test subscribes. It returns exactly want events,
// failing the test on a stall.
func subscribeAll(t *testing.T, bus *memstream.Bus, handle *stream.Handle, want int) []*stream.Event {
	t.Helper()

	sub, err := bus.Subscribe(t.Context(), handle, 0)
	require.NoError(t, err)

	defer sub.Close()

	got := make([]*stream.Event, 0, want)
	for len(got) < want {
		select {
		case stored := <-sub.C:
			ev := stored.Event
			got = append(got, &ev)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out after %d of %d events", len(got), want)
		}
	}

	return got
}
