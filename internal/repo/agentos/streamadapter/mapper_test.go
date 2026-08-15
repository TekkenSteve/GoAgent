package streamadapter

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/stretchr/testify/require"
)

// unknownEvent stands in for a runtime event kind the adapter has no mapping
// for, so the table can lock the sentinel path without touching the registry.
type unknownEvent struct {
	entity.BaseEvent
}

func (e *unknownEvent) Base() entity.BaseEvent { return e.BaseEvent }

func (e *unknownEvent) EventType() string { return "unknown.kind" }

// base builds the runtime base metadata the mapper reads for thread/run scope.
// The session is the frontend thread and the run is fixed in every fixture.
func base() entity.BaseEvent {
	return entity.BaseEvent{SessionID: "sess-1", RunID: "run-1"}
}

// usage is a small fixed accounting payload reused across the milestone cases.
func usage() entity.Usage {
	return entity.Usage{PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30, Cost: entity.Money(0.05)}
}

// TestMapEvent is the 22-case table: every runtime event kind maps to the
// expected AG-UI wire type, carries the right milestone/transient
// classification (so ProjectToCore reduces exactly the durable set), fills the
// thread/run scope, and always produces a wire-valid event. Each row pins the
// payload subset it cares about via assertPayload.
func TestMapEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		event     entity.StreamEvent
		wantType  stream.EventType
		milestone bool
		want      map[string]any
	}{
		{name: "text delta", event: &entity.TextDeltaEvent{BaseEvent: base(), Content: "Hello"}, wantType: stream.EventTextMessageContent, want: map[string]any{stream.FieldDelta: "Hello"}},
		{name: "reasoning delta", event: &entity.ReasoningDeltaEvent{BaseEvent: base(), Reasoning: "think"}, wantType: stream.EventReasoningMessageContent, want: map[string]any{stream.FieldDelta: "think"}},
		{name: "tool call start", event: &entity.ToolCallStartEvent{BaseEvent: base(), ToolCallID: "call-1", ToolName: "web_search"}, wantType: stream.EventToolCallStart, milestone: true, want: map[string]any{stream.FieldCallID: "call-1", stream.FieldName: "web_search"}},
		{name: "tool call delta", event: &entity.ToolCallDeltaEvent{BaseEvent: base(), ToolCallID: "call-1", ArgumentsDelta: `{"q":`}, wantType: stream.EventToolCallArgs, want: map[string]any{stream.FieldCallID: "call-1", stream.FieldDelta: `{"q":`}},
		{name: "tool call finish", event: &entity.ToolCallFinishEvent{BaseEvent: base(), ToolCallID: "call-1", Arguments: `{"q":"x"}`}, wantType: stream.EventToolCallEnd, want: map[string]any{stream.FieldCallID: "call-1", stream.FieldDelta: `{"q":"x"}`}},
		{name: "usage finish", event: &entity.UsageFinishEvent{BaseEvent: base(), Usage: usage(), Model: "gpt-4o"}, wantType: stream.EventCustom, want: map[string]any{stream.FieldName: customLLMPrefix + "usage", "model": "gpt-4o", stream.FieldUsage: map[string]any{"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30, "cost": entity.Money(0.05)}}},
		{name: "tool exec start", event: &entity.ToolExecStartEvent{BaseEvent: base(), ToolCallID: "call-1", ToolName: "bash"}, wantType: stream.EventCustom, want: map[string]any{stream.FieldName: customToolExecPrefix + "start", toolCallIDKey: "call-1", "toolName": "bash"}},
		{name: "tool exec stdout", event: &entity.ToolExecStdoutEvent{BaseEvent: base(), ToolCallID: "call-1", StdoutDelta: "out"}, wantType: stream.EventCustom, want: map[string]any{stream.FieldName: customToolExecPrefix + "stdout", toolCallIDKey: "call-1", stream.FieldDelta: "out"}},
		{name: "tool exec stderr", event: &entity.ToolExecStderrEvent{BaseEvent: base(), ToolCallID: "call-1", StderrDelta: "err"}, wantType: stream.EventCustom, want: map[string]any{stream.FieldName: customToolExecPrefix + "stderr", toolCallIDKey: "call-1", stream.FieldDelta: "err"}},
		{name: "tool exec finish success", event: &entity.ToolExecFinishEvent{BaseEvent: base(), ToolCallID: "call-1", Output: "done", ExitCode: 0, DurationMs: 5}, wantType: stream.EventToolCallResult, milestone: true, want: map[string]any{stream.FieldCallID: "call-1", stream.FieldResult: "done", stream.FieldMeta: map[string]any{"exit_code": 0, "duration_ms": int64(5)}}},
		{name: "tool exec finish error", event: &entity.ToolExecFinishEvent{BaseEvent: base(), ToolCallID: "call-1", Output: "boom", ExitCode: 1, IsError: true}, wantType: stream.EventToolCallError, milestone: true, want: map[string]any{stream.FieldCallID: "call-1", stream.FieldError: map[string]any{"message": "boom"}, stream.FieldMeta: map[string]any{"exit_code": 1, "duration_ms": int64(0)}}},
		{name: "agent run start", event: &entity.AgentRunStartEvent{BaseEvent: base(), AgentName: "coder"}, wantType: stream.EventRunStarted, milestone: true, want: map[string]any{"agentName": "coder"}},
		{name: "agent run finish", event: &entity.AgentRunFinishEvent{BaseEvent: base(), FinishReason: "stop", Usage: new(usage())}, wantType: stream.EventRunFinished, milestone: true, want: map[string]any{"finishReason": "stop", stream.FieldUsage: map[string]any{"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30, "cost": entity.Money(0.05)}}},
		{name: "agent run canceled", event: &entity.AgentRunCancelledEvent{BaseEvent: base()}, wantType: stream.EventRunCancelled, milestone: true, want: map[string]any{}},
		{name: "prep stage", event: &entity.PrepStageEvent{BaseEvent: base(), Stage: "ready", Progress: 100}, wantType: stream.EventCustom, want: map[string]any{stream.FieldName: customSystemPrefix + "prep_stage", "stage": "ready", "progress": 100}},
		{name: "context usage", event: &entity.ContextUsageEvent{BaseEvent: base(), MessageCount: 3, Compressed: new(true)}, wantType: stream.EventCustom, want: map[string]any{stream.FieldName: customSystemPrefix + "context_usage", "messageCount": 3, "compressed": true}},
		{name: "state delta", event: &entity.StateDeltaEvent{BaseEvent: base(), Key: "mode", Value: "fast"}, wantType: stream.EventCustom, want: map[string]any{stream.FieldName: customSystemPrefix + "state_delta", "key": "mode", "value": "fast"}},
		{name: "interrupt", event: &entity.InterruptEvent{BaseEvent: base(), Reason: "user_canceled"}, wantType: stream.EventCustom, want: map[string]any{stream.FieldName: customSystemPrefix + "interrupt", "reason": "user_canceled"}},
		{name: "fatal error", event: &entity.AgentErrorEvent{BaseEvent: base(), ErrorMessage: "kaboom", ErrorCode: "E1", Recoverable: false}, wantType: stream.EventRunError, milestone: true, want: map[string]any{stream.FieldError: map[string]any{"message": "fatal agent error: E1: kaboom"}}},
		{name: "recoverable error", event: &entity.AgentErrorEvent{BaseEvent: base(), ErrorMessage: "retry", Recoverable: true}, wantType: stream.EventCustom, want: map[string]any{stream.FieldName: customSystemPrefix + "error", stream.FieldError: map[string]any{"message": "retry", "code": ""}}},
		{name: "user command", event: &entity.UserCommandEvent{BaseEvent: base(), Command: "pause", Payload: map[string]any{"k": "v"}}, wantType: stream.EventCustom, want: map[string]any{stream.FieldName: customUserPrefix + "command", "command": "pause", "payload": map[string]any{"k": "v"}}},
		{name: "user feedback", event: &entity.UserFeedbackEvent{BaseEvent: base(), TargetEventID: "e-1", FeedbackType: "approve", Payload: "yes"}, wantType: stream.EventCustom, want: map[string]any{stream.FieldName: customUserPrefix + "feedback", "targetEventId": "e-1", "feedbackType": "approve", "payload": "yes"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ev, err := MapEvent(tt.event)
			require.NoError(t, err)
			require.Equal(t, tt.wantType, ev.Type)
			require.Equal(t, tt.milestone, stream.IsMilestone(ev.Type), "milestone classification")
			require.Equal(t, "run-1", ev.RunID, "run scope")
			require.Equal(t, "sess-1", ev.ThreadID, "thread scope")
			require.Empty(t, ev.MessageID, "message ids are stamped by the writer, never the mapper")
			require.NoError(t, ev.Validate(), "wire envelope")
			assertPayload(t, ev, tt.want)
		})
	}
}

// assertPayload checks the payload subset each table row pins down, key by key,
// so an extra mapped field never fails an unrelated case.
func assertPayload(t *testing.T, ev *stream.Event, want map[string]any) {
	t.Helper()

	for k, v := range want {
		require.Equal(t, v, ev.Payload[k], "payload[%s]", k)
	}
}

// TestMapEventUnsupported locks the sentinel: a runtime event kind outside the
// vocabulary is rejected with the package-level error (err113), never silently
// dropped at the mapper.
func TestMapEventUnsupported(t *testing.T) {
	t.Parallel()

	_, err := MapEvent(&unknownEvent{BaseEvent: base()})
	require.ErrorIs(t, err, ErrUnsupportedStreamEvent)
	require.True(t, errors.Is(err, ErrUnsupportedStreamEvent))
}
