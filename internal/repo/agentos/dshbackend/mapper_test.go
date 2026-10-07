package dshbackend

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/stretchr/testify/require"
)

// Shared literals the mapper tests repeat across input and expected values.
// goconst (min-occurrences 2) and clarity both push these into named consts.
const (
	testSessionID     = "session-1"
	testPromptText    = "hello"
	testMessageID     = "fake-msg-1"
	testCallID        = "call_1"
	testToolName      = "web_search"
	testToolOutput    = "results"
	testToolErrorText = "boom"
	testChunkText     = "chunk-hi"
	testReasoningText = "think"
	testArgsDelta     = `{"q":"x"}`
	testToolRead      = "read"
	testToolWrite     = "write"
	testToolIgnored   = "ignored"
	testToolKeepName  = "keep"
	testNameC1        = "c1"
	testNameC2        = "c2"
	testNameC3        = "c3"
)

// fakeEventTime is the fixed Unix-epoch-millis timestamp stamped on every test
// sessionEvent, so the mapped BaseEvent timestamps are deterministic.
const fakeEventTime = int64(1700000000000)

// sessionEventFor builds a sessionEvent with its data marshaled from any value.
func sessionEventFor(t *testing.T, typ string, data any) sessionEvent {
	t.Helper()

	raw, err := json.Marshal(data)
	require.NoError(t, err)

	return sessionEvent{Type: typ, Seq: 1, Time: fakeEventTime, Data: raw}
}

// wantBase builds the exact BaseEvent the mapper stamps for a given source,
// phase, and content type at the fixed test timestamp.
func wantBase(source entity.EventSource, phase entity.EventPhase, contentType entity.EventContentType) entity.BaseEvent {
	return entity.BaseEvent{
		Source:      source,
		Phase:       phase,
		ContentType: contentType,
		Timestamp:   time.UnixMilli(fakeEventTime),
	}
}

// resultMessage marshals a dsh ToolResultMessage with one text content block.
func resultMessage(t *testing.T, callID string, isErr bool, text string) json.RawMessage {
	t.Helper()

	msg := toolResultMessage{Content: []struct {
		ToolCallID string              `json:"toolCallId"`
		IsError    bool                `json:"isError"`
		Content    []toolResultContent `json:"content"`
	}{{
		ToolCallID: callID,
		IsError:    isErr,
		Content:    []toolResultContent{{Type: "text", Text: text}},
	}}}

	raw, err := json.Marshal(msg)
	require.NoError(t, err)

	return raw
}

func TestMapChunk(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		chunkType string
		chunk     streamChunk
		want      entity.StreamEvent
	}{
		{
			name:      "text delta",
			chunkType: chunkTextDelta,
			chunk:     streamChunk{Type: chunkTextDelta, Index: 2, Text: testChunkText},
			want:      &entity.TextDeltaEvent{BaseEvent: wantBase(entity.SourceLLM, entity.PhaseDelta, entity.ContentText), Content: testChunkText, Index: 2},
		},
		{
			name:      "reasoning delta",
			chunkType: chunkReasoningDelta,
			chunk:     streamChunk{Type: chunkReasoningDelta, Text: testReasoningText},
			want:      &entity.ReasoningDeltaEvent{BaseEvent: wantBase(entity.SourceLLM, entity.PhaseDelta, entity.ContentReasoning), Reasoning: testReasoningText},
		},
		{
			name:      "tool call delta",
			chunkType: chunkToolCallDelta,
			chunk:     streamChunk{Type: chunkToolCallDelta, ID: testCallID, ArgumentsDelta: testArgsDelta},
			want:      &entity.ToolCallDeltaEvent{BaseEvent: wantBase(entity.SourceLLM, entity.PhaseDelta, entity.ContentToolCall), ToolCallID: testCallID, ArgumentsDelta: testArgsDelta},
		},
		{
			name:      "usage",
			chunkType: chunkUsage,
			chunk:     streamChunk{Type: chunkUsage, Usage: tokenUsage{InputTokens: 1, OutputTokens: 2, TotalTokens: 3}},
			want:      &entity.UsageFinishEvent{BaseEvent: wantBase(entity.SourceLLM, entity.PhaseFinish, entity.ContentStatus), Usage: entity.Usage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ev := sessionEventFor(t, eventAssistantChunk, assistantChunkData{Turn: 1, Step: 1, Chunk: tc.chunk})

			require.Equal(t, []entity.StreamEvent{tc.want}, mapChunk(ev))
		})
	}
}

func TestMapChunkSkipsBookkeeping(t *testing.T) {
	t.Parallel()

	for _, chunkType := range []string{chunkBlockStart, chunkBlockEnd, chunkFinish} {
		ev := sessionEventFor(t, eventAssistantChunk, assistantChunkData{Chunk: streamChunk{Type: chunkType}})
		require.Nil(t, mapChunk(ev), "chunk type %s", chunkType)
	}
}

func TestMapEventToolCall(t *testing.T) {
	t.Parallel()

	ev := sessionEventFor(t, eventToolCall, toolCallData{Turn: 1, Step: 1, CallID: testCallID, Name: testToolName, Arguments: `{}`})

	got, done := mapEvent(ev, false)
	require.False(t, done)
	require.Equal(t, []entity.StreamEvent{&entity.ToolCallStartEvent{
		BaseEvent:  wantBase(entity.SourceLLM, entity.PhaseStart, entity.ContentToolCall),
		ToolCallID: testCallID,
		ToolName:   testToolName,
	}}, got)
}

func TestMapEventToolCallDropsEmptyID(t *testing.T) {
	t.Parallel()

	ev := sessionEventFor(t, eventToolCall, toolCallData{Name: testToolName})

	got, done := mapEvent(ev, false)
	require.Nil(t, got)
	require.False(t, done)
}

func TestMapEventToolResult(t *testing.T) {
	t.Parallel()

	ev := sessionEventFor(t, eventToolResult, toolResultData{Turn: 1, Step: 1, Message: resultMessage(t, testCallID, false, testToolOutput)})

	got, done := mapEvent(ev, false)
	require.False(t, done)

	require.Equal(t, []entity.StreamEvent{
		&entity.ToolCallFinishEvent{BaseEvent: wantBase(entity.SourceLLM, entity.PhaseFinish, entity.ContentToolCall), ToolCallID: testCallID},
		&entity.ToolExecStartEvent{BaseEvent: wantBase(entity.SourceTool, entity.PhaseStart, entity.ContentToolResult), ToolCallID: testCallID},
		&entity.ToolExecFinishEvent{BaseEvent: wantBase(entity.SourceTool, entity.PhaseFinish, entity.ContentToolResult), ToolCallID: testCallID, Output: testToolOutput, ExitCode: 0},
	}, got)
}

func TestMapEventToolResultBlockError(t *testing.T) {
	t.Parallel()

	ev := sessionEventFor(t, eventToolResult, toolResultData{Turn: 1, Step: 1, Message: resultMessage(t, testCallID, true, testToolErrorText)})

	got, _ := mapEvent(ev, false)
	finish, ok := got[len(got)-1].(*entity.ToolExecFinishEvent)
	require.True(t, ok)
	require.Equal(t, testToolErrorText, finish.Output)
	require.Equal(t, 1, finish.ExitCode)
	require.True(t, finish.IsError)
}

func TestMapEventToolResultTopLevelError(t *testing.T) {
	t.Parallel()

	ev := sessionEventFor(t, eventToolResult, toolResultData{
		Turn:    1,
		Step:    1,
		Message: resultMessage(t, testCallID, false, testToolErrorText),
		Error:   json.RawMessage(`{"code":1,"message":"boom"}`),
	})

	got, _ := mapEvent(ev, false)
	finish, ok := got[len(got)-1].(*entity.ToolExecFinishEvent)
	require.True(t, ok)
	require.True(t, finish.IsError)
}

func TestMapEventToolResultDropsEmptyID(t *testing.T) {
	t.Parallel()

	ev := sessionEventFor(t, eventToolResult, toolResultData{Turn: 1, Step: 1, Message: resultMessage(t, "", false, "x")})

	got, done := mapEvent(ev, false)
	require.Nil(t, got)
	require.False(t, done)
}

func TestMapEventTurnEnd(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		reason   string
		canceled bool
		want     entity.StreamEvent
	}{
		{
			name:   "completed",
			reason: turnReasonCompleted,
			want:   &entity.AgentRunFinishEvent{BaseEvent: wantBase(entity.SourceAgent, entity.PhaseFinish, entity.ContentStatus), FinishReason: string(entity.FinishStop)},
		},
		{
			name:   "max tokens",
			reason: turnReasonMaxTokens,
			want:   &entity.AgentRunFinishEvent{BaseEvent: wantBase(entity.SourceAgent, entity.PhaseFinish, entity.ContentStatus), FinishReason: string(entity.FinishLength)},
		},
		{
			name:   "aborted passthrough",
			reason: turnReasonAborted,
			want:   &entity.AgentRunFinishEvent{BaseEvent: wantBase(entity.SourceAgent, entity.PhaseFinish, entity.ContentStatus), FinishReason: turnReasonAborted},
		},
		{
			name:   "error",
			reason: turnReasonError,
			want:   &entity.AgentErrorEvent{BaseEvent: wantBase(entity.SourceSystem, entity.PhaseError, entity.ContentStatus), ErrorMessage: "dsh turn ended in " + turnReasonError, ErrorCode: "dsh.turn." + turnReasonError},
		},
		{
			name:   "blocked",
			reason: turnReasonBlocked,
			want:   &entity.AgentErrorEvent{BaseEvent: wantBase(entity.SourceSystem, entity.PhaseError, entity.ContentStatus), ErrorMessage: "dsh turn ended in " + turnReasonBlocked, ErrorCode: "dsh.turn." + turnReasonBlocked},
		},
		{
			name:   "interrupted",
			reason: turnReasonInterrupted,
			want:   &entity.AgentRunCanceledEvent{BaseEvent: wantBase(entity.SourceSystem, entity.PhaseInterrupt, entity.ContentStatus)},
		},
		{
			name:     "cancel flag wins",
			reason:   turnReasonCompleted,
			canceled: true,
			want:     &entity.AgentRunCanceledEvent{BaseEvent: wantBase(entity.SourceSystem, entity.PhaseInterrupt, entity.ContentStatus)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ev := sessionEventFor(t, eventTurnEnd, turnEndData{Turn: 1, Reason: tc.reason})

			got, done := mapEvent(ev, tc.canceled)
			require.True(t, done)
			require.Equal(t, []entity.StreamEvent{tc.want}, got)
		})
	}
}

func TestMapEventSkipsNonMapped(t *testing.T) {
	t.Parallel()

	for _, typ := range []string{
		eventTurnStart,
		eventUserTurn,
		eventStepStart,
		eventStepEnd,
		eventAssistant,
		eventRequestHeader,
		eventRequestContext,
		eventSessionEndSeed,
	} {
		got, done := mapEvent(sessionEventFor(t, typ, map[string]any{}), false)
		require.Nil(t, got, "event type %s", typ)
		require.False(t, done, "event type %s", typ)
	}
}

func TestCollectToolNames(t *testing.T) {
	t.Parallel()

	names := make(map[string]string)

	collectToolNames(sessionEventFor(t, eventToolCall, toolCallData{CallID: testNameC1, Name: testToolRead}), names)
	require.Equal(t, testToolRead, names[testNameC1])

	collectToolNames(sessionEventFor(t, eventAssistantChunk, assistantChunkData{Chunk: streamChunk{Type: chunkToolCallDelta, ID: testNameC2, Name: testToolWrite}}), names)
	require.Equal(t, testToolWrite, names[testNameC2])

	collectToolNames(sessionEventFor(t, eventAssistantChunk, assistantChunkData{Chunk: streamChunk{Type: chunkTextDelta, ID: testNameC3, Name: testToolIgnored}}), names)
	require.NotContains(t, names, testNameC3)
}

func TestStampToolNames(t *testing.T) {
	t.Parallel()

	names := map[string]string{testNameC1: testToolName}

	events := []entity.StreamEvent{
		&entity.ToolCallFinishEvent{ToolCallID: testNameC1},
		&entity.ToolExecStartEvent{ToolCallID: testNameC1},
		&entity.ToolExecFinishEvent{ToolCallID: testNameC1},
		&entity.ToolCallStartEvent{ToolCallID: testNameC1, ToolName: testToolKeepName},
		&entity.ToolCallDeltaEvent{ToolCallID: testNameC1},
	}

	stampToolNames(events, names)

	finish, ok := events[0].(*entity.ToolCallFinishEvent)
	require.True(t, ok)
	require.Equal(t, testToolName, finish.ToolName)

	execStart, ok := events[1].(*entity.ToolExecStartEvent)
	require.True(t, ok)
	require.Equal(t, testToolName, execStart.ToolName)

	execFinish, ok := events[2].(*entity.ToolExecFinishEvent)
	require.True(t, ok)
	require.Equal(t, testToolName, execFinish.ToolName)

	callStart, ok := events[3].(*entity.ToolCallStartEvent)
	require.True(t, ok)
	require.Equal(t, testToolKeepName, callStart.ToolName)

	callDelta, ok := events[4].(*entity.ToolCallDeltaEvent)
	require.True(t, ok)
	require.Equal(t, "", callDelta.ArgumentsDelta)
}

func TestTerminalStatusFor(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		events []entity.StreamEvent
		want   string
	}{
		{name: "empty", want: stateCompleted},
		{name: "finish", events: []entity.StreamEvent{&entity.AgentRunFinishEvent{}}, want: stateCompleted},
		{name: "canceled", events: []entity.StreamEvent{&entity.AgentRunCanceledEvent{}}, want: stateCanceled},
		{name: "error", events: []entity.StreamEvent{&entity.AgentErrorEvent{}}, want: stateFailed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, terminalStatusFor(tc.events))
		})
	}
}
