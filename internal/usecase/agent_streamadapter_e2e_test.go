package usecase_test

import (
	"context"
	"errors"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/memstream"
	agent "github.com/TekkenSteve/GoAgent/internal/usecase/agent"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// errChatUnused is the unreachable Chat path: LLMStreamCall drives ChatStream,
// and the stub fails loudly rather than returning a per-test dynamic error
// (err113).
var errChatUnused = errors.New("unused: LLMStreamCall uses ChatStream")

// streamableLLM satisfies both repo.LLMProvider and repo.LLMStreamProvider:
// it embeds the generated streaming mock (ChatStream) and stubs Chat (never
// called by LLMStreamCall) so the composite passes agent.New's LLMProvider
// parameter while gomock still drives the stream.
type streamableLLM struct {
	*MockLLMStreamProvider
}

func (s *streamableLLM) Chat(context.Context, *entity.LLMRequest) (entity.LLMResponse, error) {
	return entity.LLMResponse{}, errChatUnused
}

// TestAgentStreamAdapterE2E drives the full runtime→data-plane path for one
// streaming LLM call: the usecase's LLMStreamCall emits entity deltas, the
// PublishWriter maps and synthesizes the AG-UI timeline onto the bus, and the
// projector reduces it back to the control plane's durable milestones. This is
// the contract the production transport (Centrifugo) and every backend must
// satisfy end to end.
func TestAgentStreamAdapterE2E(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockLLM := &streamableLLM{NewMockLLMStreamProvider(ctrl)}

	chunks := streamChunks()
	ch := make(chan entity.LLMStreamChunk, len(chunks))

	for _, chunk := range chunks {
		ch <- chunk
	}

	close(ch)
	mockLLM.EXPECT().ChatStream(gomock.Any(), gomock.Any()).Return(ch, nil)

	uc := agent.New(mockLLM, nil, nil, nil, nil, nil)

	bus := memstream.New()
	handle := streamadapter.HandleForRun("acme", "run-1")
	writer := streamadapter.NewPublishWriter(bus, handle, "sess-1", "run-1", nil)

	res, err := uc.LLMStreamCall(
		t.Context(),
		"run-1",
		[]entity.Message{{Role: entity.RoleUser, Content: "Hi"}},
		nil,
		entity.LLMConfig{Model: "gpt-4o"},
		writer,
	)
	require.NoError(t, err)
	require.Len(t, res.ToolCalls, 1)
	require.Equal(t, 8, res.Usage.TotalTokens)
	require.Equal(t, "stop", res.FinishReason)

	sub, err := bus.Subscribe(t.Context(), handle, 0)
	require.NoError(t, err)

	defer sub.Close()

	wantTypes := []stream.EventType{
		stream.EventTextMessageStart,
		stream.EventTextMessageContent,
		stream.EventTextMessageContent,
		stream.EventReasoningMessageStart,
		stream.EventReasoningMessageContent,
		stream.EventTextMessageEnd,
		stream.EventReasoningMessageEnd,
		stream.EventToolCallStart,
		stream.EventToolCallArgs,
		stream.EventToolCallEnd,
	}

	stored := collectStreamEvents(t, sub, len(wantTypes))
	assertTimeline(t, stored, wantTypes)

	// Only the message end and the tool call start survive projection: token
	// bytes and start markers are transient by design.
	projectMilestones(t, handle, stored, []agentoscore.EventType{
		agentoscore.EventAgentMessageCompleted,
		agentoscore.EventToolCallStarted,
	})
}

// streamChunks is one streaming LLM call's worth of entity deltas: two text
// deltas, one reasoning delta, a tool call streaming in, and the final turn
// carrying the assembled tool call, finish reason, and usage.
func streamChunks() []entity.LLMStreamChunk {
	return []entity.LLMStreamChunk{
		{Content: "Hel"},
		{Content: "lo"},
		{Reasoning: "think"},
		{ToolCallDeltas: []entity.ToolCallDelta{{Index: 0, ToolCallID: "call-1", Name: "web_search"}}},
		{ToolCallDeltas: []entity.ToolCallDelta{{ToolCallID: "call-1", ArgsDelta: `{"q":"x"}`}}},
		{ToolCalls: []entity.ToolCall{{
			ID:       "call-1",
			Type:     "function",
			Function: entity.ToolCallFunction{Name: "web_search", Arguments: `{"q":"x"}`},
		}}, FinishReason: entity.FinishStop, Usage: entity.Usage{PromptTokens: 5, CompletionTokens: 3, TotalTokens: 8}},
	}
}

// assertTimeline checks the AG-UI timeline the writer synthesized: event order,
// run/thread scope, message id pairing for the synthesized messages, and the
// payload deltas each row cares about.
func assertTimeline(t *testing.T, stored []stream.StoredEvent, wantTypes []stream.EventType) {
	t.Helper()

	require.Len(t, stored, len(wantTypes), "synthesized sequence length")

	for i, want := range wantTypes {
		require.Equal(t, want, stored[i].Event.Type, "event %d", i)
		require.Equal(t, "run-1", stored[i].Event.RunID, "run scope on %d", i)
		require.Equal(t, "sess-1", stored[i].Event.ThreadID, "thread scope on %d", i)
	}

	// The two text deltas share one synthesized message; reasoning is separate.
	require.Equal(t, "m-1", stored[0].Event.MessageID)
	require.Equal(t, "m-1", stored[1].Event.MessageID)
	require.Equal(t, "m-1", stored[2].Event.MessageID)
	require.Equal(t, "m-1", stored[5].Event.MessageID)
	require.Equal(t, "m-2", stored[3].Event.MessageID)
	require.Equal(t, "m-2", stored[4].Event.MessageID)
	require.Equal(t, "m-2", stored[6].Event.MessageID)

	require.Equal(t, "Hel", stored[1].Event.Payload[stream.FieldDelta])
	require.Equal(t, "lo", stored[2].Event.Payload[stream.FieldDelta])
	require.Equal(t, "think", stored[4].Event.Payload[stream.FieldDelta])
	require.Equal(t, "call-1", stored[7].Event.Payload[stream.FieldCallID])
	require.Equal(t, "web_search", stored[7].Event.Payload[stream.FieldName])
	require.Equal(t, `{"q":"x"}`, stored[8].Event.Payload[stream.FieldDelta])
	require.Equal(t, `{"q":"x"}`, stored[9].Event.Payload[stream.FieldDelta])
}

// projectMilestones reduces the timeline through ProjectToCore and asserts the
// durable milestone set — the audit trail the control plane keeps, with token
// bytes left on the data plane.
func projectMilestones(t *testing.T, handle *stream.Handle, stored []stream.StoredEvent, want []agentoscore.EventType) {
	t.Helper()

	var milestones []agentoscore.EventType

	for i := range stored {
		if projected, ok := stream.ProjectToCore(handle, &stored[i]); ok {
			milestones = append(milestones, projected.EventType)
			require.Equal(t, handle.Channel, projected.Source, "audit source on %s", projected.EventType)
		}
	}

	require.Equal(t, want, milestones)
}

// collectStreamEvents reads exactly want stored events off the subscription,
// failing the test on a stall. The bus replays the full retained history, so
// no live delivery race exists after the call returns.
func collectStreamEvents(t *testing.T, sub *stream.Subscription, want int) []stream.StoredEvent {
	t.Helper()

	got := make([]stream.StoredEvent, 0, want)
	for len(got) < want {
		select {
		case ev := <-sub.C:
			got = append(got, ev)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out after %d of %d events", len(got), want)
		}
	}

	return got
}
