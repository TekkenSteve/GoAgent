package agent

import (
	"context"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
)

// LLMStreamCall performs a single streaming LLM call, writing delta events
// to the writer as chunks arrive. It returns the accumulated result (tool calls,
// usage, finish reason) for workflow-level decision making.
func (uc *UseCase) LLMStreamCall(ctx context.Context, runID string, messages []entity.Message, tools []entity.ToolDef, config entity.LLMConfig, writer usecase.StreamEventWriter) (*LLMStepResult, error) {
	streamLLM, ok := uc.llm.(repo.LLMStreamProvider)
	if !ok {
		return nil, &entity.AgentError{
			Code:        entity.ErrorCodeInternal,
			Message:     "LLM provider does not support ChatStream",
			UserMessage: "Streaming is not available with the configured AI provider.",
			Recoverable: false,
			Retryable:   false,
		}
	}

	streamCh, err := streamLLM.ChatStream(ctx, &entity.LLMRequest{
		Messages: messages,
		Tools:    tools,
		Config:   config,
	})
	if err != nil {
		return nil, classifyLLMError(err)
	}

	toolCalls, llmUsage, finishReason := uc.collectStreamChunks(ctx, writer, streamCh)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	uc.emitToolCallFinishEvents(ctx, writer, toolCalls)

	assistantMsg := entity.Message{Role: entity.RoleAssistant}
	if len(toolCalls) > 0 {
		assistantMsg.ToolCalls = toolCalls
	}

	uc.appendMessageToWAL(ctx, runID, assistantMsg)

	return &LLMStepResult{
		AssistantMsg: assistantMsg,
		ToolCalls:    toolCalls,
		Usage:        llmUsage,
		FinishReason: finishReason,
	}, nil
}

// collectStreamChunks reads all chunks from the stream channel, emitting
// text/reasoning/tool-call delta events. It returns the accumulated tool calls,
// usage, and finish reason.
func (uc *UseCase) collectStreamChunks(ctx context.Context, writer usecase.StreamEventWriter, streamCh <-chan entity.LLMStreamChunk) ([]entity.ToolCall, entity.Usage, string) {
	var (
		toolCalls    []entity.ToolCall
		llmUsage     entity.Usage
		finishReason string
	)

	for chunk := range streamCh {
		if uc.emitContentDelta(ctx, writer, &chunk) {
			return toolCalls, llmUsage, finishReason
		}

		if uc.emitReasoningDelta(ctx, writer, &chunk) {
			return toolCalls, llmUsage, finishReason
		}

		uc.emitToolCallDeltas(ctx, writer, chunk.ToolCallDeltas)

		if ctx.Err() != nil {
			return toolCalls, llmUsage, finishReason
		}

		uc.collectChunkMetadata(&chunk, &toolCalls, &llmUsage, &finishReason)
	}

	return toolCalls, llmUsage, finishReason
}

// emitContentDelta emits a text delta event for the chunk and returns true if context was canceled.
func (uc *UseCase) emitContentDelta(ctx context.Context, writer usecase.StreamEventWriter, chunk *entity.LLMStreamChunk) bool {
	if chunk.Content == "" {
		return false
	}

	if e := writer.WriteEvent(ctx, entity.NewTextDeltaEvent(chunk.Content, 0)); e != nil {
		_ = e
	}

	return ctx.Err() != nil
}

// emitReasoningDelta emits a reasoning delta event for the chunk and returns true if context was canceled.
func (uc *UseCase) emitReasoningDelta(ctx context.Context, writer usecase.StreamEventWriter, chunk *entity.LLMStreamChunk) bool {
	if chunk.Reasoning == "" {
		return false
	}

	if e := writer.WriteEvent(ctx, entity.NewReasoningDeltaEvent(chunk.Reasoning)); e != nil {
		_ = e
	}

	return ctx.Err() != nil
}

// emitToolCallDeltas emits tool call start and delta events for each delta in the list.
func (uc *UseCase) emitToolCallDeltas(ctx context.Context, writer usecase.StreamEventWriter, deltas []entity.ToolCallDelta) {
	for _, d := range deltas {
		if d.Name != "" && d.ToolCallID != "" {
			if e := writer.WriteEvent(ctx, entity.NewToolCallStartEvent(d.ToolCallID, d.Name, d.Index)); e != nil {
				_ = e
			}
		} else if d.ArgsDelta != "" {
			if e := writer.WriteEvent(ctx, entity.NewToolCallDeltaEvent(d.ToolCallID, d.ArgsDelta)); e != nil {
				_ = e
			}
		}
	}
}

// collectChunkMetadata updates the accumulated tool calls, usage, and finish reason from the chunk.
func (uc *UseCase) collectChunkMetadata(chunk *entity.LLMStreamChunk, toolCalls *[]entity.ToolCall, llmUsage *entity.Usage, finishReason *string) {
	if len(chunk.ToolCalls) > 0 {
		*toolCalls = chunk.ToolCalls
	}

	if chunk.Usage.TotalTokens > 0 {
		*llmUsage = chunk.Usage
	}

	if chunk.FinishReason != "" {
		*finishReason = string(chunk.FinishReason)
	}
}

// emitToolCallFinishEvents writes a ToolCallFinish event for every complete
// tool call. Stops early if context is canceled.
func (uc *UseCase) emitToolCallFinishEvents(ctx context.Context, writer usecase.StreamEventWriter, toolCalls []entity.ToolCall) {
	for _, tc := range toolCalls {
		if ctx.Err() != nil {
			return
		}

		if err := writer.WriteEvent(ctx, entity.NewToolCallFinishEvent(
			tc.ID, tc.Function.Name, tc.Function.Arguments,
		)); err != nil {
			_ = err
		}
	}
}
