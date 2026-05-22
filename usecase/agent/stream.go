package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/repo"
	"github.com/TekkenSteve/GoAgent/usecase"
)

const prepStageReadyPercent = 100

// ExecuteStream runs the agent with streaming output.
// Prep is synchronous; the event loop runs in a background goroutine.
func (uc *UseCase) ExecuteStream(ctx context.Context, req *entity.StreamRequest, writer usecase.StreamEventWriter) error {
	// Auto-populate tool definitions from registry if not explicitly provided
	tools := req.Tools
	if len(tools) == 0 && uc.toolDefs != nil {
		tools = uc.toolDefs.Definitions()
	}

	// Emit agent run start
	if err := writer.WriteEvent(ctx, entity.NewAgentRunStartEvent(req.Message)); err != nil {
		return fmt.Errorf("AgentUseCase - ExecuteStream - write start event: %w", err)
	}

	// Emit prep_stage: initializing
	if err := writer.WriteEvent(ctx, entity.NewPrepStageEvent("initializing", 0)); err != nil {
		return fmt.Errorf("AgentUseCase - ExecuteStream - write init event: %w", err)
	}

	prepResult, err := uc.Prep(ctx, &PrepRequest{
		SystemPrompt: req.SystemPrompt,
		UserMessage:  req.Message,
		History:      req.History,
		Tools:        tools,
		Config:       req.Config,
	})
	if err != nil {
		return fmt.Errorf("AgentUseCase - ExecuteStream - Prep: %w", err)
	}

	// Emit prep_stage: ready
	if err := writer.WriteEvent(ctx, entity.NewPrepStageEvent("ready", prepStageReadyPercent)); err != nil {
		return fmt.Errorf("AgentUseCase - ExecuteStream - write ready event: %w", err)
	}

	if _, ok := uc.llm.(repo.LLMStreamProvider); !ok {
		return &entity.AgentError{
			Code:        entity.ErrorCodeInternal,
			Message:     "LLM provider does not support ChatStream",
			UserMessage: "Streaming is not available with the configured AI provider.",
			Recoverable: false,
			Retryable:   false,
		}
	}

	messages := prepResult.Messages
	tools = prepResult.Tools

	go uc.executeStreamLoop(ctx, req, writer, messages, tools)

	return nil
}

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

// ExecuteStreamSync is the synchronous version of ExecuteStream.
// It blocks until execution completes. Used by Temporal activities
// to keep the activity alive for the duration of streaming execution.
func (uc *UseCase) ExecuteStreamSync(ctx context.Context, req *entity.StreamRequest, writer usecase.StreamEventWriter) error {
	tools := req.Tools
	if len(tools) == 0 && uc.toolDefs != nil {
		tools = uc.toolDefs.Definitions()
	}

	if err := writer.WriteEvent(ctx, entity.NewAgentRunStartEvent(req.Message)); err != nil {
		_ = err
	}

	if err := writer.WriteEvent(ctx, entity.NewPrepStageEvent("initializing", 0)); err != nil {
		_ = err
	}

	prepResult, err := uc.Prep(ctx, &PrepRequest{
		SystemPrompt: req.SystemPrompt,
		UserMessage:  req.Message,
		History:      req.History,
		Tools:        tools,
		Config:       req.Config,
	})
	if err != nil {
		return fmt.Errorf("AgentUseCase - ExecuteStreamSync - Prep: %w", err)
	}

	if err := writer.WriteEvent(ctx, entity.NewPrepStageEvent("ready", prepStageReadyPercent)); err != nil {
		_ = fmt.Sprintf("stream write: %v", err)
	}

	if _, ok := uc.llm.(repo.LLMStreamProvider); !ok {
		return &entity.AgentError{
			Code:        entity.ErrorCodeInternal,
			Message:     "LLM provider does not support ChatStream",
			UserMessage: "Streaming is not available with the configured AI provider.",
			Recoverable: false,
			Retryable:   false,
		}
	}

	uc.executeStreamLoop(ctx, req, writer, prepResult.Messages, prepResult.Tools)

	return nil
}

// executeStreamLoop is the shared LLM + tool execution loop.
// It blocks until all tool rounds complete or context is canceled.
func (uc *UseCase) executeStreamLoop(ctx context.Context, req *entity.StreamRequest, writer usecase.StreamEventWriter, messages []entity.Message, tools []entity.ToolDef) {
	streamLLM, ok := uc.llm.(repo.LLMStreamProvider)
	if !ok {
		return
	}

	for range maxToolRounds {
		messages = uc.compressStreamContext(ctx, req, writer, messages)

		streamCh, err := streamLLM.ChatStream(ctx, &entity.LLMRequest{
			Messages: messages,
			Tools:    tools,
			Config:   req.Config,
		})
		if err != nil {
			uc.writeStreamError(ctx, writer, err)

			return
		}

		toolCalls, llmUsage, finishReason := uc.collectStreamChunks(ctx, writer, streamCh)
		if ctx.Err() != nil {
			return
		}

		if uc.completeStreamRound(ctx, req, writer, &messages, toolCalls, llmUsage, finishReason) {
			return
		}
	}
}

// compressStreamContext emits a context usage event and tries to compress
// messages when approaching context limits. Returns the (possibly compressed)
// message slice.
func (uc *UseCase) compressStreamContext(ctx context.Context, req *entity.StreamRequest, writer usecase.StreamEventWriter, messages []entity.Message) []entity.Message {
	messagesBefore := len(messages)
	if err := writer.WriteEvent(ctx, entity.NewContextUsageEvent(messagesBefore, nil, nil, nil)); err != nil {
		_ = fmt.Sprintf("stream write: %v", err)
	}

	if uc.compressor == nil {
		return messages
	}

	compressedMsgs, compressed, err := uc.compressor.Compress(ctx, messages, req.Config)
	if err == nil && compressed {
		if e := writer.WriteEvent(ctx, entity.NewContextUsageEvent(
			len(compressedMsgs),
			new(messagesBefore),
			new(len(compressedMsgs)),
			new(compressed),
		)); e != nil {
			_ = fmt.Sprintf("stream write: %v", e)
		}

		return compressedMsgs
	}

	return messages
}

// writeStreamError emits an LLM error event for the stream.
func (uc *UseCase) writeStreamError(ctx context.Context, writer usecase.StreamEventWriter, err error) {
	if e := writer.WriteEvent(ctx, entity.NewAgentErrorEvent("LLM_ERROR", classifyLLMError(err).Error())); e != nil {
		_ = e
	}
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

// completeStreamRound handles the post-stream tasks for one round: emitting
// tool-call finish events, building the assistant message, and either
// finishing (no tool calls) or executing the requested tools. Returns true
// when the loop should stop (stream complete or context canceled).
func (uc *UseCase) completeStreamRound(ctx context.Context, req *entity.StreamRequest, writer usecase.StreamEventWriter, messages *[]entity.Message, toolCalls []entity.ToolCall, llmUsage entity.Usage, finishReason string) bool {
	uc.emitToolCallFinishEvents(ctx, writer, toolCalls)

	if ctx.Err() != nil {
		return true
	}

	assistantMsg := entity.Message{Role: entity.RoleAssistant}
	if len(toolCalls) > 0 {
		assistantMsg.ToolCalls = toolCalls
	}

	*messages = append(*messages, assistantMsg)
	uc.appendMessageToWAL(ctx, req.RunID, assistantMsg)

	if len(toolCalls) == 0 {
		var usage *entity.Usage
		if llmUsage.TotalTokens > 0 {
			usage = &llmUsage
		}

		if e := writer.WriteEvent(ctx, entity.NewAgentRunFinishEvent(finishReason, usage)); e != nil {
			_ = e
		}

		return true
	}

	*messages = uc.executeStreamTools(ctx, req, writer, *messages, toolCalls)

	return false
}

// executeStreamTools iterates over tool calls, executing each and appending
// tool-result messages.
func (uc *UseCase) executeStreamTools(ctx context.Context, req *entity.StreamRequest, writer usecase.StreamEventWriter, messages []entity.Message, toolCalls []entity.ToolCall) []entity.Message {
	for _, tc := range toolCalls {
		msg, err := uc.executeStreamTool(ctx, req, writer, tc)
		if err != nil {
			return messages
		}

		messages = append(messages, msg)
	}

	return messages
}

// executeStreamTool executes a single tool call, emits start/finish events,
// and returns the tool-result message.
func (uc *UseCase) executeStreamTool(ctx context.Context, req *entity.StreamRequest, writer usecase.StreamEventWriter, tc entity.ToolCall) (entity.Message, error) {
	if e := writer.WriteEvent(ctx, entity.NewToolExecStartEvent(tc.ID, tc.Function.Name)); e != nil {
		_ = e
	}

	if ctx.Err() != nil {
		return entity.Message{}, ctx.Err()
	}

	toolReq := entity.ToolRequest{
		RunID:      req.RunID,
		ToolCallID: tc.ID,
		ToolName:   tc.Function.Name,
		Args:       parseArgsJSON(tc.Function.Arguments),
	}

	execStart := time.Now()
	result, execErr := uc.tools.Execute(ctx, &toolReq)
	durationMs := time.Since(execStart).Milliseconds()

	uc.appendToolResultToWAL(ctx, req.RunID, &result, tc)

	output, exitCode, isError := formatToolOutput(tc.Function.Name, execErr, &result)

	if e := writer.WriteEvent(ctx, entity.NewToolExecFinishEvent(tc.ID, tc.Function.Name, output, exitCode, isError, durationMs)); e != nil {
		_ = e
	}

	content := buildToolContent(tc.Function.Name, execErr, &result)
	toolMsg := entity.Message{
		Role:       entity.RoleTool,
		ToolCallID: tc.ID,
		Content:    content,
	}
	uc.appendMessageToWAL(ctx, req.RunID, toolMsg)

	return toolMsg, nil
}

// formatToolOutput builds the output string, exit code, and error flag from a
// tool execution result.
func formatToolOutput(toolName string, execErr error, result *entity.ToolResult) (output string, exitCode int, isError bool) {
	if execErr != nil {
		return fmt.Sprintf("Error executing tool %q: %v", toolName, execErr), 1, true
	}

	if result.Output != nil {
		if b, err := json.Marshal(result.Output); err == nil {
			return string(b), 0, false
		}
	}

	return "", 0, false
}

// buildToolContent constructs the message content string from a tool
// execution result.
func buildToolContent(toolName string, execErr error, result *entity.ToolResult) string {
	if execErr != nil {
		return fmt.Sprintf("Error executing tool %q: %v", toolName, execErr)
	}

	if result.Output != nil {
		return fmt.Sprintf("%v", result.Output)
	}

	return ""
}
