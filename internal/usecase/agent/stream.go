package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
)

// ExecuteStream runs the agent with streaming output.
// Prep is synchronous; the event loop runs in a background goroutine.
func (uc *UseCase) ExecuteStream(ctx context.Context, req entity.StreamRequest, writer usecase.StreamEventWriter) error {
	// Auto-populate tool definitions from registry if not explicitly provided
	tools := req.Tools
	if len(tools) == 0 && uc.toolDefs != nil {
		tools = uc.toolDefs.Definitions()
	}

	// Emit agent run start
	writer.WriteEvent(ctx, entity.NewAgentRunStartEvent(req.Message))

	// Emit prep_stage: initializing
	writer.WriteEvent(ctx, entity.NewPrepStageEvent("initializing", 0))

	prepResult, err := uc.Prep(ctx, PrepRequest{
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
	writer.WriteEvent(ctx, entity.NewPrepStageEvent("ready", 100))

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

// ExecuteStreamSync is the synchronous version of ExecuteStream.
// It blocks until execution completes. Used by Temporal activities
// to keep the activity alive for the duration of streaming execution.
func (uc *UseCase) ExecuteStreamSync(ctx context.Context, req entity.StreamRequest, writer usecase.StreamEventWriter) error {
	tools := req.Tools
	if len(tools) == 0 && uc.toolDefs != nil {
		tools = uc.toolDefs.Definitions()
	}

	writer.WriteEvent(ctx, entity.NewAgentRunStartEvent(req.Message))
	writer.WriteEvent(ctx, entity.NewPrepStageEvent("initializing", 0))

	prepResult, err := uc.Prep(ctx, PrepRequest{
		SystemPrompt: req.SystemPrompt,
		UserMessage:  req.Message,
		History:      req.History,
		Tools:        tools,
		Config:       req.Config,
	})
	if err != nil {
		return fmt.Errorf("AgentUseCase - ExecuteStreamSync - Prep: %w", err)
	}

	writer.WriteEvent(ctx, entity.NewPrepStageEvent("ready", 100))

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
// It blocks until all tool rounds complete or context is cancelled.
func (uc *UseCase) executeStreamLoop(ctx context.Context, req entity.StreamRequest, writer usecase.StreamEventWriter, messages []entity.Message, tools []entity.ToolDef) {
	streamLLM := uc.llm.(repo.LLMStreamProvider)
	var finalUsage *entity.Usage
	toolCallIndex := 0

	for range maxToolRounds {
		// Context usage: emit message count before compression
		messagesBefore := len(messages)
		writer.WriteEvent(ctx, entity.NewContextUsageEvent(messagesBefore, nil, nil, nil))

		// Compress messages if approaching context limits
		var compressed bool
		if uc.compressor != nil {
			var err error
			var compressedMsgs []entity.Message
			compressedMsgs, compressed, err = uc.compressor.Compress(ctx, messages, req.Config)
			if err == nil && compressed {
				writer.WriteEvent(ctx, entity.NewContextUsageEvent(
					len(compressedMsgs),
					intPtr(messagesBefore),
					intPtr(len(compressedMsgs)),
					boolPtr(compressed),
				))
				messages = compressedMsgs
			}
		}

		llmReq := entity.LLMRequest{
			Messages: messages,
			Tools:    tools,
			Config:   req.Config,
		}

		streamCh, err := streamLLM.ChatStream(ctx, llmReq)
		if err != nil {
			writer.WriteEvent(ctx, entity.NewAgentErrorEvent("LLM_ERROR", classifyLLMError(err).Error()))
			return
		}

		var toolCalls []entity.ToolCall
		var llmUsage entity.Usage
		var finishReason string

		for chunk := range streamCh {
			// Emit content delta
			if chunk.Content != "" {
				writer.WriteEvent(ctx, entity.NewTextDeltaEvent(chunk.Content, 0))
				if ctx.Err() != nil {
					return
				}
			}

			// Emit reasoning delta
			if chunk.Reasoning != "" {
				writer.WriteEvent(ctx, entity.NewReasoningDeltaEvent(chunk.Reasoning))
				if ctx.Err() != nil {
					return
				}
			}

			// Emit tool call deltas (intermediate streaming)
			for _, d := range chunk.ToolCallDeltas {
				if d.Name != "" && d.ToolCallID != "" {
					writer.WriteEvent(ctx, entity.NewToolCallStartEvent(d.ToolCallID, d.Name, d.Index))
				} else if d.ArgsDelta != "" {
					writer.WriteEvent(ctx, entity.NewToolCallDeltaEvent(d.ToolCallID, d.ArgsDelta))
				}
				if ctx.Err() != nil {
					return
				}
			}

			// Accumulate complete tool calls
			if len(chunk.ToolCalls) > 0 {
				toolCalls = chunk.ToolCalls
			}

			if chunk.Usage.TotalTokens > 0 {
				llmUsage = chunk.Usage
			}

			if chunk.FinishReason != "" {
				finishReason = string(chunk.FinishReason)
			}
		}

		if ctx.Err() != nil {
			return
		}

		// Emit ToolCallFinish for each complete tool call
		for _, tc := range toolCalls {
			toolCallIndex++
			writer.WriteEvent(ctx, entity.NewToolCallFinishEvent(
				tc.ID, tc.Function.Name, tc.Function.Arguments,
			))
			if ctx.Err() != nil {
				return
			}
		}

		// Build assistant message for message loop
		assistantMsg := entity.Message{Role: entity.RoleAssistant}
		if len(toolCalls) > 0 {
			assistantMsg.ToolCalls = toolCalls
		}
		messages = append(messages, assistantMsg)
		uc.appendMessageToWAL(ctx, req.RunID, assistantMsg)

		if len(toolCalls) == 0 {
			// Done: emit finish event with usage
			if llmUsage.TotalTokens > 0 {
				finalUsage = &llmUsage
			}
			writer.WriteEvent(ctx, entity.NewAgentRunFinishEvent(finishReason, finalUsage))
			return
		}

		// Execute tools and emit events
		for _, tc := range toolCalls {
			writer.WriteEvent(ctx, entity.NewToolExecStartEvent(tc.ID, tc.Function.Name))
			if ctx.Err() != nil {
				return
			}

			toolReq := entity.ToolRequest{
				RunID:      req.RunID,
				ToolCallID: tc.ID,
				ToolName:   tc.Function.Name,
				Args:       parseArgsJSON(tc.Function.Arguments),
			}

			execStart := time.Now()
			result, execErr := uc.tools.Execute(ctx, toolReq)
			durationMs := time.Since(execStart).Milliseconds()

			uc.appendToolResultToWAL(ctx, req.RunID, result, tc)

			// Build tool output string
			output := ""
			exitCode := 0
			isError := execErr != nil

			if execErr != nil {
				output = fmt.Sprintf("Error executing tool %q: %v", tc.Function.Name, execErr)
				exitCode = 1
			} else if result.Output != nil {
				if b, err := json.Marshal(result.Output); err == nil {
					output = string(b)
				}
			}

			writer.WriteEvent(ctx, entity.NewToolExecFinishEvent(tc.ID, tc.Function.Name, output, exitCode, isError, durationMs))
			if ctx.Err() != nil {
				return
			}

			content := ""
			if execErr != nil {
				content = fmt.Sprintf("Error executing tool %q: %v", tc.Function.Name, execErr)
			} else if result.Output != nil {
				content = fmt.Sprintf("%v", result.Output)
			}

			toolMsg := entity.Message{
				Role:       entity.RoleTool,
				ToolCallID: tc.ID,
				Content:    content,
			}
			messages = append(messages, toolMsg)
			uc.appendMessageToWAL(ctx, req.RunID, toolMsg)
		}

		// Phase 4 placeholder: non-blocking command channel check
		// select {
		// case cmd := <-uc.commandCh:
		//     // handle cancel/pause/resume
		// default:
		// }
	}
}

func intPtr(n int) *int     { return &n }
func boolPtr(b bool) *bool { return &b }
