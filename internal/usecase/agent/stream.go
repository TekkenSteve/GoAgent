package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
)

// ExecuteStream runs the agent with streaming output.
// Prep is synchronous; the event loop runs in a background goroutine.
// Events are written to the provided StreamEventWriter (e.g. Redis Stream).
func (uc *UseCase) ExecuteStream(ctx context.Context, req entity.StreamRequest, writer usecase.StreamEventWriter) error {
	// Auto-populate tool definitions from registry if not explicitly provided
	tools := req.Tools
	if len(tools) == 0 && uc.toolDefs != nil {
		tools = uc.toolDefs.Definitions()
	}

	// Emit prep_stage: initializing before Prep begins
	writer.WriteEvent(ctx, entity.StreamEvent{
		Type:     entity.StreamEventPrepStage,
		Stage:    "initializing",
		Progress: 0,
	})

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

	// Emit prep_stage: ready after successful Prep
	writer.WriteEvent(ctx, entity.StreamEvent{
		Type:     entity.StreamEventPrepStage,
		Stage:    "ready",
		Progress: 100,
	})

	streamLLM, ok := uc.llm.(repo.LLMStreamProvider)
	if !ok {
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

	go func() {
		for range maxToolRounds {
			// Emit thinking indicator before each LLM call
			writer.WriteEvent(ctx, entity.StreamEvent{
				Type: entity.StreamEventThinking,
			})

			// Context usage: emit message count before compression
			messagesBefore := len(messages)
			writer.WriteEvent(ctx, entity.StreamEvent{
				Type:         entity.StreamEventContextUsage,
				MessageCount: messagesBefore,
			})

			// Compress messages if approaching context limits
			var compressed bool
			if uc.compressor != nil {
				var err error
				var compressedMsgs []entity.Message
				compressedMsgs, compressed, err = uc.compressor.Compress(ctx, messages, req.Config)
				if err == nil && compressed {
					// Emit context_usage after compression with before/after counts
					writer.WriteEvent(ctx, entity.StreamEvent{
						Type:           entity.StreamEventContextUsage,
						MessageCount:   len(compressedMsgs),
						MessagesBefore: &messagesBefore,
						MessagesAfter:  intPtr(len(compressedMsgs)),
						Compressed:     &compressed,
					})
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
				writer.WriteEvent(ctx, entity.StreamEvent{
					Type:  entity.StreamEventError,
					Error: classifyLLMError(err).Error(),
				})
				return
			}

			var toolCalls []entity.ToolCall
			var llmUsage entity.Usage
			var finishReason string

			for chunk := range streamCh {
				if chunk.Content != "" {
					writer.WriteEvent(ctx, entity.StreamEvent{
						Type:    entity.StreamEventContent,
						Content: chunk.Content,
					})
					if ctx.Err() != nil {
						return
					}
				}
				if chunk.Reasoning != "" {
					writer.WriteEvent(ctx, entity.StreamEvent{
						Type:      entity.StreamEventReasoning,
						Reasoning: chunk.Reasoning,
					})
					if ctx.Err() != nil {
						return
					}
				}
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

			// Build assistant message for message loop
			assistantMsg := entity.Message{Role: entity.RoleAssistant}
			if len(toolCalls) > 0 {
				assistantMsg.ToolCalls = toolCalls
			}
			messages = append(messages, assistantMsg)
			uc.appendMessageToWAL(ctx, req.RunID, assistantMsg)

			if len(toolCalls) == 0 {
				// Done: emit final event with usage and finish reason
				var usagePtr *entity.Usage
				if llmUsage.TotalTokens > 0 {
					usagePtr = &llmUsage
				}
				writer.WriteEvent(ctx, entity.StreamEvent{
					Type:         entity.StreamEventDone,
					Usage:        usagePtr,
					FinishReason: finishReason,
				})
				return
			}

			// Execute tools and emit events
			for _, tc := range toolCalls {
				writer.WriteEvent(ctx, entity.StreamEvent{
					Type:      entity.StreamEventToolCall,
					ToolName:  tc.Function.Name,
					ToolInput: tc.Function.Arguments,
				})
				if ctx.Err() != nil {
					return
				}

				toolReq := entity.ToolRequest{
					RunID:      req.RunID,
					ToolCallID: tc.ID,
					ToolName:   tc.Function.Name,
					Args:       parseArgsJSON(tc.Function.Arguments),
				}

				result, execErr := uc.tools.Execute(ctx, toolReq)
				if execErr != nil {
					result = entity.ToolResult{
						RunID:      req.RunID,
						ToolCallID: tc.ID,
						ToolName:   tc.Function.Name,
					}
				}

				uc.appendToolResultToWAL(ctx, req.RunID, result, tc)

				// Build tool output string
				content := ""
				toolOutput := ""
				if execErr != nil {
					content = fmt.Sprintf("Error executing tool %q: %v", tc.Function.Name, execErr)
					toolOutput = fmt.Sprintf("Error: %v", execErr)
				} else if result.Output != nil {
					content = fmt.Sprintf("%v", result.Output)
					if b, err := json.Marshal(result.Output); err == nil {
						toolOutput = string(b)
					}
				}

				writer.WriteEvent(ctx, entity.StreamEvent{
					Type:       entity.StreamEventToolResult,
					ToolName:   tc.Function.Name,
					ToolOutput: toolOutput,
				})
				if ctx.Err() != nil {
					return
				}

				toolMsg := entity.Message{
					Role:       entity.RoleTool,
					ToolCallID: tc.ID,
					Content:    content,
				}
				messages = append(messages, toolMsg)
				uc.appendMessageToWAL(ctx, req.RunID, toolMsg)
			}
		}
	}()

	return nil
}

func intPtr(n int) *int { return &n }
