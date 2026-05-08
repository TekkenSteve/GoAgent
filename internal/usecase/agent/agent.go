package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
)

const maxToolRounds = 10

// StepRequest is the input for a single agent step (one LLM call + tool rounds).
type StepRequest struct {
	RunID        string
	SystemPrompt string // optional system instructions, injected on first step
	Message      string
	History      []entity.Message
	Tools        []entity.ToolDef
	Config       entity.LLMConfig
}

// StepResult is the output of a single agent step.
type StepResult struct {
	// Messages contains the new messages generated (delta from History), for logging/inspection.
	// Callers should use CompleteState to replace accumulated history.
	Messages []entity.Message
	// CompleteState is the full accumulated message state after this step.
	// When compression occurs, this replaces the prior history entirely.
	CompleteState []entity.Message
	ToolResults   []entity.ToolResult
	Usage         entity.Usage
	FinishReason  entity.FinishReason
}

// UseCase -.
type UseCase struct {
	llm        repo.LLMProvider
	tools      repo.ToolExecutor
	wal        repo.WALAppender
	compressor repo.ContextCompressor
	toolDefs   usecase.ToolDefProvider
}

// New -.
func New(llm repo.LLMProvider, tools repo.ToolExecutor, wal repo.WALAppender, compressor repo.ContextCompressor, toolDefs usecase.ToolDefProvider) *UseCase {
	return &UseCase{llm: llm, tools: tools, wal: wal, compressor: compressor, toolDefs: toolDefs}
}

// ExecuteStep runs one LLM invocation plus subsequent tool rounds.
// It returns the messages generated, tool results, and usage statistics.
func (uc *UseCase) ExecuteStep(ctx context.Context, req StepRequest) (*StepResult, error) {
	// Auto-populate tool definitions from registry if not explicitly provided
	tools := req.Tools
	if len(tools) == 0 && uc.toolDefs != nil {
		tools = uc.toolDefs.Definitions()
	}

	// Phase 1: Prep — validate and initialise execution context
	prepResult, err := uc.Prep(ctx, PrepRequest{
		SystemPrompt: req.SystemPrompt,
		UserMessage:  req.Message,
		History:      req.History,
		Tools:        tools,
		Config:       req.Config,
	})
	if err != nil {
		return nil, fmt.Errorf("AgentUseCase - ExecuteStep - Prep: %w", err)
	}

	messages := prepResult.Messages
	tools = prepResult.Tools

	// Phase 2: Execute — LLM call + tool execution loop
	var allToolResults []entity.ToolResult
	var finalUsage entity.Usage

	for range maxToolRounds {
		// Compress messages if approaching context limits (before LLM call)
		if uc.compressor != nil {
			if compressed, _, err := uc.compressor.Compress(ctx, messages, req.Config); err == nil {
				messages = compressed
			} // on error, continue with original messages
		}

		llmReq := entity.LLMRequest{
			Messages: messages,
			Tools:    tools,
			Config:   req.Config,
		}

		resp, err := uc.llm.Chat(ctx, llmReq)
		if err != nil {
			return nil, classifyLLMError(err)
		}

		finalUsage = resp.Usage

		assistantMsg := entity.Message{
			Role:    entity.RoleAssistant,
			Content: resp.Content,
		}

		if len(resp.ToolCalls) > 0 {
			assistantMsg.ToolCalls = resp.ToolCalls
		}
		messages = append(messages, assistantMsg)
		uc.appendMessageToWAL(ctx, req.RunID, assistantMsg)

		if len(resp.ToolCalls) == 0 {
			break
		}

		for _, tc := range resp.ToolCalls {
			toolReq := entity.ToolRequest{
				RunID:      req.RunID,
				ToolCallID: tc.ID,
				ToolName:   tc.Function.Name,
				Args:       parseArgsJSON(tc.Function.Arguments),
			}

			toolResult, execErr := uc.tools.Execute(ctx, toolReq)
			if execErr != nil {
				toolResult = entity.ToolResult{
					RunID:      req.RunID,
					ToolCallID: tc.ID,
					ToolName:   tc.Function.Name,
				}
			}

			allToolResults = append(allToolResults, toolResult)
			uc.appendToolResultToWAL(ctx, req.RunID, toolResult, tc)

			content := ""
			if execErr != nil {
				content = fmt.Sprintf("Error executing tool %q: %v", tc.Function.Name, execErr)
			} else if toolResult.Output != nil {
				content = fmt.Sprintf("%v", toolResult.Output)
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

	// Compute delta safely: after compression messages may be shorter than history.
	var delta []entity.Message
	if len(messages) >= len(req.History) {
		delta = messages[len(req.History):]
	} else {
		delta = messages
	}

	return &StepResult{
		Messages:      delta,
		CompleteState: messages,
		ToolResults:   allToolResults,
		Usage:         finalUsage,
		FinishReason:  entity.FinishStop,
	}, nil
}

// classifyLLMError wraps common LLM provider errors into structured AgentError types.
func classifyLLMError(err error) error {
	errStr := err.Error()

	switch {
	case containsAny(errStr, "timeout", "deadline exceeded", "context deadline"):
		return &entity.AgentError{
			Code:        entity.ErrorCodeLLMTimeout,
			Message:     errStr,
			UserMessage: "The AI model took too long to respond. Please try again.",
			Recoverable: true,
			Retryable:   true,
			Err:         err,
		}
	case containsAny(errStr, "rate limit", "429", "too many requests"):
		return &entity.AgentError{
			Code:        entity.ErrorCodeLLMRateLimit,
			Message:     errStr,
			UserMessage: "The AI service is currently rate-limited. Please wait and try again.",
			Recoverable: true,
			Retryable:   true,
			Err:         err,
		}
	case containsAny(errStr, "content_filter", "content filter", "safety system"):
		return &entity.AgentError{
			Code:        entity.ErrorCodeLLMContentFilter,
			Message:     errStr,
			UserMessage: "The response was filtered due to content safety guidelines.",
			Recoverable: false,
			Retryable:   false,
			Err:         err,
		}
	case containsAny(errStr, "context_length", "maximum context length", "token limit"):
		return &entity.AgentError{
			Code:        entity.ErrorCodeContextLength,
			Message:     errStr,
			UserMessage: "The conversation is too long for the AI model to process.",
			Recoverable: true,
			Retryable:   false,
			Err:         err,
		}
	default:
		return &entity.AgentError{
			Code:        entity.ErrorCodeLLM,
			Message:     errStr,
			UserMessage: "The AI model returned an unexpected error. Please try again.",
			Recoverable: true,
			Retryable:   true,
			Err:         err,
		}
	}
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func parseArgsJSON(raw string) map[string]any {
	if raw == "" {
		return nil
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return nil
	}
	return args
}

// appendMessageToWAL best-effort appends a message record to the write-ahead log.
func (uc *UseCase) appendMessageToWAL(ctx context.Context, runID string, msg entity.Message) {
	if uc.wal == nil {
		return
	}
	toolCallID := ""
	if len(msg.ToolCalls) > 0 {
		toolCallID = msg.ToolCalls[0].ID
	}
	if err := uc.wal.AppendMessage(ctx, runID, entity.MessageRecord{
		RunID:      runID,
		Role:       string(msg.Role),
		Content:    msg.Content,
		ToolCallID: toolCallID,
	}); err != nil {
		fmt.Printf("WARN: WAL append message failed (run=%s, role=%s): %v\n", runID, msg.Role, err)
	}
}

// appendToolResultToWAL best-effort appends a tool result record to the write-ahead log.
func (uc *UseCase) appendToolResultToWAL(ctx context.Context, runID string, result entity.ToolResult, tc entity.ToolCall) {
	if uc.wal == nil {
		return
	}
	var resultJSON string
	if result.Output != nil {
		b, err := json.Marshal(result.Output)
		if err == nil {
			resultJSON = string(b)
		}
	}
	if err := uc.wal.AppendToolResult(ctx, runID, entity.ToolResultRecord{
		RunID:      runID,
		ToolCallID: tc.ID,
		ToolName:   tc.Function.Name,
		ResultJSON: resultJSON,
	}); err != nil {
		fmt.Printf("WARN: WAL append tool result failed (run=%s, tool=%s): %v\n", runID, tc.Function.Name, err)
	}
}
