package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo"
)

const maxToolRounds = 10

// StepRequest is the input for a single agent step (one LLM call + tool rounds).
type StepRequest struct {
	RunID   string
	Message string
	History []entity.Message
	Tools   []entity.ToolDef
	Config  entity.LLMConfig
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
}

// New -.
func New(llm repo.LLMProvider, tools repo.ToolExecutor, wal repo.WALAppender, compressor repo.ContextCompressor) *UseCase {
	return &UseCase{llm: llm, tools: tools, wal: wal, compressor: compressor}
}

// ExecuteStep runs one LLM invocation plus subsequent tool rounds.
// It returns the messages generated, tool results, and usage statistics.
func (uc *UseCase) ExecuteStep(ctx context.Context, req StepRequest) (*StepResult, error) {
	messages := make([]entity.Message, 0, len(req.History)+1+maxToolRounds*2)
	messages = append(messages, req.History...)
	if req.Message != "" {
		messages = append(messages, entity.Message{Role: entity.RoleUser, Content: req.Message})
	}

	var allToolResults []entity.ToolResult
	var finalUsage entity.Usage

	for range maxToolRounds {
		// Compress messages if approaching context limits (before LLM call)
		if uc.compressor != nil {
			compressed, _, err := uc.compressor.Compress(ctx, messages, req.Config)
			if err == nil {
				messages = compressed
			} // on error, continue with original messages
		}

		llmReq := entity.LLMRequest{
			Messages: messages,
			Tools:    req.Tools,
			Config:   req.Config,
		}

		resp, err := uc.llm.Chat(ctx, llmReq)
		if err != nil {
			return nil, fmt.Errorf("AgentUseCase - ExecuteStep - uc.llm.Chat: %w", err)
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
		delta = messages // delta concept breaks down, return full state
	}

	return &StepResult{
		Messages:      delta,
		CompleteState: messages,
		ToolResults:   allToolResults,
		Usage:         finalUsage,
		FinishReason:  entity.FinishStop,
	}, nil
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
