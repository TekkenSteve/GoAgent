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
	Messages    []entity.Message
	ToolResults []entity.ToolResult
	Usage       entity.Usage
	FinishReason entity.FinishReason
}

// UseCase -.
type UseCase struct {
	llm   repo.LLMProvider
	tools repo.ToolExecutor
	state repo.WarmStateRepo
}

// New -.
func New(llm repo.LLMProvider, tools repo.ToolExecutor, state repo.WarmStateRepo) *UseCase {
	return &UseCase{llm: llm, tools: tools, state: state}
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

	for round := 0; round < maxToolRounds; round++ {
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

			content := ""
			if execErr != nil {
				content = fmt.Sprintf("Error executing tool %q: %v", tc.Function.Name, execErr)
			} else if toolResult.Output != nil {
				content = fmt.Sprintf("%v", toolResult.Output)
			}

			messages = append(messages, entity.Message{
				Role:       entity.RoleTool,
				ToolCallID: tc.ID,
				Content:    content,
			})
		}
	}

	return &StepResult{
		Messages:     messages[len(req.History):],
		ToolResults:  allToolResults,
		Usage:        finalUsage,
		FinishReason: entity.FinishStop,
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
