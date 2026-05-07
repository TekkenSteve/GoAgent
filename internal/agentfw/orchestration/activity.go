package orchestration

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agent"
)

const (
	// AgentStepActivityName is the activity name for a single agent execution step.
	AgentStepActivityName = "agentfw.agent-step-activity.v1"
)

// StepActivityInput is the serializable input for AgentStepActivity.
type StepActivityInput struct {
	RunID   string
	Message string
	History []entity.Message
	Tools   []entity.ToolDef
	Config  entity.LLMConfig
}

// StepActivityOutput is the serializable output for AgentStepActivity.
type StepActivityOutput struct {
	Messages     []entity.Message
	FullMessages []entity.Message // full accumulated state, replaces prior history when non-nil
	ToolResults  []entity.ToolResult
	Usage        entity.Usage
	FinishReason string
}

// AgentActivities provides Temporal activity implementations for agent execution.
type AgentActivities struct {
	agentUC *agent.UseCase
}

// NewAgentActivities creates activities wired to the agent usecase.
func NewAgentActivities(uc *agent.UseCase) *AgentActivities {
	return &AgentActivities{agentUC: uc}
}

// ExecuteStep runs one agent step (LLM call + tool rounds) via the usecase.
func (a *AgentActivities) ExecuteStep(ctx context.Context, input StepActivityInput) (StepActivityOutput, error) {
	req := agent.StepRequest{
		RunID:   input.RunID,
		Message: input.Message,
		History: input.History,
		Tools:   input.Tools,
		Config:  input.Config,
	}

	result, err := a.agentUC.ExecuteStep(ctx, req)
	if err != nil {
		return StepActivityOutput{}, fmt.Errorf("AgentActivities - ExecuteStep - agentUC.ExecuteStep: %w", err)
	}

	return StepActivityOutput{
		Messages:     result.Messages,
		FullMessages: result.CompleteState,
		ToolResults:  result.ToolResults,
		Usage:        result.Usage,
		FinishReason: string(result.FinishReason),
	}, nil
}
