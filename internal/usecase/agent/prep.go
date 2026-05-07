package agent

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// PrepRequest is the input for agent pre-execution initialization.
type PrepRequest struct {
	SystemPrompt string
	UserMessage  string
	History      []entity.Message
	Tools        []entity.ToolDef
	Config       entity.LLMConfig
}

// PrepResult contains the prepared execution context after initialization.
type PrepResult struct {
	Messages []entity.Message
	Tools    []entity.ToolDef
}

// Prep initialises the execution context before the first step.
// It validates tool definitions and constructs the initial message list
// with the system prompt injected at the front.
func (uc *UseCase) Prep(ctx context.Context, req PrepRequest) (*PrepResult, error) {
	// Validate tool definitions
	for i, tool := range req.Tools {
		if tool.Function.Name == "" {
			return nil, &entity.AgentError{
				Code:        entity.ErrorCodeValidation,
				Message:     fmt.Sprintf("tool definition at index %d missing name", i),
				UserMessage: "Agent configuration error: a tool is missing its name",
				Recoverable: false,
				Retryable:   true,
			}
		}
		if tool.Type == "" {
			return nil, &entity.AgentError{
				Code:        entity.ErrorCodeValidation,
				Message:     fmt.Sprintf("tool %q missing type field", tool.Function.Name),
				UserMessage: "Agent configuration error: a tool is missing its type",
				Recoverable: false,
				Retryable:   true,
			}
		}
	}

	// Build initial message list
	messages := make([]entity.Message, 0, len(req.History)+2)

	// Inject system prompt only on first invocation (no prior history)
	if req.SystemPrompt != "" && len(req.History) == 0 {
		messages = append(messages, entity.Message{
			Role:    entity.RoleSystem,
			Content: req.SystemPrompt,
		})
	}

	messages = append(messages, req.History...)

	if req.UserMessage != "" {
		messages = append(messages, entity.Message{
			Role:    entity.RoleUser,
			Content: req.UserMessage,
		})
	}

	return &PrepResult{
		Messages: messages,
		Tools:    req.Tools,
	}, nil
}
