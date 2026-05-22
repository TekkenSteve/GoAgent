package agent

import (
	"context"

	"github.com/TekkenSteve/GoAgent/entity"
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

// Prep initializes the execution context before the first step.
// It constructs the initial message list with the system prompt injected at the front.
// Tool validation and pairing repair are handled by the orchestration layer
// (PrepareActivity and per-call repair in the workflow loop).
const prepBufferExtra = 2

func (uc *UseCase) Prep(_ context.Context, req *PrepRequest) (*PrepResult, error) {
	// Build initial message list
	messages := make([]entity.Message, 0, len(req.History)+prepBufferExtra)

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
