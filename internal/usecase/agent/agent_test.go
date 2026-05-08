package agent_test

import (
	"context"
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agent"
)

func TestExecuteStep_EmptyMessage(t *testing.T) {
	uc := agent.New(
		&mockLLM{response: entity.LLMResponse{Content: "Continuing!", FinishReason: "stop", Usage: entity.Usage{PromptTokens: 5, CompletionTokens: 3, TotalTokens: 8}}},
		&mockTool{},
		nil,
		nil,
	nil,
	)

	result, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:   "run-1",
		Message: "",
		History: []entity.Message{{Role: entity.RoleUser, Content: "Hi"}, {Role: entity.RoleAssistant, Content: "Hello!"}},
		Config:  entity.LLMConfig{Model: "gpt-4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Result only contains new messages (not history)
	if len(result.Messages) != 1 {
		t.Fatalf("expected 1 new message, got %d", len(result.Messages))
	}
	if result.Messages[0].Role != entity.RoleAssistant || result.Messages[0].Content != "Continuing!" {
		t.Errorf("expected assistant message 'Continuing!', got %v", result.Messages[0])
	}
}

func TestExecuteStep_TextOnly(t *testing.T) {
	uc := agent.New(
		&mockLLM{response: entity.LLMResponse{Content: "Hello!", FinishReason: "stop", Usage: entity.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15}}},
		&mockTool{},
		nil,
		nil,
	nil,
	)

	result, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:   "run-1",
		Message: "Hi",
		Config:  entity.LLMConfig{Model: "gpt-4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FinishReason != "stop" {
		t.Errorf("expected stop, got %v", result.FinishReason)
	}
	if len(result.Messages) != 2 {
		t.Fatalf("expected 2 messages (user+assistant), got %d", len(result.Messages))
	}
	if result.Messages[0].Role != entity.RoleUser || result.Messages[0].Content != "Hi" {
		t.Errorf("expected user message 'Hi', got %v", result.Messages[0])
	}
	if result.Messages[1].Role != entity.RoleAssistant || result.Messages[1].Content != "Hello!" {
		t.Errorf("expected assistant message 'Hello!', got %v", result.Messages[1])
	}
	if result.Usage.TotalTokens != 15 {
		t.Errorf("expected 15 total tokens, got %d", result.Usage.TotalTokens)
	}
	if result.Usage.PromptTokens != 10 {
		t.Errorf("expected 10 prompt tokens, got %d", result.Usage.PromptTokens)
	}
}

func TestExecuteStep_ToolCallThenText(t *testing.T) {
	callCount := 0
	uc := agent.New(
		&mockLLM{
			responses: []entity.LLMResponse{
				{
					ToolCalls: []entity.ToolCall{{
						ID: "call-1", Type: "function",
						Function: entity.ToolCallFunction{Name: "search", Arguments: `{"q":"weather"}`},
					}},
					FinishReason: "tool_calls",
					Usage:        entity.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
				},
				{
					Content:      "The weather is sunny!",
					FinishReason: "stop",
					Usage:        entity.Usage{PromptTokens: 20, CompletionTokens: 10, TotalTokens: 30},
				},
			},
			callCount: &callCount,
		},
		&mockTool{result: entity.ToolResult{
			ToolName: "search", Output: map[string]any{"temp": "72F"},
		}},
		nil,
		nil,
	nil,
	)

	result, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:   "run-1",
		Message: "What's the weather?",
		Config:  entity.LLMConfig{Model: "gpt-4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 LLM calls, got %d", callCount)
	}
	if len(result.Messages) < 3 {
		t.Fatalf("expected at least 3 messages, got %d", len(result.Messages))
	}
	if result.Usage.TotalTokens != 30 {
		t.Errorf("expected 30 total tokens (last call), got %d", result.Usage.TotalTokens)
	}
}

func TestExecuteStep_ToolExecutionError(t *testing.T) {
	var callCount int
	uc := agent.New(
		&mockLLM{
			responses: []entity.LLMResponse{
				{
					ToolCalls: []entity.ToolCall{{
						ID: "call-1", Type: "function",
						Function: entity.ToolCallFunction{Name: "fail_tool", Arguments: `{}`},
					}},
					FinishReason: "tool_calls",
					Usage:        entity.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
				},
				{
					Content:      "The tool failed but I handled it.",
					FinishReason: "stop",
					Usage:        entity.Usage{PromptTokens: 25, CompletionTokens: 8, TotalTokens: 33},
				},
			},
			callCount: &callCount,
		},
		&mockTool{err: errors.New("tool crashed")},
		nil,
		nil,
	nil,
	)

	result, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:   "run-1",
		Message: "Run failing tool",
		Config:  entity.LLMConfig{Model: "gpt-4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 LLM calls, got %d", callCount)
	}
	if len(result.ToolResults) != 1 {
		t.Fatalf("expected 1 tool result, got %d", len(result.ToolResults))
	}
	if len(result.Messages) < 4 {
		t.Fatalf("expected at least 4 messages (user+assistant+tool+assistant), got %d", len(result.Messages))
	}
	// Verify the tool error was communicated back to the LLM
	if result.Messages[2].Role != entity.RoleTool {
		t.Errorf("expected third message to be tool role, got %v", result.Messages[2].Role)
	}
}

func TestExecuteStep_MaxToolRounds(t *testing.T) {
	var callCount int
	uc := agent.New(
		&mockLLM{
			responses: func() []entity.LLMResponse {
				resps := make([]entity.LLMResponse, 12)
				for i := range resps {
					resps[i] = entity.LLMResponse{
						ToolCalls: []entity.ToolCall{{
							ID: "call-1", Type: "function",
							Function: entity.ToolCallFunction{Name: "loop_tool", Arguments: `{}`},
						}},
						FinishReason: "tool_calls",
						Usage:        entity.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
					}
				}
				return resps
			}(),
			callCount: &callCount,
		},
		&mockTool{result: entity.ToolResult{ToolName: "loop_tool", Output: map[string]any{"done": true}}},
		nil,
		nil,
	nil,
	)

	result, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:   "run-1",
		Message: "Loop tools",
		Config:  entity.LLMConfig{Model: "gpt-4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Should stop at maxToolRounds = 10 (tool rounds), meaning 11 LLM calls (1 initial + 10 tool rounds)
	if len(result.ToolResults) != 10 {
		t.Errorf("expected 10 tool results (max rounds), got %d", len(result.ToolResults))
	}
}

func TestExecuteStep_WithSystemPrompt(t *testing.T) {
	uc := agent.New(
		&mockLLM{response: entity.LLMResponse{Content: "Understood!", FinishReason: "stop", Usage: entity.Usage{PromptTokens: 10, CompletionTokens: 3, TotalTokens: 13}}},
		&mockTool{},
		nil,
		nil,
	nil,
	)

	result, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:        "run-1",
		SystemPrompt: "You are a helpful assistant.",
		Message:      "Hello",
		Config:       entity.LLMConfig{Model: "gpt-4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Messages should be: system + user + assistant
	if len(result.Messages) != 3 {
		t.Fatalf("expected 3 messages (system+user+assistant), got %d", len(result.Messages))
	}
	if result.Messages[0].Role != entity.RoleSystem {
		t.Errorf("expected first message to be system role, got %v", result.Messages[0].Role)
	}
	if result.Messages[0].Content != "You are a helpful assistant." {
		t.Errorf("expected system prompt content, got %q", result.Messages[0].Content)
	}
	if result.Messages[1].Role != entity.RoleUser {
		t.Errorf("expected second message to be user role, got %v", result.Messages[1].Role)
	}
	if result.Messages[2].Role != entity.RoleAssistant {
		t.Errorf("expected third message to be assistant role, got %v", result.Messages[2].Role)
	}
}

func TestExecuteStep_WithHistoryDoesNotReinjectSystemPrompt(t *testing.T) {
	uc := agent.New(
		&mockLLM{response: entity.LLMResponse{Content: "Continuing!", FinishReason: "stop", Usage: entity.Usage{PromptTokens: 5, CompletionTokens: 3, TotalTokens: 8}}},
		&mockTool{},
		nil,
		nil,
	nil,
	)

	result, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:        "run-1",
		SystemPrompt: "You are a helpful assistant.",
		Message:      "",
		History: []entity.Message{
			{Role: entity.RoleSystem, Content: "You are a helpful assistant."},
			{Role: entity.RoleUser, Content: "Hi"},
			{Role: entity.RoleAssistant, Content: "Hello!"},
		},
		Config: entity.LLMConfig{Model: "gpt-4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// System prompt should NOT be re-injected when history exists
	if len(result.Messages) != 1 {
		t.Fatalf("expected 1 new message (assistant), got %d", len(result.Messages))
	}
	if result.Messages[0].Role != entity.RoleAssistant {
		t.Errorf("expected assistant message, got %v", result.Messages[0].Role)
	}
}

func TestPrep_InvalidToolDefinition(t *testing.T) {
	uc := agent.New(
		&mockLLM{},
		&mockTool{},
		nil,
		nil,
	nil,
	)

	_, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:   "run-1",
		Message: "test",
		Tools: []entity.ToolDef{
			{Type: "function", Function: entity.ToolFuncDef{Name: ""}}, // missing name
		},
		Config: entity.LLMConfig{Model: "gpt-4"},
	})
	if err == nil {
		t.Fatal("expected error for invalid tool definition")
	}

	var agentErr *entity.AgentError
	if !errors.As(err, &agentErr) {
		t.Fatalf("expected entity.AgentError, got %T", err)
	}
	if agentErr.Code != entity.ErrorCodeValidation {
		t.Errorf("expected ErrorCodeValidation, got %s", agentErr.Code)
	}
}

func TestPrep_ToolMissingType(t *testing.T) {
	uc := agent.New(
		&mockLLM{},
		&mockTool{},
		nil,
		nil,
	nil,
	)

	_, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:   "run-1",
		Message: "test",
		Tools: []entity.ToolDef{
			{Function: entity.ToolFuncDef{Name: "valid_name"}}, // missing type
		},
		Config: entity.LLMConfig{Model: "gpt-4"},
	})
	if err == nil {
		t.Fatal("expected error for tool missing type")
	}

	var agentErr *entity.AgentError
	if !errors.As(err, &agentErr) {
		t.Fatalf("expected entity.AgentError, got %T", err)
	}
	if agentErr.Code != entity.ErrorCodeValidation {
		t.Errorf("expected ErrorCodeValidation, got %s", agentErr.Code)
	}
}

func TestExecuteStep_LLMError_Timeout(t *testing.T) {
	uc := agent.New(
		&mockLLM{err: errors.New("context deadline exceeded")},
		&mockTool{},
		nil,
		nil,
	nil,
	)

	_, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:   "run-1",
		Message: "Hello",
		Config:  entity.LLMConfig{Model: "gpt-4"},
	})
	if err == nil {
		t.Fatal("expected error from LLM timeout")
	}

	var agentErr *entity.AgentError
	if !errors.As(err, &agentErr) {
		t.Fatalf("expected entity.AgentError, got %T", err)
	}
	if agentErr.Code != entity.ErrorCodeLLMTimeout {
		t.Errorf("expected ErrorCodeLLMTimeout, got %s", agentErr.Code)
	}
	if !agentErr.Retryable {
		t.Errorf("timeout error should be retryable")
	}
}

func TestExecuteStep_LLMError_RateLimit(t *testing.T) {
	uc := agent.New(
		&mockLLM{err: errors.New("429 too many requests")},
		&mockTool{},
		nil,
		nil,
	nil,
	)

	_, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:   "run-1",
		Message: "Hello",
		Config:  entity.LLMConfig{Model: "gpt-4"},
	})
	if err == nil {
		t.Fatal("expected error from LLM rate limit")
	}

	var agentErr *entity.AgentError
	if !errors.As(err, &agentErr) {
		t.Fatalf("expected entity.AgentError, got %T", err)
	}
	if agentErr.Code != entity.ErrorCodeLLMRateLimit {
		t.Errorf("expected ErrorCodeLLMRateLimit, got %s", agentErr.Code)
	}
}

func TestExecuteStep_LLMError_ContentFilter(t *testing.T) {
	uc := agent.New(
		&mockLLM{err: errors.New("content filter triggered")},
		&mockTool{},
		nil,
		nil,
	nil,
	)

	_, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:   "run-1",
		Message: "Hello",
		Config:  entity.LLMConfig{Model: "gpt-4"},
	})
	if err == nil {
		t.Fatal("expected error from content filter")
	}

	var agentErr *entity.AgentError
	if !errors.As(err, &agentErr) {
		t.Fatalf("expected entity.AgentError, got %T", err)
	}
	if agentErr.Code != entity.ErrorCodeLLMContentFilter {
		t.Errorf("expected ErrorCodeLLMContentFilter, got %s", agentErr.Code)
	}
	if agentErr.Retryable {
		t.Errorf("content filter error should not be retryable")
	}
}

func TestExecuteStep_LLMError_ContextLength(t *testing.T) {
	uc := agent.New(
		&mockLLM{err: errors.New("maximum context length exceeded")},
		&mockTool{},
		nil,
		nil,
	nil,
	)

	_, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:   "run-1",
		Message: "Hello",
		Config:  entity.LLMConfig{Model: "gpt-4"},
	})
	if err == nil {
		t.Fatal("expected error from context length")
	}

	var agentErr *entity.AgentError
	if !errors.As(err, &agentErr) {
		t.Fatalf("expected entity.AgentError, got %T", err)
	}
	if agentErr.Code != entity.ErrorCodeContextLength {
		t.Errorf("expected ErrorCodeContextLength, got %s", agentErr.Code)
	}
	if agentErr.Retryable {
		t.Errorf("context length error should not be retryable")
	}
}

func TestExecuteStep_LLMError_Default(t *testing.T) {
	uc := agent.New(
		&mockLLM{err: errors.New("some unexpected API error")},
		&mockTool{},
		nil,
		nil,
	nil,
	)

	_, err := uc.ExecuteStep(context.Background(), agent.StepRequest{
		RunID:   "run-1",
		Message: "Hello",
		Config:  entity.LLMConfig{Model: "gpt-4"},
	})
	if err == nil {
		t.Fatal("expected error from LLM")
	}

	var agentErr *entity.AgentError
	if !errors.As(err, &agentErr) {
		t.Fatalf("expected entity.AgentError, got %T", err)
	}
	if agentErr.Code != entity.ErrorCodeLLM {
		t.Errorf("expected ErrorCodeLLM, got %s", agentErr.Code)
	}
	if !agentErr.Retryable {
		t.Errorf("default LLM error should be retryable")
	}
}

// -- mocks --

type mockLLM struct {
	response  entity.LLMResponse
	responses []entity.LLMResponse
	callCount *int
	err       error
}

func (m *mockLLM) Chat(_ context.Context, _ entity.LLMRequest) (entity.LLMResponse, error) {
	if m.err != nil {
		return entity.LLMResponse{}, m.err
	}
	if m.callCount != nil {
		*m.callCount++
	}
	if m.responses != nil {
		idx := 0
		if m.callCount != nil {
			idx = *m.callCount - 1
		}
		if idx < len(m.responses) {
			return m.responses[idx], nil
		}
		return m.responses[len(m.responses)-1], nil
	}
	return m.response, nil
}

type mockTool struct {
	result entity.ToolResult
	err    error
}

func (m *mockTool) Execute(_ context.Context, req entity.ToolRequest) (entity.ToolResult, error) {
	if m.err != nil {
		return entity.ToolResult{}, m.err
	}
	res := m.result
	res.RunID = req.RunID
	res.ToolCallID = req.ToolCallID
	res.ToolName = req.ToolName
	return res, nil
}
