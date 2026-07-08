package agent_test

import (
	"context"
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agent"
)

const (
	testHi              = "Hi"
	testHello           = "Hello!"
	testSystemPrompt    = "You are a helpful assistant."
	testNewSystemPrompt = "New system prompt"
	testGpt4o           = "gpt-4o"
	testNewPrompt       = "New prompt"
	testGpt5            = "gpt-5"
	testNew             = "new"
	testSearch          = "search"
)

var (
	errTestToolCrashed       = errors.New("tool crashed")
	errTestContextDeadline   = errors.New("context deadline exceeded")
	errTestRateLimit429      = errors.New("429 too many requests")
	errTestContentFilter     = errors.New("content filter triggered")
	errTestMaxContextLength  = errors.New("maximum context length exceeded")
	errTestAPIError          = errors.New("some unexpected API error")
	errTestDB                = errors.New("db error")
	errTestConnectionError   = errors.New("connection error")
	errTestUpdateFailed      = errors.New("update failed")
	errTestRateLimitExceeded = errors.New("rate limit exceeded")
	errTestToolExecFailed    = errors.New("tool execution failed")
)

func TestExecuteStep_EmptyMessage(t *testing.T) {
	t.Parallel()

	uc := agent.New(
		&mockLLM{response: entity.LLMResponse{Content: "Continuing!", FinishReason: "stop", Usage: entity.Usage{PromptTokens: 5, CompletionTokens: 3, TotalTokens: 8}}},
		&mockTool{},
		nil,
		nil,
		nil,
		nil,
	)

	result, err := uc.ExecuteStep(context.Background(), &agent.StepRequest{
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
	t.Parallel()

	uc := agent.New(
		&mockLLM{response: entity.LLMResponse{Content: "Hello!", FinishReason: "stop", Usage: entity.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15}}},
		&mockTool{},
		nil,
		nil,
		nil,
		nil,
	)

	result, err := uc.ExecuteStep(context.Background(), &agent.StepRequest{
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

	if result.Messages[0].Role != entity.RoleUser || result.Messages[0].Content != testHi {
		t.Errorf("expected user message 'Hi', got %v", result.Messages[0])
	}

	if result.Messages[1].Role != entity.RoleAssistant || result.Messages[1].Content != testHello {
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
	t.Parallel()

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
		nil,
	)

	result, err := uc.ExecuteStep(context.Background(), &agent.StepRequest{
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
	t.Parallel()

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
		&mockTool{err: errTestToolCrashed},
		nil,
		nil,
		nil,
		nil,
	)

	result, err := uc.ExecuteStep(context.Background(), &agent.StepRequest{
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
	t.Parallel()

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
		nil,
	)

	result, err := uc.ExecuteStep(context.Background(), &agent.StepRequest{
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
	t.Parallel()

	uc := agent.New(
		&mockLLM{response: entity.LLMResponse{Content: "Understood!", FinishReason: "stop", Usage: entity.Usage{PromptTokens: 10, CompletionTokens: 3, TotalTokens: 13}}},
		&mockTool{},
		nil,
		nil,
		nil,
		nil,
	)

	result, err := uc.ExecuteStep(context.Background(), &agent.StepRequest{
		RunID:        "run-1",
		SystemPrompt: testSystemPrompt,
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

	if result.Messages[0].Content != testSystemPrompt {
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
	t.Parallel()

	uc := agent.New(
		&mockLLM{response: entity.LLMResponse{Content: "Continuing!", FinishReason: "stop", Usage: entity.Usage{PromptTokens: 5, CompletionTokens: 3, TotalTokens: 8}}},
		&mockTool{},
		nil,
		nil,
		nil,
		nil,
	)

	result, err := uc.ExecuteStep(context.Background(), &agent.StepRequest{
		RunID:        "run-1",
		SystemPrompt: testSystemPrompt,
		Message:      "",
		History: []entity.Message{
			{Role: entity.RoleSystem, Content: testSystemPrompt},
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
	t.Parallel()

	// Tool validation is now handled by the orchestration layer (PrepareActivity),
	// not the usecase. The usecase passes through tools without validation.
	uc := agent.New(
		&mockLLM{response: entity.LLMResponse{Content: "ok", FinishReason: "stop", Usage: entity.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2}}},
		&mockTool{},
		nil,
		nil,
		nil,
		nil,
	)

	result, err := uc.ExecuteStep(context.Background(), &agent.StepRequest{
		RunID:   "run-1",
		Message: "test",
		Tools: []entity.ToolDef{
			{Type: "function", Function: entity.ToolFuncDef{Name: ""}}, // missing name — passes through
		},
		Config: entity.LLMConfig{Model: "gpt-4"},
	})
	if err != nil {
		t.Fatalf("tool validation is not a usecase concern; unexpected error: %v", err)
	}

	if len(result.Messages) == 0 {
		t.Fatal("expected messages from successful execution")
	}
}

func TestPrep_ToolMissingType(t *testing.T) {
	t.Parallel()

	// Tool validation is now handled by the orchestration layer (PrepareActivity),
	// not the usecase. The usecase passes through tools without validation.
	uc := agent.New(
		&mockLLM{response: entity.LLMResponse{Content: "ok", FinishReason: "stop", Usage: entity.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2}}},
		&mockTool{},
		nil,
		nil,
		nil,
		nil,
	)

	result, err := uc.ExecuteStep(context.Background(), &agent.StepRequest{
		RunID:   "run-1",
		Message: "test",
		Tools: []entity.ToolDef{
			{Function: entity.ToolFuncDef{Name: "valid_name"}}, // missing type — passes through
		},
		Config: entity.LLMConfig{Model: "gpt-4"},
	})
	if err != nil {
		t.Fatalf("tool validation is not a usecase concern; unexpected error: %v", err)
	}

	if len(result.Messages) == 0 {
		t.Fatal("expected messages from successful execution")
	}
}

type llmErrorTest struct {
	name      string
	err       error
	msg       string
	code      entity.ErrorCode
	retryable bool
}

func testLLMError(t *testing.T, tc llmErrorTest) {
	t.Helper()

	uc := agent.New(
		&mockLLM{err: tc.err},
		&mockTool{},
		nil, nil, nil, nil,
	)

	_, err := uc.ExecuteStep(context.Background(), &agent.StepRequest{
		RunID:   "run-1",
		Message: "Hello",
		Config:  entity.LLMConfig{Model: "gpt-4"},
	})
	if err == nil {
		t.Fatalf("expected error from %s", tc.name)
	}

	var agentErr *entity.AgentError
	if !errors.As(err, &agentErr) {
		t.Fatalf("expected entity.AgentError, got %T", err)
	}

	if agentErr.Code != tc.code {
		t.Errorf("%s: expected %s, got %s", tc.name, tc.code, agentErr.Code)
	}

	if agentErr.Retryable != tc.retryable {
		t.Errorf("%s: retryable=%v, want %v", tc.name, agentErr.Retryable, tc.retryable)
	}
}

func TestExecuteStep_LLMError_Timeout(t *testing.T) {
	t.Parallel()
	testLLMError(t, llmErrorTest{
		name: "timeout", err: errTestContextDeadline, msg: "context deadline",
		code: entity.ErrorCodeLLMTimeout, retryable: true,
	})
}

func TestExecuteStep_LLMError_RateLimit(t *testing.T) {
	t.Parallel()
	testLLMError(t, llmErrorTest{
		name: "rate_limit", err: errTestRateLimit429, msg: "429",
		code: entity.ErrorCodeLLMRateLimit, retryable: true,
	})
}

func TestExecuteStep_LLMError_ContentFilter(t *testing.T) {
	t.Parallel()
	testLLMError(t, llmErrorTest{
		name: "content_filter", err: errTestContentFilter, msg: "content filter",
		code: entity.ErrorCodeLLMContentFilter, retryable: false,
	})
}

func TestExecuteStep_LLMError_ContextLength(t *testing.T) {
	t.Parallel()
	testLLMError(t, llmErrorTest{
		name: "context_length", err: errTestMaxContextLength, msg: "context length",
		code: entity.ErrorCodeContextLength, retryable: false,
	})
}

func TestExecuteStep_LLMError_Default(t *testing.T) {
	t.Parallel()
	testLLMError(t, llmErrorTest{
		name: "default", err: errTestAPIError, msg: "API error",
		code: entity.ErrorCodeLLM, retryable: true,
	})
}

// -- mocks --

type mockLLM struct {
	response  entity.LLMResponse
	responses []entity.LLMResponse
	callCount *int
	err       error
}

func (m *mockLLM) Chat(_ context.Context, _ *entity.LLMRequest) (entity.LLMResponse, error) {
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

func (m *mockTool) Execute(_ context.Context, req *entity.ToolRequest) (entity.ToolResult, error) {
	if m.err != nil {
		return entity.ToolResult{}, m.err
	}

	res := m.result
	res.RunID = req.RunID
	res.ToolCallID = req.ToolCallID
	res.ToolName = req.ToolName

	return res, nil
}

// -- mockAgentRepo --

type mockAgentRepo struct {
	getFunc           func(ctx context.Context, agentID string) (entity.AgentRecord, bool, error)
	createVersionFunc func(ctx context.Context, record *entity.AgentVersionRecord) error
	updateFunc        func(ctx context.Context, agentID string, req entity.UpdateAgentRequest) (entity.AgentRecord, error)
}

func (m *mockAgentRepo) Create(_ context.Context, _ *entity.CreateAgentRequest) (entity.AgentRecord, error) {
	panic("unexpected call to Create")
}

func (m *mockAgentRepo) Get(ctx context.Context, agentID string) (entity.AgentRecord, bool, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, agentID)
	}

	panic("unexpected call to Get")
}

func (m *mockAgentRepo) Update(ctx context.Context, agentID string, req entity.UpdateAgentRequest) (entity.AgentRecord, error) {
	if m.updateFunc != nil {
		return m.updateFunc(ctx, agentID, req)
	}

	panic("unexpected call to Update")
}

func (m *mockAgentRepo) Delete(_ context.Context, _ string) error {
	panic("unexpected call to Delete")
}

func (m *mockAgentRepo) ListByAccount(_ context.Context, _ string) ([]entity.AgentRecord, error) {
	panic("unexpected call to ListByAccount")
}

func (m *mockAgentRepo) CreateVersion(ctx context.Context, record *entity.AgentVersionRecord) error {
	if m.createVersionFunc != nil {
		return m.createVersionFunc(ctx, record)
	}

	panic("unexpected call to CreateVersion")
}

func (m *mockAgentRepo) GetVersion(_ context.Context, _ string) (entity.AgentVersionRecord, bool, error) {
	panic("unexpected call to GetVersion")
}

func (m *mockAgentRepo) ListVersions(_ context.Context, _ string) ([]entity.AgentVersionRecord, error) {
	panic("unexpected call to ListVersions")
}

// -- UpdateAgent tests (auto-versioning) --

func existingAgentRecord() entity.AgentRecord {
	return entity.AgentRecord{
		AgentID:      "agent-1",
		AccountID:    "account-1",
		Name:         "Original",
		SystemPrompt: "Original prompt",
		ModelRef:     "gpt-4",
		Config:       entity.LLMConfig{Model: "gpt-4", Temperature: 0.7},
	}
}

func TestUpdateAgent_AutoVersioning(t *testing.T) {
	t.Parallel()

	t.Run("only name change does not create version", testAutoVersionNameOnly)
	t.Run("system prompt change creates version with new value", testAutoVersionSystemPrompt)
	t.Run("model ref change creates version", testAutoVersionModelRef)
	t.Run("config change creates version", testAutoVersionConfigChange)
	t.Run("same config values set does create version", testAutoVersionSameValue)
	t.Run("multiple fields changed creates version with combined snapshot", testAutoVersionMultipleFields)
}

func testAutoVersionNameOnly(t *testing.T) {
	t.Parallel()

	var versionCreated bool

	mock := &mockAgentRepo{
		getFunc: func(_ context.Context, _ string) (entity.AgentRecord, bool, error) {
			return existingAgentRecord(), true, nil
		},
		createVersionFunc: func(_ context.Context, _ *entity.AgentVersionRecord) error {
			versionCreated = true

			return nil
		},
		updateFunc: func(_ context.Context, _ string, req entity.UpdateAgentRequest) (entity.AgentRecord, error) {
			if req.CurrentVersion != nil {
				t.Error("expected no version bump for non-config change")
			}

			updated := existingAgentRecord()
			updated.Name = *req.Name

			return updated, nil
		},
	}
	uc := agent.New(nil, nil, nil, nil, nil, mock)
	newName := "Updated Name"

	_, err := uc.UpdateAgent(context.Background(), "agent-1", entity.UpdateAgentRequest{Name: &newName})
	if err != nil {
		t.Fatal(err)
	}

	if versionCreated {
		t.Error("version should not be created when only name changes")
	}
}

func testAutoVersionSystemPrompt(t *testing.T) {
	t.Parallel()

	var (
		versionCreated bool
		versionRecord  entity.AgentVersionRecord
	)

	mock := &mockAgentRepo{
		getFunc: func(_ context.Context, _ string) (entity.AgentRecord, bool, error) {
			return existingAgentRecord(), true, nil
		},
		createVersionFunc: func(_ context.Context, r *entity.AgentVersionRecord) error {
			versionCreated = true
			versionRecord = *r

			return nil
		},
		updateFunc: func(_ context.Context, _ string, req entity.UpdateAgentRequest) (entity.AgentRecord, error) {
			if req.CurrentVersion == nil {
				t.Error("expected CurrentVersion to be set when config changed")
			}

			updated := existingAgentRecord()
			updated.SystemPrompt = *req.SystemPrompt
			updated.CurrentVersion = *req.CurrentVersion

			return updated, nil
		},
	}
	uc := agent.New(nil, nil, nil, nil, nil, mock)
	newPrompt := testNewSystemPrompt

	_, err := uc.UpdateAgent(context.Background(), "agent-1", entity.UpdateAgentRequest{SystemPrompt: &newPrompt})
	if err != nil {
		t.Fatal(err)
	}

	if !versionCreated {
		t.Fatal("version should be created when system prompt changes")
	}

	if versionRecord.AgentID != "agent-1" {
		t.Errorf("expected agent-1, got %s", versionRecord.AgentID)
	}

	if versionRecord.SystemPrompt != "New system prompt" {
		t.Errorf("expected new prompt in version, got %s", versionRecord.SystemPrompt)
	}

	if versionRecord.ModelRef != "gpt-4" {
		t.Errorf("expected unchanged model_ref in version, got %s", versionRecord.ModelRef)
	}

	if versionRecord.ChangeDescription != "auto-saved on config update" {
		t.Errorf("expected auto-save description, got %s", versionRecord.ChangeDescription)
	}
}

func testAutoVersionModelRef(t *testing.T) {
	t.Parallel()

	var versionCreated bool

	mock := &mockAgentRepo{
		getFunc: func(_ context.Context, _ string) (entity.AgentRecord, bool, error) {
			return existingAgentRecord(), true, nil
		},
		createVersionFunc: func(_ context.Context, r *entity.AgentVersionRecord) error {
			versionCreated = true

			if r.ModelRef != testGpt4o {
				t.Errorf("expected new model_ref gpt-4o, got %s", r.ModelRef)
			}

			return nil
		},
		updateFunc: func(_ context.Context, _ string, _ entity.UpdateAgentRequest) (entity.AgentRecord, error) {
			return existingAgentRecord(), nil
		},
	}
	uc := agent.New(nil, nil, nil, nil, nil, mock)
	newModel := "gpt-4o"

	_, err := uc.UpdateAgent(context.Background(), "agent-1", entity.UpdateAgentRequest{ModelRef: &newModel})
	if err != nil {
		t.Fatal(err)
	}

	if !versionCreated {
		t.Error("version should be created when model_ref changes")
	}
}

func testAutoVersionConfigChange(t *testing.T) {
	t.Parallel()

	var versionCreated bool

	mock := &mockAgentRepo{
		getFunc: func(_ context.Context, _ string) (entity.AgentRecord, bool, error) {
			return existingAgentRecord(), true, nil
		},
		createVersionFunc: func(_ context.Context, _ *entity.AgentVersionRecord) error {
			versionCreated = true

			return nil
		},
		updateFunc: func(_ context.Context, _ string, _ entity.UpdateAgentRequest) (entity.AgentRecord, error) {
			return existingAgentRecord(), nil
		},
	}
	uc := agent.New(nil, nil, nil, nil, nil, mock)
	newConfig := entity.LLMConfig{Model: "gpt-4", Temperature: 0.9}

	_, err := uc.UpdateAgent(context.Background(), "agent-1", entity.UpdateAgentRequest{Config: &newConfig})
	if err != nil {
		t.Fatal(err)
	}

	if !versionCreated {
		t.Error("version should be created when config changes")
	}
}

func testAutoVersionSameValue(t *testing.T) {
	t.Parallel()

	var versionCreated bool

	mock := &mockAgentRepo{
		getFunc: func(_ context.Context, _ string) (entity.AgentRecord, bool, error) {
			return existingAgentRecord(), true, nil
		},
		createVersionFunc: func(_ context.Context, _ *entity.AgentVersionRecord) error {
			versionCreated = true

			return nil
		},
		updateFunc: func(_ context.Context, _ string, _ entity.UpdateAgentRequest) (entity.AgentRecord, error) {
			return existingAgentRecord(), nil
		},
	}
	uc := agent.New(nil, nil, nil, nil, nil, mock)
	// Same value but non-nil pointer — considered a change
	samePrompt := "Original prompt"

	_, err := uc.UpdateAgent(context.Background(), "agent-1", entity.UpdateAgentRequest{SystemPrompt: &samePrompt})
	if err != nil {
		t.Fatal(err)
	}

	if versionCreated {
		t.Error("version should NOT be created when value is unchanged")
	}
}

func testAutoVersionMultipleFields(t *testing.T) {
	t.Parallel()

	var versionRecord entity.AgentVersionRecord

	mock := &mockAgentRepo{
		getFunc: func(_ context.Context, _ string) (entity.AgentRecord, bool, error) {
			return existingAgentRecord(), true, nil
		},
		createVersionFunc: func(_ context.Context, r *entity.AgentVersionRecord) error {
			versionRecord = *r

			return nil
		},
		updateFunc: func(_ context.Context, _ string, _ entity.UpdateAgentRequest) (entity.AgentRecord, error) {
			return existingAgentRecord(), nil
		},
	}
	uc := agent.New(nil, nil, nil, nil, nil, mock)
	newPrompt := testNewPrompt
	newModel := testGpt5

	_, err := uc.UpdateAgent(context.Background(), "agent-1", entity.UpdateAgentRequest{
		SystemPrompt: &newPrompt,
		ModelRef:     &newModel,
	})
	if err != nil {
		t.Fatal(err)
	}

	if versionRecord.SystemPrompt != "New prompt" {
		t.Errorf("expected new prompt in version, got %s", versionRecord.SystemPrompt)
	}

	if versionRecord.ModelRef != "gpt-5" {
		t.Errorf("expected new model_ref in version, got %s", versionRecord.ModelRef)
	}
}

func TestUpdateAgent_RepoIsNil(t *testing.T) {
	t.Parallel()

	uc := agent.New(nil, nil, nil, nil, nil, nil)

	_, err := uc.UpdateAgent(context.Background(), "agent-1", entity.UpdateAgentRequest{})
	if err == nil {
		t.Fatal("expected error when agent repo is nil")
	}
}

func TestUpdateAgent_NotFound(t *testing.T) {
	t.Parallel()

	mock := &mockAgentRepo{
		getFunc: func(_ context.Context, _ string) (entity.AgentRecord, bool, error) {
			return entity.AgentRecord{}, false, nil
		},
	}
	uc := agent.New(nil, nil, nil, nil, nil, mock)

	_, err := uc.UpdateAgent(context.Background(), "missing", entity.UpdateAgentRequest{})
	if err == nil {
		t.Fatal("expected error when agent not found")
	}
}

func TestUpdateAgent_CreateVersionFails(t *testing.T) {
	t.Parallel()

	mock := &mockAgentRepo{
		getFunc: func(_ context.Context, _ string) (entity.AgentRecord, bool, error) {
			return entity.AgentRecord{
				AgentID:      "agent-1",
				SystemPrompt: "old",
				ModelRef:     "gpt-4",
			}, true, nil
		},
		createVersionFunc: func(_ context.Context, _ *entity.AgentVersionRecord) error {
			return errTestDB
		},
	}
	uc := agent.New(nil, nil, nil, nil, nil, mock)
	newPrompt := testNew

	_, err := uc.UpdateAgent(context.Background(), "agent-1", entity.UpdateAgentRequest{SystemPrompt: &newPrompt})
	if err == nil {
		t.Fatal("expected error when CreateVersion fails")
	}
}

func TestUpdateAgent_GetFails(t *testing.T) {
	t.Parallel()

	mock := &mockAgentRepo{
		getFunc: func(_ context.Context, _ string) (entity.AgentRecord, bool, error) {
			return entity.AgentRecord{}, false, errTestConnectionError
		},
	}
	uc := agent.New(nil, nil, nil, nil, nil, mock)

	_, err := uc.UpdateAgent(context.Background(), "agent-1", entity.UpdateAgentRequest{})
	if err == nil {
		t.Fatal("expected error when Get fails")
	}
}

func TestUpdateAgent_UpdateFailsAfterVersionCreated(t *testing.T) {
	t.Parallel()

	var versionCreated bool

	mock := &mockAgentRepo{
		getFunc: func(_ context.Context, _ string) (entity.AgentRecord, bool, error) {
			return entity.AgentRecord{
				AgentID:      "agent-1",
				SystemPrompt: "old",
				ModelRef:     "gpt-4",
			}, true, nil
		},
		createVersionFunc: func(_ context.Context, _ *entity.AgentVersionRecord) error {
			versionCreated = true

			return nil
		},
		updateFunc: func(_ context.Context, _ string, _ entity.UpdateAgentRequest) (entity.AgentRecord, error) {
			return entity.AgentRecord{}, errTestUpdateFailed
		},
	}
	uc := agent.New(nil, nil, nil, nil, nil, mock)
	newPrompt := testNew

	_, err := uc.UpdateAgent(context.Background(), "agent-1", entity.UpdateAgentRequest{SystemPrompt: &newPrompt})
	if err == nil {
		t.Fatal("expected error when Update fails")
	}

	if !versionCreated {
		t.Error("version should have been created before the failed update")
	}
}

// --- Prep tests ---

func TestPrep_WithSystemPrompt(t *testing.T) {
	t.Parallel()

	uc := agent.New(nil, nil, nil, nil, nil, nil)

	result, err := uc.Prep(context.Background(), &agent.PrepRequest{
		SystemPrompt: testSystemPrompt,
		UserMessage:  "Hello",
		Config:       entity.LLMConfig{Model: "gpt-4"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Messages) != 2 {
		t.Fatalf("expected 2 messages (system+user), got %d", len(result.Messages))
	}

	if result.Messages[0].Role != entity.RoleSystem {
		t.Errorf("expected system role, got %v", result.Messages[0].Role)
	}

	if result.Messages[0].Content != testSystemPrompt {
		t.Errorf("expected system prompt, got %q", result.Messages[0].Content)
	}

	if result.Messages[1].Role != entity.RoleUser || result.Messages[1].Content != "Hello" {
		t.Errorf("expected user message, got %v", result.Messages[1])
	}
}

func TestPrep_WithHistoryNoSystemPrompt(t *testing.T) {
	t.Parallel()

	uc := agent.New(nil, nil, nil, nil, nil, nil)

	result, err := uc.Prep(context.Background(), &agent.PrepRequest{
		SystemPrompt: testSystemPrompt,
		UserMessage:  "Continue",
		History: []entity.Message{
			{Role: entity.RoleUser, Content: "Hi"},
			{Role: entity.RoleAssistant, Content: "Hello!"},
		},
		Config: entity.LLMConfig{Model: "gpt-4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// System prompt should NOT be re-injected when history exists
	if len(result.Messages) != 3 {
		t.Fatalf("expected 3 messages (history+user), got %d", len(result.Messages))
	}

	if result.Messages[0].Role != entity.RoleUser || result.Messages[0].Content != testHi {
		t.Errorf("expected first message from history, got %v", result.Messages[0])
	}
}

func TestPrep_EmptyMessage(t *testing.T) {
	t.Parallel()

	uc := agent.New(nil, nil, nil, nil, nil, nil)

	result, err := uc.Prep(context.Background(), &agent.PrepRequest{
		SystemPrompt: testSystemPrompt,
		Config:       entity.LLMConfig{Model: "gpt-4"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Messages) != 1 {
		t.Fatalf("expected 1 message (system only), got %d", len(result.Messages))
	}

	if result.Messages[0].Role != entity.RoleSystem {
		t.Errorf("expected system role, got %v", result.Messages[0].Role)
	}
}

func TestPrep_WithTools(t *testing.T) {
	t.Parallel()

	uc := agent.New(nil, nil, nil, nil, nil, nil)
	tools := []entity.ToolDef{
		{Type: "function", Function: entity.ToolFuncDef{Name: "search", Description: "Search the web"}},
	}

	result, err := uc.Prep(context.Background(), &agent.PrepRequest{
		UserMessage: "Search something",
		Tools:       tools,
		Config:      entity.LLMConfig{Model: "gpt-4"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Tools) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(result.Tools))
	}

	if result.Tools[0].Function.Name != testSearch {
		t.Errorf("expected search tool, got %s", result.Tools[0].Function.Name)
	}
}

// --- LLMStep tests ---

func TestLLMStep_TextResponse(t *testing.T) {
	t.Parallel()

	uc := agent.New(
		&mockLLM{response: entity.LLMResponse{
			Content: "Hello!", FinishReason: "stop",
			Usage: entity.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
		}},
		&mockTool{},
		nil, nil, nil, nil,
	)

	result, err := uc.LLMStep(
		context.Background(), "run-1",
		[]entity.Message{{Role: entity.RoleUser, Content: "Hi"}},
		[]entity.ToolDef{},
		entity.LLMConfig{Model: "gpt-4"},
	)
	if err != nil {
		t.Fatal(err)
	}

	if result.AssistantMsg.Role != entity.RoleAssistant {
		t.Errorf("expected assistant role, got %v", result.AssistantMsg.Role)
	}

	if result.AssistantMsg.Content != "Hello!" {
		t.Errorf("expected 'Hello!', got %q", result.AssistantMsg.Content)
	}

	if len(result.ToolCalls) != 0 {
		t.Errorf("expected no tool calls, got %d", len(result.ToolCalls))
	}

	if result.Usage.TotalTokens != 15 {
		t.Errorf("expected 15 total tokens, got %d", result.Usage.TotalTokens)
	}
}

func TestLLMStep_ToolCallResponse(t *testing.T) {
	t.Parallel()

	uc := agent.New(
		&mockLLM{response: entity.LLMResponse{
			ToolCalls: []entity.ToolCall{{
				ID: "call-1", Type: "function",
				Function: entity.ToolCallFunction{Name: "search", Arguments: `{"q":"weather"}`},
			}},
			FinishReason: "tool_calls",
			Usage:        entity.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
		}},
		&mockTool{},
		nil, nil, nil, nil,
	)

	result, err := uc.LLMStep(
		context.Background(), "run-1",
		[]entity.Message{{Role: entity.RoleUser, Content: "Weather?"}},
		[]entity.ToolDef{{Type: "function", Function: entity.ToolFuncDef{Name: "search"}}},
		entity.LLMConfig{Model: "gpt-4"},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.ToolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(result.ToolCalls))
	}

	if result.ToolCalls[0].Function.Name != "search" {
		t.Errorf("expected search tool, got %s", result.ToolCalls[0].Function.Name)
	}

	if result.AssistantMsg.Role != entity.RoleAssistant {
		t.Errorf("expected assistant role, got %v", result.AssistantMsg.Role)
	}
}

func TestLLMStep_LLMError(t *testing.T) {
	t.Parallel()

	uc := agent.New(
		&mockLLM{err: errTestRateLimitExceeded},
		&mockTool{},
		nil, nil, nil, nil,
	)

	_, err := uc.LLMStep(
		context.Background(), "run-1",
		[]entity.Message{{Role: entity.RoleUser, Content: "Hi"}},
		nil,
		entity.LLMConfig{Model: "gpt-4"},
	)
	if err == nil {
		t.Fatal("expected error")
	}

	var agentErr *entity.AgentError
	if !errors.As(err, &agentErr) {
		t.Fatalf("expected AgentError, got %T", err)
	}

	if agentErr.Code != entity.ErrorCodeLLMRateLimit {
		t.Errorf("expected rate limit code, got %s", agentErr.Code)
	}
}

// --- ExecTool tests ---

func TestExecTool_Success(t *testing.T) {
	t.Parallel()

	uc := agent.New(
		&mockLLM{},
		&mockTool{result: entity.ToolResult{
			ToolName: "search", Output: map[string]any{"temp": "72F"},
		}},
		nil, nil, nil, nil,
	)

	result, err := uc.ExecTool(context.Background(), "run-1", entity.ToolCall{
		ID: "call-1", Type: "function",
		Function: entity.ToolCallFunction{Name: "search", Arguments: `{"q":"weather"}`},
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.Output == "" {
		t.Error("expected non-empty output")
	}

	if result.IsError {
		t.Error("expected no error")
	}

	if result.ToolMsg.Role != entity.RoleTool {
		t.Errorf("expected tool role, got %v", result.ToolMsg.Role)
	}
}

func TestExecTool_Error(t *testing.T) {
	t.Parallel()

	uc := agent.New(
		&mockLLM{},
		&mockTool{err: errTestToolExecFailed},
		nil, nil, nil, nil,
	)

	result, err := uc.ExecTool(context.Background(), "run-1", entity.ToolCall{
		ID: "call-1", Type: "function",
		Function: entity.ToolCallFunction{Name: "fail_tool", Arguments: `{}`},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !result.IsError {
		t.Error("expected isError=true")
	}

	if result.ExitCode != 1 {
		t.Errorf("expected exit code 1, got %d", result.ExitCode)
	}

	if result.Output == "" {
		t.Error("expected error message in output")
	}
}

func TestExecTool_InvalidArgs(t *testing.T) {
	t.Parallel()

	uc := agent.New(
		&mockLLM{},
		&mockTool{result: entity.ToolResult{ToolName: "noop", Output: map[string]any{"ok": true}}},
		nil, nil, nil, nil,
	)

	result, err := uc.ExecTool(context.Background(), "run-1", entity.ToolCall{
		ID: "call-1", Type: "function",
		Function: entity.ToolCallFunction{Name: "noop", Arguments: ""},
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.IsError {
		t.Error("expected no error")
	}
}
