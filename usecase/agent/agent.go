package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/google/uuid"
)

var (
	ErrAgentRepoNotAvailable = errors.New("agent repo not available")
	ErrAgentNotFound         = errors.New("agent not found")
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
	llm        usecase.LLMProvider
	tools      usecase.ToolExecutor
	wal        usecase.WALAppender
	compressor usecase.ContextCompressor
	toolDefs   usecase.ToolDefProvider
	agentRepo  usecase.AgentRepo // optional; nil in tests or when agent management is not wired
	log        logger.Interface
}

// SetLogger attaches a logger to the use case for best-effort WAL diagnostics.
// Safe to call with nil — logging calls are no-ops when no logger is set.
func (uc *UseCase) SetLogger(l logger.Interface) {
	uc.log = l
}

// New -.
func New(llm usecase.LLMProvider, tools usecase.ToolExecutor, wal usecase.WALAppender, compressor usecase.ContextCompressor, toolDefs usecase.ToolDefProvider, agentRepo usecase.AgentRepo) *UseCase {
	return &UseCase{llm: llm, tools: tools, wal: wal, compressor: compressor, toolDefs: toolDefs, agentRepo: agentRepo}
}

// ——— Fine-grained step types for Temporal-native orchestration ———

// LLMStepReq is the input for a single LLM call.
type LLMStepReq struct {
	RunID    string
	Messages []entity.Message
	Tools    []entity.ToolDef
	Config   entity.LLMConfig
}

// LLMStepResult is the output of a single LLM call.
type LLMStepResult struct {
	AssistantMsg entity.Message
	ToolCalls    []entity.ToolCall
	Usage        entity.Usage
	FinishReason string
}

// ToolExecResult is the output of a single tool execution.
type ToolExecResult struct {
	ToolMsg    entity.Message
	Output     string
	ExitCode   int
	IsError    bool
	DurationMs int64
}

// LLMStep performs a single LLM call with optional compression.
// It writes the assistant message to WAL and returns the result.
func (uc *UseCase) LLMStep(ctx context.Context, runID string, messages []entity.Message, tools []entity.ToolDef, config entity.LLMConfig) (*LLMStepResult, error) {
	llmReq := entity.LLMRequest{
		Messages: messages,
		Tools:    tools,
		Config:   config,
	}

	resp, err := uc.llm.Chat(ctx, &llmReq)
	if err != nil {
		return nil, classifyLLMError(err)
	}

	assistantMsg := entity.Message{
		Role:    entity.RoleAssistant,
		Content: resp.Content,
	}
	if len(resp.ToolCalls) > 0 {
		assistantMsg.ToolCalls = resp.ToolCalls
	}

	// Best-effort WAL append
	uc.appendMessageToWAL(ctx, runID, assistantMsg)

	return &LLMStepResult{
		AssistantMsg: assistantMsg,
		ToolCalls:    resp.ToolCalls,
		Usage:        resp.Usage,
	}, nil
}

// ExecTool executes a single tool call and writes results to WAL.
func (uc *UseCase) ExecTool(ctx context.Context, runID string, tc entity.ToolCall) (*ToolExecResult, error) {
	toolReq := entity.ToolRequest{
		RunID:      runID,
		ToolCallID: tc.ID,
		ToolName:   tc.Function.Name,
		Args:       parseArgsJSON(tc.Function.Arguments),
	}

	execStart := time.Now()
	result, execErr := uc.tools.Execute(ctx, &toolReq)
	durationMs := time.Since(execStart).Milliseconds()

	uc.appendToolResultToWAL(ctx, runID, &result, tc)

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
	uc.appendMessageToWAL(ctx, runID, toolMsg)

	return &ToolExecResult{
		ToolMsg:    toolMsg,
		Output:     output,
		ExitCode:   exitCode,
		IsError:    isError,
		DurationMs: durationMs,
	}, nil
}

// CompressIfNeeded compresses messages when approaching context limits.
// Returns the (possibly compressed) messages and whether compression occurred.
func (uc *UseCase) CompressIfNeeded(ctx context.Context, messages []entity.Message, config entity.LLMConfig) ([]entity.Message, bool, error) {
	if uc.compressor == nil {
		return messages, false, nil
	}

	compressedMsgs, compressed, err := uc.compressor.Compress(ctx, messages, config)
	if err != nil {
		return messages, false, err
	}

	return compressedMsgs, compressed, nil
}

// ——— Agent management ———

// GetAgent returns the agent record for the given ID, using cache when available.
func (uc *UseCase) GetAgent(ctx context.Context, agentID string) (entity.AgentRecord, error) {
	if uc.agentRepo == nil {
		return entity.AgentRecord{}, ErrAgentRepoNotAvailable
	}

	record, exists, err := uc.agentRepo.Get(ctx, agentID)
	if err != nil {
		return entity.AgentRecord{}, fmt.Errorf("GetAgent: %w", err)
	}

	if !exists {
		return entity.AgentRecord{}, fmt.Errorf("%w: %s", ErrAgentNotFound, agentID)
	}

	return record, nil
}

// UpdateAgent updates an agent record. If config fields (system_prompt, model_ref,
// config) change, it auto-creates a version snapshot before applying the update.
func (uc *UseCase) UpdateAgent(ctx context.Context, agentID string, req entity.UpdateAgentRequest) (entity.AgentRecord, error) {
	if uc.agentRepo == nil {
		return entity.AgentRecord{}, ErrAgentRepoNotAvailable
	}

	// Fetch current record to detect field changes
	current, err := uc.GetAgent(ctx, agentID)
	if err != nil {
		return entity.AgentRecord{}, err
	}

	// Detect config-relevant field changes and auto-version
	if hasConfigChanges(req, &current) {
		version := buildAgentVersionSnapshot(agentID, req, &current)
		if err := uc.agentRepo.CreateVersion(ctx, &version); err != nil {
			return entity.AgentRecord{}, fmt.Errorf("UpdateAgent - create version: %w", err)
		}

		req.CurrentVersion = &version.VersionID
	}

	// Delegate to repo for the actual DB write
	return uc.agentRepo.Update(ctx, agentID, req)
}

// buildAgentVersionSnapshot creates a version record snapshot from the current
// agent state, using update values where provided.
func buildAgentVersionSnapshot(agentID string, req entity.UpdateAgentRequest, current *entity.AgentRecord) entity.AgentVersionRecord {
	now := time.Now().UTC()

	systemPrompt := current.SystemPrompt
	if req.SystemPrompt != nil {
		systemPrompt = *req.SystemPrompt
	}

	modelRef := current.ModelRef
	if req.ModelRef != nil {
		modelRef = *req.ModelRef
	}

	cfg := current.Config
	if req.Config != nil {
		cfg = *req.Config
	}

	return entity.AgentVersionRecord{
		VersionID:         uuid.New().String(),
		AgentID:           agentID,
		VersionName:       fmt.Sprintf("v-%s", now.Format("20060102-150405")),
		SystemPrompt:      systemPrompt,
		ModelRef:          modelRef,
		Config:            cfg,
		ChangeDescription: "auto-saved on config update",
		CreatedAt:         now,
	}
}

// ExecuteStep runs one LLM invocation plus subsequent tool rounds.
// It returns the messages generated, tool results, and usage statistics.
func (uc *UseCase) ExecuteStep(ctx context.Context, req *StepRequest) (*StepResult, error) {
	// Auto-populate tool definitions from registry if not explicitly provided
	tools := req.Tools
	if len(tools) == 0 && uc.toolDefs != nil {
		tools = uc.toolDefs.Definitions()
	}

	// Phase 1: Prep — validate and initialize execution context
	prepResult, err := uc.Prep(ctx, &PrepRequest{
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
	var (
		allToolResults []entity.ToolResult
		finalUsage     entity.Usage
	)

	for range maxToolRounds {
		messages = uc.compressStepMessages(ctx, messages, req.Config)

		resp, err := uc.llm.Chat(ctx, &entity.LLMRequest{
			Messages: messages,
			Tools:    tools,
			Config:   req.Config,
		})
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

		allToolResults, messages = uc.executeStepToolCalls(ctx, req.RunID, resp.ToolCalls, allToolResults, messages)
	}

	delta := computeStepDelta(messages, req.History)

	return &StepResult{
		Messages:      delta,
		CompleteState: messages,
		ToolResults:   allToolResults,
		Usage:         finalUsage,
		FinishReason:  entity.FinishStop,
	}, nil
}

// compressStepMessages compresses messages when approaching context limits.
// Returns the (possibly compressed) messages. On error, returns the originals.
func (uc *UseCase) compressStepMessages(ctx context.Context, messages []entity.Message, config entity.LLMConfig) []entity.Message {
	if uc.compressor == nil {
		return messages
	}

	compressed, _, err := uc.compressor.Compress(ctx, messages, config)
	if err == nil {
		return compressed
	}

	return messages
}

// executeStepToolCalls iterates over tool calls from an LLM response,
// executes each, and returns the accumulated results and updated messages.
func (uc *UseCase) executeStepToolCalls(ctx context.Context, runID string, toolCalls []entity.ToolCall, allToolResults []entity.ToolResult, messages []entity.Message) ([]entity.ToolResult, []entity.Message) {
	for _, tc := range toolCalls {
		allToolResults, messages = uc.executeStepToolCall(ctx, runID, tc, allToolResults, messages)
	}

	return allToolResults, messages
}

// executeStepToolCall executes a single tool call and appends the result to
// the message list.
func (uc *UseCase) executeStepToolCall(ctx context.Context, runID string, tc entity.ToolCall, allToolResults []entity.ToolResult, messages []entity.Message) ([]entity.ToolResult, []entity.Message) {
	toolReq := entity.ToolRequest{
		RunID:      runID,
		ToolCallID: tc.ID,
		ToolName:   tc.Function.Name,
		Args:       parseArgsJSON(tc.Function.Arguments),
	}

	toolResult, execErr := uc.tools.Execute(ctx, &toolReq)
	if execErr != nil {
		toolResult = entity.ToolResult{
			RunID:      runID,
			ToolCallID: tc.ID,
			ToolName:   tc.Function.Name,
		}
	}

	allToolResults = append(allToolResults, toolResult)
	uc.appendToolResultToWAL(ctx, runID, &toolResult, tc)

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
	uc.appendMessageToWAL(ctx, runID, toolMsg)

	return allToolResults, messages
}

// computeStepDelta computes the delta from history safely, handling the case
// where compression may have made messages shorter than the original history.
func computeStepDelta(messages, history []entity.Message) []entity.Message {
	if len(messages) >= len(history) {
		return messages[len(history):]
	}

	return messages
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
		if uc.log != nil {
			uc.log.Warn("WAL append message failed (run=%s, role=%s): %v", runID, msg.Role, err)
		}
	}
}

// appendToolResultToWAL best-effort appends a tool result record to the write-ahead log.
func (uc *UseCase) appendToolResultToWAL(ctx context.Context, runID string, result *entity.ToolResult, tc entity.ToolCall) {
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
		if uc.log != nil {
			uc.log.Warn("WAL append tool result failed (run=%s, tool=%s): %v", runID, tc.Function.Name, err)
		}
	}
}

// hasConfigChanges checks whether the request would modify agent configuration.
func hasConfigChanges(req entity.UpdateAgentRequest, current *entity.AgentRecord) bool {
	return (req.SystemPrompt != nil && *req.SystemPrompt != current.SystemPrompt) ||
		(req.ModelRef != nil && *req.ModelRef != current.ModelRef) ||
		req.Config != nil
}
