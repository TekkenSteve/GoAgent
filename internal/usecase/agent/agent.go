// Package agent implements the agent execution use cases.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/google/uuid"
)

var (
	// ErrAgentRepoNotAvailable is returned when the agent repository is not configured.
	ErrAgentRepoNotAvailable = errors.New("agent repo not available")
	// ErrInvalidRunIdentity is returned when a run's tenant is not known, which
	// makes work that must be authorized under it impossible to authorize.
	ErrInvalidRunIdentity = errors.New("invalid run identity")
	// ErrAgentNotFound is returned when the requested agent does not exist.
	ErrAgentNotFound = errors.New("agent not found")
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
	compressor repo.ContextCompressor
	toolDefs   usecase.ToolDefProvider
	agentRepo  repo.AgentRepo // optional; nil in tests or when agent management is not wired
	log        logger.Interface
}

// SetLogger attaches a logger to the use case for diagnostics. Safe to call
// with nil — logging calls are no-ops when no logger is set.
func (uc *UseCase) SetLogger(l logger.Interface) {
	uc.log = l
}

// New -.
func New(llm repo.LLMProvider, tools repo.ToolExecutor, compressor repo.ContextCompressor, toolDefs usecase.ToolDefProvider, agentRepo repo.AgentRepo) *UseCase {
	return &UseCase{llm: llm, tools: tools, compressor: compressor, toolDefs: toolDefs, agentRepo: agentRepo}
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

// LLMStep performs a single LLM call with optional compression. It computes;
// durability belongs to the run's fact timeline (the writer's milestones), not
// to this call.
func (uc *UseCase) LLMStep(ctx context.Context, messages []entity.Message, tools []entity.ToolDef, config entity.LLMConfig) (*LLMStepResult, error) {
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

	return &LLMStepResult{
		AssistantMsg: assistantMsg,
		ToolCalls:    resp.ToolCalls,
		Usage:        resp.Usage,
	}, nil
}

// ExecTool executes a single tool call.
// RunIdentity is the tenant a run belongs to, as everything the run does sees
// it.
//
// It is a required parameter rather than an optional field because a tool call
// that cannot name its tenant cannot be authorized: the pipeline builds the
// principal from the account and the tenant scope from the project, and refuses
// what is missing. A caller that forgets the identity fails here, with a
// sentence that says so, instead of failing every tool call at runtime.
type RunIdentity struct {
	RunID     string
	AccountID string
	ProjectID string
}

// Validate reports whether the identity names a tenant and a run.
func (i RunIdentity) Validate() error {
	if i.RunID == "" {
		return fmt.Errorf("%w: run id is required", ErrInvalidRunIdentity)
	}

	if _, err := agentoscore.NewTenantScope(i.AccountID, i.ProjectID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRunIdentity, err)
	}

	return nil
}

// ExecTool executes one tool call as the run's tenant.
//
// It has two failure kinds and they mean different things:
//
//   - a tool that fails is the tool's answer: the result says IsError, carries
//     the message the model should see, and the error is nil;
//   - a call that cannot be authorized never reaches a tool, so there is no
//     answer to report and the result is nil with an error — the caller must
//     fail rather than tell the model a tool failed when none was called.
func (uc *UseCase) ExecTool(ctx context.Context, identity RunIdentity, tc entity.ToolCall) (*ToolExecResult, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}

	toolReq := entity.ToolRequest{
		RunID:      identity.RunID,
		AccountID:  identity.AccountID,
		ProjectID:  identity.ProjectID,
		ToolCallID: tc.ID,
		ToolName:   tc.Function.Name,
		Args:       parseArgsJSON(tc.Function.Arguments),
		// A tool call is identified by its id, so a platform-level retry of
		// the enclosing activity can deduplicate against the first attempt
		// instead of re-running a side effect.
		IdempotencyKey: entity.ToolCallIdempotencyKey(tc.ID),
	}

	execStart := time.Now()
	result, execErr := uc.tools.Execute(ctx, &toolReq)
	durationMs := time.Since(execStart).Milliseconds()

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
	tools := req.Tools
	if len(tools) == 0 && uc.toolDefs != nil {
		tools = uc.toolDefs.Definitions()
	}

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
		// A tool call is identified by its id, so a platform-level retry of
		// the enclosing activity can deduplicate against the first attempt
		// instead of re-running a side effect.
		IdempotencyKey: entity.ToolCallIdempotencyKey(tc.ID),
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

// hasConfigChanges checks whether the request would modify agent configuration.
func hasConfigChanges(req entity.UpdateAgentRequest, current *entity.AgentRecord) bool {
	return (req.SystemPrompt != nil && *req.SystemPrompt != current.SystemPrompt) ||
		(req.ModelRef != nil && *req.ModelRef != current.ModelRef) ||
		req.Config != nil
}
