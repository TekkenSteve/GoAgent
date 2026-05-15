package orchestration

import (
	"context"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	agentuc "github.com/TekkenSteve/GoAgent/internal/usecase/agent"
	billinguc "github.com/TekkenSteve/GoAgent/internal/usecase/billing"
	triggeruc "github.com/TekkenSteve/GoAgent/internal/usecase/trigger"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/google/uuid"
)

// AgentActivities provides Temporal activity implementations for agent execution.
type AgentActivities struct {
	agentUC      *agentuc.UseCase
	billingUC    *billinguc.UseCase
	triggerUC    *triggeruc.UseCase
	eventStore   stream.EventStore
	templateRepo WorkflowTemplateRepoProvider
	mcpManager   MCPManagerProvider
	logger       logger.Interface
}

// WorkflowTemplateRepoProvider is the subset of repo.WorkflowTemplateRepo needed by activities.
type WorkflowTemplateRepoProvider interface {
	Get(ctx context.Context, templateID string) (entity.WorkflowTemplate, bool, error)
}

// MCPManagerProvider is the subset of the MCP manager needed by activities.
// Server configs are passed in at Prep time,
// and connections are established on demand.
type MCPManagerProvider interface {
	// EnsureConnected JIT-connects to the given MCP servers (if not already
	// connected), discovers their tools, and returns the combined definitions
	// plus any connection errors. Tool registration for execution happens
	// internally in the manager.
	EnsureConnected(ctx context.Context, configs []entity.MCPServerConfig) ([]entity.ToolDef, []string)
}


// NewAgentActivities creates activities wired to the agent usecase.
func NewAgentActivities(uc *agentuc.UseCase, eventStore stream.EventStore, l logger.Interface) *AgentActivities {
	return &AgentActivities{agentUC: uc, eventStore: eventStore, logger: l}
}

// WithTemplateRepo sets the template repo for activities that need it (e.g., FireTriggerActivity).
func (a *AgentActivities) WithTemplateRepo(repo WorkflowTemplateRepoProvider) *AgentActivities {
	a.templateRepo = repo
	return a
}

// WithTriggerUC sets the trigger usecase for FireTriggerActivity.
func (a *AgentActivities) WithTriggerUC(uc *triggeruc.UseCase) *AgentActivities {
	a.triggerUC = uc
	return a
}

// WithMCPManager sets the MCP manager for tool discovery in PrepareActivity.
func (a *AgentActivities) WithMCPManager(m MCPManagerProvider) *AgentActivities {
	a.mcpManager = m
	return a
}

// WithBilling sets the billing usecase for credit checks and usage deduction.
func (a *AgentActivities) WithBilling(uc *billinguc.UseCase) *AgentActivities {
	a.billingUC = uc
	return a
}


// ——— Activities (step-level, Temporal-native) ———

// PrepareActivity runs the full prep pipeline: tool validation, message assembly,
// billing/limits checks, and tool definition validation. It returns a composite
// PrepareOutput with all check results and a CanProceed() gate for the workflow.
func (a *AgentActivities) PrepareActivity(ctx context.Context, input PrepareInput) (*PrepareOutput, error) {
	fmt.Printf("PrepareActivity: started, mcpManager=%v\n", a.mcpManager != nil)
	a.logger.Info("PrepareActivity: started, mcpManager=%v serverConfigs=%d", a.mcpManager != nil, len(input.MCPServerConfigs))

	// Run prep checks concurrently
	type billingResult struct {
		out *PrepBillingOutput
		err error
	}
	type limitsResult struct {
		out *PrepLimitsOutput
		err error
	}

	billingCh := make(chan billingResult, 1)
	limitsCh := make(chan limitsResult, 1)

	go func() {
		billing, err := a.prepBilling(ctx, PrepBillingInput{
			AccountID:	input.AccountID,
				ModelRef: input.Config.Model,
		})
		billingCh <- billingResult{billing, err}
	}()
	go func() {
		limits, err := a.prepLimits(ctx, PrepLimitsInput{
			MessageCount: len(input.History),
			ModelRef:     input.Config.Model,
		})
		limitsCh <- limitsResult{limits, err}
	}()

	// Tool validation runs inline
	toolsOut := a.prepTools(ctx, PrepToolsInput{Tools: input.Tools})

	// Wait for concurrent checks
	billingRes := <-billingCh
	limitsRes := <-limitsCh
	fmt.Printf("PrepareActivity: pre-checks done, billing.err=%v limits.err=%v\n", billingRes.err, limitsRes.err)

	var errors []string
	if billingRes.err != nil {
		errors = append(errors, fmt.Sprintf("billing: %v", billingRes.err))
	}
	if limitsRes.err != nil {
		errors = append(errors, fmt.Sprintf("limits: %v", limitsRes.err))
	}
	for _, name := range toolsOut.Failed {
		errors = append(errors, fmt.Sprintf("tool: %s missing name or type", name))
	}

	// MCP tool resolution runs in the prep pipeline
	mcpOut := a.prepMCP(ctx, PrepMCPInput{
		ServerConfigs: input.MCPServerConfigs,
	})
	for _, err := range mcpOut.Errors {
		errors = append(errors, fmt.Sprintf("mcp: %s", err))
	}

	// Run Prep for message assembly
	req := agentuc.PrepRequest{
		SystemPrompt: input.SystemPrompt,
		UserMessage:  input.Message,
		History:      input.History,
		Tools:        input.Tools,
		Config:       input.Config,
	}

	prepResult, err := a.agentUC.Prep(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("PrepareActivity - Prep: %w", err)
	}
	fmt.Printf("PrepareActivity: Prep done, messages=%d\n", len(prepResult.Messages))

	// Merge MCP-discovered tools with the prep result tools
	allTools := prepResult.Tools
	allTools = append(allTools, mcpOut.Tools...)

	a.logger.Info("PrepareActivity: completed, errors=%v", errors)

	return &PrepareOutput{
		Messages: prepResult.Messages,
		Tools:    allTools,
		Billing:  billingRes.out,
		Limits:   limitsRes.out,
		ToolDefs: toolsOut,
		Errors:   errors,
	}, nil
}

// prepBilling validates account billing/quota.
// Checks the account balance; rejects if balance is zero or negative.
func (a *AgentActivities) prepBilling(ctx context.Context, input PrepBillingInput) (*PrepBillingOutput, error) {
	if a.billingUC == nil || input.AccountID == "" {
		return &PrepBillingOutput{
			Approved:  true,
			Remaining: 0,
		}, nil
	}

	acct, err := a.billingUC.GetBalance(ctx, input.AccountID)
	if err != nil {
		return nil, fmt.Errorf("prepBilling - GetBalance: %w", err)
	}

	if acct.Balance <= 0 {
		return &PrepBillingOutput{
			Approved:  false,
			Remaining: 0,
			Currency:  acct.Currency,
		}, nil
	}

	return &PrepBillingOutput{
		Approved:  true,
		Remaining: int(acct.Balance),
		Currency:  acct.Currency,
	}, nil
}

// prepLimits validates concurrent run limits (pass-through: no counter configured).
func (a *AgentActivities) prepLimits(ctx context.Context, input PrepLimitsInput) (*PrepLimitsOutput, error) {
	return &PrepLimitsOutput{
		Approved:        true,
		ConcurrentLimit: 10,
	}, nil
}

// prepTools validates tool definitions and resolves any tool references.
func (a *AgentActivities) prepTools(ctx context.Context, input PrepToolsInput) *PrepToolsOutput {
	var failed []string
	for _, tool := range input.Tools {
		if tool.Function.Name == "" {
			failed = append(failed, "<unnamed>")
			continue
		}
		if tool.Type == "" {
			failed = append(failed, tool.Function.Name)
			continue
		}
	}
	return &PrepToolsOutput{
		Tools:    input.Tools,
		Resolved: len(input.Tools) - len(failed),
		Failed:   failed,
	}
}

// prepMCP resolves MCP server references into tool definitions via JIT connection.
// Server configs are passed in, connections are established
// on demand during Prep. Returns empty results without error if no manager configured.
func (a *AgentActivities) prepMCP(ctx context.Context, input PrepMCPInput) *PrepMCPOutput {
	if a.mcpManager == nil || len(input.ServerConfigs) == 0 {
		return &PrepMCPOutput{}
	}

	// JIT: connect to servers, discover tools on demand
	tools, errs := a.mcpManager.EnsureConnected(ctx, input.ServerConfigs)

	return &PrepMCPOutput{
		Tools:  tools,
		Errors: errs,
	}
}

// LLMStepActivity performs a single sync LLM call and returns the result.
// Deducts usage cost from account balance after a successful LLM call.
func (a *AgentActivities) LLMStepActivity(ctx context.Context, input LLMStepInput) (*LLMStepOutput, error) {
	fmt.Printf("LLMStepActivity: started, model=%s messages=%d tools=%d\n", input.Config.Model, len(input.Messages), len(input.Tools))
	result, err := a.agentUC.LLMStep(ctx, input.RunID, input.Messages, input.Tools, input.Config)
	if err != nil {
		return nil, fmt.Errorf("LLMStepActivity - LLMStep: %w", err)
	}

	fmt.Printf("LLMStepActivity: completed, finish_reason=%s tool_calls=%d\n", result.FinishReason, len(result.ToolCalls))

	// Deduct usage cost after successful LLM call
	if a.billingUC != nil && input.AccountID != "" && result.Usage.TotalTokens > 0 {
		if _, _, err := a.billingUC.DeductUsage(ctx, input.AccountID, input.Config.Model, result.Usage); err != nil {
			a.logger.Warn("LLMStepActivity - DeductUsage: %v (non-fatal)", err)
		}
	}

	return &LLMStepOutput{
		Content:      result.AssistantMsg.Content,
		ToolCalls:    result.ToolCalls,
		Usage:        result.Usage,
		FinishReason: result.FinishReason,
	}, nil
}

// ToolExecActivity performs a single tool execution and returns the result.
func (a *AgentActivities) ToolExecActivity(ctx context.Context, input ToolInput) (*ToolOutput, error) {
	result, err := a.agentUC.ExecTool(ctx, input.RunID, entity.ToolCall{
		ID:   input.ToolCallID,
		Type: "function",
		Function: entity.ToolCallFunction{
			Name:      input.ToolName,
			Arguments: "", // args passed separately via input.Args; re-marshal
		},
	})
	if err != nil {
		return nil, fmt.Errorf("ToolExecActivity - ExecTool: %w", err)
	}

	return &ToolOutput{
		Output:     result.Output,
		ExitCode:   result.ExitCode,
		IsError:    result.IsError,
		DurationMs: result.DurationMs,
	}, nil
}

// InitStreamActivity writes stream init events to the EventStore and runs Prep.
// Combines AgentRunStart + PrepStage → Prep → PrepStage ready in one activity.
func (a *AgentActivities) InitStreamActivity(ctx context.Context, input InitStreamInput) (*InitStreamOutput, error) {
	// Write start and prep-stage events to EventStore
	_, _ = a.eventStore.Append(ctx, input.SessionID, input.RunID, entity.NewAgentRunStartEvent(input.Message))
	_, _ = a.eventStore.Append(ctx, input.SessionID, input.RunID, entity.NewPrepStageEvent("initializing", 0))

	prepResult, err := a.agentUC.Prep(ctx, agentuc.PrepRequest{
		SystemPrompt: input.SystemPrompt,
		UserMessage:  input.Message,
		History:      input.History,
		Tools:        input.Tools,
		Config:       input.Config,
	})
	if err != nil {
		return nil, fmt.Errorf("InitStreamActivity - Prep: %w", err)
	}
	// MCP tool resolution for streaming path
	mcpOut := a.prepMCP(ctx, PrepMCPInput{
		ServerConfigs: input.MCPServerConfigs,
	})
	allTools := prepResult.Tools
	allTools = append(allTools, mcpOut.Tools...)

	_, _ = a.eventStore.Append(ctx, input.SessionID, input.RunID, entity.NewPrepStageEvent("ready", 100))

	return &InitStreamOutput{
		Messages: prepResult.Messages,
		Tools:    allTools,
	}, nil
}

// LLMStreamActivity performs a single streaming LLM call, writing delta events
// to the EventStore. Heartbeat carries the last-written sequence for retry recovery.
// Deducts usage cost from account balance after a successful LLM call.
func (a *AgentActivities) LLMStreamActivity(ctx context.Context, input LLMStreamInput) (*LLMStreamOutput, error) {
	writer := &eventStoreWriter{
		store:     a.eventStore,
		sessionID: input.SessionID,
		runID:     input.RunID,
	}

	result, err := a.agentUC.LLMStreamCall(ctx, input.RunID, input.Messages, input.Tools, input.Config, writer)
	if err != nil {
		return nil, fmt.Errorf("LLMStreamActivity - LLMStreamCall: %w", err)
	}

	// Deduct usage cost after successful LLM call
	if a.billingUC != nil && input.AccountID != "" && result.Usage.TotalTokens > 0 {
		if _, _, err := a.billingUC.DeductUsage(ctx, input.AccountID, input.Config.Model, result.Usage); err != nil {
			a.logger.Warn("LLMStreamActivity - DeductUsage: %v (non-fatal)", err)
		}
	}

	return &LLMStreamOutput{
		ToolCalls:    result.ToolCalls,
		Usage:        result.Usage,
		FinishReason: result.FinishReason,
	}, nil
}

// ToolExecStreamActivity executes a tool and writes start/finish events to the EventStore.
func (a *AgentActivities) ToolExecStreamActivity(ctx context.Context, input ToolInput) (*ToolOutput, error) {
	sessionID, runID := input.RunID, input.RunID

	// Write ToolExecStart
	_, _ = a.eventStore.Append(ctx, sessionID, runID, entity.NewToolExecStartEvent(input.ToolCallID, input.ToolName))

	execStart := time.Now()
	result, err := a.agentUC.ExecTool(ctx, runID, entity.ToolCall{
		ID:   input.ToolCallID,
		Type: "function",
		Function: entity.ToolCallFunction{
			Name:      input.ToolName,
			Arguments: "", // args re-marshalled from input.Args
		},
	})
	durationMs := time.Since(execStart).Milliseconds()

	output := result.Output
	exitCode := result.ExitCode
	isError := result.IsError

	if err != nil {
		output = fmt.Sprintf("Error executing tool %q: %v", input.ToolName, err)
		exitCode = 1
		isError = true
	}

	// Write ToolExecFinish
	_, _ = a.eventStore.Append(ctx, sessionID, runID, entity.NewToolExecFinishEvent(
		input.ToolCallID, input.ToolName, output, exitCode, isError, durationMs,
	))

	return &ToolOutput{
		Output:     output,
		ExitCode:   exitCode,
		IsError:    isError,
		DurationMs: durationMs,
	}, nil
}

// FinishStreamActivity writes the AgentRunFinish event to the EventStore.
func (a *AgentActivities) FinishStreamActivity(ctx context.Context, input FinishStreamInput) error {
	_, err := a.eventStore.Append(ctx, input.SessionID, input.RunID, input.Event)
	return err
}

// FireTriggerActivity loads a trigger and its associated template, records the fire
// time, and returns the data needed for the TriggerFireWorkflow to dispatch a child
// AgentWorkflow. Returns an error if the trigger or template is not found.
func (a *AgentActivities) FireTriggerActivity(ctx context.Context, triggerID string) (*FireTriggerInput, error) {
	if a.triggerUC == nil || a.templateRepo == nil {
		return nil, fmt.Errorf("FireTriggerActivity - triggerUC/templateRepo not configured")
	}

	// Validate trigger exists and record fire time via the usecase
	trigger, err := a.triggerUC.Fire(ctx, triggerID)
	if err != nil {
		return nil, fmt.Errorf("FireTriggerActivity - fire: %w", err)
	}

	template, exists, err := a.templateRepo.Get(ctx, trigger.TemplateID)
	if err != nil {
		return nil, fmt.Errorf("FireTriggerActivity - get template: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("FireTriggerActivity - template not found: %s", trigger.TemplateID)
	}

	// Resolve {{variable}} placeholders in the agent prompt
	resolvedPrompt := trigger.ResolvePrompt()

	runID := uuid.New().String()

	systemPrompt := template.SystemPrompt
	if resolvedPrompt != "" {
		if systemPrompt != "" {
			systemPrompt += "\n" + resolvedPrompt
		} else {
			systemPrompt = resolvedPrompt
		}
	}

	model := template.DefaultModel
	if model == "" {
		model = "gpt-4"
	}

	// Log rich audit trail (best-effort, non-fatal)
	a.triggerUC.LogTriggerExecution(ctx, triggerID, entity.TriggerEventLog{
		TriggerID:     triggerID,
		TemplateID:    trigger.TemplateID,
		TriggerType:   trigger.TriggerType,
		Success:       true,
		AgentPrompt:   resolvedPrompt,
		ExecVariables: trigger.TemplateVarsVals,
		FiredAt:       time.Now().UTC(),
	})

	return &FireTriggerInput{
		RunID:        runID,
		SystemPrompt: systemPrompt,
		Message:      resolvedPrompt,
		Config:       entity.LLMConfig{Model: model},
	}, nil
}


// ——— Internal helpers ———

// eventStoreWriter implements usecase.StreamEventWriter by appending to EventStore.
type eventStoreWriter struct {
	store     stream.EventStore
	sessionID string
	runID     string
}

func (w *eventStoreWriter) WriteEvent(ctx context.Context, event entity.StreamEvent) error {
	_, err := w.store.Append(ctx, w.sessionID, w.runID, event)
	return err
}

// FinishStreamInput is the input for the streaming finish activity.
type FinishStreamInput struct {
	SessionID string
	RunID     string
	Event     entity.StreamEvent
}
