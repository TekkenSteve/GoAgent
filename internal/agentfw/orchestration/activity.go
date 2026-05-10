package orchestration

import (
	"context"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	agentuc "github.com/TekkenSteve/GoAgent/internal/usecase/agent"
)

// AgentActivities provides Temporal activity implementations for agent execution.
type AgentActivities struct {
	agentUC    *agentuc.UseCase
	eventStore stream.EventStore
}

// NewAgentActivities creates activities wired to the agent usecase.
func NewAgentActivities(uc *agentuc.UseCase, eventStore stream.EventStore) *AgentActivities {
	return &AgentActivities{agentUC: uc, eventStore: eventStore}
}

// ——— Activities (step-level, Temporal-native) ———

// PrepareActivity runs the full prep pipeline: tool validation, message assembly,
// billing/limits checks, and tool definition validation. It returns a composite
// PrepareOutput with all check results and a CanProceed() gate for the workflow.
func (a *AgentActivities) PrepareActivity(ctx context.Context, input PrepareInput) (*PrepareOutput, error) {
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

	return &PrepareOutput{
		Messages: prepResult.Messages,
		Tools:    prepResult.Tools,
		Billing:  billingRes.out,
		Limits:   limitsRes.out,
		ToolDefs: toolsOut,
		Errors:   errors,
	}, nil
}

// prepBilling validates account billing/quota.
// Currently a pass-through; will integrate with billing service when available.
func (a *AgentActivities) prepBilling(ctx context.Context, input PrepBillingInput) (*PrepBillingOutput, error) {
	return &PrepBillingOutput{
		Approved:  true,
		Remaining: 1000,
	}, nil
}

// prepLimits validates rate limits and context limits.
func (a *AgentActivities) prepLimits(ctx context.Context, input PrepLimitsInput) (*PrepLimitsOutput, error) {
	maxMessages := 500
	if input.MessageCount > maxMessages {
		return &PrepLimitsOutput{
			Approved:     false,
			ContextLimit: maxMessages,
		}, nil
	}
	return &PrepLimitsOutput{
		Approved:     true,
		ContextLimit: maxMessages,
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

// LLMStepActivity performs a single sync LLM call and returns the result.
func (a *AgentActivities) LLMStepActivity(ctx context.Context, input LLMStepInput) (*LLMStepOutput, error) {
	result, err := a.agentUC.LLMStep(ctx, input.RunID, input.Messages, input.Tools, input.Config)
	if err != nil {
		return nil, fmt.Errorf("LLMStepActivity - LLMStep: %w", err)
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

	_, _ = a.eventStore.Append(ctx, input.SessionID, input.RunID, entity.NewPrepStageEvent("ready", 100))

	return &InitStreamOutput{
		Messages: prepResult.Messages,
		Tools:    prepResult.Tools,
	}, nil
}

// LLMStreamActivity performs a single streaming LLM call, writing delta events
// to the EventStore. Heartbeat carries the last-written sequence for retry recovery.
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
