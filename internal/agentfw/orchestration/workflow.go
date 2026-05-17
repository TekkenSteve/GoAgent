package orchestration

import (
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ErrPrepChecksFailed is returned when agent workflow prep checks fail.
var ErrPrepChecksFailed = errors.New("agent workflow - prep checks failed")

// AgentWorkflow is the step-level agent workflow with fine-grained activities.
// Each LLM call and tool execution is a separate Temporal activity, providing
// full visibility into the agent loop via workflow history.
//
// Signal (agent-command):
//   - "cancel": terminates the workflow immediately
//   - "pause":   blocks until "resume" or "cancel"
//   - "resume":  exits the pause loop
func AgentWorkflow(ctx workflow.Context, input *AgentWorkflowInput) (WorkflowResult, error) {
	signalCh := workflow.GetSignalChannel(ctx, AgentCommandSignal)
	ctx = setupAgentActivityOptions(ctx)

	status := RunStatus{
		RunID:          input.RunID,
		LifecycleState: "running",
		Step:           input.Continuation.CarriedStep,
		UpdatedAt:      workflow.Now(ctx),
	}

	if err := workflow.SetQueryHandler(ctx, QueryRunStatus, func() (RunStatus, error) {
		return status, nil
	}); err != nil {
		return WorkflowResult{}, err
	}

	// Init phase: Prepare
	messages, tools, baseRequestedAt, err := agentWorkflowInit(ctx, input, &status)
	if err != nil {
		return makeWorkflowResult(ctx, &status), err
	}

	// Agent loop
	for round := range maxToolRounds {
		done, err := agentWorkflowRound(ctx, signalCh, input, &status, messages, tools, baseRequestedAt, round)
		if err != nil {
			return makeWorkflowResult(ctx, &status), err
		}

		if done {
			return makeWorkflowResult(ctx, &status), nil
		}
	}

	status.LifecycleState = string(entity.LifecycleCompleted)

	return makeWorkflowResult(ctx, &status), nil
}

// setupAgentActivityOptions configures activity options for the agent workflow.
func setupAgentActivityOptions(ctx workflow.Context) workflow.Context {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: activityStartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumInterval: time.Minute,
			MaximumAttempts: activityMaxRetries,
		},
	}

	return workflow.WithActivityOptions(ctx, ao)
}

// makeWorkflowResult builds a WorkflowResult from the current status.
func makeWorkflowResult(ctx workflow.Context, status *RunStatus) WorkflowResult {
	return WorkflowResult{
		RunID:          status.RunID,
		LifecycleState: status.LifecycleState,
		Step:           status.Step,
		CompletedAt:    workflow.Now(ctx),
	}
}

// agentWorkflowInit runs the prepare activity and returns initialized state.
func agentWorkflowInit(ctx workflow.Context, input *AgentWorkflowInput, status *RunStatus) ([]entity.Message, []entity.ToolDef, time.Time, error) {
	var prepResult PrepareOutput
	if err := workflow.ExecuteActivity(ctx, PrepareActivityName, &PrepareInput{
		AccountID:        input.AccountID,
		SystemPrompt:     input.SystemPrompt,
		Message:          input.Message,
		History:          input.History,
		Tools:            input.Tools,
		Config:           input.Config,
		MCPServerConfigs: input.MCPServerConfigs,
	}).Get(ctx, &prepResult); err != nil {
		status.LifecycleState = string(entity.LifecycleFailed)

		return nil, nil, time.Time{}, err
	}

	if !prepResult.CanProceed() {
		status.LifecycleState = string(entity.LifecycleFailed)

		return nil, nil, time.Time{}, fmt.Errorf("%w: billing=%v limits=%v errors=%v",
			ErrPrepChecksFailed, prepResult.Billing, prepResult.Limits, prepResult.Errors)
	}

	baseRequestedAt := input.Continuation.InitialRequestedAt
	if baseRequestedAt.IsZero() {
		baseRequestedAt = workflow.Now(ctx)
	}

	return prepResult.Messages, prepResult.Tools, baseRequestedAt, nil
}

// executeAgentToolCalls runs each tool call activity and appends tool result messages.
// Returns (done, error) where done indicates the workflow should exit.
func executeAgentToolCalls(
	ctx workflow.Context,
	signalCh workflow.ReceiveChannel,
	input *AgentWorkflowInput,
	status *RunStatus,
	messages *[]entity.Message,
	toolCalls []entity.ToolCall,
) (bool, error) {
	for _, tc := range toolCalls {
		if canceled, err := checkStreamSignal(signalCh, ctx); err != nil {
			_ = err
		} else if canceled {
			status.LifecycleState = string(entity.LifecycleCancelled)

			return true, nil
		}

		var toolResult ToolOutput
		if err := workflow.ExecuteActivity(ctx, ToolExecActivityName, ToolInput{
			RunID:      input.RunID,
			ToolCallID: tc.ID,
			ToolName:   tc.Function.Name,
			Args:       parseArgsJSON(tc.Function.Arguments),
		}).Get(ctx, &toolResult); err != nil {
			status.LifecycleState = string(entity.LifecycleFailed)

			return true, fmt.Errorf("agent workflow - tool exec %s: %w", tc.Function.Name, err)
		}

		toolMsg := entity.Message{
			Role:       entity.RoleTool,
			ToolCallID: tc.ID,
			Content:    toolResult.Output,
		}
		*messages = append(*messages, toolMsg)
	}

	return false, nil
}

// buildContinueAsNewInput checks whether the workflow should continue as new
// and returns the input for the continued workflow if so.
func buildContinueAsNewInput(input *AgentWorkflowInput, status *RunStatus, messages []entity.Message, baseRequestedAt time.Time, ctx workflow.Context) (*AgentWorkflowInput, bool) {
	decision := EvaluateContinueAsNew(input.ContinuePolicy, ContinueAsNewSnapshot{
		HistoryLength:   len(messages),
		StateSizeBytes:  0,
		Step:            status.Step,
		Elapsed:         workflow.Now(ctx).Sub(baseRequestedAt),
		ContinuationCnt: input.Continuation.ContinuationCount,
	})
	if !decision.ShouldContinue {
		return nil, false
	}

	payload := ContinuationPayload{
		RunID:              status.RunID,
		ContinuationCount:  input.Continuation.ContinuationCount + 1,
		PreviousWorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		CarriedStep:        status.Step,
		CarriedAt:          workflow.Now(ctx),
		InitialRequestedAt: baseRequestedAt,
	}
	nextInput := *input
	nextInput.Continuation = payload

	return &nextInput, true
}

// agentWorkflowRound executes one iteration of the agent loop.
// Returns true if the workflow should exit (completed, canceled, failed, or continued-as-new).
func agentWorkflowRound(
	ctx workflow.Context,
	signalCh workflow.ReceiveChannel,
	input *AgentWorkflowInput,
	status *RunStatus,
	messages []entity.Message,
	tools []entity.ToolDef,
	baseRequestedAt time.Time,
	round int,
) (bool, error) {
	if canceled, err := checkStreamSignal(signalCh, ctx); err != nil {
		return false, nil
	} else if canceled {
		status.LifecycleState = string(entity.LifecycleCancelled)

		return true, nil
	}

	// Repair tool call pairing before LLM call — the workflow owns
	// message lifecycle, ensuring no orphaned tool results or
	// unanswered tool calls leak through to the model.
	messages = entity.RepairToolCallPairing(messages)

	// Single sync LLM call
	var llmResult LLMStepOutput
	if err := workflow.ExecuteActivity(ctx, LLMStepActivityName, LLMStepInput{
		AccountID: input.AccountID,
		RunID:     input.RunID,
		Messages:  messages,
		Tools:     tools,
		Config:    input.Config,
	}).Get(ctx, &llmResult); err != nil {
		status.LifecycleState = string(entity.LifecycleFailed)

		return true, fmt.Errorf("agent workflow - llm round %d: %w", round, err)
	}

	// Track assistant message
	assistantMsg := entity.Message{Role: entity.RoleAssistant}
	if len(llmResult.ToolCalls) > 0 {
		assistantMsg.ToolCalls = llmResult.ToolCalls
	}

	messages = append(messages, assistantMsg)

	status.Step++
	status.UpdatedAt = workflow.Now(ctx)

	if len(llmResult.ToolCalls) == 0 {
		status.LifecycleState = string(entity.LifecycleCompleted)

		return true, nil
	}

	// Execute each tool call
	if done, err := executeAgentToolCalls(ctx, signalCh, input, status, &messages, llmResult.ToolCalls); err != nil || done {
		return true, err
	}

	// Evaluate Continue-As-New after each round
	if nextInput, ok := buildContinueAsNewInput(input, status, messages, baseRequestedAt, ctx); ok {
		return true, workflow.NewContinueAsNewError(ctx, AgentWorkflow, nextInput)
	}

	return false, nil
}
