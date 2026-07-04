package orchestration

import (
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const waitingInput = "waiting_input"

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
	if input == nil {
		return WorkflowResult{}, nonRetryableWorkflowValidationError(
			fmt.Errorf("%w: AgentWorkflowInput is required", ErrWorkflowTaskQueuesInvalid),
		)
	}

	if err := input.TaskQueues.ValidateNativeAgent(); err != nil {
		return WorkflowResult{}, nonRetryableWorkflowValidationError(err)
	}

	signalCh := workflow.GetSignalChannel(ctx, AgentCommandSignal)
	userMessageCh := workflow.GetSignalChannel(ctx, AgentMessageSignal)
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

	// Separate domain tools from meta-tools so delegate_to_agent
	// is never leaked into child workflows. The LLM sees allTools
	// (can invoke delegate_to_agent), but only domainTools are
	// passable to delegate children.
	domainTools := tools
	allTools := make([]entity.ToolDef, 0, len(domainTools)+1)
	allTools = append(allTools, domainTools...)
	allTools = append(allTools, DelegateToolDef())

	// Agent loop
	for round := range maxToolRounds {
		done, updatedMessages, err := agentWorkflowRound(ctx, signalCh, userMessageCh, input, &status, messages, allTools, domainTools, baseRequestedAt, round)
		messages = updatedMessages

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

func withRequiredActivityTaskQueue(ctx workflow.Context, taskQueue string) workflow.Context {
	return workflow.WithTaskQueue(ctx, taskQueue)
}

// makeWorkflowResult builds a WorkflowResult from the current status.
func makeWorkflowResult(ctx workflow.Context, status *RunStatus) WorkflowResult {
	return WorkflowResult{
		RunID:          status.RunID,
		LifecycleState: status.LifecycleState,
		Step:           status.Step,
		CompletedAt:    workflow.Now(ctx),
		Output:         status.Output,
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
	domainTools []entity.ToolDef,
) (bool, error) {
	for _, tc := range toolCalls {
		if canceled, err := checkStreamSignal(signalCh, ctx); err != nil {
			_ = err
		} else if canceled {
			status.LifecycleState = string(entity.LifecycleCancelled)

			return true, nil
		}

		// Intercept delegate_to_agent calls — spawn a child workflow
		// instead of routing through ToolExecActivity, giving the
		// sub-agent full conversational isolation.
		if isDelegateToolCall(tc) {
			resultContent, err := executeDelegateTool(ctx, tc, input.Config, input.AccountID, domainTools, input.MCPServerConfigs, &input.TaskQueues)
			if err != nil {
				resultContent = fmt.Sprintf("Error delegating task: %v", err)
			}

			toolMsg := entity.Message{
				Role:       entity.RoleTool,
				ToolCallID: tc.ID,
				Content:    resultContent,
			}
			*messages = append(*messages, toolMsg)

			continue
		}

		var toolResult ToolOutput

		toolCtx := withRequiredActivityTaskQueue(ctx, input.TaskQueues.NativeTool)
		if err := workflow.ExecuteActivity(toolCtx, ToolExecActivityName, ToolInput{
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
// allTools is the full tool set exposed to the LLM (including meta-tools like delegate_to_agent).
// domainTools is the subset of tools allowed to be passed to child workflows.
func agentWorkflowRound(
	ctx workflow.Context,
	signalCh workflow.ReceiveChannel,
	userMessageCh workflow.ReceiveChannel,
	input *AgentWorkflowInput,
	status *RunStatus,
	messages []entity.Message,
	allTools []entity.ToolDef,
	domainTools []entity.ToolDef,
	baseRequestedAt time.Time,
	round int,
) (bool, []entity.Message, error) {
	if canceled, err := checkStreamSignal(signalCh, ctx); err != nil {
		return false, messages, nil
	} else if canceled {
		status.LifecycleState = string(entity.LifecycleCancelled)

		return true, messages, nil
	}

	// Repair tool call pairing before LLM call — the workflow owns
	// message lifecycle, ensuring no orphaned tool results or
	// unanswered tool calls leak through to the model.
	messages = entity.RepairToolCallPairing(messages)

	// Single sync LLM call
	llmResult, assistantMsg, err := callLLMAndTrackMessage(ctx, input, messages, allTools, round)
	if err != nil {
		status.LifecycleState = string(entity.LifecycleFailed)

		return true, messages, err
	}

	messages = append(messages, assistantMsg)
	status.Step++
	status.UpdatedAt = workflow.Now(ctx)

	if len(llmResult.ToolCalls) == 0 {
		status.Output = llmResult.Content
		if !input.AwaitUserInput {
			status.LifecycleState = string(entity.LifecycleCompleted)

			return true, messages, nil
		}

		nextMessages, canceled := waitForUserMessage(ctx, signalCh, userMessageCh, status, messages)
		if canceled {
			status.LifecycleState = string(entity.LifecycleCancelled)

			return true, nextMessages, nil
		}

		return false, nextMessages, nil
	}

	// Execute each tool call
	if done, err := executeAgentToolCalls(ctx, signalCh, input, status, &messages, llmResult.ToolCalls, domainTools); err != nil || done {
		return true, messages, err
	}

	// Evaluate Continue-As-New after each round
	if nextInput, ok := buildContinueAsNewInput(input, status, messages, baseRequestedAt, ctx); ok {
		return true, messages, workflow.NewContinueAsNewError(ctx, AgentWorkflow, nextInput)
	}

	return false, messages, nil
}

func callLLMAndTrackMessage(ctx workflow.Context, input *AgentWorkflowInput, messages []entity.Message, allTools []entity.ToolDef, round int) (LLMStepOutput, entity.Message, error) {
	var llmResult LLMStepOutput

	llmCtx := withRequiredActivityTaskQueue(ctx, input.TaskQueues.NativeLLM)
	if err := workflow.ExecuteActivity(llmCtx, LLMStepActivityName, LLMStepInput{
		AccountID: input.AccountID,
		RunID:     input.RunID,
		Messages:  messages,
		Tools:     allTools,
		Config:    input.Config,
	}).Get(ctx, &llmResult); err != nil {
		return llmResult, entity.Message{}, fmt.Errorf("agent workflow - llm round %d: %w", round, err)
	}

	assistantMsg := entity.Message{
		Role:    entity.RoleAssistant,
		Content: llmResult.Content,
	}
	if len(llmResult.ToolCalls) > 0 {
		assistantMsg.ToolCalls = llmResult.ToolCalls
	}

	return llmResult, assistantMsg, nil
}

func waitForUserMessage(
	ctx workflow.Context,
	signalCh workflow.ReceiveChannel,
	userMessageCh workflow.ReceiveChannel,
	status *RunStatus,
	messages []entity.Message,
) ([]entity.Message, bool) {
	status.LifecycleState = waitingInput
	status.UpdatedAt = workflow.Now(ctx)

	selector := workflow.NewSelector(ctx)

	var (
		nextMessages []entity.Message
		canceled     bool
		received     bool
	)

	selector.AddReceive(userMessageCh, func(c workflow.ReceiveChannel, _ bool) {
		var signal UserMessageSignal
		c.Receive(ctx, &signal)

		nextMessages = append([]entity.Message{}, messages...)
		nextMessages = append(nextMessages, entity.Message{
			Role:    entity.RoleUser,
			Content: signal.Content,
		})
		status.LifecycleState = string(entity.LifecycleRunning)
		status.UpdatedAt = workflow.Now(ctx)
		received = true
	})
	selector.AddReceive(signalCh, func(c workflow.ReceiveChannel, _ bool) {
		if handleNativeWaitControlSignal(c, ctx, status) {
			canceled = true
			received = true
		}
	})

	for !received {
		selector.Select(ctx)
	}

	if nextMessages == nil {
		nextMessages = messages
	}

	return nextMessages, canceled
}

func handleNativeWaitControlSignal(signalCh workflow.ReceiveChannel, ctx workflow.Context, status *RunStatus) bool {
	var signal string
	signalCh.Receive(ctx, &signal)

	switch signal {
	case AgentCmdCancel:
		return true
	case AgentCmdPause:
		status.LifecycleState = string(entity.LifecyclePaused)
		status.UpdatedAt = workflow.Now(ctx)

		canceled, err := waitForResume(signalCh, ctx)
		if err != nil {
			return false
		}

		if canceled {
			return true
		}

		status.LifecycleState = waitingInput
		status.UpdatedAt = workflow.Now(ctx)
	}

	return false
}
