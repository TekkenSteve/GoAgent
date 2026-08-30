package orchestration

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const waitingInput = "waiting_input"

// defaultAwaitUserInputTimeout bounds the waiting_input state so a run can
// never wait for user input indefinitely. A zero AwaitUserInputTimeout on the
// workflow input falls back to this default.
const defaultAwaitUserInputTimeout = 24 * time.Hour

// awaitingInputTimeoutReason marks a run that completed because the user did
// not respond within the wait timeout.
const awaitingInputTimeoutReason = "awaiting_user_input_timed_out"

// awaitInputTimeoutVersionMarker is the workflow.GetVersion marker guarding
// the awaiting-user-input timeout. In-flight runs started before the timeout
// was added replay with workflow.DefaultVersion and keep the previous
// indefinite wait (their recorded history has no timer at that point);
// new runs get the bounded wait. Bump the version to 2 only if the wait
// structure changes again.
const awaitInputTimeoutVersionMarker = "agentfw-await-user-input-timeout"

// searchAttributesVersionMarker guards the visibility upserts (see
// syncRunSearchAttributes). In-flight runs started before the marker replay
// without the upsert calls, which keeps their history deterministic.
const searchAttributesVersionMarker = "agentfw-search-attributes"

// historyRefVersionMarker guards the claim-check history snapshot on
// Continue-As-New and the reload during init. Both are structural changes
// (new activities where old histories had none); in-flight runs replay
// without them and carry the conversation in state as before.
const historyRefVersionMarker = "agentfw-history-ref"

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

	// Version guard: the awaiting-user-input timeout is a structural change
	// (a timer where old history has none). Old in-flight runs must replay
	// with the pre-change branch or they fail with a non-determinism error.
	awaitInputTimeoutVersion := workflow.GetVersion(ctx, awaitInputTimeoutVersionMarker, workflow.DefaultVersion, 1)

	// Version guard: visibility upserts emit history events, so old in-flight
	// runs replay without them.
	searchAttrsVersion := workflow.GetVersion(ctx, searchAttributesVersionMarker, workflow.DefaultVersion, 1)

	// Version guard: Continue-As-New history snapshots are structural changes
	// (activities where old histories had none).
	historyRefVersion := workflow.GetVersion(ctx, historyRefVersionMarker, workflow.DefaultVersion, 1)

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
	messages, tools, baseRequestedAt, err := agentWorkflowInit(ctx, input, &status, historyRefVersion)
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
		done, updatedMessages, err := agentWorkflowRound(ctx, signalCh, userMessageCh, input, &status, messages, allTools, domainTools, baseRequestedAt, round, awaitInputTimeoutVersion, searchAttrsVersion, historyRefVersion)
		messages = updatedMessages

		if err != nil {
			return makeWorkflowResult(ctx, &status), err
		}

		if done {
			return makeWorkflowResult(ctx, &status), nil
		}
	}

	status.LifecycleState = string(entity.LifecycleCompleted)
	syncRunSearchAttributes(ctx, searchAttrsVersion, status.RunID, input.Config.Model, status.LifecycleState, maxToolRounds, "")

	return makeWorkflowResult(ctx, &status), nil
}

// setupAgentActivityOptions configures activity options for the agent workflow.
// Activities report liveness via heartbeats (see heartbeat.go); HeartbeatTimeout
// bounds how quickly a crashed worker is detected and retried.
func setupAgentActivityOptions(ctx workflow.Context) workflow.Context {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: activityStartToCloseTimeout,
		HeartbeatTimeout:    heartbeatTimeout,
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
// A continued-as-new run carrying a history snapshot (claim-check ref) reloads
// the authoritative conversation instead of rebuilding from the initial
// request — that is what keeps the conversation alive across the boundary
// without carrying it in the workflow input.
func agentWorkflowInit(ctx workflow.Context, input *AgentWorkflowInput, status *RunStatus, historyRefVersion workflow.Version) ([]entity.Message, []entity.ToolDef, time.Time, error) {
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

	if historyRefVersion >= 1 && input.Continuation.HistoryRef != "" {
		var loadOut LoadHistoryOutput
		if err := workflow.ExecuteActivity(ctx, LoadHistoryActivityName, &LoadHistoryInput{Ref: input.Continuation.HistoryRef}).Get(ctx, &loadOut); err != nil {
			status.LifecycleState = string(entity.LifecycleFailed)

			return nil, nil, time.Time{}, fmt.Errorf("agent workflow - restore history snapshot: %w", err)
		}

		prepResult.Messages = loadOut.Messages
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
			status.LifecycleState = string(entity.LifecycleCanceled)

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

// estimateMessagesStateSizeBytes deterministically estimates the serialized
// size of the message history for continuation decisions. json.Marshal output
// is a pure function of its input, so replays of the same history produce the
// same estimate (workflow-safe).
func estimateMessagesStateSizeBytes(messages []entity.Message) int {
	if len(messages) == 0 {
		return 0
	}

	b, err := json.Marshal(messages)
	if err != nil {
		return 0
	}

	return len(b)
}

// buildContinueAsNewInput checks whether the workflow should continue as new
// and returns the input for the continued workflow if so. When it does, the
// current conversation is snapshotted outside history (claim-check) and only
// the reference crosses the boundary; the continued run reloads it in init.
func buildContinueAsNewInput(input *AgentWorkflowInput, status *RunStatus, messages []entity.Message, baseRequestedAt time.Time, ctx workflow.Context, historyRefVersion workflow.Version) (*AgentWorkflowInput, bool, error) {
	decision := EvaluateContinueAsNew(input.ContinuePolicy, ContinueAsNewSnapshot{
		HistoryLength:   len(messages),
		StateSizeBytes:  estimateMessagesStateSizeBytes(messages),
		Step:            status.Step,
		Elapsed:         workflow.Now(ctx).Sub(baseRequestedAt),
		ContinuationCnt: input.Continuation.ContinuationCount,
	})
	if !decision.ShouldContinue {
		return nil, false, nil
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

	// Snapshot the conversation so the continued run can reload it. Without
	// the snapshot the continued run would restart from the initial request;
	// fail loudly instead of silently losing history.
	if historyRefVersion >= 1 {
		var snapOut SnapshotHistoryOutput
		if err := workflow.ExecuteActivity(ctx, SnapshotHistoryActivityName, &SnapshotHistoryInput{
			RunID:    status.RunID,
			Round:    int(input.Continuation.ContinuationCount + 1),
			Messages: messages,
		}).Get(ctx, &snapOut); err != nil {
			return nil, true, fmt.Errorf("agent workflow - snapshot history before continue-as-new: %w", err)
		}

		nextInput.Continuation.HistoryRef = snapOut.Ref
	}

	return &nextInput, true, nil
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
	awaitInputTimeoutVersion workflow.Version,
	searchAttrsVersion workflow.Version,
	historyRefVersion workflow.Version,
) (bool, []entity.Message, error) {
	// Sync visibility attributes once per round, on every exit path. The defer
	// reads llmResult as it is populated below, so the last tool requested this
	// round is visible to operators mid-run.
	var llmResult LLMStepOutput

	defer syncRunSearchAttributesForRound(ctx, searchAttrsVersion, status, input, round, &llmResult)

	if canceled, err := checkStreamSignal(signalCh, ctx); err != nil {
		return false, messages, nil
	} else if canceled {
		status.LifecycleState = string(entity.LifecycleCanceled)

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
		done, updatedMessages := finalizeRoundWithoutToolCalls(ctx, signalCh, userMessageCh, input, status, messages, llmResult.Content, awaitInputTimeoutVersion, searchAttrsVersion)

		return done, updatedMessages, nil
	}

	// Execute each tool call
	if done, err := executeAgentToolCalls(ctx, signalCh, input, status, &messages, llmResult.ToolCalls, domainTools); err != nil || done {
		return true, messages, err
	}

	// Evaluate Continue-As-New after each round
	nextInput, continueAsNew, err := buildContinueAsNewInput(input, status, messages, baseRequestedAt, ctx, historyRefVersion)
	if err != nil {
		return true, messages, err
	}

	if continueAsNew {
		return true, messages, workflow.NewContinueAsNewError(ctx, AgentWorkflow, nextInput)
	}

	return false, messages, nil
}

// syncRunSearchAttributesForRound publishes the round's visibility attributes
// on every exit path. Called via defer so the last tool requested this round is
// visible to operators mid-run.
func syncRunSearchAttributesForRound(ctx workflow.Context, enabled workflow.Version, status *RunStatus, input *AgentWorkflowInput, round int, llmResult *LLMStepOutput) {
	lastTool := ""
	if len(llmResult.ToolCalls) > 0 {
		lastTool = llmResult.ToolCalls[len(llmResult.ToolCalls)-1].Function.Name
	}

	syncRunSearchAttributes(ctx, enabled, status.RunID, input.Config.Model, status.LifecycleState, round+1, lastTool)
}

// finalizeRoundWithoutToolCalls finishes a round that produced no tool calls:
// either complete immediately, or (when awaiting user input) wait for the user
// message with a timeout. Returns done=true when the workflow should end.
func finalizeRoundWithoutToolCalls(
	ctx workflow.Context,
	signalCh workflow.ReceiveChannel,
	userMessageCh workflow.ReceiveChannel,
	input *AgentWorkflowInput,
	status *RunStatus,
	messages []entity.Message,
	output string,
	awaitInputTimeoutVersion workflow.Version,
	searchAttrsVersion workflow.Version,
) (bool, []entity.Message) {
	status.Output = output

	if !input.AwaitUserInput {
		status.LifecycleState = string(entity.LifecycleCompleted)

		return true, messages
	}

	timeout := input.AwaitUserInputTimeout
	if timeout <= 0 {
		timeout = defaultAwaitUserInputTimeout
	}

	nextMessages, outcome := waitForUserMessage(ctx, signalCh, userMessageCh, status, messages, timeout, awaitInputTimeoutVersion, searchAttrsVersion)
	switch outcome {
	case waitOutcomeReceived:
		return false, nextMessages
	case waitOutcomeCanceled:
		status.LifecycleState = string(entity.LifecycleCanceled)

		return true, nextMessages
	case waitOutcomeTimedOut:
		return true, nextMessages
	}

	return false, nextMessages
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

// waitOutcome describes how a user-input wait ended.
type waitOutcome int

const (
	// waitOutcomeReceived means a user message arrived and the agent loop continues.
	waitOutcomeReceived waitOutcome = iota
	// waitOutcomeCanceled means the run was canceled while waiting.
	waitOutcomeCanceled
	// waitOutcomeTimedOut means the wait expired without user input; the run
	// completes with status reason awaitingInputTimeoutReason.
	waitOutcomeTimedOut
)

func waitForUserMessage(
	ctx workflow.Context,
	signalCh workflow.ReceiveChannel,
	userMessageCh workflow.ReceiveChannel,
	status *RunStatus,
	messages []entity.Message,
	timeout time.Duration,
	awaitInputTimeoutVersion workflow.Version,
	searchAttrsVersion workflow.Version,
) ([]entity.Message, waitOutcome) {
	status.LifecycleState = waitingInput
	status.UpdatedAt = workflow.Now(ctx)

	// The waiting_input state is the most common "why is this run not moving"
	// question — make it visible in visibility immediately.
	syncRunSearchAttributes(ctx, searchAttrsVersion, status.RunID, "", status.LifecycleState, 0, "")

	selector := workflow.NewSelector(ctx)

	// The wait timer is canceled once any other branch fires so the abandoned
	// timer cannot fire later and corrupt replay determinism.
	timerCtx, cancelTimer := workflow.WithCancel(ctx)

	var (
		nextMessages []entity.Message
		outcome      waitOutcome
		received     bool
	)

	selector.AddReceive(userMessageCh, func(c workflow.ReceiveChannel, _ bool) {
		cancelTimer()

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
			cancelTimer()

			outcome = waitOutcomeCanceled
			received = true
		}
	})

	if awaitInputTimeoutVersion >= 1 && timeout > 0 {
		selector.AddFuture(workflow.NewTimer(timerCtx, timeout), awaitInputTimeoutCallback(timerCtx, status, &outcome, &received))
	}

	for !received {
		selector.Select(ctx)
	}

	if nextMessages == nil {
		nextMessages = messages
	}

	return nextMessages, outcome
}

// awaitInputTimeoutCallback completes the wait when the timeout expires. If a
// user message or control signal branch fires first, the timer context is
// canceled and the callback returns without touching state — an abandoned
// timer must never advance the state machine on replay.
func awaitInputTimeoutCallback(timerCtx workflow.Context, status *RunStatus, outcome *waitOutcome, received *bool) func(workflow.Future) {
	return func(f workflow.Future) {
		if err := f.Get(timerCtx, nil); err != nil {
			return
		}

		status.LifecycleState = string(entity.LifecycleCompleted)
		status.Reason = awaitingInputTimeoutReason
		status.UpdatedAt = workflow.Now(timerCtx)
		*outcome = waitOutcomeTimedOut
		*received = true
	}
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
