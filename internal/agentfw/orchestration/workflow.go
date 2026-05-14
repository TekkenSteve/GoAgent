package orchestration

import (
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// AgentWorkflow is the step-level agent workflow with fine-grained activities.
// Each LLM call and tool execution is a separate Temporal activity, providing
// full visibility into the agent loop via workflow history.
//
// Signal (agent-command):
//   - "cancel": terminates the workflow immediately
//   - "pause":   blocks until "resume" or "cancel"
//   - "resume":  exits the pause loop
func AgentWorkflow(ctx workflow.Context, input AgentWorkflowInput) (WorkflowResult, error) {
	signalCh := workflow.GetSignalChannel(ctx, AgentCommandSignal)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumInterval: time.Minute,
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

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
	var prepResult PrepareOutput
	if err := workflow.ExecuteActivity(ctx, PrepareActivityName, PrepareInput{
		SystemPrompt: input.SystemPrompt,
		Message:      input.Message,
		History:      input.History,
		Tools:        input.Tools,
		Config:       input.Config,
		MCPServerConfigs: input.MCPServerConfigs,
	}).Get(ctx, &prepResult); err != nil {
		status.LifecycleState = "failed"
		return WorkflowResult{
			RunID:          input.RunID,
			LifecycleState: status.LifecycleState,
			Step:           status.Step,
			CompletedAt:    workflow.Now(ctx),
		}, err
	}

	messages := prepResult.Messages
	tools := prepResult.Tools

	if !prepResult.CanProceed() {
		status.LifecycleState = "failed"
		return WorkflowResult{
			RunID:          input.RunID,
			LifecycleState: status.LifecycleState,
			Step:           status.Step,
			CompletedAt:    workflow.Now(ctx),
		}, fmt.Errorf("agent workflow - prep checks failed: billing=%v limits=%v errors=%v",
			prepResult.Billing, prepResult.Limits, prepResult.Errors)
	}

	// Track base time for Continue-As-New wall-clock check
	baseRequestedAt := input.Continuation.InitialRequestedAt
	if baseRequestedAt.IsZero() {
		baseRequestedAt = workflow.Now(ctx)
	}

	// Agent loop
	for round := 0; round < maxToolRounds; round++ {
		if cancelled, _ := checkStreamSignal(signalCh, ctx); cancelled {
			status.LifecycleState = "cancelled"
			return WorkflowResult{
				RunID:          status.RunID,
				LifecycleState: status.LifecycleState,
				Step:           status.Step,
				CompletedAt:    workflow.Now(ctx),
			}, nil
		}

		// Repair tool call pairing before LLM call — the workflow owns
		// message lifecycle, ensuring no orphaned tool results or
		// unanswered tool calls leak through to the model.
		messages = entity.RepairToolCallPairing(messages)

		// Single sync LLM call
		var llmResult LLMStepOutput
		if err := workflow.ExecuteActivity(ctx, LLMStepActivityName, LLMStepInput{
			RunID:    input.RunID,
			Messages: messages,
			Tools:    tools,
			Config:   input.Config,
		}).Get(ctx, &llmResult); err != nil {
			status.LifecycleState = "failed"
			return WorkflowResult{
				RunID:          status.RunID,
				LifecycleState: status.LifecycleState,
				Step:           status.Step,
				CompletedAt:    workflow.Now(ctx),
			}, fmt.Errorf("agent workflow - llm round %d: %w", round, err)
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
			status.LifecycleState = "completed"
			return WorkflowResult{
				RunID:          status.RunID,
				LifecycleState: status.LifecycleState,
				Step:           status.Step,
				CompletedAt:    workflow.Now(ctx),
			}, nil
		}

		// Execute each tool call
		for _, tc := range llmResult.ToolCalls {
			if cancelled, _ := checkStreamSignal(signalCh, ctx); cancelled {
				status.LifecycleState = "cancelled"
				return WorkflowResult{
					RunID:          status.RunID,
					LifecycleState: status.LifecycleState,
					Step:           status.Step,
					CompletedAt:    workflow.Now(ctx),
				}, nil
			}

			var toolResult ToolOutput
			if err := workflow.ExecuteActivity(ctx, ToolExecActivityName, ToolInput{
				RunID:      input.RunID,
				ToolCallID: tc.ID,
				ToolName:   tc.Function.Name,
				Args:       parseArgsJSON(tc.Function.Arguments),
			}).Get(ctx, &toolResult); err != nil {
				status.LifecycleState = "failed"
				return WorkflowResult{
					RunID:          status.RunID,
					LifecycleState: status.LifecycleState,
					Step:           status.Step,
					CompletedAt:    workflow.Now(ctx),
				}, fmt.Errorf("agent workflow - tool exec %s: %w", tc.Function.Name, err)
			}

			toolMsg := entity.Message{
				Role:       entity.RoleTool,
				ToolCallID: tc.ID,
				Content:    toolResult.Output,
			}
			messages = append(messages, toolMsg)
		}

		// Evaluate Continue-As-New after each round
		decision := EvaluateContinueAsNew(input.ContinuePolicy, ContinueAsNewSnapshot{
			HistoryLength:   len(messages),
			StateSizeBytes:  0, // not tracked at workflow level
			Step:            status.Step,
			Elapsed:         workflow.Now(ctx).Sub(baseRequestedAt),
			ContinuationCnt: input.Continuation.ContinuationCount,
		})
		if decision.ShouldContinue {
			payload := ContinuationPayload{
				RunID:              status.RunID,
				ContinuationCount:  input.Continuation.ContinuationCount + 1,
				PreviousWorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
				CarriedStep:        status.Step,
				CarriedAt:          workflow.Now(ctx),
				InitialRequestedAt: baseRequestedAt,
			}
			nextInput := input
			nextInput.Continuation = payload
			return WorkflowResult{}, workflow.NewContinueAsNewError(ctx, AgentWorkflow, nextInput)
		}
	}

	status.LifecycleState = "completed"
	return WorkflowResult{
		RunID:          status.RunID,
		LifecycleState: status.LifecycleState,
		Step:           status.Step,
		CompletedAt:    workflow.Now(ctx),
	}, nil
}
