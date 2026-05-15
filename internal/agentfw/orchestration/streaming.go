package orchestration

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const maxToolRounds = 10

// StreamAgentWorkflow is the streaming agent workflow with step-level activities.
// Each LLM call and tool execution is a separate Temporal activity, providing
// full visibility into the agent loop via workflow history.
//
// Signals (agent-command):
//   - "cancel": terminates the workflow immediately
//   - "pause":   blocks until "resume" or "cancel"
//   - "resume":  exits the pause loop
func StreamAgentWorkflow(ctx workflow.Context, input InitStreamInput) error {
	signalCh := workflow.GetSignalChannel(ctx, AgentCommandSignal)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumInterval: time.Minute,
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Init phase: write start events + Prep
	var initResult InitStreamOutput
	if err := workflow.ExecuteActivity(ctx, InitStreamActivityName, input).Get(ctx, &initResult); err != nil {
		return fmt.Errorf("stream workflow - init: %w", err)
	}

	messages := initResult.Messages
	tools := initResult.Tools

	// Agent loop
	for round := 0; round < maxToolRounds; round++ {
		if cancelled, _ := checkStreamSignal(signalCh, ctx); cancelled {
			return nil
		}

		// Repair tool call pairing before LLM call — the workflow owns
		// message lifecycle for the streaming path as well.
		messages = entity.RepairToolCallPairing(messages)

		// Streaming LLM call — writes delta events to EventStore
		var llmResult LLMStreamOutput
		if err := workflow.ExecuteActivity(ctx, LLMStreamActivityName, LLMStreamInput{
			AccountID: input.AccountID,
			SessionID: input.SessionID,
			RunID:     input.RunID,
			Messages:  messages,
			Tools:     tools,
			Config:    input.Config,
		}).Get(ctx, &llmResult); err != nil {
			return fmt.Errorf("stream workflow - llm round %d: %w", round, err)
		}

		// Track messages for next round
		assistantMsg := entity.Message{Role: entity.RoleAssistant}
		if len(llmResult.ToolCalls) > 0 {
			assistantMsg.ToolCalls = llmResult.ToolCalls
		}
		messages = append(messages, assistantMsg)

		if len(llmResult.ToolCalls) == 0 {
			var usage *entity.Usage
			if llmResult.Usage.TotalTokens > 0 {
				usage = &llmResult.Usage
			}
			_ = workflow.ExecuteActivity(ctx, FinishStreamActivityName, FinishStreamInput{
				SessionID: input.SessionID,
				RunID:     input.RunID,
				Event:     entity.NewAgentRunFinishEvent(llmResult.FinishReason, usage),
			}).Get(ctx, nil)
			return nil
		}

		// Execute each tool call
		for _, tc := range llmResult.ToolCalls {
			if cancelled, _ := checkStreamSignal(signalCh, ctx); cancelled {
				return nil
			}

			var toolResult ToolOutput
			if err := workflow.ExecuteActivity(ctx, ToolExecStreamActivityName, ToolInput{
				RunID:      input.RunID,
				ToolCallID: tc.ID,
				ToolName:   tc.Function.Name,
				Args:       parseArgsJSON(tc.Function.Arguments),
			}).Get(ctx, &toolResult); err != nil {
				return fmt.Errorf("stream workflow - tool exec %s: %w", tc.Function.Name, err)
			}

			toolMsg := entity.Message{
				Role:       entity.RoleTool,
				ToolCallID: tc.ID,
				Content:    toolResult.Output,
			}
			messages = append(messages, toolMsg)
		}
	}

	_ = workflow.ExecuteActivity(ctx, FinishStreamActivityName, FinishStreamInput{
		SessionID: input.SessionID,
		RunID:     input.RunID,
		Event:     entity.NewAgentRunFinishEvent("max_rounds", nil),
	}).Get(ctx, nil)
	return nil
}

// checkStreamSignal does a non-blocking read on the signal channel.
// Returns true if the workflow should cancel.
func checkStreamSignal(signalCh workflow.ReceiveChannel, ctx workflow.Context) (bool, error) {
	var signal string
	if ok := signalCh.ReceiveAsync(&signal); !ok {
		return false, nil
	}
	switch signal {
	case "cancel":
		return true, nil
	case "pause":
		return waitForResume(signalCh, ctx)
	case "resume":
		return false, nil
	}
	return false, nil
}

// waitForResume blocks the workflow until "resume" or "cancel" signal arrives.
func waitForResume(signalCh workflow.ReceiveChannel, ctx workflow.Context) (bool, error) {
	for {
		var s string
		signalCh.Receive(ctx, &s)
		switch s {
		case "resume":
			return false, nil
		case "cancel":
			return true, nil
		}
	}
}

// parseArgsJSON parses a JSON object string into a map for tool execution.
// This is called in workflow code (json.Unmarshal is deterministic on map[string]any).
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
