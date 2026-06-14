package orchestration

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	maxToolRounds      = 10
	heartbeatTimeout   = 30 * time.Second
	activityMaxRetries = 3
)

// StreamAgentWorkflow is the streaming agent workflow with step-level activities.
// Each LLM call and tool execution is a separate Temporal activity, providing
// full visibility into the agent loop via workflow history.
//
// Signals (agent-command):
//   - "cancel": terminates the workflow immediately
//   - "pause":   blocks until "resume" or "cancel"
//   - "resume":  exits the pause loop
func StreamAgentWorkflow(ctx workflow.Context, input *InitStreamInput) error {
	signalCh := workflow.GetSignalChannel(ctx, AgentCommandSignal)
	ctx = setupStreamActivityOptions(ctx)

	// Init phase: write start events + Prep
	var initResult InitStreamOutput
	if err := workflow.ExecuteActivity(ctx, InitStreamActivityName, input).Get(ctx, &initResult); err != nil {
		return fmt.Errorf("stream workflow - init: %w", err)
	}

	messages := initResult.Messages
	tools := initResult.Tools

	// Separate domain tools from meta-tools so delegate_to_agent
	// is never leaked into child workflows.
	domainTools := tools
	allTools := make([]entity.ToolDef, 0, len(domainTools)+1)
	allTools = append(allTools, domainTools...)
	allTools = append(allTools, DelegateToolDef())

	// Agent loop
	for round := range maxToolRounds {
		if canceled, err := checkStreamSignal(signalCh, ctx); err != nil {
			_ = err
		} else if canceled {
			return nil
		}

		// Repair tool call pairing before LLM call — the workflow owns
		// message lifecycle for the streaming path as well.
		messages = entity.RepairToolCallPairing(messages)

		done, err := processStreamRound(ctx, signalCh, input, &messages, allTools, domainTools, round)
		if err != nil {
			return err
		}

		if done {
			return nil
		}
	}

	return finishStreamMaxRounds(ctx, input)
}

// setupStreamActivityOptions configures activity options for the streaming workflow.
func setupStreamActivityOptions(ctx workflow.Context) workflow.Context {
	ao := workflow.ActivityOptions{
		HeartbeatTimeout: heartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumInterval: time.Minute,
			MaximumAttempts: activityMaxRetries,
		},
	}

	return workflow.WithActivityOptions(ctx, ao)
}

// processStreamRound runs one LLM call and its associated tool executions.
// Returns (done, error) where done indicates the workflow should exit.
// allTools is the full tool set exposed to the LLM (including meta-tools).
// domainTools is the subset allowed to be passed to child workflows.
func processStreamRound(
	ctx workflow.Context,
	signalCh workflow.ReceiveChannel,
	input *InitStreamInput,
	messages *[]entity.Message,
	allTools []entity.ToolDef,
	domainTools []entity.ToolDef,
	round int,
) (bool, error) {
	// Streaming LLM call — writes delta events to EventStore
	var llmResult LLMStreamOutput
	if err := workflow.ExecuteActivity(ctx, LLMStreamActivityName, LLMStreamInput{
		AccountID: input.AccountID,
		SessionID: input.SessionID,
		RunID:     input.RunID,
		Messages:  *messages,
		Tools:     allTools,
		Config:    input.Config,
	}).Get(ctx, &llmResult); err != nil {
		return false, fmt.Errorf("stream workflow - llm round %d: %w", round, err)
	}

	// Track messages for next round
	assistantMsg := entity.Message{Role: entity.RoleAssistant}
	if len(llmResult.ToolCalls) > 0 {
		assistantMsg.ToolCalls = llmResult.ToolCalls
	}

	*messages = append(*messages, assistantMsg)

	if len(llmResult.ToolCalls) == 0 {
		return finishStreamRound(ctx, input, llmResult), nil
	}

	return executeStreamToolCalls(ctx, signalCh, input, messages, llmResult.ToolCalls, domainTools)
}

// executeStreamToolCalls runs tool calls for the streaming path, intercepting
// delegate_to_agent calls to spawn child workflows instead of activities.
func executeStreamToolCalls(
	ctx workflow.Context,
	signalCh workflow.ReceiveChannel,
	input *InitStreamInput,
	messages *[]entity.Message,
	toolCalls []entity.ToolCall,
	domainTools []entity.ToolDef,
) (bool, error) {
	for _, tc := range toolCalls {
		if canceled, err := checkStreamSignal(signalCh, ctx); err != nil {
			_ = err
		} else if canceled {
			return true, nil
		}

		if isDelegateToolCall(tc) {
			resultContent, err := executeDelegateTool(ctx, tc, input.Config, input.AccountID, domainTools, input.MCPServerConfigs)
			if err != nil {
				resultContent = fmt.Sprintf("Error delegating task: %v", err)
			}

			*messages = append(*messages, entity.Message{
				Role:       entity.RoleTool,
				ToolCallID: tc.ID,
				Content:    resultContent,
			})

			continue
		}

		var toolResult ToolOutput
		if err := workflow.ExecuteActivity(ctx, ToolExecStreamActivityName, ToolInput{
			RunID:      input.RunID,
			ToolCallID: tc.ID,
			ToolName:   tc.Function.Name,
			Args:       parseArgsJSON(tc.Function.Arguments),
		}).Get(ctx, &toolResult); err != nil {
			return false, fmt.Errorf("stream workflow - tool exec %s: %w", tc.Function.Name, err)
		}

		*messages = append(*messages, entity.Message{
			Role:       entity.RoleTool,
			ToolCallID: tc.ID,
			Content:    toolResult.Output,
		})
	}

	return false, nil
}

// finishStreamRound records the finish event when no tool calls remain.
func finishStreamRound(ctx workflow.Context, input *InitStreamInput, llmResult LLMStreamOutput) bool {
	var usage *entity.Usage
	if llmResult.Usage.TotalTokens > 0 {
		usage = &llmResult.Usage
	}

	if err := workflow.ExecuteActivity(ctx, FinishStreamActivityName, FinishStreamInput{
		SessionID: input.SessionID,
		RunID:     input.RunID,
		Event:     entity.NewAgentRunFinishEvent(llmResult.FinishReason, usage),
	}).Get(ctx, nil); err != nil {
		return false
	}

	return true
}

// finishStreamMaxRounds records the finish event when the max round limit is reached.
func finishStreamMaxRounds(ctx workflow.Context, input *InitStreamInput) error {
	return workflow.ExecuteActivity(ctx, FinishStreamActivityName, FinishStreamInput{
		SessionID: input.SessionID,
		RunID:     input.RunID,
		Event:     entity.NewAgentRunFinishEvent("max_rounds", nil),
	}).Get(ctx, nil)
}

// checkStreamSignal does a non-blocking read on the signal channel.
// Returns true if the workflow should cancel.
func checkStreamSignal(signalCh workflow.ReceiveChannel, ctx workflow.Context) (bool, error) {
	var signal string
	if ok := signalCh.ReceiveAsync(&signal); !ok {
		return false, nil
	}

	switch signal {
	case AgentCmdCancel:
		return true, nil
	case AgentCmdPause:
		return waitForResume(signalCh, ctx)
	case AgentCmdResume:
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
		case AgentCmdResume:
			return false, nil
		case AgentCmdCancel:
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
