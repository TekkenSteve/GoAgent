package orchestration

import (
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ——— Signal & query names ———

const (
	OrchestrationWorkflowName = "agentfw.orchestration-workflow"
	StepModifySignal          = "step-modify"
	ExternalEventSignal       = "external-event"
)

// ——— OrchestrationWorkflow ———

// OrchestrationWorkflow is the generic step-queue interpreter (Layer 2).
// It accepts an initial step queue (either directly or via TeamSpec expansion
// performed by the caller), then loops through the queue executing each step
// according to its type. The queue can be modified at runtime via Signal or
// OnResult callbacks, supporting dynamic planning and self-modifying workflows.
//
// Signals:
//   - agent-command: "cancel" / "pause" / "resume"
//   - step-modify:   entity.StepMutation payload to modify the queue
//   - external-event: payload delivered to a waiting StepWait
//
// Query:
//   - query-run-status: returns OrchestrationStatus
func OrchestrationWorkflow(ctx workflow.Context, input entity.OrchestrationInput) (*entity.OrchestrationResult, error) {
	// ——— Signal channels ———
	cmdCh := workflow.GetSignalChannel(ctx, AgentCommandSignal)
	modifyCh := workflow.GetSignalChannel(ctx, StepModifySignal)
	eventCh := workflow.GetSignalChannel(ctx, ExternalEventSignal)

	// ——— Activity options ———
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// ——— Resolve step queue ———
	steps := input.Steps
	if len(steps) == 0 && input.TeamSpec != nil {
		// TeamSpec expansion must happen before workflow start.
		// This workflow receives pre-expanded steps.
		return nil, temporal.NewApplicationError(
			"TeamSpec must be expanded before starting OrchestrationWorkflow; use ExpandSteps",
			"validation",
		)
	}

	if len(steps) == 0 {
		steps = append(steps, entity.Step{
			ID:     "default",
			Type:   entity.StepAgent,
			Name:   "default",
			Input:  map[string]any{"message": input.Message},
			Status: entity.StepPending,
		})
	}

	status := OrchestrationStatus{
		RunID:   input.RunID,
		Round:   0,
		State:   "running",
	}

	if err := workflow.SetQueryHandler(ctx, "query-run-status", func() (OrchestrationStatus, error) {
		return status, nil
	}); err != nil {
		return nil, err
	}

	maxRounds := 1000
	if input.ContinuePolicy.MaxRounds > 0 {
		maxRounds = input.ContinuePolicy.MaxRounds
	}

	// ——— Main loop ———
	for round := 0; round < maxRounds; round++ {
		status.Round = int32(round)

		// 1. Check control signal (cancel/pause/resume)
		if done := handleControlSignal(cmdCh, ctx); done {
			return buildResult(input.RunID, steps, "cancelled"), nil
		}

		// 2. Check step-modify signal (non-blocking)
		var mod entity.StepMutation
		if ok := modifyCh.ReceiveAsync(&mod); ok {
			steps = applyStepMutation(steps, mod)
		}

		// 3. Check external-event signal (non-blocking, delivered to waiting steps)
		var extEvent string
		if ok := eventCh.ReceiveAsync(&extEvent); ok {
			deliverEventToWaitingSteps(&steps, extEvent)
		}

		// 4. Find next ready step
		idx := findReadyStepIndex(steps)
		if idx < 0 {
			// No ready step — check if we're done or blocked
			if allStepsTerminal(steps) {
				break
			}
			// Some steps are in-flight (running/waiting): yield and retry
			// Use Timer to yield the workflow task without blocking.
			_ = workflow.Sleep(ctx, 10*time.Millisecond)
			continue
		}

		// 5. Execute the step
		step := &steps[idx]
		result, err := executeStep(ctx, step, cmdCh, eventCh)
		if err != nil {
			step.Status = entity.StepFailed
			step.Error = err.Error()
			// Continue with next step instead of failing the whole workflow
			continue
		}

		step.Status = entity.StepCompleted
		if result != nil {
			step.Result = result.Data

			// 6. Apply OnResult mutation
			if result.Mutation != nil {
				steps = applyStepMutation(steps, *result.Mutation)
			}
		}

		// 7. Check if queue is empty of pending work
		if !hasPendingWork(steps) {
			break
		}
	}

	return buildResult(input.RunID, steps, "completed"), nil
}

// ——— Step execution ———

type stepExecResult struct {
	Data     map[string]any
	Mutation *entity.StepMutation
}

func executeStep(
	ctx workflow.Context,
	step *entity.Step,
	cmdCh workflow.ReceiveChannel,
	eventCh workflow.ReceiveChannel,
) (*stepExecResult, error) {
	switch step.Type {
	case entity.StepAgent:
		return executeAgentStep(ctx, step)
	case entity.StepTool:
		return executeToolStep(ctx, step)
	case entity.StepWait:
		return executeWaitStep(ctx, step, eventCh)
	case entity.StepSplit:
		return executeSplitStep(ctx, step)
	case entity.StepJoin:
		return executeJoinStep(ctx, step)
	case entity.StepEval:
		return executeEvalStep(ctx, step)
	default:
		return nil, fmt.Errorf("unknown step type: %s", step.Type)
	}
}

func executeAgentStep(ctx workflow.Context, step *entity.Step) (*stepExecResult, error) {
	msg, _ := step.Input["message"].(string)
	systemPrompt, _ := step.Input["system_prompt"].(string)

	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("agentfw-orch-%s-%s", step.ID, workflow.GetInfo(ctx).WorkflowExecution.RunID),
	})

	var result WorkflowResult
	err := workflow.ExecuteChildWorkflow(childCtx, AgentWorkflowName, AgentWorkflowInput{
		RunID:        step.ID,
		Message:      msg,
		SystemPrompt: systemPrompt,
	}).Get(ctx, &result)
	if err != nil {
		return nil, fmt.Errorf("agent step %q: %w", step.ID, err)
	}

	out := map[string]any{
		"run_id":          result.RunID,
		"lifecycle_state": result.LifecycleState,
		"step":            result.Step,
	}

	// Check OnResult for mutation
	mutation := step.OnResult

	return &stepExecResult{Data: out, Mutation: mutation}, nil
}

func executeToolStep(ctx workflow.Context, step *entity.Step) (*stepExecResult, error) {
	toolName := step.Tool
	if toolName == "" {
		return nil, fmt.Errorf("tool step %q: tool name is required", step.ID)
	}

	var toolResult ToolOutput
	err := workflow.ExecuteActivity(ctx, ToolExecActivityName, ToolInput{
		RunID:      step.ID,
		ToolCallID: step.ID,
		ToolName:   toolName,
		Args:       step.Input,
	}).Get(ctx, &toolResult)
	if err != nil {
		return nil, fmt.Errorf("tool step %q: %w", step.ID, err)
	}

	out := map[string]any{
		"output":      toolResult.Output,
		"exit_code":   toolResult.ExitCode,
		"is_error":    toolResult.IsError,
		"duration_ms": toolResult.DurationMs,
	}

	return &stepExecResult{Data: out, Mutation: step.OnResult}, nil
}

func executeWaitStep(
	ctx workflow.Context,
	step *entity.Step,
	eventCh workflow.ReceiveChannel,
) (*stepExecResult, error) {
	wc := step.WaitFor
	if wc == nil {
		return nil, fmt.Errorf("wait step %q: WaitFor config is required", step.ID)
	}

	// Wait for the designated signal or timeout
	var signalPayload string
	var received bool

	if wc.Timeout != nil && *wc.Timeout > 0 {
		// Use Selector for race between signal and timer
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(eventCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &signalPayload)
			received = true
		})
		sel.AddFuture(workflow.NewTimer(ctx, *wc.Timeout), func(f workflow.Future) {
			_ = f.Get(ctx, nil)
			received = false
		})
		sel.Select(ctx)
	} else {
		// Block indefinitely for signal (cancellation handled by outer loop)
		eventCh.Receive(ctx, &signalPayload)
		received = true
	}

	if !received {
		switch wc.OnTimeout {
		case "skip":
			return &stepExecResult{Data: map[string]any{"timeout": true, "action": "skip"}}, nil
		case "fail":
			return nil, fmt.Errorf("wait step %q: timeout after %v", step.ID, *wc.Timeout)
		default:
			// "timeout" or empty — continue with empty result
			return &stepExecResult{Data: map[string]any{"timeout": true, "action": "timeout"}}, nil
		}
	}

	return &stepExecResult{
		Data:     map[string]any{"signal": signalPayload, "received": true},
		Mutation: step.OnResult,
	}, nil
}

func executeSplitStep(ctx workflow.Context, step *entity.Step) (*stepExecResult, error) {
	// StepSplit creates N sub-steps from the Input.
	// The sub-steps are injected into the queue via OnResult mutation.
	rawChildren, ok := step.Input["children"]
	if !ok {
		return nil, fmt.Errorf("split step %q: 'children' in Input is required", step.ID)
	}

	children, ok := rawChildren.([]entity.Step)
	if !ok {
		return nil, fmt.Errorf("split step %q: 'children' must be []Step", step.ID)
	}

	// Mark each child with a split group and dependency on nothing (they run in parallel)
	splitGroup := step.ID
	for i := range children {
		children[i].Status = entity.StepPending
		if children[i].Input == nil {
			children[i].Input = make(map[string]any)
		}
		children[i].Input["_split_group"] = splitGroup
	}

	mutation := &entity.StepMutation{
		AppendAfter: step.ID,
		InsertSteps: children,
	}

	// Also apply the step's own OnResult if set (chain mutations)
	if step.OnResult != nil {
		// Combine: insert children first, then apply on_result
		// For simplicity, we use the split's mutation to insert children
		// and the on_result for additional modifications.
		// We return only the children mutation here; the caller applies
		// on_result separately if present.
		_ = step.OnResult // TODO: merge mutations if needed
	}

	return &stepExecResult{
		Data:     map[string]any{"split_group": splitGroup, "count": len(children)},
		Mutation: mutation,
	}, nil
}

func executeJoinStep(ctx workflow.Context, step *entity.Step) (*stepExecResult, error) {
	// StepJoin waits for all steps in a split group to complete.
	// It reads _join_group from Input which should match the split's _split_group.
	joinGroup, _ := step.Input["_join_group"].(string)
	if joinGroup == "" {
		return nil, fmt.Errorf("join step %q: '_join_group' in Input is required", step.ID)
	}

	return &stepExecResult{
		Data:     map[string]any{"join_group": joinGroup},
		Mutation: step.OnResult,
	}, nil
}

func executeEvalStep(ctx workflow.Context, step *entity.Step) (*stepExecResult, error) {
	// StepEval is a conditional branch marker.
	// The condition is evaluated by examining the step's Input and the
	// OnResult mutation determines how the queue is modified.
	// Actual condition logic should be encoded in the mutation itself
	// (e.g., different InsertSteps for different outcomes).
	condition, _ := step.Input["condition"].(string)

	result := map[string]any{
		"condition": condition,
		"evaluated": true,
	}

	return &stepExecResult{
		Data:     result,
		Mutation: step.OnResult,
	}, nil
}

// ——— Queue management ———

func findReadyStepIndex(steps []entity.Step) int {
	completed := make(map[string]bool)
	for _, s := range steps {
		if s.Status == entity.StepCompleted {
			completed[s.ID] = true
		}
	}

	for i, s := range steps {
		if s.Status != entity.StepPending {
			continue
		}
		// Check split group — if this step has a _split_group, wait for all
		// steps in the same group that appear before it.
		if s.Input != nil {
			if group, ok := s.Input["_split_group"].(string); ok && group != "" {
				// All preceding steps in the same group must be completed
				allPrecedingComplete := true
				for j := 0; j < i; j++ {
					if g, ok := steps[j].Input["_split_group"].(string); ok && g == group {
						if steps[j].Status != entity.StepCompleted && steps[j].Status != entity.StepFailed {
							allPrecedingComplete = false
							break
						}
					}
				}
				if !allPrecedingComplete {
					continue
				}
			}
		}
		// Check explicit dependencies
		if len(s.DependsOn) > 0 {
			allMet := true
			for _, dep := range s.DependsOn {
				if !completed[dep] {
					allMet = false
					break
				}
			}
			if !allMet {
				continue
			}
		}
		return i
	}
	return -1
}

func applyStepMutation(steps []entity.Step, m entity.StepMutation) []entity.Step {
	// Delete steps
	deleteSet := make(map[string]bool, len(m.DeleteSteps))
	for _, id := range m.DeleteSteps {
		deleteSet[id] = true
	}

	// Replace step
	replaceStep := ""
	if m.ModifyStep != "" {
		replaceStep = m.ModifyStep
	}

	var result []entity.Step
	for _, s := range steps {
		if deleteSet[s.ID] {
			continue
		}
		// Modify / replace
		if s.ID == replaceStep && len(m.InsertSteps) > 0 {
			s.Status = entity.StepPending
			for _, ins := range m.InsertSteps {
				repl := ins
				repl.Status = entity.StepPending
				result = append(result, repl)
			}
			continue
		}
		result = append(result, s)
	}

	// Append after specific step
	if m.AppendAfter != "" && len(m.InsertSteps) > 0 {
		var withInsert []entity.Step
		for _, s := range result {
			withInsert = append(withInsert, s)
			if s.ID == m.AppendAfter {
				for _, ins := range m.InsertSteps {
					newStep := ins
					newStep.Status = entity.StepPending
					withInsert = append(withInsert, newStep)
				}
			}
		}
		result = withInsert
	}

	return result
}

func deliverEventToWaitingSteps(steps *[]entity.Step, event string) {
	for i := range *steps {
		if (*steps)[i].Type == entity.StepWait && (*steps)[i].Status == entity.StepWaiting {
			(*steps)[i].Status = entity.StepPending
			if (*steps)[i].Result == nil {
				(*steps)[i].Result = make(map[string]any)
			}
			(*steps)[i].Result["_external_event"] = event
		}
	}
}

func hasPendingWork(steps []entity.Step) bool {
	for _, s := range steps {
		if s.Status == entity.StepPending || s.Status == entity.StepRunning || s.Status == entity.StepWaiting {
			return true
		}
	}
	return false
}

func allStepsTerminal(steps []entity.Step) bool {
	if len(steps) == 0 {
		return true
	}
	for _, s := range steps {
		switch s.Status {
		case entity.StepPending, entity.StepRunning, entity.StepWaiting:
			return false
		}
	}
	return true
}

// ——— Signal handling ———

func handleControlSignal(cmdCh workflow.ReceiveChannel, ctx workflow.Context) bool {
	var signal string
	if ok := cmdCh.ReceiveAsync(&signal); !ok {
		return false
	}
	switch signal {
	case "cancel":
		return true
	case "pause":
		waitForResume(cmdCh, ctx)
		return false
	case "resume":
		return false
	}
	return false
}

// ——— Result builder ———

func buildResult(runID string, steps []entity.Step, state string) *entity.OrchestrationResult {
	summary := map[string]any{
		"state":        state,
		"total_steps":  len(steps),
	}

	return &entity.OrchestrationResult{
		RunID:   runID,
		Steps:   steps,
		Summary: summary,
	}
}

// OrchestrationStatus is the runtime status exposed via Query handler.
type OrchestrationStatus struct {
	RunID string `json:"run_id"`
	Round int32  `json:"round"`
	State string `json:"state"`
}
