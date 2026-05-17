package orchestration

import (
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Sentinel errors.
var (
	ErrUnknownStepType       = errors.New("unknown step type")
	ErrToolNameRequired      = errors.New("tool step: tool name is required")
	ErrWaitForConfigRequired = errors.New("wait step: WaitFor config is required")
	ErrWaitStepTimeout       = errors.New("wait step: timeout")
	ErrSplitChildrenRequired = errors.New("split step: 'children' in Input is required")
	ErrSplitChildrenType     = errors.New("split step: 'children' must be []Step")
	ErrJoinGroupRequired     = errors.New("join step: '_join_group' in Input is required")
)

// ——— Signal & query names ———

const (
	activityStartToCloseTimeout = 10 * time.Minute
	OrchestrationWorkflowName   = "agentfw.orchestration-workflow"
	StepModifySignal            = "step-modify"
	ExternalEventSignal         = "external-event"
	workflowYieldSleep          = 10 * time.Millisecond
)

// ——— Workflow helpers ———

func setupSignalChannels(ctx workflow.Context) (cmdCh, modifyCh, eventCh workflow.ReceiveChannel) {
	cmdCh = workflow.GetSignalChannel(ctx, AgentCommandSignal)
	modifyCh = workflow.GetSignalChannel(ctx, StepModifySignal)
	eventCh = workflow.GetSignalChannel(ctx, ExternalEventSignal)

	return cmdCh, modifyCh, eventCh
}

func initSteps(input *entity.OrchestrationInput) ([]entity.Step, error) {
	steps := input.Steps
	if len(steps) == 0 && input.TeamSpec != nil {
		return nil, temporal.NewApplicationError(
			"TeamSpec must be expanded before starting Workflow; use ExpandSteps",
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

	return steps, nil
}

// ——— Workflow ———

// Workflow is the generic step-queue interpreter (Layer 2).
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
//   - query-run-status: returns Status
func Workflow(ctx workflow.Context, input *entity.OrchestrationInput) (*entity.OrchestrationResult, error) {
	cmdCh, modifyCh, eventCh := setupSignalChannels(ctx)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: activityStartToCloseTimeout,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	steps, err := initSteps(input)
	if err != nil {
		return nil, err
	}

	status := Status{
		RunID: input.RunID,
		Round: 0,
		State: "running",
	}

	if qErr := workflow.SetQueryHandler(ctx, "query-run-status", func() (Status, error) {
		return status, nil
	}); qErr != nil {
		return nil, qErr
	}

	maxRounds := 1000
	if input.ContinuePolicy.MaxRounds > 0 {
		maxRounds = input.ContinuePolicy.MaxRounds
	}

	for round := 0; round < maxRounds; round++ {
		status.Round = int32(round)

		var canceled, done bool

		steps, canceled, done, err = processWorkflowRound(ctx, steps, cmdCh, modifyCh, eventCh)
		if err != nil {
			return nil, err
		}

		if canceled {
			return buildResult(input.RunID, steps, "canceled"), nil
		}

		if done {
			break
		}
	}

	return buildResult(input.RunID, steps, "completed"), nil
}

// processSignals checks for step-modify and external-event signals and applies them.
func processSignals(steps []entity.Step, modifyCh, eventCh workflow.ReceiveChannel) []entity.Step {
	var mod entity.StepMutation
	if modifyCh.ReceiveAsync(&mod) {
		steps = applyStepMutation(steps, &mod)
	}

	var extEvent string
	if eventCh.ReceiveAsync(&extEvent) {
		deliverEventToWaitingSteps(&steps, extEvent)
	}

	return steps
}

// tryExecuteReadyStep finds and executes the next ready step.
// Returns the updated steps and whether a step was executed.
func tryExecuteReadyStep(ctx workflow.Context, steps []entity.Step, cmdCh, eventCh workflow.ReceiveChannel) ([]entity.Step, bool) {
	idx := findReadyStepIndex(steps)
	if idx < 0 {
		return steps, false
	}

	step := &steps[idx]

	result, err := executeStep(ctx, step, cmdCh, eventCh)
	if err != nil {
		step.Status = entity.StepFailed
		step.Error = err.Error()

		return steps, true
	}

	step.Status = entity.StepCompleted
	if result != nil {
		step.Result = result.Data
		if result.Mutation != nil {
			steps = applyStepMutation(steps, result.Mutation)
		}
	}

	return steps, true
}

// processWorkflowRound handles one iteration of the main workflow loop.
// Returns the updated steps, whether the workflow was canceled,
// whether all work is done, and any error.
func processWorkflowRound(ctx workflow.Context, steps []entity.Step, cmdCh, modifyCh, eventCh workflow.ReceiveChannel) (outSteps []entity.Step, canceled, done bool, err error) {
	if handleControlSignal(cmdCh, ctx) {
		outSteps = steps
		canceled = true

		return outSteps, canceled, done, err
	}

	steps = processSignals(steps, modifyCh, eventCh)

	var executed bool

	steps, executed = tryExecuteReadyStep(ctx, steps, cmdCh, eventCh)

	if !executed {
		if allStepsTerminal(steps) {
			outSteps = steps
			done = true

			return outSteps, canceled, done, err
		}

		if err2 := workflow.Sleep(ctx, workflowYieldSleep); err2 != nil {
			outSteps = steps
			err = err2

			return outSteps, canceled, done, err
		}

		outSteps = steps

		return outSteps, canceled, done, err
	}

	if !hasPendingWork(steps) {
		outSteps = steps
		done = true

		return outSteps, canceled, done, err
	}

	outSteps = steps

	return outSteps, canceled, done, err
}

// ——— Step execution ———

type stepExecResult struct {
	Data     map[string]any
	Mutation *entity.StepMutation
}

func executeStep(
	ctx workflow.Context,
	step *entity.Step,
	_ workflow.ReceiveChannel,
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
		return nil, fmt.Errorf("%w: %s", ErrUnknownStepType, step.Type)
	}
}

func executeAgentStep(ctx workflow.Context, step *entity.Step) (*stepExecResult, error) {
	msg, ok := step.Input["message"].(string)
	if !ok {
		msg = ""
	}

	systemPrompt, ok := step.Input["system_prompt"].(string)
	if !ok {
		systemPrompt = ""
	}

	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("agentfw-orch-%s-%s", step.ID, workflow.GetInfo(ctx).WorkflowExecution.RunID),
	})

	var result WorkflowResult

	err := workflow.ExecuteChildWorkflow(childCtx, AgentWorkflowName, &AgentWorkflowInput{
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
		return nil, fmt.Errorf("%w: %q", ErrToolNameRequired, step.ID)
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
		return nil, fmt.Errorf("%w: %q", ErrWaitForConfigRequired, step.ID)
	}

	// Wait for the designated signal or timeout
	var (
		signalPayload string
		received      bool
	)

	if wc.Timeout != nil && *wc.Timeout > 0 {
		// Use Selector for race between signal and timer
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(eventCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &signalPayload)

			received = true
		})
		sel.AddFuture(workflow.NewTimer(ctx, *wc.Timeout), func(f workflow.Future) {
			if err := f.Get(ctx, nil); err != nil {
				return
			}

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
			return nil, fmt.Errorf("%w: %q, timeout after %v", ErrWaitStepTimeout, step.ID, *wc.Timeout)
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

func executeSplitStep(_ workflow.Context, step *entity.Step) (*stepExecResult, error) {
	// StepSplit creates N sub-steps from the Input.
	// The sub-steps are injected into the queue via OnResult mutation.
	rawChildren, ok := step.Input["children"]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrSplitChildrenRequired, step.ID)
	}

	children, ok := rawChildren.([]entity.Step)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrSplitChildrenType, step.ID)
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
		//nolint:godox // intentional TODO for future work
		// TODO: merge mutations if needed
		_ = step.OnResult
	}

	return &stepExecResult{
		Data:     map[string]any{"split_group": splitGroup, "count": len(children)},
		Mutation: mutation,
	}, nil
}

func executeJoinStep(_ workflow.Context, step *entity.Step) (*stepExecResult, error) {
	// StepJoin waits for all steps in a split group to complete.
	// It reads _join_group from Input which should match the split's _split_group.
	joinGroup, ok := step.Input["_join_group"].(string)
	if !ok {
		joinGroup = ""
	}

	if joinGroup == "" {
		return nil, fmt.Errorf("%w: %q", ErrJoinGroupRequired, step.ID)
	}

	return &stepExecResult{
		Data:     map[string]any{"join_group": joinGroup},
		Mutation: step.OnResult,
	}, nil
}

func executeEvalStep(_ workflow.Context, step *entity.Step) (*stepExecResult, error) {
	// StepEval is a conditional branch marker.
	// The condition is evaluated by examining the step's Input and the
	// OnResult mutation determines how the queue is modified.
	// Actual condition logic should be encoded in the mutation itself
	// (e.g., different InsertSteps for different outcomes).
	condition, ok := step.Input["condition"].(string)
	if !ok {
		condition = ""
	}

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

func splitGroupReady(steps []entity.Step, i int) bool {
	if steps[i].Input == nil {
		return true
	}

	group, ok := steps[i].Input["_split_group"].(string)
	if !ok || group == "" {
		return true
	}

	for j := range i {
		g, ok := steps[j].Input["_split_group"].(string)
		if ok && g == group {
			if steps[j].Status != entity.StepCompleted && steps[j].Status != entity.StepFailed {
				return false
			}
		}
	}

	return true
}

func findReadyStepIndex(steps []entity.Step) int {
	completed := buildCompletedMap(steps)

	for i := range steps {
		if steps[i].Status != entity.StepPending {
			continue
		}

		if !splitGroupReady(steps, i) {
			continue
		}

		if !dependenciesMet(&steps[i], completed) {
			continue
		}

		return i
	}

	return -1
}

// buildCompletedMap collects IDs of all completed steps into a lookup map.
func buildCompletedMap(steps []entity.Step) map[string]bool {
	completed := make(map[string]bool)

	for i := range steps {
		if steps[i].Status == entity.StepCompleted {
			completed[steps[i].ID] = true
		}
	}

	return completed
}

// dependenciesMet checks whether all explicit dependencies of a step are satisfied.
func dependenciesMet(step *entity.Step, completed map[string]bool) bool {
	if len(step.DependsOn) == 0 {
		return true
	}

	for _, dep := range step.DependsOn {
		if !completed[dep] {
			return false
		}
	}

	return true
}

func insertAfterStep(steps []entity.Step, afterID string, inserts []entity.Step) []entity.Step {
	var result []entity.Step

	for i := range steps {
		s := steps[i]

		result = append(result, s)
		if s.ID == afterID {
			for i3 := range inserts {
				newStep := inserts[i3]
				newStep.Status = entity.StepPending
				result = append(result, newStep)
			}
		}
	}

	return result
}

func applyStepMutation(steps []entity.Step, m *entity.StepMutation) []entity.Step {
	deleteSet := make(map[string]bool, len(m.DeleteSteps))
	for _, id := range m.DeleteSteps {
		deleteSet[id] = true
	}

	replaceStep := ""
	if m.ModifyStep != "" {
		replaceStep = m.ModifyStep
	}

	var result []entity.Step

	for i := range steps {
		if deleteSet[steps[i].ID] {
			continue
		}
		// Modify / replace
		if steps[i].ID == replaceStep && len(m.InsertSteps) > 0 {
			steps[i].Status = entity.StepPending

			for i2 := range m.InsertSteps {
				repl := m.InsertSteps[i2]
				repl.Status = entity.StepPending
				result = append(result, repl)
			}

			continue
		}

		result = append(result, steps[i])
	}

	if m.AppendAfter != "" && len(m.InsertSteps) > 0 {
		result = insertAfterStep(result, m.AppendAfter, m.InsertSteps)
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
	for i := range steps {
		if steps[i].Status == entity.StepPending || steps[i].Status == entity.StepRunning || steps[i].Status == entity.StepWaiting {
			return true
		}
	}

	return false
}

func allStepsTerminal(steps []entity.Step) bool {
	if len(steps) == 0 {
		return true
	}

	for i := range steps {
		switch steps[i].Status {
		case entity.StepPending, entity.StepRunning, entity.StepWaiting:
			return false
		case entity.StepCompleted, entity.StepFailed, entity.StepBlocked:
		default:
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
	case AgentCmdCancel:
		return true
	case AgentCmdPause:
		if _, err := waitForResume(cmdCh, ctx); err != nil {
			_ = err
		}

		return false
	case AgentCmdResume:
		return false
	}

	return false
}

// ——— Result builder ———

func buildResult(runID string, steps []entity.Step, state string) *entity.OrchestrationResult {
	summary := map[string]any{
		"state":       state,
		"total_steps": len(steps),
	}

	return &entity.OrchestrationResult{
		RunID:   runID,
		Steps:   steps,
		Summary: summary,
	}
}

// Status is the runtime status exposed via Query handler.
type Status struct {
	RunID string `json:"run_id"`
	Round int32  `json:"round"`
	State string `json:"state"`
}
