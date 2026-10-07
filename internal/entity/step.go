package entity

// StepType is the orchestration primitive type.
type StepType string

const (
	// StepAgent starts a child AgentWorkflow and waits for it.
	StepAgent StepType = "agent" // Start child AgentWorkflow and wait
	// StepTool executes a tool directly.
	StepTool StepType = "tool" // Execute tool directly
	// StepWait waits for a Temporal signal or timer.
	StepWait StepType = "wait" // Wait for Temporal Signal or Timer
	// StepSplit fans out into parallel sub-steps.
	StepSplit StepType = "split" // Fan-out into parallel sub-steps
	// StepJoin fans in from parallel sub-steps.
	StepJoin StepType = "join" // Fan-in from parallel sub-steps
	// StepEval branches conditionally based on a prior result.
	StepEval StepType = "eval" // Conditional branch based on prior result
)

// StepStatus is the lifecycle status of a single step.
type StepStatus string

const (
	// StepPending is the initial status of a step before execution.
	StepPending StepStatus = "pending"
	// StepRunning marks a step currently being executed.
	StepRunning StepStatus = "running"
	// StepCompleted marks a step that finished successfully.
	StepCompleted StepStatus = "completed"
	// StepFailed marks a step that failed.
	StepFailed StepStatus = "failed"
	// StepWaiting marks a step blocked on a signal or timer.
	StepWaiting StepStatus = "waiting" // blocked on signal/timer
	// StepBlocked marks a step blocked on a dependency.
	StepBlocked StepStatus = "blocked" // blocked on dependency
)

// Step is a single orchestration unit in the step queue.
type Step struct {
	ID        string         `json:"id"`
	Type      StepType       `json:"type"`
	Name      string         `json:"name"`
	AgentID   string         `json:"agent_id,omitempty"` // StepAgent
	Tool      string         `json:"tool,omitempty"`     // StepTool
	Input     map[string]any `json:"input"`
	DependsOn []string       `json:"depends_on,omitempty"` // step IDs this depends on
	WaitFor   *WaitCondition `json:"wait_for,omitempty"`   // StepWait config
	OnResult  *StepMutation  `json:"on_result,omitempty"`  // dynamic queue mutation
	Status    StepStatus     `json:"status"`
	Result    map[string]any `json:"result,omitempty"` // populated after execution
	Error     string         `json:"error,omitempty"`  // failure reason
}

// StepResult is the typed output from executing a step.
type StepResult struct {
	StepID     string         `json:"step_id"`
	StepType   StepType       `json:"step_type"`
	Data       map[string]any `json:"data,omitempty"`
	Mutation   *StepMutation  `json:"mutation,omitempty"`    // from OnResult
	EvalResult string         `json:"eval_result,omitempty"` // StepEval branch key
}
