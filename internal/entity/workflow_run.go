package entity

import "time"

// WorkflowRun models a single agent workflow execution run.
type WorkflowRun struct {
	RunID             string
	ThreadID          string
	ProjectID         string
	AccountID         string
	ModelRef          string
	AgentID           string
	AgentVersionID    string
	ToolSchemaVersion string
	AgentConfigVer    string
	Step              int32
	Lifecycle         LifecycleState
	TerminationReason string

	PendingToolCalls []ToolCallRef
	AutoContinue     AutoContinueState
	Control          ControlState
	Continuation     ContinuationState
}

// LifecycleState models the deterministic run lifecycle inside workflow logic.
type LifecycleState string

const (
	LifecycleCreated   LifecycleState = "created"
	LifecycleRunning   LifecycleState = "running"
	LifecyclePaused    LifecycleState = "paused"
	LifecycleResumed   LifecycleState = "resumed"
	LifecycleCompleted LifecycleState = "completed"
	LifecycleFailed    LifecycleState = "failed"
	LifecycleCancelled LifecycleState = "cancelled"
)

// ToolCallRef stores deterministic metadata about pending tool execution.
type ToolCallRef struct {
	ToolCallID     string
	ToolName       string
	IdempotencyKey string
	ConflictDomain string
	ArgsDigest     string
}

// AutoContinueState is the deterministic state of continuation decisions.
type AutoContinueState struct {
	Count                 int32
	Enabled               bool
	ThreadRunID           string
	ForceToolFallback     bool
	ErrorRetryCount       int32
	ToolResultTokenBudget int32
}

// ControlState contains startup-only flags carried for compatibility.
type ControlState struct {
	IsNewThread     bool
	BypassAdmission bool
}

// ContinuationState carries run continuity metadata across Continue-As-New.
type ContinuationState struct {
	ContinuationCount  int32
	PreviousWorkflowID string
	CarriedAt          time.Time
}
