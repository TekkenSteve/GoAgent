package orchestration

import "time"

// ExecuteRequest is the entry payload for starting an agent run.
type ExecuteRequest struct {
	RunID           string
	ThreadID        string
	ProjectID       string
	AccountID       string
	ModelRef        string
	AgentID         string
	AgentVersionID  string
	AgentConfigVer  string
	ToolSchemaVer   string
	UserMessage     string
	IsNewThread     bool
	BypassAdmission bool
	IdempotencyKey  string
	EventSchemaVer  string
	WorkflowVersion int
	RequestedAt     time.Time
}

// RunIdentifiers are propagated across workflow, logs, events, and storage.
type RunIdentifiers struct {
	RunID          string
	WorkflowID     string
	ThreadRunID    string
	IdempotencyKey string
}

// RunStatus is a stable status payload for query and stream layers.
type RunStatus struct {
	RunID          string
	LifecycleState string
	Step           int32
	Reason         string
	UpdatedAt      time.Time
}

// ControlOperation defines an external workflow control intent.
type ControlOperation string

const (
	ControlPause  ControlOperation = "pause"
	ControlResume ControlOperation = "resume"
	ControlCancel ControlOperation = "cancel"
)

// ControlSignal names.
const (
	SignalPause  = "agentfw.control.pause"
	SignalResume = "agentfw.control.resume"
	SignalCancel = "agentfw.control.cancel"
)

// Query names.
const (
	QueryRunStatus = "agentfw.query.run-status"
)
