package agentos

import (
	"encoding/json"
	"time"
)

// RunSpec describes a generic agent run without binding callers to GoAgent internals.
type RunSpec struct {
	RunID          string            `json:"run_id"`
	ThreadID       string            `json:"thread_id,omitempty"`
	AccountID      string            `json:"account_id,omitempty"`
	ProjectID      string            `json:"project_id,omitempty"`
	AgentID        string            `json:"agent_id,omitempty"`
	ModelRef       string            `json:"model_ref,omitempty"`
	SystemPrompt   string            `json:"system_prompt,omitempty"`
	UserMessage    string            `json:"user_message,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	RequestedAt    time.Time         `json:"requested_at,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Backend        BackendRef        `json:"backend"`
	Input          map[string]any    `json:"input,omitempty"`
}

// BackendKind identifies the execution substrate used by an agent backend.
type BackendKind string

const (
	BackendKindNative           BackendKind = "native"
	BackendKindTemporalExternal BackendKind = "temporal_external"
	BackendKindHTTP             BackendKind = "http"
	BackendKindGRPC             BackendKind = "grpc"
)

const (
	// BackendNameGoAgentNative is the built-in GoAgent native backend.
	BackendNameGoAgentNative = "goagent-native"
)

// BackendRef selects the backend that owns a run.
type BackendRef struct {
	Kind BackendKind `json:"kind"`
	Name string      `json:"name"`
}

// RunStatus is the public lifecycle view for a run.
type RunStatus struct {
	RunID          string          `json:"run_id"`
	LifecycleState string          `json:"lifecycle_state"`
	Step           int32           `json:"step,omitempty"`
	Progress       *RunProgress    `json:"progress,omitempty"`
	Artifacts      []ArtifactRef   `json:"artifacts,omitempty"`
	BudgetUsage    PlanBudgetUsage `json:"budget_usage,omitempty"`
	Reason         string          `json:"reason,omitempty"`
	UpdatedAt      time.Time       `json:"updated_at,omitempty"`
}

// RunProgress is a generic public progress view. Backend-specific details
// should be emitted as events or artifacts.
type RunProgress struct {
	Current int32  `json:"current,omitempty"`
	Total   int32  `json:"total,omitempty"`
	Label   string `json:"label,omitempty"`
}

// Event is the public stream event envelope.
type Event struct {
	EventID   string         `json:"event_id"`
	EventType EventType      `json:"event_type"`
	RunID     string         `json:"run_id,omitempty"`
	ThreadID  string         `json:"thread_id,omitempty"`
	Sequence  int64          `json:"sequence,omitempty"`
	Timestamp time.Time      `json:"timestamp,omitempty"`
	Source    string         `json:"source,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
}

// EventType identifies standard AgentOS public stream events.
type EventType string

const (
	EventRunStarted             EventType = "run.started"
	EventRunCompleted           EventType = "run.completed"
	EventRunFailed              EventType = "run.failed"
	EventRunCancelled           EventType = "run.cancelled"
	EventRunPaused              EventType = "run.paused"
	EventRunResumed             EventType = "run.resumed"
	EventAgentStepStarted       EventType = "agent.step.started"
	EventAgentStepCompleted     EventType = "agent.step.completed"
	EventAgentStepFailed        EventType = "agent.step.failed"
	EventAgentMessageDelta      EventType = "agent.message.delta"
	EventAgentMessageCompleted  EventType = "agent.message.completed"
	EventToolCallStarted        EventType = "tool.call.started"
	EventToolCallDelta          EventType = "tool.call.delta"
	EventToolCallCompleted      EventType = "tool.call.completed"
	EventToolCallFailed         EventType = "tool.call.failed"
	EventApprovalRequested      EventType = "approval.requested"
	EventApprovalResolved       EventType = "approval.resolved"
	EventUsageReported          EventType = "usage.reported"
	EventCheckpointCreated      EventType = "checkpoint.created"
	EventArtifactCreated        EventType = "artifact.created"
	EventNodeInputResolved      EventType = "node.input.resolved"
	EventNodeOutputPublished    EventType = "node.output.published"
	EventCapabilitySelected     EventType = "capability.selected"
	EventConditionEvaluated     EventType = "condition.evaluated"
	EventPlanStarted            EventType = "plan.started"
	EventPlanBlocked            EventType = "plan.blocked"
	EventPlanExpanded           EventType = "plan.expanded"
	EventPlanApproved           EventType = "plan.approved"
	EventPlanRejected           EventType = "plan.rejected"
	EventPlanSucceeded          EventType = "plan.succeeded"
	EventPlanFailed             EventType = "plan.failed"
	EventPlanCanceled           EventType = "plan.canceled"
	EventPlanNodeReady          EventType = "plan.node.ready"
	EventPlanNodeStarted        EventType = "plan.node.started"
	EventPlanNodeSucceeded      EventType = "plan.node.succeeded"
	EventPlanNodeFailed         EventType = "plan.node.failed"
	EventPlanNodeRetryScheduled EventType = "plan.node.retry_scheduled"
	EventPlanNodeSkipped        EventType = "plan.node.skipped"
	EventPlanNodeCanceled       EventType = "plan.node.canceled"
)

// Message is a public conversation message.
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
}

// ToolCall describes a model-requested tool invocation.
type ToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function ToolCallFunction `json:"function"`
}

// ToolCallFunction contains function-call details.
type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolDef describes a function/tool available to a model.
type ToolDef struct {
	Type     string      `json:"type"`
	Function ToolFuncDef `json:"function"`
}

// ToolFuncDef is the function definition inside a tool.
type ToolFuncDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// StreamScope selects events for a run/thread.
type StreamScope struct {
	RunID         string `json:"run_id,omitempty"`
	ThreadID      string `json:"thread_id,omitempty"`
	AfterSequence int64  `json:"after_sequence,omitempty"`
}
