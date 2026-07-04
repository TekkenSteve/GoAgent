package core

import (
	"encoding/json"
	"time"
)

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
	ProcessID string         `json:"process_id,omitempty"`
	ThreadID  string         `json:"thread_id,omitempty"`
	Sequence  int64          `json:"sequence,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
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
	EventProcessStarted         EventType = "process.started"
	EventProcessWaiting         EventType = "process.waiting"
	EventProcessBlocked         EventType = "process.blocked"
	EventProcessSucceeded       EventType = "process.succeeded"
	EventProcessFailed          EventType = "process.failed"
	EventProcessCanceled        EventType = "process.canceled"
	EventProcessTimerScheduled  EventType = "process.timer.scheduled"
	EventProcessTimerFired      EventType = "process.timer.fired"
	EventProcessSignalReceived  EventType = "process.signal.received"
	EventProcessControlReceived EventType = "process.control.received"
)

// ArtifactKind identifies public artifact payload categories.
type ArtifactKind string

const (
	ArtifactKindObject    ArtifactKind = "object"
	ArtifactKindText      ArtifactKind = "text"
	ArtifactKindFile      ArtifactKind = "file"
	ArtifactKindPatch     ArtifactKind = "patch"
	ArtifactKindReport    ArtifactKind = "report"
	ArtifactKindReference ArtifactKind = "reference"
	ArtifactKindPlanDelta ArtifactKind = "plan_delta"
)

// ArtifactRef points to an artifact outside Temporal workflow history.
type ArtifactRef struct {
	ArtifactID string            `json:"artifact_id"`
	PlanID     string            `json:"plan_id,omitempty"`
	NodeID     string            `json:"node_id,omitempty"`
	RunID      string            `json:"run_id,omitempty"`
	Name       string            `json:"name"`
	Kind       ArtifactKind      `json:"kind"`
	MediaType  string            `json:"media_type,omitempty"`
	URI        string            `json:"uri,omitempty"`
	SizeBytes  int64             `json:"size_bytes,omitempty"`
	Digest     string            `json:"digest,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	CreatedAt  time.Time         `json:"created_at,omitzero" schema:"optional"`
}

// Artifact is the public artifact document returned by ArtifactStore-backed
// control-plane APIs. Payloads are never embedded in Temporal workflow history.
type Artifact struct {
	Ref     ArtifactRef `json:"ref"`
	Payload any         `json:"payload,omitempty"`
}

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

// Subscription is a stream of run events.
type Subscription interface {
	Events() <-chan Event
	Close() error
}
