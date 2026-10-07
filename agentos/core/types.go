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
	// EventRunStarted is emitted when a run starts.
	EventRunStarted EventType = "run.started"
	// EventRunCompleted is emitted when a run completes successfully.
	EventRunCompleted EventType = "run.completed"
	// EventRunFailed is emitted when a run fails.
	EventRunFailed EventType = "run.failed"
	// EventRunCanceled is emitted when a run is canceled.
	EventRunCanceled EventType = "run.canceled"
	// EventRunPaused is emitted when a run pauses.
	EventRunPaused EventType = "run.paused"
	// EventRunResumed is emitted when a run resumes.
	EventRunResumed EventType = "run.resumed"
	// EventAgentStepStarted is emitted when an agent step starts.
	EventAgentStepStarted EventType = "agent.step.started"
	// EventAgentStepCompleted is emitted when an agent step completes.
	EventAgentStepCompleted EventType = "agent.step.completed"
	// EventAgentStepFailed is emitted when an agent step fails.
	EventAgentStepFailed EventType = "agent.step.failed"
	// EventAgentMessageDelta is emitted for each message delta produced by an agent.
	EventAgentMessageDelta EventType = "agent.message.delta"
	// EventAgentMessageCompleted is emitted when an agent message completes.
	EventAgentMessageCompleted EventType = "agent.message.completed"
	// EventToolCallStarted is emitted when a tool call starts.
	EventToolCallStarted EventType = "tool.call.started"
	// EventToolCallDelta is emitted for each tool call delta.
	EventToolCallDelta EventType = "tool.call.delta"
	// EventToolCallCompleted is emitted when a tool call completes.
	EventToolCallCompleted EventType = "tool.call.completed"
	// EventToolCallFailed is emitted when a tool call fails.
	EventToolCallFailed EventType = "tool.call.failed"
	// EventApprovalRequested is emitted when an approval is requested.
	EventApprovalRequested EventType = "approval.requested"
	// EventApprovalResolved is emitted when a pending approval is resolved.
	EventApprovalResolved EventType = "approval.resolved"
	// EventUsageReported is emitted when usage is reported.
	EventUsageReported EventType = "usage.reported"
	// EventCheckpointCreated is emitted when a checkpoint is created.
	EventCheckpointCreated EventType = "checkpoint.created"
	// EventArtifactCreated is emitted when an artifact is created.
	EventArtifactCreated EventType = "artifact.created"
	// EventNodeInputResolved is emitted when a node input is resolved.
	EventNodeInputResolved EventType = "node.input.resolved"
	// EventNodeOutputPublished is emitted when a node output is published.
	EventNodeOutputPublished EventType = "node.output.published"
	// EventCapabilitySelected is emitted when a capability is selected.
	EventCapabilitySelected EventType = "capability.selected"
	// EventConditionEvaluated is emitted when a condition is evaluated.
	EventConditionEvaluated EventType = "condition.evaluated"
	// EventPlanStarted is emitted when a plan starts.
	EventPlanStarted EventType = "plan.started"
	// EventPlanBlocked is emitted when a plan blocks waiting for input.
	EventPlanBlocked EventType = "plan.blocked"
	// EventPlanExpanded is emitted when a plan is expanded.
	EventPlanExpanded EventType = "plan.expanded"
	// EventPlanApproved is emitted when a plan is approved.
	EventPlanApproved EventType = "plan.approved"
	// EventPlanRejected is emitted when a plan is rejected.
	EventPlanRejected EventType = "plan.rejected"
	// EventPlanSucceeded is emitted when a plan succeeds.
	EventPlanSucceeded EventType = "plan.succeeded"
	// EventPlanFailed is emitted when a plan fails.
	EventPlanFailed EventType = "plan.failed"
	// EventPlanCanceled is emitted when a plan is canceled.
	EventPlanCanceled EventType = "plan.canceled"
	// EventPlanNodeReady is emitted when a plan node becomes ready.
	EventPlanNodeReady EventType = "plan.node.ready"
	// EventPlanNodeStarted is emitted when a plan node starts.
	EventPlanNodeStarted EventType = "plan.node.started"
	// EventPlanNodeSucceeded is emitted when a plan node succeeds.
	EventPlanNodeSucceeded EventType = "plan.node.succeeded"
	// EventPlanNodeFailed is emitted when a plan node fails.
	EventPlanNodeFailed EventType = "plan.node.failed"
	// EventPlanNodeRetryScheduled is emitted when a plan node retry is scheduled.
	EventPlanNodeRetryScheduled EventType = "plan.node.retry_scheduled"
	// EventPlanNodeSkipped is emitted when a plan node is skipped.
	EventPlanNodeSkipped EventType = "plan.node.skipped"
	// EventPlanNodeCanceled is emitted when a plan node is canceled.
	EventPlanNodeCanceled EventType = "plan.node.canceled"
	// EventProcessStarted is emitted when a process starts.
	EventProcessStarted EventType = "process.started"
	// EventProcessWaiting is emitted when a process enters the waiting state.
	EventProcessWaiting EventType = "process.waiting"
	// EventProcessBlocked is emitted when a process blocks.
	EventProcessBlocked EventType = "process.blocked"
	// EventProcessSucceeded is emitted when a process succeeds.
	EventProcessSucceeded EventType = "process.succeeded"
	// EventProcessFailed is emitted when a process fails.
	EventProcessFailed EventType = "process.failed"
	// EventProcessCanceled is emitted when a process is canceled.
	EventProcessCanceled EventType = "process.canceled"
	// EventProcessTimerScheduled is emitted when a process timer is scheduled.
	EventProcessTimerScheduled EventType = "process.timer.scheduled"
	// EventProcessTimerFired is emitted when a process timer fires.
	EventProcessTimerFired EventType = "process.timer.fired"
	// EventProcessSignalReceived is emitted when a process receives a signal.
	EventProcessSignalReceived EventType = "process.signal.received"
	// EventProcessControlReceived is emitted when a process receives a control request.
	EventProcessControlReceived EventType = "process.control.received"
)

// ArtifactKind identifies public artifact payload categories.
type ArtifactKind string

const (
	// ArtifactKindObject marks a generic structured object artifact.
	ArtifactKindObject ArtifactKind = "object"
	// ArtifactKindText marks a plain text artifact.
	ArtifactKindText ArtifactKind = "text"
	// ArtifactKindFile marks a file artifact.
	ArtifactKindFile ArtifactKind = "file"
	// ArtifactKindPatch marks a patch artifact.
	ArtifactKindPatch ArtifactKind = "patch"
	// ArtifactKindReport marks a report artifact.
	ArtifactKindReport ArtifactKind = "report"
	// ArtifactKindReference marks a reference artifact.
	ArtifactKindReference ArtifactKind = "reference"
	// ArtifactKindPlanDelta marks a plan delta artifact.
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
