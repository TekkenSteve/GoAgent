package state

import "time"

// Layer identifies the persistence layer for a field/group in the state model.
type Layer string

const (
	LayerHot  Layer = "hot"
	LayerWarm Layer = "warm"
	LayerCold Layer = "cold"
)

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

// WorkflowHotState is the minimal deterministic state kept in workflow history.
//
// NOTE: Only place replay-safe fields here.
type WorkflowHotState struct {
	RunID             string         `json:"run_id" layer:"hot"`
	ThreadID          string         `json:"thread_id" layer:"hot"`
	ProjectID         string         `json:"project_id" layer:"hot"`
	AccountID         string         `json:"account_id" layer:"hot"`
	ModelRef          string         `json:"model_ref" layer:"hot"`
	AgentID           string         `json:"agent_id" layer:"hot"`
	AgentVersionID    string         `json:"agent_version_id" layer:"hot"`
	ToolSchemaVersion string         `json:"tool_schema_version" layer:"hot"`
	AgentConfigVer    string         `json:"agent_config_version" layer:"hot"`
	Step              int32          `json:"step" layer:"hot"`
	Lifecycle         LifecycleState `json:"lifecycle" layer:"hot"`
	TerminationReason string         `json:"termination_reason,omitempty" layer:"hot"`

	PendingToolCalls []ToolCallRef     `json:"pending_tool_calls,omitempty" layer:"hot"`
	AutoContinue     AutoContinueState `json:"auto_continue" layer:"hot"`
	Control          ControlState      `json:"control" layer:"hot"`
	Continuation     ContinuationState `json:"continuation" layer:"hot"`
}

// ToolCallRef stores deterministic metadata about pending tool execution.
type ToolCallRef struct {
	ToolCallID     string `json:"tool_call_id" layer:"hot"`
	ToolName       string `json:"tool_name" layer:"hot"`
	IdempotencyKey string `json:"idempotency_key,omitempty" layer:"hot"`
	ConflictDomain string `json:"conflict_domain,omitempty" layer:"hot"`
	ArgsDigest     string `json:"args_digest,omitempty" layer:"hot"`
}

// AutoContinueState is the deterministic state of continuation decisions.
type AutoContinueState struct {
	Count                 int32  `json:"count" layer:"hot"`
	Enabled               bool   `json:"enabled" layer:"hot"`
	ThreadRunID           string `json:"thread_run_id,omitempty" layer:"hot"`
	ForceToolFallback     bool   `json:"force_tool_fallback" layer:"hot"`
	ErrorRetryCount       int32  `json:"error_retry_count" layer:"hot"`
	ToolResultTokenBudget int32  `json:"tool_result_token_budget" layer:"hot"`
}

// ControlState contains startup-only flags carried for compatibility.
type ControlState struct {
	IsNewThread     bool `json:"is_new_thread" layer:"hot"`
	BypassAdmission bool `json:"bypass_admission" layer:"hot"`
}

// ContinuationState carries run continuity metadata across Continue-As-New.
type ContinuationState struct {
	ContinuationCount  int32     `json:"continuation_count" layer:"hot"`
	PreviousWorkflowID string    `json:"previous_workflow_id,omitempty" layer:"hot"`
	CarriedAt          time.Time `json:"carried_at" layer:"hot"`
}

// WarmRefs points to operational data persisted outside workflow history.
type WarmRefs struct {
	MessageStoreRef       string `json:"message_store_ref,omitempty" layer:"warm"`
	ToolResultStoreRef    string `json:"tool_result_store_ref,omitempty" layer:"warm"`
	PersistenceOutboxRef  string `json:"persistence_outbox_ref,omitempty" layer:"warm"`
	ToolResultOutboxRef   string `json:"tool_result_outbox_ref,omitempty" layer:"warm"`
	RunTimingRef          string `json:"run_timing_ref,omitempty" layer:"warm"`
	InitialUserMessageRef string `json:"initial_user_message_ref,omitempty" layer:"warm"`
	StreamChannelID       string `json:"stream_channel_id,omitempty" layer:"warm"`
}

// ColdRefs points to archival and large payload storage.
type ColdRefs struct {
	PromptSnapshotRef  string   `json:"prompt_snapshot_ref,omitempty" layer:"cold"`
	ContextArchiveRefs []string `json:"context_archive_refs,omitempty" layer:"cold"`
	LargePayloadRefs   []string `json:"large_payload_refs,omitempty" layer:"cold"`
}
