package entity

import "time"

// WorkflowRun models a single agent workflow execution run.
type WorkflowRun struct {
	RunID             string            `json:"run_id"              example:"run-550e8400-e29b-41d4-a716-446655440000"`
	ThreadID          string            `json:"thread_id"            example:"thread-550e8400-e29b-41d4-a716-446655440000"`
	ProjectID         string            `json:"project_id"           example:"proj-550e8400-e29b-41d4-a716-446655440000"`
	AccountID         string            `json:"account_id"           example:"acct-550e8400-e29b-41d4-a716-446655440000"`
	ModelRef          string            `json:"model_ref"            example:"gpt-4.1-mini"`
	AgentID           string            `json:"agent_id"             example:"agent-550e8400-e29b-41d4-a716-446655440000"`
	AgentVersionID    string            `json:"agent_version_id"     example:"v1"`
	ToolSchemaVersion string            `json:"tool_schema_version"  example:"v1"`
	AgentConfigVer    string            `json:"agent_config_ver"     example:"v1"`
	Step              int32             `json:"step"                 example:"3"`
	Lifecycle         LifecycleState    `json:"lifecycle"            example:"running"`
	TerminationReason string            `json:"termination_reason"   example:""`
	PendingToolCalls  []ToolCallRef     `json:"pending_tool_calls"`
	AutoContinue      AutoContinueState `json:"auto_continue"`
	Control           ControlState      `json:"control"`
	Continuation      ContinuationState `json:"continuation"`
} // @name entity.WorkflowRun

// LifecycleState models the deterministic run lifecycle inside workflow logic.
type LifecycleState string // @name entity.LifecycleState

const (
	LifecycleCreated   LifecycleState = "created"
	LifecycleRunning   LifecycleState = "running"
	LifecyclePaused    LifecycleState = "paused"
	LifecycleResumed   LifecycleState = "resumed"
	LifecycleCompleted LifecycleState = "completed"
	LifecycleFailed    LifecycleState = "failed"
	LifecycleCancelled LifecycleState = "canceled"
)

// ToolCallRef stores deterministic metadata about pending tool execution.
type ToolCallRef struct {
	ToolCallID     string `json:"tool_call_id"      example:"call-550e8400-e29b-41d4-a716-446655440000"`
	ToolName       string `json:"tool_name"         example:"web_search"`
	IdempotencyKey string `json:"idempotency_key"   example:"idem-550e8400-e29b-41d4-a716-446655440000"`
	ConflictDomain string `json:"conflict_domain"    example:"web_search"`
	ArgsDigest     string `json:"args_digest"        example:"abc123"`
} // @name entity.ToolCallRef

// AutoContinueState is the deterministic state of continuation decisions.
type AutoContinueState struct {
	Count                 int32  `json:"count"                   example:"2"`
	Enabled               bool   `json:"enabled"                 example:"true"`
	ThreadRunID           string `json:"thread_run_id"           example:"thread-run-550e8400-e29b-41d4-a716-446655440000"`
	ForceToolFallback     bool   `json:"force_tool_fallback"     example:"false"`
	ErrorRetryCount       int32  `json:"error_retry_count"       example:"0"`
	ToolResultTokenBudget int32  `json:"tool_result_token_budget" example:"4096"`
} // @name entity.AutoContinueState

// ControlState contains startup-only flags carried for compatibility.
type ControlState struct {
	IsNewThread     bool `json:"is_new_thread"     example:"false"`
	BypassAdmission bool `json:"bypass_admission" example:"false"`
} // @name entity.ControlState

// ContinuationState carries run continuity metadata across Continue-As-New.
type ContinuationState struct {
	ContinuationCount  int32     `json:"continuation_count"   example:"1"`
	PreviousWorkflowID string    `json:"previous_workflow_id" example:"agentfw-run-run-550e8400-e29b-41d4-a716-446655440000"`
	CarriedAt          time.Time `json:"carried_at"           example:"2026-01-01T00:00:00Z"`
} // @name entity.ContinuationState
