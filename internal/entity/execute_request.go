package entity

import "time"

// ExecuteRequest is the entry payload for starting an agent run.
type ExecuteRequest struct {
	RunID            string            `json:"run_id"          example:"run-550e8400-e29b-41d4-a716-446655440000"`
	ThreadID         string            `json:"thread_id"        example:"thread-550e8400-e29b-41d4-a716-446655440000"`
	ProjectID        string            `json:"project_id"       example:"proj-550e8400-e29b-41d4-a716-446655440000"`
	AccountID        string            `json:"account_id"       example:"acct-550e8400-e29b-41d4-a716-446655440000"`
	ModelRef         string            `json:"model_ref"        example:"gpt-4.1-mini"`
	AgentID          string            `json:"agent_id"         example:"agent-550e8400-e29b-41d4-a716-446655440000"`
	AgentVersionID   string            `json:"agent_version_id" example:"v1"`
	AgentConfigVer   string            `json:"agent_config_ver" example:"v1"`
	ToolSchemaVer    string            `json:"tool_schema_ver"  example:"v1"`
	SystemPrompt     string            `json:"system_prompt"    example:"You are a helpful assistant."`
	UserMessage      string            `json:"user_message"     example:"Hello, can you help me?"`
	IsNewThread      bool              `json:"is_new_thread"    example:"false"`
	BypassAdmission  bool              `json:"bypass_admission" example:"false"`
	IdempotencyKey   string            `json:"idempotency_key"  example:"idem-550e8400-e29b-41d4-a716-446655440000"`
	EventSchemaVer   string            `json:"event_schema_ver"  example:"v1"`
	WorkflowVersion  int               `json:"workflow_version"  example:"1"`
	MCPServerConfigs []MCPServerConfig `json:"mcp_server_configs,omitempty"`
	RequestedAt      time.Time         `json:"requested_at"      example:"2026-01-01T00:00:00Z"`
} // @name entity.ExecuteRequest

// RunIdentifiers are propagated across workflow, logs, events, and storage.
type RunIdentifiers struct {
	RunID          string `json:"run_id"           example:"run-550e8400-e29b-41d4-a716-446655440000"`
	WorkflowID     string `json:"workflow_id"       example:"agentfw-run-run-550e8400-e29b-41d4-a716-446655440000"`
	ThreadRunID    string `json:"thread_run_id"      example:"thread-run-550e8400-e29b-41d4-a716-446655440000"`
	IdempotencyKey string `json:"idempotency_key"   example:"idem-550e8400-e29b-41d4-a716-446655440000"`
} // @name entity.RunIdentifiers

// ControlOperation defines an external workflow control intent.
type ControlOperation string // @name entity.ControlOperation

const (
	ControlPause  ControlOperation = "pause"
	ControlResume ControlOperation = "resume"
	ControlCancel ControlOperation = "cancel"
)

// RunStatus is a stable status payload for query and stream layers.
type RunStatus struct {
	RunID          string    `json:"run_id"           example:"run-550e8400-e29b-41d4-a716-446655440000"`
	LifecycleState string    `json:"lifecycle_state"    example:"running"`
	Step           int32     `json:"step"              example:"3"`
	Reason         string    `json:"reason"            example:""`
	UpdatedAt      time.Time `json:"updated_at"         example:"2026-01-01T00:00:00Z"`
} // @name entity.RunStatus
