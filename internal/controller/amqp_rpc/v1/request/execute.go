package request

// Execute -.
type Execute struct {
	RunID          string `json:"run_id"          validate:"required"`
	ThreadID        string `json:"thread_id"`
	ProjectID       string `json:"project_id"`
	AccountID       string `json:"account_id"      validate:"required"`
	ModelRef        string `json:"model_ref"`
	AgentID         string `json:"agent_id"`
	AgentVersionID  string `json:"agent_version_id"`
	ToolSchemaVer   string `json:"tool_schema_ver"`
	UserMessage     string `json:"user_message"      validate:"required"`
	IsNewThread     bool   `json:"is_new_thread"`
	BypassAdmission bool   `json:"bypass_admission"`
	IdempotencyKey  string `json:"idempotency_key"`
	EventSchemaVer  string `json:"event_schema_ver"`
	WorkflowVersion int    `json:"workflow_version"`
}
