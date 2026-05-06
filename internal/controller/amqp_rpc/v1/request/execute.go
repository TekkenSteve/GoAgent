package request

// Execute -.
type Execute struct {
	RunID          string `json:"run_id"          validate:"required" example:"run-550e8400-e29b-41d4-a716-446655440000"`
	ThreadID        string `json:"thread_id"                                    example:"thread-550e8400-e29b-41d4-a716-446655440000"`
	ProjectID       string `json:"project_id"                                   example:"proj-550e8400-e29b-41d4-a716-446655440000"`
	AccountID       string `json:"account_id"      validate:"required" example:"acct-550e8400-e29b-41d4-a716-446655440000"`
	ModelRef        string `json:"model_ref"                                     example:"gpt-4.1-mini"`
	AgentID         string `json:"agent_id"                                       example:"agent-550e8400-e29b-41d4-a716-446655440000"`
	AgentVersionID  string `json:"agent_version_id"                              example:"v1"`
	ToolSchemaVer   string `json:"tool_schema_ver"                                example:"v1"`
	UserMessage     string `json:"user_message"      validate:"required" example:"Hello, can you help me?"`
	IsNewThread     bool   `json:"is_new_thread"                                 example:"false"`
	BypassAdmission bool   `json:"bypass_admission"                              example:"false"`
	IdempotencyKey  string `json:"idempotency_key"                               example:"idem-550e8400-e29b-41d4-a716-446655440000"`
	EventSchemaVer  string `json:"event_schema_ver"                               example:"v1"`
	WorkflowVersion int    `json:"workflow_version"                               example:"1"`
}
