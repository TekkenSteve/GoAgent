package entity

// Tier is a subscription tier used by tool authorization policy.
type Tier string // @name entity.Tier

const (
	TierFree       Tier = "free"
	TierPro        Tier = "pro"
	TierEnterprise Tier = "enterprise"
)

// ToolRequest describes a single tool call request.
type ToolRequest struct {
	RunID          string         `json:"run_id"           example:"run-550e8400-e29b-41d4-a716-446655440000"`
	ToolCallID     string         `json:"tool_call_id"      example:"call-550e8400-e29b-41d4-a716-446655440000"`
	ToolName       string         `json:"tool_name"         example:"web_search"`
	AccountID      string         `json:"account_id"        example:"acct-550e8400-e29b-41d4-a716-446655440000"`
	ProjectID      string         `json:"project_id"        example:"proj-550e8400-e29b-41d4-a716-446655440000"`
	Tier           Tier           `json:"tier"              example:"pro"`
	IdempotencyKey string         `json:"idempotency_key"   example:"idem-550e8400-e29b-41d4-a716-446655440000"`
	ConflictDomain string         `json:"conflict_domain"    example:"web_search"`
	SideEffecting  bool           `json:"side_effecting"    example:"true"`
	Args           map[string]any `json:"args"              example:"{\"query\":\"weather in NY\"}"`
} // @name entity.ToolRequest

// ToolResult is normalized tool execution output.
type ToolResult struct {
	RunID             string         `json:"run_id"              example:"run-550e8400-e29b-41d4-a716-446655440000"`
	ToolCallID        string         `json:"tool_call_id"         example:"call-550e8400-e29b-41d4-a716-446655440000"`
	ToolName          string         `json:"tool_name"            example:"web_search"`
	Output            map[string]any `json:"output"               example:"{\"temperature\":\"72F\"}"`
	PersistedRef      string         `json:"persisted_ref"        example:"ref-550e8400-e29b-41d4-a716-446655440000"`
	FromIdempotent    bool           `json:"from_idempotent"       example:"false"`
	Attempts          int            `json:"attempts"             example:"1"`
	ExecutionIsolation ExecutionIsolation `json:"execution_isolation" example:"read_committed"`
} // @name entity.ToolResult

// ExecutionIsolation indicates the execution isolation semantics.
type ExecutionIsolation string // @name entity.ExecutionIsolation

const (
	IsolationNone          ExecutionIsolation = "none"
	IsolationReadCommitted ExecutionIsolation = "read_committed"
	IsolationSerializable  ExecutionIsolation = "serializable"
)

// ToolResultRecord is a warm-state normalized tool output payload.
type ToolResultRecord struct {
	RunID      string `json:"run_id"       example:"run-550e8400-e29b-41d4-a716-446655440000"`
	ToolCallID string `json:"tool_call_id"  example:"call-550e8400-e29b-41d4-a716-446655440000"`
	ToolName   string `json:"tool_name"     example:"web_search"`
	ResultJSON string `json:"result_json"   example:"{\"temperature\":\"72F\"}"`
} // @name entity.ToolResultRecord

// ArchiveRecord is a cold-state archival payload descriptor.
type ArchiveRecord struct {
	RunID       string `json:"run_id"        example:"run-550e8400-e29b-41d4-a716-446655440000"`
	PayloadType string `json:"payload_type"   example:"prompt_snapshot"`
	Content     []byte `json:"content"        example:"base64-encoded-content"`
} // @name entity.ArchiveRecord
