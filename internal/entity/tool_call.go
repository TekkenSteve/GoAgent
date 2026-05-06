package entity

// Tier is a subscription tier used by tool authorization policy.
type Tier string

const (
	TierFree       Tier = "free"
	TierPro        Tier = "pro"
	TierEnterprise Tier = "enterprise"
)

// ToolRequest describes a single tool call request.
type ToolRequest struct {
	RunID          string
	ToolCallID     string
	ToolName       string
	AccountID      string
	ProjectID      string
	Tier           Tier
	IdempotencyKey string
	ConflictDomain string
	SideEffecting  bool
	Args           map[string]any
}

// ToolResult is normalized tool execution output.
type ToolResult struct {
	RunID              string
	ToolCallID         string
	ToolName           string
	Output             map[string]any
	PersistedRef       string
	FromIdempotent     bool
	Attempts           int
	ExecutionIsolation ExecutionIsolation
}

// ExecutionIsolation indicates the execution isolation semantics.
type ExecutionIsolation string

const (
	IsolationNone          ExecutionIsolation = "none"
	IsolationReadCommitted ExecutionIsolation = "read_committed"
	IsolationSerializable  ExecutionIsolation = "serializable"
)

// ToolResultRecord is a warm-state normalized tool output payload.
type ToolResultRecord struct {
	RunID      string
	ToolCallID string
	ToolName   string
	ResultJSON string
}

// ArchiveRecord is a cold-state archival payload descriptor.
type ArchiveRecord struct {
	RunID       string
	PayloadType string
	Content     []byte
}
