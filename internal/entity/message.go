package entity

// MessageRole represents the role of a message in a conversation.
type MessageRole string // @name entity.MessageRole

const (
	RoleUser      MessageRole = "user"
	RoleAssistant MessageRole = "assistant"
	RoleTool      MessageRole = "tool"
	RoleSystem    MessageRole = "system"
)

// ToolCallFunction contains the function details of a tool call.
type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
} // @name entity.ToolCallFunction

// ToolCall represents an LLM tool call request.
type ToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function ToolCallFunction `json:"function"`
} // @name entity.ToolCall

// Message is a complete conversation message for LLM exchanges.
type Message struct {
	Role       MessageRole `json:"role"`
	Content    string      `json:"content,omitempty"`
	ToolCallID string      `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall  `json:"tool_calls,omitempty"`
} // @name entity.Message

// LLMConfig is the configuration for an LLM invocation.
type LLMConfig struct {
	Model       string  `json:"model"`
	MaxTokens   int     `json:"max_tokens"`
	Temperature float64 `json:"temperature"`
} // @name entity.LLMConfig

// ToolDef is a tool definition for LLM function calling.
type ToolDef struct {
	Type     string      `json:"type"`
	Function ToolFuncDef `json:"function"`
} // @name entity.ToolDef

// ToolFuncDef is the function definition within a tool.
type ToolFuncDef struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  any    `json:"parameters"`
} // @name entity.ToolFuncDef

// FinishReason is the reason an LLM finished generating.
type FinishReason string // @name entity.FinishReason

const (
	FinishStop          FinishReason = "stop"
	FinishToolCalls     FinishReason = "tool_calls"
	FinishLength        FinishReason = "length"
	FinishContentFilter FinishReason = "content_filter"
)

// Usage contains token usage statistics.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
} // @name entity.Usage

// LLMRequest is a request to an LLM provider.
type LLMRequest struct {
	Messages []Message `json:"messages"`
	Tools    []ToolDef `json:"tools,omitempty"`
	Config   LLMConfig `json:"config"`
} // @name entity.LLMRequest

// LLMResponse is the response from an LLM provider.
type LLMResponse struct {
	Content      string       `json:"content"`
	ToolCalls    []ToolCall   `json:"tool_calls,omitempty"`
	FinishReason FinishReason `json:"finish_reason"`
	Usage        Usage        `json:"usage"`
} // @name entity.LLMResponse

// MessageRecord is a warm-state message payload.
type MessageRecord struct {
	RunID   string `json:"run_id"    example:"run-550e8400-e29b-41d4-a716-446655440000"`
	Role    string `json:"role"      example:"user"`
	Content string `json:"content"    example:"Hello, can you help me?"`
} // @name entity.MessageRecord

// WarmRefs points to operational data persisted outside workflow history.
type WarmRefs struct {
	MessageStoreRef       string `json:"message_store_ref"        example:"msg-ref-550e8400-e29b-41d4-a716-446655440000"`
	ToolResultStoreRef    string `json:"tool_result_store_ref"    example:"tool-ref-550e8400-e29b-41d4-a716-446655440000"`
	PersistenceOutboxRef  string `json:"persistence_outbox_ref"   example:"outbox-550e8400-e29b-41d4-a716-446655440000"`
	ToolResultOutboxRef   string `json:"tool_result_outbox_ref"   example:"tool-outbox-550e8400-e29b-41d4-a716-446655440000"`
	RunTimingRef          string `json:"run_timing_ref"           example:"timing-550e8400-e29b-41d4-a716-446655440000"`
	InitialUserMessageRef string `json:"initial_user_message_ref" example:"init-msg-550e8400-e29b-41d4-a716-446655440000"`
	StreamChannelID       string `json:"stream_channel_id"         example:"stream-550e8400-e29b-41d4-a716-446655440000"`
} // @name entity.WarmRefs

// ColdRefs points to archival and large payload storage.
type ColdRefs struct {
	PromptSnapshotRef  string   `json:"prompt_snapshot_ref"  example:"prompt-550e8400-e29b-41d4-a716-446655440000"`
	ContextArchiveRefs []string `json:"context_archive_refs" example:"[\"archive-550e8400-e29b-41d4-a716-446655440000\"]"`
	LargePayloadRefs   []string `json:"large_payload_refs"   example:"[\"large-550e8400-e29b-41d4-a716-446655440000\"]"`
} // @name entity.ColdRefs
