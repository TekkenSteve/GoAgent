package entity

const defaultMaxTokens = 4096

// MessageRole represents the role of a message in a conversation.
type MessageRole string // @name entity.MessageRole

const (
	// RoleUser is the message role for user input.
	RoleUser MessageRole = "user"
	// RoleAssistant is the message role for model output.
	RoleAssistant MessageRole = "assistant"
	// RoleTool is the message role for tool execution results.
	RoleTool MessageRole = "tool"
	// RoleSystem is the message role for system instructions.
	RoleSystem MessageRole = "system"
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
	Provider    string  `json:"provider,omitempty"` // target provider name; empty = use default
	Model       string  `json:"model"`
	MaxTokens   int     `json:"max_tokens"`
	Temperature float64 `json:"temperature"`
} // @name entity.LLMConfig

// DefaultLLMConfig returns a sensible baseline for LLM inference parameters.
// Callers can override individual fields per-request.
func DefaultLLMConfig() LLMConfig {
	return LLMConfig{
		Model:       "gpt-4.1-mini",
		MaxTokens:   defaultMaxTokens,
		Temperature: 0,
	}
}

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
	// FinishStop indicates the model finished generating normally.
	FinishStop FinishReason = "stop"
	// FinishToolCalls indicates the model finished because tool calls were requested.
	FinishToolCalls FinishReason = "tool_calls"
	// FinishLength indicates the model stopped because the token limit was reached.
	FinishLength FinishReason = "length"
	// FinishContentFilter indicates the model stopped because the response was filtered.
	FinishContentFilter FinishReason = "content_filter"
)

// Usage contains token usage statistics.
type Usage struct {
	PromptTokens     int   `json:"prompt_tokens"`
	CompletionTokens int   `json:"completion_tokens"`
	TotalTokens      int   `json:"total_tokens"`
	Cost             Money `json:"cost,omitempty"`
} // @name entity.Usage

// LLMRequest is a request to an LLM provider.
// Scenario specifies the usage scenario for model selection when a ScenarioRouter
// is configured; empty means use the default provider/model.
type LLMRequest struct {
	Messages []Message `json:"messages"`
	Tools    []ToolDef `json:"tools,omitempty"`
	Config   LLMConfig `json:"config"`
	Scenario string    `json:"scenario,omitempty"`
} // @name entity.LLMRequest

// LLMResponse is the response from an LLM provider.
type LLMResponse struct {
	Content      string       `json:"content"`
	ToolCalls    []ToolCall   `json:"tool_calls,omitempty"`
	FinishReason FinishReason `json:"finish_reason"`
	Usage        Usage        `json:"usage"`
} // @name entity.LLMResponse
