package entity

// StreamEventType categorises streaming events sent to the client.
type StreamEventType string

const (
	StreamEventContent       StreamEventType = "content"
	StreamEventReasoning     StreamEventType = "reasoning"
	StreamEventToolCall      StreamEventType = "tool_call"
	StreamEventToolResult    StreamEventType = "tool_result"
	StreamEventError         StreamEventType = "error"
	StreamEventDone          StreamEventType = "done"
	StreamEventThinking      StreamEventType = "thinking"
	StreamEventPrepStage     StreamEventType = "prep_stage"
	StreamEventContextUsage  StreamEventType = "context_usage"
)

// StreamEvent is a single event yielded during streaming execution.
type StreamEvent struct {
	Type         StreamEventType `json:"type"`
	Content      string          `json:"content,omitempty"`
	Reasoning    string          `json:"reasoning,omitempty"`
	ToolName     string          `json:"tool_name,omitempty"`
	ToolInput    string          `json:"tool_input,omitempty"`
	ToolOutput   string          `json:"tool_output,omitempty"`
	Error        string          `json:"error,omitempty"`
	Usage        *Usage          `json:"usage,omitempty"`
	FinishReason string          `json:"finish_reason,omitempty"`

	// Prep stage fields
	Stage    string `json:"stage,omitempty"`    // "initializing", "ready", "summarizing"
	Progress int    `json:"progress,omitempty"` // 0-100 progress percentage

	// Context usage fields
	MessageCount   int   `json:"message_count,omitempty"`    // current message count
	MessagesBefore *int  `json:"messages_before,omitempty"`  // message count before compression
	MessagesAfter  *int  `json:"messages_after,omitempty"`   // message count after compression
	Compressed     *bool `json:"compressed,omitempty"`       // whether compression was applied
}

// LLMStreamChunk is a single chunk from an LLM streaming response.
type LLMStreamChunk struct {
	Content      string
	Reasoning    string
	ToolCalls    []ToolCall
	FinishReason FinishReason
	Usage        Usage
}

// StreamRequest is an execution request for the streaming agent path.
type StreamRequest struct {
	RunID        string
	SystemPrompt string
	Message      string
	History      []Message
	Tools        []ToolDef
	Config       LLMConfig
}
