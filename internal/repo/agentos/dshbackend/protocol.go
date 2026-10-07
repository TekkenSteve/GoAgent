package dshbackend

import (
	"encoding/json"
)

// ——— JSON-RPC 2.0 method names ———

const (
	methodInitialize    = "initialize"
	methodSessionPrompt = "session/prompt"
	methodShutdown      = "shutdown"
	methodSessionEvent  = "session.event"
	methodSessionStatus = "session.status"
)

// ——— JSON-RPC 2.0 wire envelope ———

// sdkMessage is one JSON-RPC 2.0 frame on the dsh SDK stdio transport. dsh
// emits newline-delimited JSON; a frame carries either a request (id + method),
// a response (id + result/error), or a notification (method without id). The
// id is echoed verbatim, so it is kept as raw JSON to survive string/number
// ambiguity.
type sdkMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *sdkError       `json:"error,omitempty"`
}

// sdkError is the JSON-RPC error object returned on a failed call.
type sdkError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// sdkResponse is the parsed outcome of a request matched by id.
type sdkResponse struct {
	Result json.RawMessage
	Error  *sdkError
}

// sdkNotification is a server-pushed notification forwarded to a session's
// listener. Params is kept raw so each notification kind decodes its own shape.
type sdkNotification struct {
	Method string
	Params json.RawMessage
}

// ——— Session notifications ———

// sessionEventNotification is the params of a session.event notification: a
// SessionEvent envelope keyed to a session.
type sessionEventNotification struct {
	SessionID string       `json:"sessionId"`
	Event     sessionEvent `json:"event"`
}

// sessionStatusNotification is the params of a session.status notification.
// The status vocabulary is "running"/"idle"; the adapter treats turn/end as the
// terminal signal and carries the status for diagnostics only.
type sessionStatusNotification struct {
	SessionID string `json:"sessionId"`
	Status    string `json:"status"`
}

// sessionEvent is the dsh SessionEvent envelope. Data is type-dependent and
// decoded by the mapper.
type sessionEvent struct {
	Type string          `json:"type"`
	Seq  int             `json:"seq"`
	Time int64           `json:"time"` // Unix epoch milliseconds
	Data json.RawMessage `json:"data"`
}

// ——— Session event data shapes ———

// assistantChunkData is the data of an assistant/chunk event. Chunk is a
// StreamChunk whose own type discriminates the payload.
type assistantChunkData struct {
	Turn  int         `json:"turn"`
	Step  int         `json:"step"`
	Chunk streamChunk `json:"chunk"`
}

// streamChunk is the flat union of every StreamChunk variant. Only the fields
// relevant to a chunk's Type are populated; the rest stay zero. Keeping one
// struct avoids a json.RawMessage union dance while staying exact per field.
type streamChunk struct {
	Type           string     `json:"type"`
	Index          int        `json:"index"`
	Text           string     `json:"text"`
	ID             string     `json:"id"`
	Name           string     `json:"name"`
	ArgumentsDelta string     `json:"argumentsDelta"`
	Usage          tokenUsage `json:"usage"`
}

// tokenUsage is the dsh TokenUsage counters attached to a usage chunk.
type tokenUsage struct {
	InputTokens      int `json:"inputTokens"`
	OutputTokens     int `json:"outputTokens"`
	TotalTokens      int `json:"totalTokens"`
	CacheReadTokens  int `json:"cacheReadTokens"`
	CacheWriteTokens int `json:"cacheWriteTokens"`
	ReasoningTokens  int `json:"reasoningTokens"`
}

// toolCallData is the data of a tool/call event.
type toolCallData struct {
	Turn      int    `json:"turn"`
	Step      int    `json:"step"`
	CallID    string `json:"callId"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// toolResultData is the data of a tool/result event. The message is a dsh
// ToolResultMessage whose single content block carries the tool's correlated
// result; Error is present (non-null) when the call failed.
type toolResultData struct {
	Turn    int             `json:"turn"`
	Step    int             `json:"step"`
	Message json.RawMessage `json:"message"`
	Error   json.RawMessage `json:"error,omitempty"`
}

// toolResultMessage decodes the correlated fields of a dsh ToolResultMessage:
// {content: [{type:"tool-result", toolCallId, content: ContentBlock[], isError}]}.
type toolResultMessage struct {
	Content []struct {
		ToolCallID string              `json:"toolCallId"`
		IsError    bool                `json:"isError"`
		Content    []toolResultContent `json:"content"`
	} `json:"content"`
}

// toolResultContent is one model-facing block inside a tool result, decoded for
// its text only.
type toolResultContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// turnEndData is the data of a turn/end event.
type turnEndData struct {
	Turn   int    `json:"turn"`
	Reason string `json:"reason"`
}

// ——— Request params ———

// initializeParams is the initialize request. Provider and model route the dsh
// session's model traffic; cwd anchors the spawned harness.
type initializeParams struct {
	Cwd      string `json:"cwd"`
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

// sessionPromptParams is the session/prompt request. dsh accepts plain text
// content blocks and answers with a synchronous message id.
type sessionPromptParams struct {
	SessionID     string               `json:"sessionId"`
	ContentBlocks []promptContentBlock `json:"contentBlocks"`
}

// promptContentBlock is one prompt content block.
type promptContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}
