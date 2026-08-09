// Package entity provides domain types with zero internal dependencies.
package entity

import "fmt"

// ErrorCode categorizes agent execution failures for structured error handling.
type ErrorCode string

const (
	// ErrorCodeLLM marks failures originating from the LLM provider itself.
	ErrorCodeLLM ErrorCode = "LLM_ERROR"
	// ErrorCodeLLMTimeout marks LLM calls that exceeded the configured timeout.
	ErrorCodeLLMTimeout ErrorCode = "LLM_TIMEOUT"
	// ErrorCodeLLMRateLimit marks LLM calls rejected due to rate limiting.
	ErrorCodeLLMRateLimit ErrorCode = "LLM_RATE_LIMIT"
	// ErrorCodeLLMContentFilter marks LLM responses blocked by the content filter.
	ErrorCodeLLMContentFilter ErrorCode = "LLM_CONTENT_FILTER"
	// ErrorCodeToolNotFound marks requests for a tool that is not registered.
	ErrorCodeToolNotFound ErrorCode = "TOOL_NOT_FOUND"
	// ErrorCodeToolExecution marks failures that occurred while executing a tool.
	ErrorCodeToolExecution ErrorCode = "TOOL_EXECUTION_ERROR"
	// ErrorCodeValidation marks inputs rejected as invalid before execution.
	ErrorCodeValidation ErrorCode = "VALIDATION_ERROR"
	// ErrorCodeContextLength marks requests that exceed the model context window.
	ErrorCodeContextLength ErrorCode = "CONTEXT_LENGTH"
	// ErrorCodeInternal marks unexpected internal failures.
	ErrorCodeInternal ErrorCode = "INTERNAL_ERROR"
)

// AgentError is a structured domain error with user-facing message and recovery hints.
type AgentError struct {
	Code        ErrorCode
	Message     string // developer-facing technical detail
	UserMessage string // safe to show to end user
	Recoverable bool   // can the agent recover and continue?
	Retryable   bool   // can the operation be retried as-is?
	Err         error  // wrapped root cause
}

func (e *AgentError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func (e *AgentError) Unwrap() error {
	return e.Err
}
