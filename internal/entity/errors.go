// Package entity provides domain types with zero internal dependencies.
package entity

import "fmt"

// ErrorCode categorizes agent execution failures for structured error handling.
type ErrorCode string

const (
	ErrorCodeLLM              ErrorCode = "LLM_ERROR"
	ErrorCodeLLMTimeout       ErrorCode = "LLM_TIMEOUT"
	ErrorCodeLLMRateLimit     ErrorCode = "LLM_RATE_LIMIT"
	ErrorCodeLLMContentFilter ErrorCode = "LLM_CONTENT_FILTER"
	ErrorCodeToolNotFound     ErrorCode = "TOOL_NOT_FOUND"
	ErrorCodeToolExecution    ErrorCode = "TOOL_EXECUTION_ERROR"
	ErrorCodeValidation       ErrorCode = "VALIDATION_ERROR"
	ErrorCodeContextLength    ErrorCode = "CONTEXT_LENGTH"
	ErrorCodeInternal         ErrorCode = "INTERNAL_ERROR"
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
