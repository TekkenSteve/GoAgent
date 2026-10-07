package tool

import (
	"context"
	"errors"
	"time"
)

// Request describes a single tool call request.
type Request struct {
	RunID      string
	ToolCallID string
	ToolName   string
	AccountID  string
	ProjectID  string
	// IdempotencyKey identifies this call across attempts. A tool that is
	// retried — by the pipeline or by the platform — carries the same key, so
	// a store can answer with the first attempt's result instead of running
	// the side effect twice.
	IdempotencyKey string
	// SideEffecting marks a call that changes something outside this process.
	// It is what keeps the pipeline from retrying it.
	SideEffecting bool
	Args          map[string]any
}

// RawResult is the executor output before normalization.
type RawResult struct {
	Payload map[string]any
}

// Result is a finished tool call.
type Result struct {
	RunID          string
	ToolCallID     string
	ToolName       string
	Output         map[string]any
	FromIdempotent bool
	Attempts       int
}

// Policy defines per-tool timeout/retry/idempotency rules.
type Policy struct {
	// Timeout bounds one attempt.
	Timeout time.Duration
	// MaxAttempts bounds how many times the executor runs. One is the
	// default: retrying a call that may already have had its side effect is
	// how a transient failure becomes a duplicate.
	MaxAttempts int
	// RetryBackoff waits between attempts.
	RetryBackoff time.Duration
	// EnableIdempotent makes the pipeline consult the idempotency store
	// before executing, so a repeated key answers with the recorded result.
	EnableIdempotent bool
}

// Validator validates request payload before authorization/execution.
type Validator interface {
	Validate(ctx context.Context, req *Request) error
}

// Authorizer checks request authorization before execution.
type Authorizer interface {
	Authorize(ctx context.Context, req *Request) error
}

// Executor performs the tool call.
type Executor interface {
	Execute(ctx context.Context, req *Request) (RawResult, error)
}

// PolicyProvider returns per-tool policy.
type PolicyProvider interface {
	GetPolicy(toolName string) Policy
}

// IdempotencyStore records and resolves idempotency results.
type IdempotencyStore interface {
	Get(ctx context.Context, key string) (Result, bool, error)
	Put(ctx context.Context, key string, result *Result) error
}

// ErrValidation indicates the request payload is invalid.
var ErrValidation = errors.New("tool validation failed")

// ErrAuthorization indicates a policy denied execution.
var ErrAuthorization = errors.New("tool authorization failed")

// ErrExecution indicates the tool call itself failed.
var ErrExecution = errors.New("tool execution failed")
