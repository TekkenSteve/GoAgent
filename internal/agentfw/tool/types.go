package tool

import (
	"context"
	"errors"
	"time"
)

// Tier is a subscription tier used by tool authorization policy.
type Tier string

// TierFree, TierPro, and TierEnterprise are the subscription tiers used by
// tool authorization policy.
const (
	TierFree       Tier = "free"
	TierPro        Tier = "pro"
	TierEnterprise Tier = "enterprise"
)

// Request describes a single tool call request.
type Request struct {
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

// RawResult is the executor output before normalization.
type RawResult struct {
	Payload map[string]any
}

// IsolationLevel indicates the execution isolation semantics.
type IsolationLevel string

// IsolationNone, IsolationReadCommitted, and IsolationSerializable are the
// execution isolation semantics available to tools.
const (
	IsolationNone          IsolationLevel = "none"
	IsolationReadCommitted IsolationLevel = "read_committed"
	IsolationSerializable  IsolationLevel = "serializable"
)

// Result is normalized tool execution output.
type Result struct {
	RunID              string
	ToolCallID         string
	ToolName           string
	Output             map[string]any
	PersistedRef       string
	FromIdempotent     bool
	Attempts           int
	ExecutionIsolation ExecutionIsolation
}

// Policy defines per-tool timeout/retry/idempotency rules.
type Policy struct {
	Timeout          time.Duration
	MaxAttempts      int
	RetryBackoff     time.Duration
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

// Normalizer converts raw executor payload into standard output.
type Normalizer interface {
	Normalize(ctx context.Context, req *Request, raw RawResult) (map[string]any, error)
}

// Persister stores normalized output and returns a reference id.
type Persister interface {
	Persist(ctx context.Context, req *Request, normalized map[string]any) (string, error)
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

// ErrAuthorization indicates tier/policy denied execution.
var ErrAuthorization = errors.New("tool authorization failed")

// ErrExecution indicates the tool call itself failed.
var ErrExecution = errors.New("tool execution failed")
