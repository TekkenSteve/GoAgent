// Package tool defines tool call authorization, execution, and the pipeline
// stages a tool call passes through.
package tool

import (
	"context"
	"time"
)

// AuditRecord captures one policy decision about a tool call.
type AuditRecord struct {
	RunID      string
	AccountID  string
	ProjectID  string
	ToolName   string
	Allowed    bool
	Reason     string
	OccurredAt time.Time
}

// AuditSink writes audit records. A decision that cannot be recorded is
// returned to the caller, which fails the call rather than letting an
// unrecorded denial pass for an allowance.
type AuditSink interface {
	Write(ctx context.Context, record *AuditRecord) error
}
