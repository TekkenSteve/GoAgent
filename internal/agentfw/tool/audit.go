package tool

import (
	"context"
	"fmt"
)

// AuditLogger is the logging surface the audit sink writes through.
type AuditLogger interface {
	Info(message string, args ...any)
}

// LoggerAuditSink records every tool authorization decision as a structured
// log line.
//
// It is the sink a deployment gets by default: decisions are recorded from the
// first day, in the operator's log stream, with the run, the account, the tool
// and the reason. A durable, queryable, tamper-evident audit store is a
// separate concern — the decisions are already being made and a sink that
// keeps them nowhere would be the one thing worse than a plain log line.
type LoggerAuditSink struct {
	logger AuditLogger
}

// NewLoggerAuditSink records decisions through the given logger.
func NewLoggerAuditSink(logger AuditLogger) *LoggerAuditSink {
	return &LoggerAuditSink{logger: logger}
}

// Write records one decision.
func (s *LoggerAuditSink) Write(_ context.Context, record *AuditRecord) error {
	if s == nil || s.logger == nil {
		return fmt.Errorf("%w: no audit logger", ErrPipelineAssembly)
	}

	s.logger.Info("tool authorization decision",
		"run_id", record.RunID,
		"account_id", record.AccountID,
		"project_id", record.ProjectID,
		"tool", record.ToolName,
		"allowed", record.Allowed,
		"reason", record.Reason,
		"occurred_at", record.OccurredAt.Format(timeLayout),
	)

	return nil
}

// timeLayout keeps the recorded instant unambiguous across zones.
const timeLayout = "2006-01-02T15:04:05.000Z07:00"
