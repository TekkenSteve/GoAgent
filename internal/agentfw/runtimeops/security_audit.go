package runtimeops

import (
	"context"
	"strings"
)

// SecurityAuditEvent captures security-sensitive decisions.
type SecurityAuditEvent struct {
	RunID         string
	ActorID       string
	Action        string
	CorrelationID string
	Payload       map[string]any
}

// RedactionPolicy redacts sensitive telemetry fields.
type RedactionPolicy interface {
	Redact(payload map[string]any) map[string]any
}

// SecurityAuditSink stores or forwards security audit events.
type SecurityAuditSink interface {
	Write(ctx context.Context, event SecurityAuditEvent) error
}

// SecurityAuditor enforces redaction before emitting audit events.
type SecurityAuditor struct {
	Redactor RedactionPolicy
	Sink     SecurityAuditSink
}

// Emit writes redacted security audit event.
func (a SecurityAuditor) Emit(ctx context.Context, event SecurityAuditEvent) error {
	if a.Redactor != nil {
		event.Payload = a.Redactor.Redact(event.Payload)
	}

	if a.Sink == nil {
		return nil
	}

	return a.Sink.Write(ctx, event)
}

// DefaultRedactionPolicy masks known sensitive fields.
type DefaultRedactionPolicy struct{}

// Redact masks security-sensitive values.
func (DefaultRedactionPolicy) Redact(payload map[string]any) map[string]any {
	return redactMap(payload)
}

func redactMap(payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}

	out := make(map[string]any, len(payload))
	for k, v := range payload {
		if isSensitiveAuditKey(k) {
			out[k] = "[REDACTED]"
		} else if nested, ok := v.(map[string]any); ok {
			out[k] = redactMap(nested)
		} else {
			out[k] = v
		}
	}

	return out
}

func isSensitiveAuditKey(key string) bool {
	switch strings.ToLower(key) {
	case "token", "secret", "password", "api_key", "authorization":
		return true
	default:
		return false
	}
}
