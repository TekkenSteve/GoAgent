package tool

import (
	"strings"
)

// IsolationLevel controls routing for tool execution boundaries.
type IsolationLevel string

const (
	IsolationShared   IsolationLevel = "shared"
	IsolationIsolated IsolationLevel = "isolated"
)

// IsolationPolicy resolves execution isolation level for a request.
type IsolationPolicy interface {
	Resolve(req ToolRequest) IsolationLevel
}

// SideEffectIsolationPolicy routes side-effecting tools to isolated pools.
type SideEffectIsolationPolicy struct{}

// Resolve returns isolated level when tool is side-effecting.
func (SideEffectIsolationPolicy) Resolve(req ToolRequest) IsolationLevel {
	if req.SideEffecting {
		return IsolationIsolated
	}
	return IsolationShared
}

// SecretRedactor masks sensitive values before persistence/telemetry.
type SecretRedactor interface {
	Redact(input map[string]any) map[string]any
}

// DefaultSecretRedactor masks sensitive keys.
type DefaultSecretRedactor struct{}

// Redact masks known sensitive keys in output payload.
func (DefaultSecretRedactor) Redact(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for k, v := range input {
		lk := strings.ToLower(k)
		if strings.Contains(lk, "secret") || strings.Contains(lk, "token") || strings.Contains(lk, "password") || strings.Contains(lk, "api_key") {
			out[k] = "[REDACTED]"
			continue
		}
		out[k] = v
	}
	return out
}
