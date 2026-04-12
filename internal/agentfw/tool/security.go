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
	return redactMap(input)
}

func isSensitiveKey(k string) bool {
	lk := strings.ToLower(k)
	return strings.Contains(lk, "secret") ||
		strings.Contains(lk, "token") ||
		strings.Contains(lk, "password") ||
		strings.Contains(lk, "api_key") ||
		strings.Contains(lk, "apikey") ||
		strings.Contains(lk, "credential")
}

func redactMap(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for k, v := range input {
		if isSensitiveKey(k) {
			out[k] = "[REDACTED]"
			continue
		}
		switch val := v.(type) {
		case map[string]any:
			out[k] = redactMap(val)
		case []any:
			out[k] = redactSlice(val)
		default:
			out[k] = v
		}
	}
	return out
}

func redactSlice(input []any) []any {
	out := make([]any, len(input))
	for i, v := range input {
		switch val := v.(type) {
		case map[string]any:
			out[i] = redactMap(val)
		case []any:
			out[i] = redactSlice(val)
		default:
			out[i] = v
		}
	}
	return out
}
