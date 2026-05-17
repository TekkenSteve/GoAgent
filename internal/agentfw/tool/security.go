package tool

import (
	"strings"
)

// ExecutionIsolation indicates where a tool should be executed.
type ExecutionIsolation string

const (
	ExecutionIsolationShared   ExecutionIsolation = "shared"   // Execute in shared worker pool
	ExecutionIsolationIsolated ExecutionIsolation = "isolated" // Execute in isolated sandbox
)

// IsolationPolicy resolves execution isolation boundary for a request.
type IsolationPolicy interface {
	Resolve(req *Request) ExecutionIsolation
}

// SideEffectIsolationPolicy routes side-effecting tools to isolated execution boundaries.
type SideEffectIsolationPolicy struct{}

// Resolve returns isolated boundary for side-effecting tools, shared otherwise.
func (SideEffectIsolationPolicy) Resolve(req *Request) ExecutionIsolation {
	if req.SideEffecting {
		return ExecutionIsolationIsolated
	}

	return ExecutionIsolationShared
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
