package tool

import (
	"strings"
)

// SecretRedactor masks sensitive values before a tool's output is recorded —
// in the conversation, in run events, and in logs. A tool's output is authored
// elsewhere and routinely contains the credential it used.
type SecretRedactor interface {
	Redact(input map[string]any) map[string]any
}

// RedactionKeys is the set of payload keys whose values are masked. Matching is
// exact (case-insensitive) plus the listed suffixes, rather than a search for
// suspicious substrings: "token_count" is a number a tool legitimately returns,
// while "access_token" is a credential, and only the operator's list can tell
// the difference.
type RedactionKeys struct {
	// Exact names, compared case-insensitively.
	Exact []string
	// Suffixes, compared case-insensitively to the end of a key.
	Suffixes []string
}

// DefaultRedactionKeys are the credential-shaped keys the framework masks when
// a deployment configures nothing.
func DefaultRedactionKeys() RedactionKeys {
	return RedactionKeys{
		Exact: []string{
			"authorization",
			"api_key",
			"apikey",
			"password",
			"secret",
			"private_key",
			"credentials",
			"client_secret",
			"token",
			"access_token",
			"refresh_token",
			"id_token",
			"secret_key",
		},
		Suffixes: []string{
			"_secret",
			"_password",
			"_token",
			"_api_key",
			"_private_key",
		},
	}
}

// Redactor masks sensitive values using the operator's key list.
type Redactor struct {
	keys RedactionKeys
}

// NewRedactor builds a redactor. A nil key set takes the defaults; pass an
// explicitly empty RedactionKeys to mask nothing, which is a deployment's
// decision rather than a silent default.
func NewRedactor(keys *RedactionKeys) *Redactor {
	if keys == nil {
		defaults := DefaultRedactionKeys()
		keys = &defaults
	}

	lowered := RedactionKeys{
		Exact:    make([]string, 0, len(keys.Exact)),
		Suffixes: make([]string, 0, len(keys.Suffixes)),
	}

	for _, key := range keys.Exact {
		lowered.Exact = append(lowered.Exact, strings.ToLower(key))
	}

	for _, suffix := range keys.Suffixes {
		lowered.Suffixes = append(lowered.Suffixes, strings.ToLower(suffix))
	}

	return &Redactor{keys: lowered}
}

// Redact returns a copy of the payload with sensitive values masked, walking
// nested objects and arrays.
func (r *Redactor) Redact(input map[string]any) map[string]any {
	if r == nil {
		return input
	}

	return r.redactMap(input)
}

func (r *Redactor) redactMap(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))

	for key, value := range input {
		if r.sensitive(key) {
			out[key] = "[REDACTED]"

			continue
		}

		switch typed := value.(type) {
		case map[string]any:
			out[key] = r.redactMap(typed)
		case []any:
			out[key] = r.redactSlice(typed)
		default:
			out[key] = value
		}
	}

	return out
}

func (r *Redactor) redactSlice(input []any) []any {
	out := make([]any, len(input))

	for i, value := range input {
		switch typed := value.(type) {
		case map[string]any:
			out[i] = r.redactMap(typed)
		case []any:
			out[i] = r.redactSlice(typed)
		default:
			out[i] = value
		}
	}

	return out
}

func (r *Redactor) sensitive(key string) bool {
	lowered := strings.ToLower(key)

	for _, exact := range r.keys.Exact {
		if lowered == exact {
			return true
		}
	}

	for _, suffix := range r.keys.Suffixes {
		if strings.HasSuffix(lowered, suffix) {
			return true
		}
	}

	return false
}
