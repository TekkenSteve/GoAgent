package process

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestValidateResourceRefRequiresTenantScopedIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ref  ResourceRef
	}{
		{name: "kind", ref: ResourceRef{ResourceID: "resource-1", AccountID: "acct-1", ProjectID: "proj-1"}},
		{name: "resource id", ref: ResourceRef{Kind: "resource-kind", AccountID: "acct-1", ProjectID: "proj-1"}},
		{name: "account id", ref: ResourceRef{Kind: "resource-kind", ResourceID: "resource-1", ProjectID: "proj-1"}},
		{name: "project id", ref: ResourceRef{Kind: "resource-kind", ResourceID: "resource-1", AccountID: "acct-1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateResourceRef(tt.ref)
			if !errors.Is(err, core.ErrInvalidResourceRef) {
				t.Fatalf("ValidateResourceRef error = %v, want core.ErrInvalidResourceRef", err)
			}
		})
	}
}

func TestValidateProcessSpecAcceptsGenericDurableProcess(t *testing.T) {
	t.Parallel()

	spec := validProcessSpec()
	spec.Timers = []TimerSpec{
		{TimerID: "sla", AfterSeconds: 300, Signal: "sla.expired"},
		{TimerID: "follow-up", FireAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)},
	}
	spec.Policy = Policy{MaxHistoryEvents: 1000, ContinueAsNewEvents: 800}

	if err := ValidateProcessSpec(&spec); err != nil {
		t.Fatalf("ValidateProcessSpec: %v", err)
	}
}

func TestValidateProcessSpecRequiresStableStartIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*Spec)
	}{
		{name: "process id", edit: func(spec *Spec) { spec.ProcessID = "" }},
		{name: "kind", edit: func(spec *Spec) { spec.Kind = "" }},
		{name: "account id", edit: func(spec *Spec) { spec.AccountID = "" }},
		{name: "project id", edit: func(spec *Spec) { spec.ProjectID = "" }},
		{name: "idempotency key", edit: func(spec *Spec) { spec.IdempotencyKey = "" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := validProcessSpec()
			tt.edit(&spec)

			err := ValidateProcessSpec(&spec)
			if !errors.Is(err, core.ErrInvalidProcess) {
				t.Fatalf("ValidateProcessSpec error = %v, want core.ErrInvalidProcess", err)
			}
		})
	}
}

func TestValidateProcessSpecRejectsCrossTenantResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*Spec)
	}{
		{name: "account", edit: func(spec *Spec) { spec.Resource.AccountID = "acct-2" }},
		{name: "project", edit: func(spec *Spec) { spec.Resource.ProjectID = "proj-2" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := validProcessSpec()
			tt.edit(&spec)

			err := ValidateProcessSpec(&spec)
			if !errors.Is(err, core.ErrInvalidProcess) {
				t.Fatalf("ValidateProcessSpec error = %v, want core.ErrInvalidProcess", err)
			}
		})
	}
}

func TestValidateProcessSpecRejectsInvalidTimers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		timer TimerSpec
	}{
		{name: "missing id", timer: TimerSpec{AfterSeconds: 1}},
		{name: "missing schedule", timer: TimerSpec{TimerID: "sla"}},
		{name: "two schedules", timer: TimerSpec{
			TimerID:      "sla",
			FireAt:       time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
			AfterSeconds: 1,
		}},
		{name: "negative schedule", timer: TimerSpec{TimerID: "sla", AfterSeconds: -1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := validProcessSpec()
			spec.Timers = []TimerSpec{tt.timer}

			err := ValidateProcessSpec(&spec)
			if !errors.Is(err, core.ErrInvalidProcess) {
				t.Fatalf("ValidateProcessSpec error = %v, want core.ErrInvalidProcess", err)
			}
		})
	}
}

func TestValidateProcessScopesRequireTenantScope(t *testing.T) {
	t.Parallel()

	refErr := ValidateProcessRef(Ref{ProcessID: "process-1", AccountID: "acct-1"})
	if !errors.Is(refErr, core.ErrInvalidProcessScope) {
		t.Fatalf("ValidateProcessRef error = %v, want core.ErrInvalidProcessScope", refErr)
	}

	streamErr := ValidateProcessStreamScope(nil)
	if !errors.Is(streamErr, core.ErrInvalidProcessScope) {
		t.Fatalf("ValidateProcessStreamScope error = %v, want core.ErrInvalidProcessScope", streamErr)
	}

	eventErr := ValidateProcessEventScope(&EventScope{ProcessID: "process-1", AccountID: "acct-1", ProjectID: "proj-1", Limit: -1})
	if !errors.Is(eventErr, core.ErrInvalidProcessScope) {
		t.Fatalf("ValidateProcessEventScope error = %v, want core.ErrInvalidProcessScope", eventErr)
	}
}

func TestProcessSpecJSONSchema(t *testing.T) {
	t.Parallel()

	data, err := SpecJSONSchema()
	if err != nil {
		t.Fatalf("SpecJSONSchema: %v", err)
	}

	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("schema json: %v", err)
	}

	if len(schema) == 0 {
		t.Fatal("schema is empty")
	}

	assertSchemaDoesNotRequire(t, schema, []string{"requested_at", "policy"})
}

func assertSchemaDoesNotRequire(t *testing.T, schema map[string]any, fields []string) {
	t.Helper()

	requiredValues, ok := schema["required"].([]any)
	if !ok {
		requiredValues = nil
	}

	required := make(map[string]bool, len(requiredValues))
	for _, value := range requiredValues {
		name, ok := value.(string)
		if !ok {
			t.Fatalf("required contains non-string %#v", value)
		}

		required[name] = true
	}

	for _, field := range fields {
		if required[field] {
			t.Fatalf("schema unexpectedly requires optional field %q", field)
		}
	}
}

func validProcessSpec() Spec {
	return Spec{
		ProcessID:      "process-1",
		Kind:           "resource-lifecycle",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "process-key-1",
		Resource: ResourceRef{
			Kind:       "resource-kind",
			ResourceID: "resource-1",
			AccountID:  "acct-1",
			ProjectID:  "proj-1",
		},
		RequestedAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
		Inputs: map[string]any{
			"priority": "high",
		},
	}
}
