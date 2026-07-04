package agentos

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
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
			if !errors.Is(err, ErrInvalidResourceRef) {
				t.Fatalf("ValidateResourceRef error = %v, want ErrInvalidResourceRef", err)
			}
		})
	}
}

func TestValidateProcessSpecAcceptsGenericDurableProcess(t *testing.T) {
	t.Parallel()

	spec := validProcessSpec()
	spec.Timers = []ProcessTimerSpec{
		{TimerID: "sla", AfterSeconds: 300, Signal: "sla.expired"},
		{TimerID: "follow-up", FireAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)},
	}
	spec.Policy = ProcessPolicy{MaxHistoryEvents: 1000, ContinueAsNewEvents: 800}

	if err := ValidateProcessSpec(&spec); err != nil {
		t.Fatalf("ValidateProcessSpec: %v", err)
	}
}

func TestValidateProcessSpecRequiresStableStartIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*ProcessSpec)
	}{
		{name: "process id", edit: func(spec *ProcessSpec) { spec.ProcessID = "" }},
		{name: "kind", edit: func(spec *ProcessSpec) { spec.Kind = "" }},
		{name: "account id", edit: func(spec *ProcessSpec) { spec.AccountID = "" }},
		{name: "project id", edit: func(spec *ProcessSpec) { spec.ProjectID = "" }},
		{name: "idempotency key", edit: func(spec *ProcessSpec) { spec.IdempotencyKey = "" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := validProcessSpec()
			tt.edit(&spec)

			err := ValidateProcessSpec(&spec)
			if !errors.Is(err, ErrInvalidProcess) {
				t.Fatalf("ValidateProcessSpec error = %v, want ErrInvalidProcess", err)
			}
		})
	}
}

func TestValidateProcessSpecRejectsCrossTenantResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*ProcessSpec)
	}{
		{name: "account", edit: func(spec *ProcessSpec) { spec.Resource.AccountID = "acct-2" }},
		{name: "project", edit: func(spec *ProcessSpec) { spec.Resource.ProjectID = "proj-2" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := validProcessSpec()
			tt.edit(&spec)

			err := ValidateProcessSpec(&spec)
			if !errors.Is(err, ErrInvalidProcess) {
				t.Fatalf("ValidateProcessSpec error = %v, want ErrInvalidProcess", err)
			}
		})
	}
}

func TestValidateProcessSpecRejectsInvalidTimers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		timer ProcessTimerSpec
	}{
		{name: "missing id", timer: ProcessTimerSpec{AfterSeconds: 1}},
		{name: "missing schedule", timer: ProcessTimerSpec{TimerID: "sla"}},
		{name: "two schedules", timer: ProcessTimerSpec{
			TimerID:      "sla",
			FireAt:       time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
			AfterSeconds: 1,
		}},
		{name: "negative schedule", timer: ProcessTimerSpec{TimerID: "sla", AfterSeconds: -1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := validProcessSpec()
			spec.Timers = []ProcessTimerSpec{tt.timer}

			err := ValidateProcessSpec(&spec)
			if !errors.Is(err, ErrInvalidProcess) {
				t.Fatalf("ValidateProcessSpec error = %v, want ErrInvalidProcess", err)
			}
		})
	}
}

func TestValidateProcessScopesRequireTenantScope(t *testing.T) {
	t.Parallel()

	refErr := ValidateProcessRef(ProcessRef{ProcessID: "process-1", AccountID: "acct-1"})
	if !errors.Is(refErr, ErrInvalidProcessScope) {
		t.Fatalf("ValidateProcessRef error = %v, want ErrInvalidProcessScope", refErr)
	}

	streamErr := ValidateProcessStreamScope(nil)
	if !errors.Is(streamErr, ErrInvalidProcessScope) {
		t.Fatalf("ValidateProcessStreamScope error = %v, want ErrInvalidProcessScope", streamErr)
	}

	eventErr := ValidateProcessEventScope(&ProcessEventScope{ProcessID: "process-1", AccountID: "acct-1", ProjectID: "proj-1", Limit: -1})
	if !errors.Is(eventErr, ErrInvalidProcessScope) {
		t.Fatalf("ValidateProcessEventScope error = %v, want ErrInvalidProcessScope", eventErr)
	}
}

func TestProcessSpecJSONSchema(t *testing.T) {
	t.Parallel()

	data, err := ProcessSpecJSONSchema()
	if err != nil {
		t.Fatalf("ProcessSpecJSONSchema: %v", err)
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

func validProcessSpec() ProcessSpec {
	return ProcessSpec{
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
