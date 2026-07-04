package process

import (
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestValidateLedgerEntrySpecAcceptsProcessOrResourceScopedEntry(t *testing.T) {
	t.Parallel()

	processEntry := validLedgerEntrySpec()
	processEntry.Resource = ResourceRef{}

	if err := ValidateLedgerEntrySpec(&processEntry); err != nil {
		t.Fatalf("ValidateLedgerEntrySpec process entry: %v", err)
	}

	resourceEntry := validLedgerEntrySpec()
	resourceEntry.ProcessID = ""

	if err := ValidateLedgerEntrySpec(&resourceEntry); err != nil {
		t.Fatalf("ValidateLedgerEntrySpec resource entry: %v", err)
	}
}

func TestValidateLedgerEntrySpecRequiresStableIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*LedgerEntrySpec)
	}{
		{name: "entry id", edit: func(spec *LedgerEntrySpec) { spec.EntryID = "" }},
		{name: "idempotency key", edit: func(spec *LedgerEntrySpec) { spec.IdempotencyKey = "" }},
		{name: "account id", edit: func(spec *LedgerEntrySpec) { spec.AccountID = "" }},
		{name: "project id", edit: func(spec *LedgerEntrySpec) { spec.ProjectID = "" }},
		{name: "kind", edit: func(spec *LedgerEntrySpec) { spec.Kind = "" }},
		{name: "scope", edit: func(spec *LedgerEntrySpec) {
			spec.ProcessID = ""
			spec.Resource = ResourceRef{}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := validLedgerEntrySpec()
			tt.edit(&spec)

			err := ValidateLedgerEntrySpec(&spec)
			if !errors.Is(err, core.ErrInvalidLedgerEntry) {
				t.Fatalf("ValidateLedgerEntrySpec error = %v, want core.ErrInvalidLedgerEntry", err)
			}
		})
	}
}

func TestValidateLedgerEntrySpecRejectsInvalidDataRefs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ref  LedgerDataRef
	}{
		{name: "kind", ref: LedgerDataRef{URI: "s3://bucket/ref.json"}},
		{name: "target", ref: LedgerDataRef{Kind: "evidence"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := validLedgerEntrySpec()
			spec.DataRefs = []LedgerDataRef{tt.ref}

			err := ValidateLedgerEntrySpec(&spec)
			if !errors.Is(err, core.ErrInvalidLedgerEntry) {
				t.Fatalf("ValidateLedgerEntrySpec error = %v, want core.ErrInvalidLedgerEntry", err)
			}
		})
	}
}

func TestValidateLedgerScopeRequiresTenantScope(t *testing.T) {
	t.Parallel()

	err := ValidateLedgerScope(&LedgerScope{AccountID: "acct-1", Limit: -1})
	if !errors.Is(err, core.ErrInvalidLedgerScope) {
		t.Fatalf("ValidateLedgerScope error = %v, want core.ErrInvalidLedgerScope", err)
	}

	scope := LedgerScope{
		AccountID: "acct-1",
		ProjectID: "proj-1",
		Resource: ResourceRef{
			Kind:       "resource-kind",
			ResourceID: "resource-1",
			AccountID:  "acct-other",
			ProjectID:  "proj-1",
		},
	}

	err = ValidateLedgerScope(&scope)
	if !errors.Is(err, core.ErrInvalidLedgerScope) {
		t.Fatalf("ValidateLedgerScope resource error = %v, want core.ErrInvalidLedgerScope", err)
	}
}

func validLedgerEntrySpec() LedgerEntrySpec {
	return LedgerEntrySpec{
		EntryID:        "ledger-1",
		IdempotencyKey: "ledger-key-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		ProcessID:      "process-1",
		Resource: ResourceRef{
			Kind:       "resource-kind",
			ResourceID: "resource-1",
			AccountID:  "acct-1",
			ProjectID:  "proj-1",
		},
		Kind:       LedgerEntryDecision,
		OccurredAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
		DataRefs: []LedgerDataRef{{
			Kind: "evidence",
			URI:  "s3://bucket/evidence.json",
		}},
	}
}
