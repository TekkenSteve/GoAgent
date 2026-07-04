package agentos

import (
	"errors"
	"testing"
	"time"
)

func TestValidateGovernedActionSpecAcceptsGenericAction(t *testing.T) {
	t.Parallel()

	spec := validGovernedActionSpec()

	if err := ValidateGovernedActionSpec(&spec); err != nil {
		t.Fatalf("ValidateGovernedActionSpec: %v", err)
	}
}

func TestValidateGovernedActionSpecRequiresActionIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*GovernedActionSpec)
	}{
		{name: "action id", edit: func(spec *GovernedActionSpec) { spec.ActionID = "" }},
		{name: "idempotency key", edit: func(spec *GovernedActionSpec) { spec.IdempotencyKey = "" }},
		{name: "kind", edit: func(spec *GovernedActionSpec) { spec.Kind = "" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := validGovernedActionSpec()
			tt.edit(&spec)

			err := ValidateGovernedActionSpec(&spec)
			if !errors.Is(err, ErrInvalidGovernedAction) {
				t.Fatalf("ValidateGovernedActionSpec error = %v, want ErrInvalidGovernedAction", err)
			}
		})
	}
}

func TestValidateGovernedActionSpecRequiresTenantAndProcessOrResourceScope(t *testing.T) {
	t.Parallel()

	account := validGovernedActionSpec()
	account.AccountID = ""

	accountErr := ValidateGovernedActionSpec(&account)
	if !errors.Is(accountErr, ErrInvalidGovernedAction) {
		t.Fatalf("account scope error = %v, want ErrInvalidGovernedAction", accountErr)
	}

	project := validGovernedActionSpec()
	project.ProjectID = ""

	projectErr := ValidateGovernedActionSpec(&project)
	if !errors.Is(projectErr, ErrInvalidGovernedAction) {
		t.Fatalf("project scope error = %v, want ErrInvalidGovernedAction", projectErr)
	}

	scope := validGovernedActionSpec()
	scope.ProcessID = ""
	scope.Resource = ResourceRef{}

	scopeErr := ValidateGovernedActionSpec(&scope)
	if !errors.Is(scopeErr, ErrInvalidGovernedAction) {
		t.Fatalf("process/resource scope error = %v, want ErrInvalidGovernedAction", scopeErr)
	}
}

func TestValidateGovernedActionSpecRejectsCrossTenantResource(t *testing.T) {
	t.Parallel()

	spec := validGovernedActionSpec()
	spec.Resource.AccountID = "acct-other"

	err := ValidateGovernedActionSpec(&spec)
	if !errors.Is(err, ErrInvalidGovernedAction) {
		t.Fatalf("ValidateGovernedActionSpec error = %v, want ErrInvalidGovernedAction", err)
	}
}

func TestValidateGovernedActionSpecRejectsInvalidRefs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*GovernedActionSpec)
	}{
		{name: "input ref", edit: func(spec *GovernedActionSpec) {
			spec.InputRefs = []LedgerDataRef{{Kind: "input"}}
		}},
		{name: "compensation ref", edit: func(spec *GovernedActionSpec) {
			spec.CompensationRef = &LedgerDataRef{URI: "s3://bucket/undo.json"}
		}},
		{name: "risk evidence", edit: func(spec *GovernedActionSpec) {
			spec.Risk.EvidenceRefs = []LedgerDataRef{{Kind: "risk"}}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := validGovernedActionSpec()
			tt.edit(&spec)

			err := ValidateGovernedActionSpec(&spec)
			if !errors.Is(err, ErrInvalidGovernedAction) {
				t.Fatalf("ValidateGovernedActionSpec error = %v, want ErrInvalidGovernedAction", err)
			}
		})
	}
}

func TestValidateActionScopeRequiresTenantScope(t *testing.T) {
	t.Parallel()

	err := ValidateActionScope(&ActionScope{AccountID: "acct-1", Limit: -1})
	if !errors.Is(err, ErrInvalidGovernedActionScope) {
		t.Fatalf("ValidateActionScope error = %v, want ErrInvalidGovernedActionScope", err)
	}

	scope := ActionScope{
		AccountID: "acct-1",
		ProjectID: "proj-1",
		Resource: ResourceRef{
			Kind:       "resource-kind",
			ResourceID: "resource-1",
			AccountID:  "acct-other",
			ProjectID:  "proj-1",
		},
	}

	err = ValidateActionScope(&scope)
	if !errors.Is(err, ErrInvalidGovernedActionScope) {
		t.Fatalf("ValidateActionScope resource error = %v, want ErrInvalidGovernedActionScope", err)
	}
}

func validGovernedActionSpec() GovernedActionSpec {
	return GovernedActionSpec{
		ActionID:       "action-1",
		IdempotencyKey: "action-key-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		ProcessID:      "process-1",
		Resource: ResourceRef{
			Kind:       "resource-kind",
			ResourceID: "resource-1",
			AccountID:  "acct-1",
			ProjectID:  "proj-1",
		},
		Kind:             "remediate-resource",
		Intent:           "perform a governed resource update",
		RequestedBy:      ActorRef{Kind: "agent", ActorID: "agent-1"},
		RequestedAt:      time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
		DryRunRequired:   true,
		ApprovalRequired: true,
		Risk: ActionRiskAssessment{
			Level:  ActionRiskHigh,
			Reason: "mutates external resource",
			EvidenceRefs: []LedgerDataRef{{
				Kind: "risk",
				URI:  "s3://bucket/risk.json",
			}},
		},
		InputRefs: []LedgerDataRef{{
			Kind: "input",
			URI:  "s3://bucket/input.json",
		}},
		CompensationRef: &LedgerDataRef{
			Kind: "compensation",
			URI:  "s3://bucket/undo.json",
		},
	}
}
