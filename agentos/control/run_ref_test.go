package control_test

import (
	"errors"
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/agentos/core"
)

func principalFor(t *testing.T, accountID string) core.Principal {
	t.Helper()

	principal, err := core.NewPrincipal(accountID, "", core.PrincipalSourceJWT)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}

	return principal
}

// TestNewRunRefTakesTheAccountFromTheCredential is the point of the
// constructor: a reference built from a request cannot name another account.
func TestNewRunRefTakesTheAccountFromTheCredential(t *testing.T) {
	t.Parallel()

	ref, err := agentos.NewRunRef(principalFor(t, "acct-1"), "run-1")
	if err != nil {
		t.Fatalf("NewRunRef: %v", err)
	}

	if ref.RunID != "run-1" || ref.AccountID != "acct-1" {
		t.Fatalf("ref = %#v", ref)
	}

	if ref.ProjectID != "" {
		t.Fatalf("project = %q, want empty: a by-id request never names one", ref.ProjectID)
	}
}

func TestNewRunRefRefusesAnUnusableInput(t *testing.T) {
	t.Parallel()

	valid := principalFor(t, "acct-1")

	cases := map[string]func() (agentos.RunRef, error){
		// An account-less principal cannot even be built (NewPrincipal refuses
		// it), so the only ways to reach the runtime with an unusable reference
		// are a missing principal and a missing run id.
		"no principal": func() (agentos.RunRef, error) { return agentos.NewRunRef(core.Principal{}, "run-1") },
		"no run id":    func() (agentos.RunRef, error) { return agentos.NewRunRef(valid, "") },
		"blank run id": func() (agentos.RunRef, error) { return agentos.NewRunRef(valid, "   ") },
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, err := build(); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestRunRefValidateRequiresARunID(t *testing.T) {
	t.Parallel()

	if err := (agentos.RunRef{RunID: "run-1"}).Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if err := (agentos.RunRef{}).Validate(); !errors.Is(err, core.ErrInvalidRunSpec) {
		t.Fatalf("Validate error = %v, want ErrInvalidRunSpec", err)
	}
}

// TestRunRefMatchesSpellsOutTheEnforcementRule keeps the rule readable in one
// place: the account always binds, the project narrows when known, and an empty
// account is the stated in-process case.
func TestRunRefMatchesSpellsOutTheEnforcementRule(t *testing.T) {
	t.Parallel()

	owned := agentos.RunBackendOwnership{RunID: "run-1", AccountID: "acct-1", ProjectID: "proj-1"}

	cases := map[string]struct {
		ref   agentos.RunRef
		match bool
	}{
		"owning account":             {ref: agentos.RunRef{RunID: "run-1", AccountID: "acct-1"}, match: true},
		"owning account and project": {ref: agentos.RunRef{RunID: "run-1", AccountID: "acct-1", ProjectID: "proj-1"}, match: true},
		"another account":            {ref: agentos.RunRef{RunID: "run-1", AccountID: "acct-2"}, match: false},
		"another project":            {ref: agentos.RunRef{RunID: "run-1", AccountID: "acct-1", ProjectID: "proj-2"}, match: false},
		"no identity stated":         {ref: agentos.RunRef{RunID: "run-1"}, match: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := tc.ref.Matches(&owned); got != tc.match {
				t.Fatalf("Matches = %v, want %v", got, tc.match)
			}
		})
	}
}
