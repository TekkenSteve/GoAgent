//go:build postgres_integration

package persistent

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// TestWorkflowTemplatePostgresPartialUpdate pins the partial-update contract
// the generated UpdateWorkflowTemplate statement implements: a NULL parameter
// keeps the stored value, while an explicitly supplied — including empty —
// value overwrites it.
func TestWorkflowTemplatePostgresPartialUpdate(t *testing.T) {
	ctx, pg, _ := newAgentOSPlanPostgresIntegrationDB(t)

	repo := NewWorkflowTemplateRepo(pg)

	created, err := repo.Create(ctx, &entity.CreateWorkflowTemplateRequest{
		AccountID:    "11111111-1111-1111-1111-111111111111",
		Name:         "original",
		Description:  "desc",
		TeamSpec:     entity.TeamSpec{},
		SystemPrompt: "prompt",
		DefaultModel: "model-a",
		Tags:         []string{"one"},
		IsEnabled:    true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	name := "renamed"
	disabled := false

	updated, err := repo.Update(ctx, created.AccountID, created.ID, entity.UpdateWorkflowTemplateRequest{
		Name:      &name,
		IsEnabled: &disabled,
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	if updated.Name != "renamed" || updated.IsEnabled {
		t.Fatalf("updated fields = %q/%v, want renamed/false", updated.Name, updated.IsEnabled)
	}

	if updated.Description != "desc" || updated.SystemPrompt != "prompt" || updated.DefaultModel != "model-a" {
		t.Fatalf("omitted fields changed: desc=%q prompt=%q model=%q", updated.Description, updated.SystemPrompt, updated.DefaultModel)
	}

	if len(updated.Tags) != 1 || updated.Tags[0] != "one" {
		t.Fatalf("omitted tags changed: %v", updated.Tags)
	}

	emptyTags := []string{}

	cleared, err := repo.Update(ctx, created.AccountID, created.ID, entity.UpdateWorkflowTemplateRequest{Tags: &emptyTags})
	if err != nil {
		t.Fatalf("Update empty tags: %v", err)
	}

	if len(cleared.Tags) != 0 {
		t.Fatalf("explicit empty tags = %v, want empty", cleared.Tags)
	}
}

// TestWorkflowTemplatePostgresRoundTrip covers the read and delete paths: a
// created template is readable by ID and by account, and a deleted one is
// gone.
func TestWorkflowTemplatePostgresRoundTrip(t *testing.T) {
	ctx, pg, _ := newAgentOSPlanPostgresIntegrationDB(t)

	repo := NewWorkflowTemplateRepo(pg)
	accountID := "22222222-2222-2222-2222-222222222222"

	created, err := repo.Create(ctx, &entity.CreateWorkflowTemplateRequest{
		AccountID:    accountID,
		Name:         "round-trip",
		Description:  "desc",
		TeamSpec:     entity.TeamSpec{},
		SystemPrompt: "prompt",
		DefaultModel: "model-b",
		Tags:         nil,
		IsEnabled:    true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if len(created.Tags) != 0 {
		t.Fatalf("nil tags stored as %v, want empty", created.Tags)
	}

	loaded, exists, err := repo.Get(ctx, accountID, created.ID)
	if err != nil || !exists {
		t.Fatalf("Get = %v/%v, want found", exists, err)
	}

	if loaded.Name != created.Name || loaded.DefaultModel != created.DefaultModel {
		t.Fatalf("loaded = %+v, want %+v", loaded, created)
	}

	listed, err := repo.ListByAccount(ctx, accountID)
	if err != nil {
		t.Fatalf("ListByAccount: %v", err)
	}

	if len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("listed = %+v, want the created template", listed)
	}

	if err := repo.Delete(ctx, accountID, created.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, exists, err := repo.Get(ctx, accountID, created.ID); err != nil || exists {
		t.Fatalf("Get after delete = %v/%v, want missing", exists, err)
	}
}

// A template belongs to its account, so the account is part of the lookup
// rather than a check applied to the result. These two tests are the same
// question asked of a read and a delete: another account's id must behave
// exactly like an id that does not exist, or the endpoint becomes a way to
// discover that it does.
func TestWorkflowTemplatePostgresReadIsScopedToItsAccount(t *testing.T) {
	ctx, pg, _ := newAgentOSPlanPostgresIntegrationDB(t)

	repo := NewWorkflowTemplateRepo(pg)

	owner := "33333333-3333-3333-3333-333333333333"
	other := "44444444-4444-4444-4444-444444444444"

	created, err := repo.Create(ctx, &entity.CreateWorkflowTemplateRequest{
		AccountID: owner,
		Name:      "owned",
		TeamSpec:  entity.TeamSpec{},
		IsEnabled: true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, exists, err := repo.Get(ctx, owner, created.ID); err != nil || !exists {
		t.Fatalf("the owner must read its template: exists=%v err=%v", exists, err)
	}

	foreign, exists, err := repo.Get(ctx, other, created.ID)
	if err != nil {
		t.Fatalf("a foreign read must not fail loudly: %v", err)
	}

	if exists || foreign.ID != "" {
		t.Fatalf("another account read the template: exists=%v record=%+v", exists, foreign)
	}

	// And an unknown id answers identically, so the two cases are
	// indistinguishable from outside.
	unknown, exists, err := repo.Get(ctx, other, "no-such-template")
	if err != nil || exists || unknown.ID != "" {
		t.Fatalf("unknown id = %+v/%v/%v, want not found and no error", unknown, exists, err)
	}
}

func TestWorkflowTemplatePostgresDeleteIsScopedToItsAccount(t *testing.T) {
	ctx, pg, _ := newAgentOSPlanPostgresIntegrationDB(t)

	repo := NewWorkflowTemplateRepo(pg)

	owner := "55555555-5555-5555-5555-555555555555"
	other := "66666666-6666-6666-6666-666666666666"

	created, err := repo.Create(ctx, &entity.CreateWorkflowTemplateRequest{
		AccountID: owner,
		Name:      "delete-me",
		TeamSpec:  entity.TeamSpec{},
		IsEnabled: true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	err = repo.Delete(ctx, other, created.ID)
	if !errors.Is(err, ErrTemplateNotFound) {
		t.Fatalf("a foreign delete must report not found, got %v", err)
	}

	if _, exists, err := repo.Get(ctx, owner, created.ID); err != nil || !exists {
		t.Fatalf("the template survived: exists=%v err=%v", exists, err)
	}

	if err := repo.Delete(ctx, owner, created.ID); err != nil {
		t.Fatalf("the owner must be able to delete it: %v", err)
	}

	if _, exists, err := repo.Get(ctx, owner, created.ID); err != nil || exists {
		t.Fatalf("the template was not deleted: exists=%v err=%v", exists, err)
	}
}
