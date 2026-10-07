//go:build postgres_integration

package persistent

import (
	"testing"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

const (
	agentIntegrationAccountID = "33333333-3333-3333-3333-333333333333"
	agentIntegrationVersionID = "44444444-4444-4444-4444-444444444444"
)

// TestAgentPostgresRoundTrip covers the agent definition lifecycle against a
// real database: the ID is minted by the database, partial updates keep
// omitted fields, and delete removes the record.
func TestAgentPostgresRoundTrip(t *testing.T) {
	ctx, pg, _ := newAgentOSPlanPostgresIntegrationDB(t)
	applyIntegrationMigration(t, pg, "20260823000001_create_agents.up.sql")

	repo := NewAgentRepo(pg)

	created, err := repo.Create(ctx, &entity.CreateAgentRequest{
		AccountID:    agentIntegrationAccountID,
		Name:         "round-trip",
		Description:  "desc",
		SystemPrompt: "prompt",
		ModelRef:     "model-a",
		Config:       entity.LLMConfig{Model: "model-a"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if created.AgentID == "" {
		t.Fatal("Create: database did not mint an agent id")
	}

	if created.IsDefault {
		t.Fatal("Create: a new agent must not be the account default")
	}

	if created.Config.Model != "model-a" {
		t.Fatalf("Create config = %+v, want the submitted config", created.Config)
	}

	loaded, exists, err := repo.Get(ctx, created.AgentID)
	if err != nil || !exists {
		t.Fatalf("Get = %v/%v, want found", exists, err)
	}

	if loaded.Name != created.Name || loaded.CreatedAt.IsZero() {
		t.Fatalf("loaded = %+v, want the created record", loaded)
	}

	name := "renamed"
	isDefault := true

	updated, err := repo.Update(ctx, created.AgentID, entity.UpdateAgentRequest{Name: &name, IsDefault: &isDefault})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	if updated.Name != "renamed" || !updated.IsDefault {
		t.Fatalf("updated = %q/%v, want renamed/true", updated.Name, updated.IsDefault)
	}

	if updated.Description != "desc" || updated.SystemPrompt != "prompt" || updated.ModelRef != "model-a" {
		t.Fatalf("omitted fields changed: %+v", updated)
	}

	listed, err := repo.ListByAccount(ctx, agentIntegrationAccountID)
	if err != nil {
		t.Fatalf("ListByAccount: %v", err)
	}

	if len(listed) != 1 || listed[0].AgentID != created.AgentID {
		t.Fatalf("listed = %+v, want the created agent", listed)
	}

	if err := repo.Delete(ctx, created.AgentID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, exists, err := repo.Get(ctx, created.AgentID); err != nil || exists {
		t.Fatalf("Get after delete = %v/%v, want missing", exists, err)
	}
}

// TestAgentVersionPostgresRoundTrip covers the version snapshot lifecycle,
// including the JSON tool bindings document.
func TestAgentVersionPostgresRoundTrip(t *testing.T) {
	ctx, pg, _ := newAgentOSPlanPostgresIntegrationDB(t)
	applyIntegrationMigration(t, pg, "20260823000001_create_agents.up.sql")

	repo := NewAgentRepo(pg)

	created, err := repo.Create(ctx, &entity.CreateAgentRequest{
		AccountID:    agentIntegrationAccountID,
		Name:         "versioned",
		SystemPrompt: "prompt",
		ModelRef:     "model-b",
		Config:       entity.LLMConfig{Model: "model-b"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	version := &entity.AgentVersionRecord{
		VersionID:         agentIntegrationVersionID,
		AgentID:           created.AgentID,
		VersionName:       "v1",
		SystemPrompt:      "prompt-v1",
		ModelRef:          "model-b",
		Config:            entity.LLMConfig{Model: "model-b"},
		ToolBindings:      []entity.ToolBinding{{Name: "search"}},
		ChangeDescription: "first snapshot",
	}

	if err := repo.CreateVersion(ctx, version); err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}

	loaded, exists, err := repo.GetVersion(ctx, agentIntegrationVersionID)
	if err != nil || !exists {
		t.Fatalf("GetVersion = %v/%v, want found", exists, err)
	}

	if loaded.VersionName != "v1" || loaded.ChangeDescription != "first snapshot" {
		t.Fatalf("loaded version = %+v, want the stored snapshot", loaded)
	}

	if len(loaded.ToolBindings) != 1 || loaded.ToolBindings[0].Name != "search" {
		t.Fatalf("tool bindings = %+v, want the stored binding", loaded.ToolBindings)
	}

	versions, err := repo.ListVersions(ctx, created.AgentID)
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}

	if len(versions) != 1 || versions[0].VersionID != agentIntegrationVersionID {
		t.Fatalf("versions = %+v, want the created snapshot", versions)
	}

	if err := repo.Delete(ctx, created.AgentID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, exists, err := repo.GetVersion(ctx, agentIntegrationVersionID); err != nil || exists {
		t.Fatalf("GetVersion after agent delete = %v/%v, want cascaded removal", exists, err)
	}
}
