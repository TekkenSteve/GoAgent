//go:build postgres_integration

package persistent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
)

// applyIntegrationMigration runs one migration file against an integration
// database. The plan-scoped helper carries the AgentOS migration set only;
// domains outside it bring in the migrations they need through this.
func applyIntegrationMigration(t *testing.T, pg *postgres.Postgres, migration string) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", migration))
	if err != nil {
		t.Fatalf("read migration %s: %v", migration, err)
	}

	if _, err := pg.Pool.Exec(t.Context(), string(data)); err != nil {
		t.Fatalf("apply migration %s: %v", migration, err)
	}
}
