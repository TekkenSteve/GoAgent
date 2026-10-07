package agentos_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	agentosroot "github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/stretchr/testify/require"
)

// The guard exists for one situation: a library newer than its database. This
// test builds that situation for real — every migration but the newest — and
// then closes the gap, so the refusal is proven against a database rather than
// against a stub.
func TestCheckSchemaRefusesARolledBackDatabase(t *testing.T) {
	t.Parallel()

	pgURL := os.Getenv("AGENTOS_TEST_PG_URL")
	if pgURL == "" {
		t.Skip("AGENTOS_TEST_PG_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	migrations := migrationVersions(t)
	require.GreaterOrEqual(t, len(migrations), 2, "the test needs at least two migrations")

	// The database stands at the second-to-last migration.
	pg := newSchemaGuardSchema(ctx, t, pgURL, migrations[:len(migrations)-1])

	status, err := agentosroot.CheckSchema(ctx, pg.Pool)
	require.ErrorIs(t, err, agentosroot.ErrSchemaTooOld,
		"a database one migration behind must be refused, got status %+v", status)
	require.Less(t, status.Version, agentosroot.MinSchemaVersion)

	// And becomes servable when the last migration is applied — the history row
	// the migrate CLI writes for it included.
	applyMigration(ctx, t, pg, migrations[len(migrations)-1])
	recordMigration(ctx, t, pg, agentosroot.MinSchemaVersion, false)

	status, err = agentosroot.CheckSchema(ctx, pg.Pool)
	require.NoError(t, err, "the same database must be servable once migrated")
	require.Equal(t, agentosroot.MinSchemaVersion, status.Version)
}

// A migration that failed partway is refused as well: the schema is neither at
// the old version nor at the new one, and queries would fail unpredictably.
func TestCheckSchemaRefusesADirtyMigration(t *testing.T) {
	t.Parallel()

	pgURL := os.Getenv("AGENTOS_TEST_PG_URL")
	if pgURL == "" {
		t.Skip("AGENTOS_TEST_PG_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	migrations := migrationVersions(t)
	pg := newSchemaGuardSchema(ctx, t, pgURL, migrations)

	if _, err := pg.Pool.Exec(ctx, `UPDATE schema_migrations SET dirty = true`); err != nil {
		t.Fatalf("mark the migration dirty: %v", err)
	}

	status, err := agentosroot.CheckSchema(ctx, pg.Pool)

	require.ErrorIs(t, err, agentosroot.ErrSchemaDirty)
	require.True(t, status.Dirty)
}

// migrationVersions lists this repository's migrations oldest-first.
func migrationVersions(t *testing.T) []string {
	t.Helper()

	entries, err := os.ReadDir(filepath.Join("..", "migrations"))
	require.NoError(t, err)

	names := make([]string, 0, len(entries))

	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".up.sql") {
			names = append(names, entry.Name())
		}
	}

	sort.Strings(names)

	return names
}

// newSchemaGuardSchema builds a throwaway schema, applies the given migrations
// to it, and records the highest applied version as the migration history — the
// row the migrate CLI would have written, and the one the guard reads.
//
// A schema rather than a database: the guard reads schema_migrations through
// the connection's search_path, which keeps the test from having to create and
// drop databases.
func newSchemaGuardSchema(ctx context.Context, t *testing.T, pgURL string, migrations []string) *postgres.Postgres {
	t.Helper()

	admin, err := postgres.New(pgURL, postgres.MaxPoolSize(2), postgres.ConnAttempts(3), postgres.ConnTimeout(time.Second))
	require.NoError(t, err, "connect to the test database")

	t.Cleanup(admin.Close)

	schemaName := fmt.Sprintf("schema_guard_%d", time.Now().UnixNano())

	if _, err := admin.Pool.Exec(ctx, "CREATE SCHEMA "+schemaName); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()

		if _, err := admin.Pool.Exec(cleanupCtx, "DROP SCHEMA "+schemaName+" CASCADE"); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("drop schema %s: %v", schemaName, err)
		}
	})

	// The schema travels in the connection string, so every pooled connection
	// sees the same tables.
	scoped, err := postgres.New(withSearchPath(pgURL, schemaName), postgres.MaxPoolSize(2), postgres.ConnAttempts(3), postgres.ConnTimeout(time.Second))
	require.NoError(t, err, "connect to the test schema")

	t.Cleanup(scoped.Close)

	if _, err := scoped.Pool.Exec(ctx, `CREATE TABLE schema_migrations (version bigint NOT NULL, dirty boolean NOT NULL)`); err != nil {
		t.Fatalf("create schema_migrations: %v", err)
	}

	final := int64(0)

	for _, name := range migrations {
		applyMigration(ctx, t, scoped, name)

		version, err := migrationVersion(name)
		require.NoError(t, err)

		final = version
	}

	recordMigration(ctx, t, scoped, final, false)

	return scoped
}

// applyMigration runs one migration file's statements.
func applyMigration(ctx context.Context, t *testing.T, pg *postgres.Postgres, name string) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("..", "migrations", name))
	require.NoError(t, err, "read migration %s", name)

	if _, err := pg.Pool.Exec(ctx, string(data)); err != nil {
		t.Fatalf("apply migration %s: %v", name, err)
	}
}

func recordMigration(ctx context.Context, t *testing.T, pg *postgres.Postgres, version int64, dirty bool) {
	t.Helper()

	if _, err := pg.Pool.Exec(ctx, `TRUNCATE schema_migrations`); err != nil {
		t.Fatalf("clear migration history: %v", err)
	}

	if _, err := pg.Pool.Exec(ctx, `INSERT INTO schema_migrations (version, dirty) VALUES ($1, $2)`, version, dirty); err != nil {
		t.Fatalf("record migration version %d: %v", version, err)
	}
}

// errMigrationName reports a migration file that does not carry its version.
var errMigrationName = errors.New("migration is not named <version>_<name>.up.sql")

// migrationVersion reads the version out of a migration file name.
func migrationVersion(name string) (int64, error) {
	raw, _, found := strings.Cut(name, "_")
	if !found {
		return 0, fmt.Errorf("%w: %s", errMigrationName, name)
	}

	return strconv.ParseInt(raw, 10, 64)
}

func withSearchPath(pgURL, schema string) string {
	parsed, err := url.Parse(pgURL)
	if err != nil {
		return pgURL
	}

	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()

	return parsed.String()
}
