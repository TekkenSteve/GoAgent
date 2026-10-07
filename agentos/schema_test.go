package agentos_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// The library's stated minimum schema version is a promise about this
// repository's migrations, and a promise nothing enforces drifts. This test
// reads the migrations directory, so adding a migration without raising the
// constant fails here instead of in a host's production database.
func TestMinSchemaVersionMatchesNewestMigration(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(filepath.Join("..", "migrations"))
	require.NoError(t, err)

	newest := int64(0)

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}

		raw, _, found := strings.Cut(name, "_")
		if !found {
			t.Fatalf("migration %q is not named <version>_<name>.up.sql", name)
		}

		version, err := strconv.ParseInt(raw, 10, 64)
		require.NoError(t, err, "migration %q has a non-numeric version", name)

		if version > newest {
			newest = version
		}
	}

	require.NotZero(t, newest, "no migrations found")
	require.Equal(t, newest, agentos.MinSchemaVersion,
		"MinSchemaVersion must name the newest migration; a library that needs a schema must say so")
}

// A database one migration behind is refused, and the refusal names both
// versions: the failure a host sees at startup is the one it can act on.
func TestCheckSchemaRefusesOlderSchema(t *testing.T) {
	t.Parallel()

	db := &stubSchemaQuerier{status: agentos.SchemaStatus{Version: agentos.MinSchemaVersion - 1}}

	status, err := agentos.CheckSchema(t.Context(), db)

	require.ErrorIs(t, err, agentos.ErrSchemaTooOld)
	require.Equal(t, agentos.MinSchemaVersion-1, status.Version)
	require.Contains(t, err.Error(), strconv.FormatInt(agentos.MinSchemaVersion, 10))
	require.Contains(t, err.Error(), "run the migrations")
}

func TestCheckSchemaRefusesDirtySchema(t *testing.T) {
	t.Parallel()

	db := &stubSchemaQuerier{status: agentos.SchemaStatus{Version: agentos.MinSchemaVersion, Dirty: true}}

	status, err := agentos.CheckSchema(t.Context(), db)

	require.ErrorIs(t, err, agentos.ErrSchemaDirty)
	require.True(t, status.Dirty)
}

func TestCheckSchemaRefusesMissingHistory(t *testing.T) {
	t.Parallel()

	db := &stubSchemaQuerier{err: pgx.ErrNoRows}

	_, err := agentos.CheckSchema(t.Context(), db)

	require.ErrorIs(t, err, agentos.ErrSchemaMissing)
	require.Contains(t, err.Error(), "run the migrations")
}

func TestCheckSchemaAcceptsCurrentAndNewer(t *testing.T) {
	t.Parallel()

	for _, version := range []int64{agentos.MinSchemaVersion, agentos.MinSchemaVersion + 1} {
		db := &stubSchemaQuerier{status: agentos.SchemaStatus{Version: version}}

		status, err := agentos.CheckSchema(t.Context(), db)
		require.NoError(t, err, "version %d must be servable", version)
		require.Equal(t, version, status.Version)
	}
}

var (
	errStubScanArity   = errors.New("unexpected scan arity")
	errStubVersionDest = errors.New("unexpected version destination")
	errStubDirtyDest   = errors.New("unexpected dirty destination")
)

// stubSchemaQuerier stands in for a database handle.
type stubSchemaQuerier struct {
	status agentos.SchemaStatus
	err    error
}

func (s *stubSchemaQuerier) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return stubSchemaRow{status: s.status, err: s.err}
}

type stubSchemaRow struct {
	status agentos.SchemaStatus
	err    error
}

func (r stubSchemaRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}

	if len(dest) != 2 {
		return errStubScanArity
	}

	version, ok := dest[0].(*int64)
	if !ok {
		return errStubVersionDest
	}

	dirty, ok := dest[1].(*bool)
	if !ok {
		return errStubDirtyDest
	}

	*version = r.status.Version
	*dirty = r.status.Dirty

	return nil
}
