// Package agentos is the embedding contract of the AgentOS control plane: what
// a host process must have in place before it may serve.
//
// The Go module version and the database schema version are independent — a
// host can build against a newer library and run against an older database —
// and the failure mode of getting that wrong is a variety of strange runtime
// errors rather than one clear one. CheckSchema turns it into one clear one, at
// startup, before anything is served.
package agentos

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// MinSchemaVersion is the oldest database schema this library serves.
//
// It is the newest migration in this repository's history: a library that
// expects the newest schema must say so, because nothing else will notice that
// the database is a version behind until a query fails in production. The value
// is kept honest by TestMinSchemaVersionMatchesNewestMigration, which reads the
// migrations directory.
const MinSchemaVersion int64 = 20261010000001

var (
	// ErrSchemaTooOld reports a database migrated to an older version than
	// this library requires.
	ErrSchemaTooOld = errors.New("agentos: database schema is older than this library requires")
	// ErrSchemaDirty reports a database whose last migration failed, leaving
	// the schema half-applied.
	ErrSchemaDirty = errors.New("agentos: database schema is dirty")
	// ErrSchemaMissing reports a database that has never been migrated.
	ErrSchemaMissing = errors.New("agentos: database has no migration history")
)

// SchemaQuerier is the database handle CheckSchema needs: one row, two columns.
// It is satisfied by *pgxpool.Pool, *pgx.Conn and pgx.Tx, so a host passes
// whatever handle it already holds.
type SchemaQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// SchemaStatus is what a database reports about itself.
type SchemaStatus struct {
	// Version is the migration the database is at.
	Version int64
	// Dirty reports a migration that failed partway.
	Dirty bool
}

// CheckSchema reports whether a database may be served by this library.
//
// A missing migration history, a failed migration, and a version older than
// MinSchemaVersion are all refusals: each one means queries will fail in ways
// that do not name the cause, and the point of checking is to name it.
func CheckSchema(ctx context.Context, db SchemaQuerier) (SchemaStatus, error) {
	var status SchemaStatus

	err := db.QueryRow(ctx, `SELECT version, dirty FROM schema_migrations`).Scan(&status.Version, &status.Dirty)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SchemaStatus{}, fmt.Errorf("%w: run the migrations (see cmd/agentos-migrate)", ErrSchemaMissing)
		}

		return SchemaStatus{}, fmt.Errorf("%w: read schema_migrations: %w", ErrSchemaMissing, err)
	}

	if status.Dirty {
		return status, fmt.Errorf(
			"%w: version %d did not complete; fix it forward or down before serving (%v)",
			ErrSchemaDirty, status.Version, status,
		)
	}

	if status.Version < MinSchemaVersion {
		return status, fmt.Errorf(
			"%w: database is at %d, this library needs %d or newer; run the migrations (see cmd/agentos-migrate)",
			ErrSchemaTooOld, status.Version, MinSchemaVersion,
		)
	}

	return status, nil
}
