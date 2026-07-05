package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/jackc/pgx/v5"
)

func marshalProcessPlatformJSON(name string, value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%s - marshal: %w", name, err)
	}

	return data, nil
}

func unmarshalProcessPlatformJSON[T any](name string, data []byte) (T, error) {
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		return value, fmt.Errorf("%s - decode: %w", name, err)
	}

	return value, nil
}

func processPlatformResourceColumns(resource agentos.ResourceRef) (kind, id string) {
	return string(resource.Kind), resource.ResourceID
}

func processPlatformNullableTime(ts time.Time) any {
	if ts.IsZero() {
		return nil
	}

	return ts
}

func scanSingleJSON[T any](row pgx.Row, name string) (T, error) {
	var data []byte
	if err := row.Scan(&data); err != nil {
		var zero T

		return zero, err
	}

	return unmarshalProcessPlatformJSON[T](name, data)
}

func scanOptionalJSON[T any](row pgx.Row, name string) (value T, exists bool, err error) {
	value, err = scanSingleJSON[T](row, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return value, false, nil
	}

	if err != nil {
		return value, false, err
	}

	return value, true, nil
}

func scanSpecStatusJSON[Spec, Status any](row pgx.Row, name string) (spec Spec, status Status, exists bool, err error) {
	var specJSON, statusJSON []byte
	if err := row.Scan(&specJSON, &statusJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return spec, status, false, nil
		}

		return spec, status, false, fmt.Errorf("%s - query: %w", name, err)
	}

	spec, err = unmarshalProcessPlatformJSON[Spec](name+" spec", specJSON)
	if err != nil {
		return spec, status, false, err
	}

	status, err = unmarshalProcessPlatformJSON[Status](name+" status", statusJSON)
	if err != nil {
		return spec, status, false, err
	}

	return spec, status, true, nil
}

func scanJSONRows[T any](rows pgx.Rows, name string) ([]T, error) {
	var values []T

	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("%s - scan: %w", name, err)
		}

		value, err := unmarshalProcessPlatformJSON[T](name, data)
		if err != nil {
			return nil, err
		}

		values = append(values, value)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s - rows: %w", name, err)
	}

	return values, nil
}

type processPlatformTenantRef struct {
	ID        string
	AccountID string
	ProjectID string
}

func getProcessPlatformProjection[Spec, Status any](
	ctx context.Context,
	ref processPlatformTenantRef,
	lookup func(context.Context, string) (Spec, Status, bool, error),
	validateTenant func(Spec) error,
) (spec Spec, status Status, exists bool, err error) {
	spec, status, exists, err = lookup(ctx, ref.ID)
	if err != nil || !exists {
		return spec, status, exists, err
	}

	if err := validateTenant(spec); err != nil {
		return spec, status, false, err
	}

	return spec, status, true, nil
}

type processPlatformStatusListQuery struct {
	Name           string
	Table          string
	OrderColumn    string
	AccountID      string
	ProjectID      string
	ProcessID      string
	ResourceKind   string
	ResourceID     string
	Kind           string
	LifecycleState string
	Limit          int
}

func listProcessPlatformStatuses[T any](ctx context.Context, pg *postgres.Postgres, query *processPlatformStatusListQuery) ([]T, error) {
	builder := pg.Builder.
		Select("status_json").
		From(query.Table).
		Where(sq.Eq{"account_id": query.AccountID, "project_id": query.ProjectID}).
		OrderBy("updated_at DESC", query.OrderColumn+" ASC")

	if query.ProcessID != "" {
		builder = builder.Where(sq.Eq{"process_id": query.ProcessID})
	}

	if query.ResourceKind != "" {
		builder = builder.Where(sq.Eq{"resource_kind": query.ResourceKind, "resource_id": query.ResourceID})
	}

	if query.Kind != "" {
		builder = builder.Where(sq.Eq{"kind": query.Kind})
	}

	if query.LifecycleState != "" {
		builder = builder.Where(sq.Eq{"lifecycle_state": query.LifecycleState})
	}

	if query.Limit > 0 {
		builder = builder.Limit(uint64(query.Limit))
	}

	sqlQuery, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s - builder: %w", query.Name, err)
	}

	rows, err := pg.Pool.Query(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("%s - query: %w", query.Name, err)
	}
	defer rows.Close()

	return scanJSONRows[T](rows, query.Name)
}

type processPlatformProjectionInsert struct {
	Name           string
	Table          string
	IDColumn       string
	ID             string
	AccountID      string
	ProjectID      string
	ProcessID      string
	ResourceKind   string
	ResourceID     string
	Kind           string
	LifecycleState string
	IdempotencyKey string
	SpecJSON       []byte
	StatusJSON     []byte
	RequestedAt    time.Time
	UpdatedAt      time.Time
}

func buildProcessPlatformProjectionInsert(
	name string,
	table string,
	idColumn string,
	resource agentos.ResourceRef,
	specJSON []byte,
	statusJSON []byte,
	values *processPlatformProjectionValues,
) processPlatformProjectionInsert {
	resourceKind, resourceID := processPlatformResourceColumns(resource)

	return processPlatformProjectionInsert{
		Name:           name,
		Table:          table,
		IDColumn:       idColumn,
		ID:             values.ID,
		AccountID:      values.AccountID,
		ProjectID:      values.ProjectID,
		ProcessID:      values.ProcessID,
		ResourceKind:   resourceKind,
		ResourceID:     resourceID,
		Kind:           values.Kind,
		LifecycleState: values.LifecycleState,
		IdempotencyKey: values.IdempotencyKey,
		SpecJSON:       specJSON,
		StatusJSON:     statusJSON,
		RequestedAt:    values.RequestedAt,
		UpdatedAt:      values.UpdatedAt,
	}
}

type processPlatformProjectionValues struct {
	ID             string
	AccountID      string
	ProjectID      string
	ProcessID      string
	Kind           string
	LifecycleState string
	IdempotencyKey string
	RequestedAt    time.Time
	UpdatedAt      time.Time
}

func insertProcessPlatformProjection[T any](ctx context.Context, pg *postgres.Postgres, row *processPlatformProjectionInsert) (T, error) {
	query, args, err := pg.Builder.
		Insert(row.Table).
		Columns(
			row.IDColumn,
			"account_id",
			"project_id",
			"process_id",
			"resource_kind",
			"resource_id",
			"kind",
			"lifecycle_state",
			"idempotency_key",
			"spec_json",
			"status_json",
			"requested_at",
			"updated_at",
		).
		Values(
			row.ID,
			row.AccountID,
			row.ProjectID,
			row.ProcessID,
			row.ResourceKind,
			row.ResourceID,
			row.Kind,
			row.LifecycleState,
			row.IdempotencyKey,
			row.SpecJSON,
			row.StatusJSON,
			processPlatformNullableTime(row.RequestedAt),
			row.UpdatedAt,
		).
		Suffix("RETURNING status_json").
		ToSql()
	if err != nil {
		var zero T

		return zero, fmt.Errorf("%s - builder: %w", row.Name, err)
	}

	return scanSingleJSON[T](pg.Pool.QueryRow(ctx, query, args...), row.Name)
}

type processPlatformProjectionCreateConfig[Spec, Status any] struct {
	SpecMarshalName   string
	StatusMarshalName string
	Normalize         func(*Spec, *Status) Status
	BuildRow          func(*Spec, *Status, []byte, []byte) processPlatformProjectionInsert
	ResolveInsertErr  func(context.Context, error, *Spec) (Status, error)
}

func createProcessPlatformProjection[Spec, Status any](
	ctx context.Context,
	pg *postgres.Postgres,
	spec *Spec,
	status *Status,
	config processPlatformProjectionCreateConfig[Spec, Status],
) (createdStatus Status, created bool, err error) {
	normalized := config.Normalize(spec, status)

	specJSON, err := marshalProcessPlatformJSON(config.SpecMarshalName, spec)
	if err != nil {
		return createdStatus, false, err
	}

	statusJSON, err := marshalProcessPlatformJSON(config.StatusMarshalName, &normalized)
	if err != nil {
		return createdStatus, false, err
	}

	row := config.BuildRow(spec, &normalized, specJSON, statusJSON)

	inserted, err := insertProcessPlatformProjection[Status](ctx, pg, &row)
	if err != nil {
		resolved, resolveErr := config.ResolveInsertErr(ctx, err, spec)

		return resolved, false, resolveErr
	}

	return inserted, true, nil
}

func resolveProcessPlatformProjectionInsertErr[Status any](
	ctx context.Context,
	insertErr error,
	specID string,
	idempotencyErr error,
	insertErrPrefix string,
	conflictMessage string,
	findExisting func(context.Context) (Status, bool, error),
) (status Status, err error) {
	if !isPostgresUniqueViolation(insertErr) {
		return status, fmt.Errorf("%s: %w", insertErrPrefix, insertErr)
	}

	existing, exists, err := findExisting(ctx)
	if err != nil {
		return status, err
	}

	if exists {
		return existing, nil
	}

	return status, fmt.Errorf("%w: %s %q already exists", idempotencyErr, conflictMessage, specID)
}

type processPlatformStatusClaim struct {
	ClaimName       string
	ExistingName    string
	InsertErrPrefix string
	SelectErrPrefix string
	InsertSQL       string
	InsertArgs      []any
	SelectSQL       string
	SelectArgs      []any
}

func claimProcessPlatformStatus[T any](ctx context.Context, tx pgx.Tx, claim *processPlatformStatusClaim) (claimed bool, value T, err error) {
	value, err = scanSingleJSON[T](tx.QueryRow(ctx, claim.InsertSQL, claim.InsertArgs...), claim.ClaimName)
	if err == nil {
		return true, value, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return false, value, fmt.Errorf("%s: %w", claim.InsertErrPrefix, err)
	}

	existing, err := scanSingleJSON[T](tx.QueryRow(ctx, claim.SelectSQL, claim.SelectArgs...), claim.ExistingName)
	if err != nil {
		return false, value, fmt.Errorf("%s: %w", claim.SelectErrPrefix, err)
	}

	return false, existing, nil
}
