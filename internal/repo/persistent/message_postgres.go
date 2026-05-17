package persistent

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	sq "github.com/Masterminds/squirrel"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/pkg/postgres"
	"github.com/jackc/pgx/v5"
)

// MessageRepo implements repo.WarmStateRepo and repo.ColdStateRepo with Postgres.
type MessageRepo struct {
	*postgres.Postgres
}

// NewMessageRepo creates a Postgres-backed message repository.
func NewMessageRepo(pg *postgres.Postgres) *MessageRepo {
	return &MessageRepo{pg}
}

// insertRecord is a shared helper for single-row inserts with RETURNING id.
func (r *MessageRepo) insertRecord(ctx context.Context, table string, columns []string, values ...any) (string, error) {
	sql, args, err := r.Builder.
		Insert(table).
		Columns(columns...).
		Values(values...).
		Suffix("RETURNING id").
		ToSql()
	if err != nil {
		return "", fmt.Errorf("MessageRepo - insert %s - builder: %w", table, err)
	}

	var id int64
	if err := r.Pool.QueryRow(ctx, sql, args...).Scan(&id); err != nil {
		return "", fmt.Errorf("MessageRepo - insert %s - query: %w", table, err)
	}

	return strconv.FormatInt(id, 10), nil
}

// getRecordByRef is a shared helper for single-row queries by integer ref ID.
func getRecordByRef[T any](ctx context.Context, r *MessageRepo, ref, table string, columns []string, dest func(*T) []any) (record T, exists bool, err error) {
	var zero T

	id, err := strconv.ParseInt(ref, 10, 64)
	if err != nil {
		return zero, false, fmt.Errorf("MessageRepo - get %s - parse ref: %w", table, err)
	}

	sql, args, err := r.Builder.
		Select(columns...).
		From(table).
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return zero, false, fmt.Errorf("MessageRepo - get %s - builder: %w", table, err)
	}

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(dest(&record)...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return zero, false, nil
		}

		return zero, false, fmt.Errorf("MessageRepo - get %s - query: %w", table, err)
	}

	return record, true, nil
}

// PersistMessage inserts a message record and returns its ref ID.
func (r *MessageRepo) PersistMessage(ctx context.Context, record entity.MessageRecord) (string, error) {
	return r.insertRecord(
		ctx, "messages",
		[]string{"run_id", "role", "content", "tool_call_id"},
		record.RunID, record.Role, record.Content, record.ToolCallID,
	)
}

// GetMessage retrieves a message record by its ref ID.
func (r *MessageRepo) GetMessage(ctx context.Context, ref string) (entity.MessageRecord, bool, error) {
	return getRecordByRef(
		ctx, r, ref, "messages",
		[]string{"run_id", "role", "content", "tool_call_id"},
		func(m *entity.MessageRecord) []any {
			return []any{&m.RunID, &m.Role, &m.Content, &m.ToolCallID}
		},
	)
}

// PersistToolResult inserts a tool result record and returns its ref ID.
func (r *MessageRepo) PersistToolResult(ctx context.Context, record entity.ToolResultRecord) (string, error) {
	return r.insertRecord(
		ctx, "tool_results",
		[]string{"run_id", "tool_call_id", "tool_name", "result_json"},
		record.RunID, record.ToolCallID, record.ToolName, record.ResultJSON,
	)
}

// GetToolResult retrieves a tool result record by its ref ID.
func (r *MessageRepo) GetToolResult(ctx context.Context, ref string) (entity.ToolResultRecord, bool, error) {
	return getRecordByRef(
		ctx, r, ref, "tool_results",
		[]string{"run_id", "tool_call_id", "tool_name", "result_json"},
		func(t *entity.ToolResultRecord) []any {
			return []any{&t.RunID, &t.ToolCallID, &t.ToolName, &t.ResultJSON}
		},
	)
}

// PersistArchive inserts an archive record and returns its ref ID.
func (r *MessageRepo) PersistArchive(ctx context.Context, record entity.ArchiveRecord) (string, error) {
	return r.insertRecord(
		ctx, "archives",
		[]string{"run_id", "payload_type", "content"},
		record.RunID, record.PayloadType, record.Content,
	)
}

// scanRows is a generic helper that iterates pgx.Rows and scans each row into T.
func scanRows[T any](rows pgx.Rows, scan func(*T) []any) ([]T, error) {
	var records []T

	for rows.Next() {
		var record T
		if err := rows.Scan(scan(&record)...); err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

// scanMessageRows iterates pgx.Rows and scans MessageRecords.
func scanMessageRows(rows pgx.Rows) ([]entity.MessageRecord, error) {
	return scanRows(rows, func(m *entity.MessageRecord) []any {
		return []any{&m.RunID, &m.Role, &m.Content, &m.ToolCallID}
	})
}

// scanToolResultRecords iterates pgx.Rows and scans ToolResultRecords.
func scanToolResultRecords(rows pgx.Rows) ([]entity.ToolResultRecord, error) {
	return scanRows(rows, func(t *entity.ToolResultRecord) []any {
		return []any{&t.RunID, &t.ToolCallID, &t.ToolName, &t.ResultJSON}
	})
}

func buildQuery(builder sq.StatementBuilderType, columns []string, table, runID string, limit, offset uint64) (sql string, args []any, err error) {
	sql, args, err = builder.
		Select(columns...).
		From(table).
		Where(sq.Eq{"run_id": runID}).
		OrderBy("id ASC").
		Limit(limit).
		Offset(offset).
		ToSql()

	return sql, args, err
}

// queryAndScan is a generic helper that builds a query, executes it, and scans the results.
func queryAndScan[T any](ctx context.Context, r *MessageRepo, table string, columns []string, runID string, limit, offset uint64, scanFn func(pgx.Rows) ([]T, error), name string) ([]T, error) {
	sql, args, err := buildQuery(r.Builder, columns, table, runID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("MessageRepo - %s - builder: %w", name, err)
	}

	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("MessageRepo - %s - query: %w", name, err)
	}
	defer rows.Close()

	records, err := scanFn(rows)
	if err != nil {
		return nil, fmt.Errorf("MessageRepo - %s - scan: %w", name, err)
	}

	return records, nil
}

// ListMessagesByRun retrieves paginated message records for a run.
func (r *MessageRepo) ListMessagesByRun(ctx context.Context, runID string, limit, offset uint64) ([]entity.MessageRecord, error) {
	return queryAndScan(ctx, r, "messages", []string{"run_id", "role", "content", "tool_call_id"}, runID, limit, offset, scanMessageRows, "ListMessagesByRun")
}

// ListToolResultsByRun retrieves paginated tool result records for a run.
func (r *MessageRepo) ListToolResultsByRun(ctx context.Context, runID string, limit, offset uint64) ([]entity.ToolResultRecord, error) {
	return queryAndScan(ctx, r, "tool_results", []string{"run_id", "tool_call_id", "tool_name", "result_json"}, runID, limit, offset, scanToolResultRecords, "ListToolResultsByRun")
}

// GetArchive retrieves an archive record by its ref ID.
func (r *MessageRepo) GetArchive(ctx context.Context, ref string) (entity.ArchiveRecord, bool, error) {
	return getRecordByRef(
		ctx, r, ref, "archives",
		[]string{"run_id", "payload_type", "content"},
		func(a *entity.ArchiveRecord) []any {
			return []any{&a.RunID, &a.PayloadType, &a.Content}
		},
	)
}
