package persistent

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/pkg/postgres"
)

// MessageRepo implements repo.WarmStateRepo and repo.ColdStateRepo with Postgres.
type MessageRepo struct {
	*postgres.Postgres
}

// NewMessageRepo creates a Postgres-backed message repository.
func NewMessageRepo(pg *postgres.Postgres) *MessageRepo {
	return &MessageRepo{pg}
}

// PersistMessage inserts a message record and returns its ref ID.
func (r *MessageRepo) PersistMessage(ctx context.Context, record entity.MessageRecord) (string, error) {
	sql, args, err := r.Builder.
		Insert("messages").
		Columns("run_id", "role", "content", "tool_call_id").
		Values(record.RunID, record.Role, record.Content, record.ToolCallID).
		Suffix("RETURNING id").
		ToSql()
	if err != nil {
		return "", fmt.Errorf("MessageRepo - PersistMessage - builder: %w", err)
	}

	var id int64
	if err := r.Pool.QueryRow(ctx, sql, args...).Scan(&id); err != nil {
		return "", fmt.Errorf("MessageRepo - PersistMessage - query: %w", err)
	}

	return strconv.FormatInt(id, 10), nil
}

// GetMessage retrieves a message record by its ref ID.
func (r *MessageRepo) GetMessage(ctx context.Context, ref string) (entity.MessageRecord, bool, error) {
	id, err := strconv.ParseInt(ref, 10, 64)
	if err != nil {
		return entity.MessageRecord{}, false, fmt.Errorf("MessageRepo - GetMessage - parse ref: %w", err)
	}

	sql, args, err := r.Builder.
		Select("run_id", "role", "content", "tool_call_id").
		From("messages").
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return entity.MessageRecord{}, false, fmt.Errorf("MessageRepo - GetMessage - builder: %w", err)
	}

	var record entity.MessageRecord
	err = r.Pool.QueryRow(ctx, sql, args...).Scan(&record.RunID, &record.Role, &record.Content, &record.ToolCallID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.MessageRecord{}, false, nil
		}
		return entity.MessageRecord{}, false, fmt.Errorf("MessageRepo - GetMessage - query: %w", err)
	}

	return record, true, nil
}

// PersistToolResult inserts a tool result record and returns its ref ID.
func (r *MessageRepo) PersistToolResult(ctx context.Context, record entity.ToolResultRecord) (string, error) {
	sql, args, err := r.Builder.
		Insert("tool_results").
		Columns("run_id", "tool_call_id", "tool_name", "result_json").
		Values(record.RunID, record.ToolCallID, record.ToolName, record.ResultJSON).
		Suffix("RETURNING id").
		ToSql()
	if err != nil {
		return "", fmt.Errorf("MessageRepo - PersistToolResult - builder: %w", err)
	}

	var id int64
	if err := r.Pool.QueryRow(ctx, sql, args...).Scan(&id); err != nil {
		return "", fmt.Errorf("MessageRepo - PersistToolResult - query: %w", err)
	}

	return strconv.FormatInt(id, 10), nil
}

// GetToolResult retrieves a tool result record by its ref ID.
func (r *MessageRepo) GetToolResult(ctx context.Context, ref string) (entity.ToolResultRecord, bool, error) {
	id, err := strconv.ParseInt(ref, 10, 64)
	if err != nil {
		return entity.ToolResultRecord{}, false, fmt.Errorf("MessageRepo - GetToolResult - parse ref: %w", err)
	}

	sql, args, err := r.Builder.
		Select("run_id", "tool_call_id", "tool_name", "result_json").
		From("tool_results").
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return entity.ToolResultRecord{}, false, fmt.Errorf("MessageRepo - GetToolResult - builder: %w", err)
	}

	var record entity.ToolResultRecord
	err = r.Pool.QueryRow(ctx, sql, args...).Scan(&record.RunID, &record.ToolCallID, &record.ToolName, &record.ResultJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.ToolResultRecord{}, false, nil
		}
		return entity.ToolResultRecord{}, false, fmt.Errorf("MessageRepo - GetToolResult - query: %w", err)
	}

	return record, true, nil
}

// PersistArchive inserts an archive record and returns its ref ID.
func (r *MessageRepo) PersistArchive(ctx context.Context, record entity.ArchiveRecord) (string, error) {
	sql, args, err := r.Builder.
		Insert("archives").
		Columns("run_id", "payload_type", "content").
		Values(record.RunID, record.PayloadType, record.Content).
		Suffix("RETURNING id").
		ToSql()
	if err != nil {
		return "", fmt.Errorf("MessageRepo - PersistArchive - builder: %w", err)
	}

	var id int64
	if err := r.Pool.QueryRow(ctx, sql, args...).Scan(&id); err != nil {
		return "", fmt.Errorf("MessageRepo - PersistArchive - query: %w", err)
	}

	return strconv.FormatInt(id, 10), nil
}

// ListMessagesByRun retrieves paginated message records for a run.
func (r *MessageRepo) ListMessagesByRun(ctx context.Context, runID string, limit, offset uint64) ([]entity.MessageRecord, error) {
	sql, args, err := r.Builder.
		Select("run_id", "role", "content", "tool_call_id").
		From("messages").
		Where(sq.Eq{"run_id": runID}).
		OrderBy("id ASC").
		Limit(limit).
		Offset(offset).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("MessageRepo - ListMessagesByRun - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("MessageRepo - ListMessagesByRun - query: %w", err)
	}
	defer rows.Close()

	var records []entity.MessageRecord
	for rows.Next() {
		var record entity.MessageRecord
		if err := rows.Scan(&record.RunID, &record.Role, &record.Content, &record.ToolCallID); err != nil {
			return nil, fmt.Errorf("MessageRepo - ListMessagesByRun - scan: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("MessageRepo - ListMessagesByRun - rows: %w", err)
	}

	return records, nil
}

// ListToolResultsByRun retrieves paginated tool result records for a run.
func (r *MessageRepo) ListToolResultsByRun(ctx context.Context, runID string, limit, offset uint64) ([]entity.ToolResultRecord, error) {
	sql, args, err := r.Builder.
		Select("run_id", "tool_call_id", "tool_name", "result_json").
		From("tool_results").
		Where(sq.Eq{"run_id": runID}).
		OrderBy("id ASC").
		Limit(limit).
		Offset(offset).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("MessageRepo - ListToolResultsByRun - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("MessageRepo - ListToolResultsByRun - query: %w", err)
	}
	defer rows.Close()

	var records []entity.ToolResultRecord
	for rows.Next() {
		var record entity.ToolResultRecord
		if err := rows.Scan(&record.RunID, &record.ToolCallID, &record.ToolName, &record.ResultJSON); err != nil {
			return nil, fmt.Errorf("MessageRepo - ListToolResultsByRun - scan: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("MessageRepo - ListToolResultsByRun - rows: %w", err)
	}

	return records, nil
}

// GetArchive retrieves an archive record by its ref ID.
func (r *MessageRepo) GetArchive(ctx context.Context, ref string) (entity.ArchiveRecord, bool, error) {
	id, err := strconv.ParseInt(ref, 10, 64)
	if err != nil {
		return entity.ArchiveRecord{}, false, fmt.Errorf("MessageRepo - GetArchive - parse ref: %w", err)
	}

	sql, args, err := r.Builder.
		Select("run_id", "payload_type", "content").
		From("archives").
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return entity.ArchiveRecord{}, false, fmt.Errorf("MessageRepo - GetArchive - builder: %w", err)
	}

	var record entity.ArchiveRecord
	err = r.Pool.QueryRow(ctx, sql, args...).Scan(&record.RunID, &record.PayloadType, &record.Content)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.ArchiveRecord{}, false, nil
		}
		return entity.ArchiveRecord{}, false, fmt.Errorf("MessageRepo - GetArchive - query: %w", err)
	}

	return record, true, nil
}
