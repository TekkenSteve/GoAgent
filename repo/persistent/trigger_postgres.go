package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/pkg/postgres"
	"github.com/jackc/pgx/v5"
)

// Sentinel errors.
var ErrTriggerNotFound = errors.New("trigger not found")

// TriggerRepo implements repo.TriggerRepo with Postgres.
type TriggerRepo struct {
	*postgres.Postgres
}

// NewTriggerRepo creates a Postgres-backed trigger repository.
func NewTriggerRepo(pg *postgres.Postgres) *TriggerRepo {
	return &TriggerRepo{pg}
}

// Create inserts a new trigger.
func (r *TriggerRepo) Create(ctx context.Context, req *entity.CreateTriggerRequest) (entity.TriggerSpec, error) {
	configJSON, err := json.Marshal(req.Config)
	if err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerRepo - Create - marshal config: %w", err)
	}

	varsValsJSON, err := json.Marshal(req.TemplateVarsVals)
	if err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerRepo - Create - marshal template_vars_vals: %w", err)
	}

	vars := ensureSlice(req.TemplateVars)

	sql, args, err := r.Builder.
		Insert("workflow_triggers").
		Columns("template_id", "account_id", "name", "description", "trigger_type",
			"cron_expression", "event_slug", "agent_prompt", "config", "template_vars", "is_active", "template_vars_vals").
		Values(req.TemplateID, req.AccountID, req.Name, req.Description, string(req.TriggerType),
			req.CronExpression, req.EventSlug, req.AgentPrompt, string(configJSON), vars, req.IsActive, string(varsValsJSON)).
		Suffix("RETURNING id, template_id, account_id, name, description, trigger_type, " +
			"cron_expression, event_slug, agent_prompt, config, template_vars, is_active, last_fired_at, created_at, updated_at, template_vars_vals").
		ToSql()
	if err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerRepo - Create - builder: %w", err)
	}

	var (
		record      entity.TriggerSpec
		configStr   string
		varsStr     []string
		varsValsStr string
	)

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.ID, &record.TemplateID, &record.AccountID, &record.Name, &record.Description,
		&record.TriggerType, &record.CronExpression, &record.EventSlug, &record.AgentPrompt,
		&configStr, &varsStr, &record.IsActive, &record.LastFiredAt,
		&record.CreatedAt, &record.UpdatedAt, &varsValsStr,
	)
	if err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerRepo - Create - query: %w", err)
	}

	if err := json.Unmarshal([]byte(configStr), &record.Config); err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerRepo - Create - unmarshal config: %w", err)
	}

	if err := json.Unmarshal([]byte(varsValsStr), &record.TemplateVarsVals); err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerRepo - Create - unmarshal template_vars_vals: %w", err)
	}

	record.TemplateVars = varsStr

	return record, nil
}

// Get retrieves a trigger by ID.
func (r *TriggerRepo) Get(ctx context.Context, triggerID string) (entity.TriggerSpec, bool, error) {
	sql, args, err := r.Builder.
		Select("id", "template_id", "account_id", "name", "description", "trigger_type",
			"cron_expression", "event_slug", "agent_prompt", "config", "template_vars", "is_active", "last_fired_at", "created_at", "updated_at", "template_vars_vals").
		From("workflow_triggers").
		Where(sq.Eq{"id": triggerID}).
		ToSql()
	if err != nil {
		return entity.TriggerSpec{}, false, fmt.Errorf("TriggerRepo - Get - builder: %w", err)
	}

	var (
		record      entity.TriggerSpec
		configStr   string
		varsStr     []string
		varsValsStr string
	)

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.ID, &record.TemplateID, &record.AccountID, &record.Name, &record.Description,
		&record.TriggerType, &record.CronExpression, &record.EventSlug, &record.AgentPrompt,
		&configStr, &varsStr, &record.IsActive, &record.LastFiredAt,
		&record.CreatedAt, &record.UpdatedAt, &varsValsStr,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.TriggerSpec{}, false, nil
		}

		return entity.TriggerSpec{}, false, fmt.Errorf("TriggerRepo - Get - query: %w", err)
	}

	if err := json.Unmarshal([]byte(configStr), &record.Config); err != nil {
		return entity.TriggerSpec{}, false, fmt.Errorf("TriggerRepo - Get - unmarshal config: %w", err)
	}

	if err := json.Unmarshal([]byte(varsValsStr), &record.TemplateVarsVals); err != nil {
		return entity.TriggerSpec{}, false, fmt.Errorf("TriggerRepo - Get - unmarshal template_vars_vals: %w", err)
	}

	record.TemplateVars = varsStr

	return record, true, nil
}

// Update updates an existing trigger.
func (r *TriggerRepo) Update(ctx context.Context, triggerID string, req entity.UpdateTriggerRequest) (entity.TriggerSpec, error) {
	builder, err := buildTriggerUpdateQuery(r.Builder, triggerID, req)
	if err != nil {
		return entity.TriggerSpec{}, err
	}

	builder = builder.Set("updated_at", sq.Expr("NOW()"))
	builder = builder.Suffix("RETURNING id, template_id, account_id, name, description, trigger_type, " +
		"cron_expression, event_slug, agent_prompt, config, template_vars, is_active, last_fired_at, created_at, updated_at, template_vars_vals")

	sql, args, err := builder.ToSql()
	if err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerRepo - Update - builder: %w", err)
	}

	var (
		record      entity.TriggerSpec
		configStr   string
		varsStr     []string
		varsValsStr string
	)

	err = r.Pool.QueryRow(ctx, sql, args...).Scan(
		&record.ID, &record.TemplateID, &record.AccountID, &record.Name, &record.Description,
		&record.TriggerType, &record.CronExpression, &record.EventSlug, &record.AgentPrompt,
		&configStr, &varsStr, &record.IsActive, &record.LastFiredAt,
		&record.CreatedAt, &record.UpdatedAt, &varsValsStr,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.TriggerSpec{}, fmt.Errorf("TriggerRepo - Update - %w: %s", ErrTriggerNotFound, triggerID)
		}

		return entity.TriggerSpec{}, fmt.Errorf("TriggerRepo - Update - query: %w", err)
	}

	if err := json.Unmarshal([]byte(configStr), &record.Config); err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerRepo - Update - unmarshal config: %w", err)
	}

	if err := json.Unmarshal([]byte(varsValsStr), &record.TemplateVarsVals); err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerRepo - Update - unmarshal template_vars_vals: %w", err)
	}

	record.TemplateVars = varsStr

	return record, nil
}

// buildTriggerUpdateQuery builds the update builder with non-nil fields from req.
func buildTriggerUpdateQuery(b sq.StatementBuilderType, triggerID string, req entity.UpdateTriggerRequest) (sq.UpdateBuilder, error) {
	builder := b.Update("workflow_triggers").Where(sq.Eq{"id": triggerID})

	setIf := func(field string, val any) {
		switch v := val.(type) {
		case *string:
			if v != nil {
				builder = builder.Set(field, *v)
			}
		case *[]string:
			if v != nil {
				builder = builder.Set(field, *v)
			}
		}
	}

	setJSONIf := func(field string, val any) error {
		if val == nil {
			return nil
		}

		jsonBytes, err := json.Marshal(val)
		if err != nil {
			return fmt.Errorf("TriggerRepo - Update - marshal %s: %w", field, err)
		}

		builder = builder.Set(field, string(jsonBytes))

		return nil
	}

	setIf("name", req.Name)
	setIf("description", req.Description)
	setIf("cron_expression", req.CronExpression)
	setIf("event_slug", req.EventSlug)
	setIf("agent_prompt", req.AgentPrompt)

	if err := setJSONIf("config", req.Config); err != nil {
		return builder, err
	}

	setIf("template_vars", req.TemplateVars)

	if err := setJSONIf("template_vars_vals", req.TemplateVarsVals); err != nil {
		return builder, err
	}

	if req.IsActive != nil {
		builder = builder.Set("is_active", *req.IsActive)
	}

	return builder, nil
}

// Delete removes a trigger by ID.
func (r *TriggerRepo) Delete(ctx context.Context, triggerID string) error {
	sql, args, err := r.Builder.Delete("workflow_triggers").Where(sq.Eq{"id": triggerID}).ToSql()
	if err != nil {
		return fmt.Errorf("TriggerRepo - Delete - builder: %w", err)
	}

	_, err = r.Pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("TriggerRepo - Delete - exec: %w", err)
	}

	return nil
}

// ListByTemplate retrieves all triggers for a given template.
func (r *TriggerRepo) ListByTemplate(ctx context.Context, templateID string) ([]entity.TriggerSpec, error) {
	return r.list(ctx, sq.Eq{"template_id": templateID})
}

// ListByType retrieves all triggers of a given type.
func (r *TriggerRepo) ListByType(ctx context.Context, triggerType entity.TriggerType) ([]entity.TriggerSpec, error) {
	return r.list(ctx, sq.Eq{"trigger_type": string(triggerType)})
}

// ListActive retrieves all active triggers.
func (r *TriggerRepo) ListActive(ctx context.Context) ([]entity.TriggerSpec, error) {
	return r.list(ctx, sq.Eq{"is_active": true})
}

// RecordFired updates the last_fired_at timestamp for a trigger.
func (r *TriggerRepo) RecordFired(ctx context.Context, triggerID string) error {
	now := time.Now().UTC()

	sql, args, err := r.Builder.
		Update("workflow_triggers").
		Set("last_fired_at", now).
		Set("updated_at", sq.Expr("NOW()")).
		Where(sq.Eq{"id": triggerID}).
		ToSql()
	if err != nil {
		return fmt.Errorf("TriggerRepo - RecordFired - builder: %w", err)
	}

	_, err = r.Pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("TriggerRepo - RecordFired - exec: %w", err)
	}

	return nil
}

func (r *TriggerRepo) list(ctx context.Context, where sq.Eq) ([]entity.TriggerSpec, error) {
	sql, args, err := r.Builder.
		Select("id", "template_id", "account_id", "name", "description", "trigger_type",
			"cron_expression", "event_slug", "agent_prompt", "config", "template_vars", "is_active", "last_fired_at", "created_at", "updated_at", "template_vars_vals").
		From("workflow_triggers").
		Where(where).
		OrderBy("created_at DESC").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("TriggerRepo - list - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("TriggerRepo - list - query: %w", err)
	}
	defer rows.Close()

	var records []entity.TriggerSpec

	for rows.Next() {
		var (
			record      entity.TriggerSpec
			configStr   string
			varsStr     []string
			varsValsStr string
		)

		if err := rows.Scan(
			&record.ID, &record.TemplateID, &record.AccountID, &record.Name, &record.Description,
			&record.TriggerType, &record.CronExpression, &record.EventSlug, &record.AgentPrompt,
			&configStr, &varsStr, &record.IsActive, &record.LastFiredAt,
			&record.CreatedAt, &record.UpdatedAt, &varsValsStr,
		); err != nil {
			return nil, fmt.Errorf("TriggerRepo - list - scan: %w", err)
		}

		if err := json.Unmarshal([]byte(configStr), &record.Config); err != nil {
			return nil, fmt.Errorf("TriggerRepo - list - unmarshal config: %w", err)
		}

		if err := json.Unmarshal([]byte(varsValsStr), &record.TemplateVarsVals); err != nil {
			return nil, fmt.Errorf("TriggerRepo - list - unmarshal template_vars_vals: %w", err)
		}

		record.TemplateVars = varsStr
		records = append(records, record)
	}

	return records, nil
}

// InsertTriggerEvent persists an audit log entry for a trigger fire.
func (r *TriggerRepo) InsertTriggerEvent(ctx context.Context, event *entity.TriggerEventLog) error {
	varsJSON, err := json.Marshal(event.ExecVariables)
	if err != nil {
		return fmt.Errorf("TriggerRepo - InsertTriggerEvent - marshal exec_variables: %w", err)
	}

	sql, args, err := r.Builder.
		Insert("trigger_event_logs").
		Columns("trigger_id", "template_id", "trigger_type", "success", "message", "fired_at",
			"event_data", "agent_prompt", "exec_variables").
		Values(event.TriggerID, event.TemplateID, string(event.TriggerType), event.Success, event.Message, event.FiredAt,
			event.EventData, event.AgentPrompt, string(varsJSON)).
		ToSql()
	if err != nil {
		return fmt.Errorf("TriggerRepo - InsertTriggerEvent - builder: %w", err)
	}

	_, err = r.Pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("TriggerRepo - InsertTriggerEvent - exec: %w", err)
	}

	return nil
}
