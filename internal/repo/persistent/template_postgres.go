package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrTemplateNotFound is returned when a workflow template is not found.
var ErrTemplateNotFound = errors.New("template not found")

// WorkflowTemplateRepo implements repo.WorkflowTemplateRepo with Postgres.
// Statements and bindings come from queries/template.sql.
type WorkflowTemplateRepo struct {
	*postgres.Postgres

	queries *sqlcgen.Queries
}

// NewWorkflowTemplateRepo creates a Postgres-backed workflow template repository.
func NewWorkflowTemplateRepo(pg *postgres.Postgres) *WorkflowTemplateRepo {
	return &WorkflowTemplateRepo{Postgres: pg, queries: sqlcgen.New(pg.Pool)}
}

// Create inserts a new workflow template.
func (r *WorkflowTemplateRepo) Create(ctx context.Context, req *entity.CreateWorkflowTemplateRequest) (entity.WorkflowTemplate, error) {
	teamSpecJSON, err := json.Marshal(req.TeamSpec)
	if err != nil {
		return entity.WorkflowTemplate{}, fmt.Errorf("WorkflowTemplateRepo - Create - marshal team_spec: %w", err)
	}

	row, err := r.queries.InsertWorkflowTemplate(ctx, sqlcgen.InsertWorkflowTemplateParams{
		AccountID:    req.AccountID,
		Name:         req.Name,
		Description:  req.Description,
		TeamSpec:     teamSpecJSON,
		SystemPrompt: req.SystemPrompt,
		DefaultModel: req.DefaultModel,
		Tags:         ensureSlice(req.Tags),
		IsEnabled:    req.IsEnabled,
	})
	if err != nil {
		return entity.WorkflowTemplate{}, fmt.Errorf("WorkflowTemplateRepo - Create - query: %w", err)
	}

	return workflowTemplateFromRow("Create", &row)
}

// Get retrieves a workflow template by ID.
func (r *WorkflowTemplateRepo) Get(ctx context.Context, accountID, templateID string) (entity.WorkflowTemplate, bool, error) {
	row, err := r.queries.GetWorkflowTemplate(ctx, sqlcgen.GetWorkflowTemplateParams{ID: templateID, AccountID: accountID})
	if missingRow(err) || malformedID(err) {
		// An id that cannot be a template id identifies no template, which is
		// the same answer as an id that names nothing: a client sending a
		// malformed path segment gets a lookup miss, not a server error.
		return entity.WorkflowTemplate{}, false, nil
	}

	if err != nil {
		return entity.WorkflowTemplate{}, false, fmt.Errorf("WorkflowTemplateRepo - Get - query: %w", err)
	}

	record, err := workflowTemplateFromRow("Get", &row)
	if err != nil {
		return entity.WorkflowTemplate{}, false, err
	}

	return record, true, nil
}

// Update updates an existing workflow template. Only the fields present in the
// request reach the statement; every omitted field keeps its stored value.
func (r *WorkflowTemplateRepo) Update(ctx context.Context, accountID, templateID string, req entity.UpdateWorkflowTemplateRequest) (entity.WorkflowTemplate, error) {
	params := sqlcgen.UpdateWorkflowTemplateParams{
		ID:           templateID,
		AccountID:    accountID,
		Name:         optionalText(req.Name),
		Description:  optionalText(req.Description),
		SystemPrompt: optionalText(req.SystemPrompt),
		DefaultModel: optionalText(req.DefaultModel),
		IsEnabled:    optionalBool(req.IsEnabled),
		Tags:         optionalStrings(req.Tags),
	}

	if req.TeamSpec != nil {
		teamSpecJSON, err := json.Marshal(req.TeamSpec)
		if err != nil {
			return entity.WorkflowTemplate{}, fmt.Errorf("WorkflowTemplateRepo - Update - marshal team_spec: %w", err)
		}

		params.TeamSpec = teamSpecJSON
	}

	row, err := r.queries.UpdateWorkflowTemplate(ctx, params)
	if missingRow(err) || malformedID(err) {
		return entity.WorkflowTemplate{}, fmt.Errorf("WorkflowTemplateRepo - Update - %w: %s", ErrTemplateNotFound, templateID)
	}

	if err != nil {
		return entity.WorkflowTemplate{}, fmt.Errorf("WorkflowTemplateRepo - Update - query: %w", err)
	}

	return workflowTemplateFromRow("Update", &row)
}

// malformedID reports an id the database cannot interpret — a path segment that
// is not a uuid. It is a lookup miss, not a failure: an id that cannot exist
// means no template, and answering with a server error would turn a client's
// typo into an incident.
func malformedID(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) && pgErr.Code == "22P02"
}

// Delete removes a workflow template the account owns.
func (r *WorkflowTemplateRepo) Delete(ctx context.Context, accountID, templateID string) error {
	// A delete that matched nothing is a template this account does not own, or
	// one that does not exist. Both answer the same way, so the caller cannot
	// use the endpoint to enumerate someone else's ids.
	deleted, err := r.queries.DeleteWorkflowTemplate(ctx, sqlcgen.DeleteWorkflowTemplateParams{ID: templateID, AccountID: accountID})
	if malformedID(err) {
		return fmt.Errorf("WorkflowTemplateRepo - Delete - %w: %s", ErrTemplateNotFound, templateID)
	}

	if err != nil {
		return fmt.Errorf("WorkflowTemplateRepo - Delete - exec: %w", err)
	}

	if deleted == 0 {
		return fmt.Errorf("WorkflowTemplateRepo - Delete - %w: %s", ErrTemplateNotFound, templateID)
	}

	return nil
}

// ListByAccount retrieves all templates for a given account.
func (r *WorkflowTemplateRepo) ListByAccount(ctx context.Context, accountID string) ([]entity.WorkflowTemplate, error) {
	rows, err := r.queries.ListWorkflowTemplatesByAccount(ctx, accountID)

	return listRecords("WorkflowTemplateRepo - ListByAccount", rows, err, func(row *sqlcgen.WorkflowTemplate) (entity.WorkflowTemplate, error) {
		return workflowTemplateFromRow("ListByAccount", row)
	})
}

// ensureSlice returns a non-nil slice from a nil slice (Postgres text[] compatibility).
func ensureSlice(s []string) []string {
	if s == nil {
		return []string{}
	}

	return s
}

// workflowTemplateFromRow shapes a generated row into the entity, decoding the
// team_spec JSON document.
func workflowTemplateFromRow(name string, row *sqlcgen.WorkflowTemplate) (entity.WorkflowTemplate, error) {
	record := workflowTemplateColumns(row)

	if err := decodeRecordJSON("WorkflowTemplateRepo - "+name, "team_spec", row.TeamSpec, &record.TeamSpec); err != nil {
		return entity.WorkflowTemplate{}, err
	}

	return record, nil
}

// workflowTemplateColumns copies the stored columns of one template row; the
// JSON document column is decoded by the caller.
func workflowTemplateColumns(row *sqlcgen.WorkflowTemplate) entity.WorkflowTemplate {
	return entity.WorkflowTemplate{
		ID:           row.ID,
		AccountID:    row.AccountID,
		Name:         row.Name,
		Description:  row.Description,
		SystemPrompt: row.SystemPrompt,
		DefaultModel: row.DefaultModel,
		Tags:         row.Tags,
		IsEnabled:    row.IsEnabled,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}
