package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosprocess"
	"github.com/jackc/pgx/v5"
)

// AgentOSProcessRepo persists generic AgentOS process projections and events.
type AgentOSProcessRepo struct {
	*postgres.Postgres
}

// NewAgentOSProcessRepo creates a Postgres-backed process repository.
func NewAgentOSProcessRepo(pg *postgres.Postgres) *AgentOSProcessRepo {
	return &AgentOSProcessRepo{pg}
}

// CreateProcess persists a new process from its spec and status, returning the stored status and whether the process was newly created.
func (r *AgentOSProcessRepo) CreateProcess(ctx context.Context, spec *agentos.Spec, status *agentos.Status) (agentos.Status, bool, error) {
	if err := agentos.ValidateProcessSpec(spec); err != nil {
		return agentos.Status{}, false, err
	}

	normalizedStatus := normalizePostgresProcessStatus(spec, status)

	existingSpec, existingStatus, exists, err := r.processForCreate(ctx, spec)
	if err != nil {
		return agentos.Status{}, false, err
	}

	if exists {
		return existingStatus, false, agentosprocess.ValidateProcessStartIdempotency(&existingSpec, spec)
	}

	if err := r.insertProcess(ctx, spec, &normalizedStatus); err != nil {
		existing, lookupErr := r.resolveProcessCreateConflict(ctx, spec, err)
		if lookupErr != nil {
			return agentos.Status{}, false, lookupErr
		}

		if existing != nil {
			return *existing, false, nil
		}

		return agentos.Status{}, false, err
	}

	return normalizedStatus, true, nil
}

// GetProcessByRef loads a process spec and status by tenant-scoped reference.
func (r *AgentOSProcessRepo) GetProcessByRef(ctx context.Context, ref agentos.Ref) (agentos.Spec, agentos.Status, bool, error) {
	if err := agentos.ValidateProcessRef(ref); err != nil {
		return agentos.Spec{}, agentos.Status{}, false, err
	}

	return r.loadProcess(ctx, sq.Eq{
		_colProcessID: ref.ProcessID,
		_colAccountID: ref.AccountID,
		_colProjectID: ref.ProjectID,
	}, "GetProcessByRef")
}

// ListProcesses returns process statuses matching the given scope filters.
func (r *AgentOSProcessRepo) ListProcesses(ctx context.Context, scope *agentos.Scope) ([]agentos.Status, error) {
	if err := agentos.ValidateScope(scope); err != nil {
		return nil, err
	}

	builder := r.processListBuilder(scope)

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSProcessRepo - ListProcesses - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("AgentOSProcessRepo - ListProcesses - query: %w", err)
	}
	defer rows.Close()

	statuses, err := scanProcessStatuses(rows)
	if err != nil {
		return nil, err
	}

	return statuses, nil
}

func (r *AgentOSProcessRepo) processListBuilder(scope *agentos.Scope) sq.SelectBuilder {
	builder := r.Builder.
		Select("status_json").
		From("processes").
		Where(sq.Eq{
			_colAccountID: scope.AccountID,
			_colProjectID: scope.ProjectID,
		}).
		OrderBy("updated_at DESC", "process_id ASC")

	if scope.Resource.Kind != "" {
		builder = builder.Where(sq.Eq{
			"resource_kind": string(scope.Resource.Kind),
			"resource_id":   scope.Resource.ResourceID,
		})
	}

	if scope.ResourceKind != "" {
		builder = builder.Where(sq.Eq{"resource_kind": string(scope.ResourceKind)})
	}

	if scope.Kind != "" {
		builder = builder.Where(sq.Eq{"kind": string(scope.Kind)})
	}

	if scope.LifecycleState != "" {
		builder = builder.Where(sq.Eq{"lifecycle_state": scope.LifecycleState})
	}

	if scope.Limit > 0 {
		builder = builder.Limit(uint64(scope.Limit))
	}

	return builder
}

func scanProcessStatuses(rows pgx.Rows) ([]agentos.Status, error) {
	var statuses []agentos.Status

	for rows.Next() {
		var statusJSON []byte
		if err := rows.Scan(&statusJSON); err != nil {
			return nil, fmt.Errorf("AgentOSProcessRepo - ListProcesses - scan: %w", err)
		}

		var status agentos.Status
		if err := json.Unmarshal(statusJSON, &status); err != nil {
			return nil, fmt.Errorf("AgentOSProcessRepo - ListProcesses - decode status: %w", err)
		}

		statuses = append(statuses, status)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("AgentOSProcessRepo - ListProcesses - rows: %w", err)
	}

	return statuses, nil
}

// UpdateProcessStatus applies a process status update idempotently and returns the resulting status.
func (r *AgentOSProcessRepo) UpdateProcessStatus(
	ctx context.Context,
	status *agentos.Status,
	idempotencyKey string,
) (agentos.Status, error) {
	if err := validatePostgresProcessStatusUpdate(status, idempotencyKey); err != nil {
		return agentos.Status{}, err
	}

	spec, _, exists, err := r.GetProcessByRef(ctx, agentos.Ref{
		ProcessID: status.ProcessID,
		AccountID: status.AccountID,
		ProjectID: status.ProjectID,
	})
	if err != nil || !exists {
		return agentos.Status{}, err
	}

	normalized := normalizePostgresProcessStatus(&spec, status)

	statusJSON, err := json.Marshal(normalized)
	if err != nil {
		return agentos.Status{}, fmt.Errorf("AgentOSProcessRepo - UpdateProcessStatus - marshal status: %w", err)
	}

	existingStatus, exists, err := r.processStatusByIdempotencyKey(ctx, status.ProcessID, status.AccountID, status.ProjectID, idempotencyKey)
	if err != nil {
		return agentos.Status{}, err
	}

	if exists {
		return existingStatus, nil
	}

	return r.applyProcessStatusUpdate(ctx, &spec, &normalized, idempotencyKey, statusJSON)
}

func (r *AgentOSProcessRepo) applyProcessStatusUpdate(
	ctx context.Context,
	spec *agentos.Spec,
	status *agentos.Status,
	idempotencyKey string,
	statusJSON []byte,
) (agentos.Status, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.Status{}, fmt.Errorf("AgentOSProcessRepo - UpdateProcessStatus - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	query, args, err := r.Builder.
		Update("processes").
		Set("lifecycle_state", status.LifecycleState).
		Set("reason", status.Reason).
		Set("status_json", statusJSON).
		Set("updated_at", status.UpdatedAt).
		Where(sq.Eq{
			_colProcessID: spec.ProcessID,
			_colAccountID: spec.AccountID,
			_colProjectID: spec.ProjectID,
		}).
		ToSql()
	if err != nil {
		return agentos.Status{}, fmt.Errorf("AgentOSProcessRepo - UpdateProcessStatus - builder: %w", err)
	}

	tag, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return agentos.Status{}, fmt.Errorf("AgentOSProcessRepo - UpdateProcessStatus - exec: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return agentos.Status{}, fmt.Errorf("%w: process %q not found", agentoscore.ErrProcessRouteNotFound, spec.ProcessID)
	}

	if err := r.insertProcessStatusUpdate(ctx, tx, status, idempotencyKey, statusJSON); err != nil {
		return agentos.Status{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return agentos.Status{}, fmt.Errorf("AgentOSProcessRepo - UpdateProcessStatus - commit: %w", err)
	}

	return *status, nil
}

// AppendProcessEvent appends a process event with a process-scoped sequence number, deduplicating by idempotency key.
func (r *AgentOSProcessRepo) AppendProcessEvent(
	ctx context.Context,
	event *agentos.Event,
	idempotencyKey string,
) (agentos.Event, error) {
	if err := validatePostgresProcessEvent(event, idempotencyKey); err != nil {
		return agentos.Event{}, err
	}

	existing, exists, err := r.processEventByIdempotencyKey(ctx, event.ProcessID, event.AccountID, event.ProjectID, idempotencyKey)
	if err != nil {
		return agentos.Event{}, err
	}

	if exists {
		requested := normalizePostgresProcessEventReplay(event, &existing)
		if err := agentosprocess.ValidateProcessEventIdempotency(&existing, &requested); err != nil {
			return agentos.Event{}, err
		}

		return existing, nil
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.Event{}, fmt.Errorf("AgentOSProcessRepo - AppendProcessEvent - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	scope, err := r.lockProcessEventScope(ctx, tx, event)
	if err != nil {
		return agentos.Event{}, err
	}

	prepared := normalizePostgresProcessEvent(event, &scope)
	if err := r.insertProcessEvent(ctx, tx, &prepared, idempotencyKey); err != nil {
		return agentos.Event{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return agentos.Event{}, fmt.Errorf("AgentOSProcessRepo - AppendProcessEvent - commit: %w", err)
	}

	return prepared, nil
}

// ListProcessEvents returns process events matching the given event scope, ordered by sequence.
func (r *AgentOSProcessRepo) ListProcessEvents(ctx context.Context, scope *agentos.EventScope) ([]agentos.Event, error) {
	if err := agentos.ValidateProcessEventScope(scope); err != nil {
		return nil, err
	}

	_, _, exists, err := r.GetProcessByRef(ctx, agentos.Ref{
		ProcessID: scope.ProcessID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	})
	if err != nil || !exists {
		return nil, err
	}

	builder := r.Builder.
		Select(processEventColumns()...).
		From("process_events").
		Where(sq.Eq{
			_colProcessID: scope.ProcessID,
			_colAccountID: scope.AccountID,
			_colProjectID: scope.ProjectID,
		}).
		Where(sq.Gt{"sequence": scope.AfterSequence}).
		OrderBy("sequence ASC")
	if scope.Limit > 0 {
		builder = builder.Limit(uint64(scope.Limit))
	}

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("AgentOSProcessRepo - ListProcessEvents - builder: %w", err)
	}

	rows, err := r.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("AgentOSProcessRepo - ListProcessEvents - query: %w", err)
	}
	defer rows.Close()

	var events []agentos.Event

	for rows.Next() {
		event, err := scanProcessEvent(rows)
		if err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("AgentOSProcessRepo - ListProcessEvents - rows: %w", err)
	}

	return events, nil
}

func (r *AgentOSProcessRepo) processForCreate(ctx context.Context, spec *agentos.Spec) (agentos.Spec, agentos.Status, bool, error) {
	existingSpec, existingStatus, exists, err := r.loadProcess(ctx, sq.Eq{
		_colAccountID:      spec.AccountID,
		_colProjectID:      spec.ProjectID,
		_colIDempotencyKey: spec.IdempotencyKey,
	}, "processForCreateByKey")
	if err != nil || exists {
		return existingSpec, existingStatus, exists, err
	}

	return r.loadProcess(ctx, sq.Eq{
		_colProcessID: spec.ProcessID,
		_colAccountID: spec.AccountID,
		_colProjectID: spec.ProjectID,
	}, "processForCreateByRef")
}

func (r *AgentOSProcessRepo) insertProcess(ctx context.Context, spec *agentos.Spec, status *agentos.Status) error {
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("AgentOSProcessRepo - insertProcess - marshal spec: %w", err)
	}

	statusJSON, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("AgentOSProcessRepo - insertProcess - marshal status: %w", err)
	}

	query, args, err := r.Builder.
		Insert("processes").
		Columns(
			_colProcessID,
			"kind",
			_colAccountID,
			_colProjectID,
			"resource_kind",
			"resource_id",
			_colIDempotencyKey,
			"lifecycle_state",
			"reason",
			"spec_json",
			"status_json",
			"requested_at",
			"updated_at",
		).
		Values(
			spec.ProcessID,
			string(spec.Kind),
			spec.AccountID,
			spec.ProjectID,
			string(spec.Resource.Kind),
			spec.Resource.ResourceID,
			spec.IdempotencyKey,
			status.LifecycleState,
			status.Reason,
			specJSON,
			statusJSON,
			nullableTime(spec.RequestedAt),
			status.UpdatedAt,
		).
		ToSql()
	if err != nil {
		return fmt.Errorf("AgentOSProcessRepo - insertProcess - builder: %w", err)
	}

	if _, err := r.Pool.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("AgentOSProcessRepo - insertProcess - exec: %w", err)
	}

	return nil
}

func (r *AgentOSProcessRepo) resolveProcessCreateConflict(ctx context.Context, spec *agentos.Spec, err error) (*agentos.Status, error) {
	if !isPostgresUniqueViolation(err) {
		return nil, nil
	}

	existingSpec, existingStatus, exists, lookupErr := r.processForCreate(ctx, spec)
	if lookupErr != nil {
		return nil, lookupErr
	}

	if !exists {
		return nil, nil
	}

	if validateErr := agentosprocess.ValidateProcessStartIdempotency(&existingSpec, spec); validateErr != nil {
		return nil, validateErr
	}

	return &existingStatus, nil
}

func (r *AgentOSProcessRepo) loadProcess(ctx context.Context, where sq.Eq, op string) (agentos.Spec, agentos.Status, bool, error) {
	query, args, err := r.Builder.
		Select("spec_json", "status_json").
		From("processes").
		Where(where).
		ToSql()
	if err != nil {
		return agentos.Spec{}, agentos.Status{}, false, fmt.Errorf("AgentOSProcessRepo - %s - builder: %w", op, err)
	}

	var specJSON, statusJSON []byte
	if err := r.Pool.QueryRow(ctx, query, args...).Scan(&specJSON, &statusJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.Spec{}, agentos.Status{}, false, nil
		}

		return agentos.Spec{}, agentos.Status{}, false, fmt.Errorf("AgentOSProcessRepo - %s - query: %w", op, err)
	}

	var spec agentos.Spec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return agentos.Spec{}, agentos.Status{}, false, fmt.Errorf("AgentOSProcessRepo - %s - decode spec: %w", op, err)
	}

	var status agentos.Status
	if err := json.Unmarshal(statusJSON, &status); err != nil {
		return agentos.Spec{}, agentos.Status{}, false, fmt.Errorf("AgentOSProcessRepo - %s - decode status: %w", op, err)
	}

	return spec, status, true, nil
}

type processEventScope struct {
	ProcessID    string
	AccountID    string
	ProjectID    string
	ResourceKind string
	ResourceID   string
	Sequence     int64
}

func (r *AgentOSProcessRepo) lockProcessEventScope(ctx context.Context, tx pgx.Tx, event *agentos.Event) (processEventScope, error) {
	query, args, err := r.Builder.
		Select(_colProcessID, _colAccountID, _colProjectID, "resource_kind", "resource_id", "event_sequence").
		From("processes").
		Where(sq.Eq{
			_colProcessID: event.ProcessID,
			_colAccountID: event.AccountID,
			_colProjectID: event.ProjectID,
		}).
		Suffix("FOR UPDATE").
		ToSql()
	if err != nil {
		return processEventScope{}, fmt.Errorf("AgentOSProcessRepo - AppendProcessEvent - lock builder: %w", err)
	}

	var scope processEventScope
	if err := tx.QueryRow(ctx, query, args...).Scan(
		&scope.ProcessID,
		&scope.AccountID,
		&scope.ProjectID,
		&scope.ResourceKind,
		&scope.ResourceID,
		&scope.Sequence,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return processEventScope{}, fmt.Errorf("%w: process %q not found", agentoscore.ErrProcessRouteNotFound, event.ProcessID)
		}

		return processEventScope{}, fmt.Errorf("AgentOSProcessRepo - AppendProcessEvent - lock query: %w", err)
	}

	nextSequence := scope.Sequence + 1

	updateQuery, updateArgs, err := r.Builder.
		Update("processes").
		Set("event_sequence", nextSequence).
		Where(sq.Eq{
			_colProcessID: scope.ProcessID,
			_colAccountID: scope.AccountID,
			_colProjectID: scope.ProjectID,
		}).
		ToSql()
	if err != nil {
		return processEventScope{}, fmt.Errorf("AgentOSProcessRepo - AppendProcessEvent - sequence builder: %w", err)
	}

	if _, err := tx.Exec(ctx, updateQuery, updateArgs...); err != nil {
		return processEventScope{}, fmt.Errorf("AgentOSProcessRepo - AppendProcessEvent - sequence update: %w", err)
	}

	scope.Sequence = nextSequence

	return scope, nil
}

func (r *AgentOSProcessRepo) insertProcessEvent(ctx context.Context, tx pgx.Tx, event *agentos.Event, idempotencyKey string) error {
	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("AgentOSProcessRepo - insertProcessEvent - marshal payload: %w", err)
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("AgentOSProcessRepo - insertProcessEvent - marshal event: %w", err)
	}

	query, args, err := r.Builder.
		Insert("process_events").
		Columns(
			"event_id",
			_colProcessID,
			_colAccountID,
			_colProjectID,
			"resource_kind",
			"resource_id",
			"event_type",
			"sequence",
			_colIDempotencyKey,
			"payload_json",
			"event_json",
			"timestamp",
		).
		Values(
			event.EventID,
			event.ProcessID,
			event.AccountID,
			event.ProjectID,
			string(event.Resource.Kind),
			event.Resource.ResourceID,
			string(event.EventType),
			event.Sequence,
			idempotencyKey,
			payloadJSON,
			eventJSON,
			event.Timestamp,
		).
		ToSql()
	if err != nil {
		return fmt.Errorf("AgentOSProcessRepo - insertProcessEvent - builder: %w", err)
	}

	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("AgentOSProcessRepo - insertProcessEvent - exec: %w", err)
	}

	return nil
}

func (r *AgentOSProcessRepo) processEventByIdempotencyKey(ctx context.Context, processID, accountID, projectID, idempotencyKey string) (agentos.Event, bool, error) {
	query, args, err := r.Builder.
		Select(processEventColumns()...).
		From("process_events").
		Where(sq.Eq{
			_colProcessID:      processID,
			_colAccountID:      accountID,
			_colProjectID:      projectID,
			_colIDempotencyKey: idempotencyKey,
		}).
		ToSql()
	if err != nil {
		return agentos.Event{}, false, fmt.Errorf("AgentOSProcessRepo - processEventByIdempotencyKey - builder: %w", err)
	}

	event, err := scanProcessEvent(r.Pool.QueryRow(ctx, query, args...))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.Event{}, false, nil
		}

		return agentos.Event{}, false, err
	}

	return event, true, nil
}

func (r *AgentOSProcessRepo) processStatusByIdempotencyKey(ctx context.Context, processID, accountID, projectID, idempotencyKey string) (agentos.Status, bool, error) {
	query, args, err := r.Builder.
		Select("status_json").
		From("process_status_updates").
		Where(sq.Eq{
			_colProcessID:      processID,
			_colAccountID:      accountID,
			_colProjectID:      projectID,
			_colIDempotencyKey: idempotencyKey,
		}).
		ToSql()
	if err != nil {
		return agentos.Status{}, false, fmt.Errorf("AgentOSProcessRepo - processStatusByIdempotencyKey - builder: %w", err)
	}

	var statusJSON []byte
	if err := r.Pool.QueryRow(ctx, query, args...).Scan(&statusJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentos.Status{}, false, nil
		}

		return agentos.Status{}, false, fmt.Errorf("AgentOSProcessRepo - processStatusByIdempotencyKey - query: %w", err)
	}

	var status agentos.Status
	if err := json.Unmarshal(statusJSON, &status); err != nil {
		return agentos.Status{}, false, fmt.Errorf("AgentOSProcessRepo - processStatusByIdempotencyKey - decode status: %w", err)
	}

	return status, true, nil
}

func (r *AgentOSProcessRepo) insertProcessStatusUpdate(ctx context.Context, tx pgx.Tx, status *agentos.Status, idempotencyKey string, statusJSON []byte) error {
	query, args, err := r.Builder.
		Insert("process_status_updates").
		Columns(
			_colProcessID,
			_colAccountID,
			_colProjectID,
			_colIDempotencyKey,
			"status_json",
		).
		Values(
			status.ProcessID,
			status.AccountID,
			status.ProjectID,
			idempotencyKey,
			statusJSON,
		).
		ToSql()
	if err != nil {
		return fmt.Errorf("AgentOSProcessRepo - insertProcessStatusUpdate - builder: %w", err)
	}

	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("AgentOSProcessRepo - insertProcessStatusUpdate - exec: %w", err)
	}

	return nil
}

func normalizePostgresProcessStatus(spec *agentos.Spec, status *agentos.Status) agentos.Status {
	var out agentos.Status
	if status != nil {
		out = *status
	}

	out.ProcessID = spec.ProcessID
	out.Kind = spec.Kind
	out.AccountID = spec.AccountID
	out.ProjectID = spec.ProjectID
	out.Resource = spec.Resource

	if out.LifecycleState == "" {
		out.LifecycleState = agentos.ProcessRunning
	}

	now := time.Now().UTC()

	if out.StartedAt.IsZero() {
		if spec.RequestedAt.IsZero() {
			out.StartedAt = now
		} else {
			out.StartedAt = spec.RequestedAt
		}
	}

	if out.UpdatedAt.IsZero() {
		out.UpdatedAt = out.StartedAt
	}

	return out
}

func validatePostgresProcessStatusUpdate(status *agentos.Status, idempotencyKey string) error {
	if status == nil {
		return fmt.Errorf("%w: process status is required", agentoscore.ErrInvalidProcess)
	}

	if idempotencyKey == "" {
		return fmt.Errorf("%w: process status idempotency key is required", agentoscore.ErrInvalidProcess)
	}

	return nil
}

func validatePostgresProcessEvent(event *agentos.Event, idempotencyKey string) error {
	if event == nil {
		return fmt.Errorf("%w: process event is required", agentoscore.ErrInvalidProcess)
	}

	if idempotencyKey == "" {
		return fmt.Errorf("%w: process event idempotency key is required", agentoscore.ErrInvalidProcess)
	}

	if event.ProcessID == "" {
		return fmt.Errorf("%w: process id is required", agentoscore.ErrInvalidProcess)
	}

	if event.AccountID == "" {
		return fmt.Errorf("%w: account id is required", agentoscore.ErrInvalidProcess)
	}

	if event.ProjectID == "" {
		return fmt.Errorf("%w: project id is required", agentoscore.ErrInvalidProcess)
	}

	if event.EventType == "" {
		return fmt.Errorf("%w: process event type is required", agentoscore.ErrInvalidProcess)
	}

	return nil
}

func normalizePostgresProcessEvent(event *agentos.Event, scope *processEventScope) agentos.Event {
	out := *event
	out.ProcessID = scope.ProcessID
	out.AccountID = scope.AccountID
	out.ProjectID = scope.ProjectID
	out.Resource = agentos.ResourceRef{
		Kind:       agentos.ResourceKind(scope.ResourceKind),
		ResourceID: scope.ResourceID,
		AccountID:  scope.AccountID,
		ProjectID:  scope.ProjectID,
	}
	out.ProcessID = scope.ProcessID
	out.Sequence = scope.Sequence

	if out.EventID == "" {
		out.EventID = fmt.Sprintf("%s-%d", scope.ProcessID, scope.Sequence)
	}

	if out.Timestamp.IsZero() {
		out.Timestamp = time.Now().UTC()
	}

	if out.Payload == nil {
		out.Payload = map[string]any{}
	}

	return out
}

func normalizePostgresProcessEventReplay(event, existing *agentos.Event) agentos.Event {
	out := *event
	out.EventID = existing.EventID
	out.ProcessID = existing.ProcessID
	out.AccountID = existing.AccountID
	out.ProjectID = existing.ProjectID
	out.Resource = existing.Resource
	out.Sequence = existing.Sequence
	out.ProcessID = existing.ProcessID

	if out.Timestamp.IsZero() {
		out.Timestamp = existing.Timestamp
	}

	return out
}

func processEventColumns() []string {
	return []string{
		"event_json",
	}
}

func scanProcessEvent(scanner interface{ Scan(dest ...any) error }) (agentos.Event, error) {
	var eventJSON []byte
	if err := scanner.Scan(&eventJSON); err != nil {
		return agentos.Event{}, err
	}

	var event agentos.Event
	if err := json.Unmarshal(eventJSON, &event); err != nil {
		return agentos.Event{}, fmt.Errorf("AgentOSProcessRepo - scanProcessEvent - decode event: %w", err)
	}

	return event, nil
}

var (
	_ agentosprocess.ProcessIndex      = (*AgentOSProcessRepo)(nil)
	_ agentosprocess.ProcessEventStore = (*AgentOSProcessRepo)(nil)
)
