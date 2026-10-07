package persistent

import (
	"context"
	"fmt"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosprocess"
	"github.com/jackc/pgx/v5"
)

// AgentOSProcessRepo persists generic AgentOS process projections and events.
// Statements and bindings come from queries/process.sql; this file owns the
// create/append idempotency protocols around them.
type AgentOSProcessRepo struct {
	*postgres.Postgres

	queries *sqlcgen.Queries
}

// NewAgentOSProcessRepo creates a Postgres-backed process repository.
func NewAgentOSProcessRepo(pg *postgres.Postgres) *AgentOSProcessRepo {
	return &AgentOSProcessRepo{Postgres: pg, queries: sqlcgen.New(pg.Pool)}
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

	return r.loadProcessByRef(ctx, "GetProcessByRef", agentos.Ref{
		ProcessID: ref.ProcessID,
		AccountID: ref.AccountID,
		ProjectID: ref.ProjectID,
	})
}

// ListProcesses returns process statuses matching the given scope filters.
func (r *AgentOSProcessRepo) ListProcesses(ctx context.Context, scope *agentos.Scope) ([]agentos.Status, error) {
	if err := agentos.ValidateScope(scope); err != nil {
		return nil, err
	}

	rows, err := r.queries.ListProcesses(ctx, sqlcgen.ListProcessesParams{
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
		ResourceKind:   string(scope.Resource.Kind),
		ResourceID:     scope.Resource.ResourceID,
		KindFilter:     string(scope.Kind),
		LifecycleState: scope.LifecycleState,
		RowLimit:       optionalInt8(scope.Limit),
	})

	return listRecords("AgentOSProcessRepo - ListProcesses", rows, err, func(row *[]byte) (agentos.Status, error) {
		return decodeProcessPlatformJSON[agentos.Status]("AgentOSProcessRepo - ListProcesses", *row)
	})
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

	existingStatus, exists, err := r.processStatusByIdempotencyKey(ctx, status.ProcessID, status.AccountID, status.ProjectID, idempotencyKey)
	if err != nil {
		return agentos.Status{}, err
	}

	if exists {
		return existingStatus, nil
	}

	return r.applyProcessStatusUpdate(ctx, &spec, &normalized, idempotencyKey)
}

func (r *AgentOSProcessRepo) applyProcessStatusUpdate(
	ctx context.Context,
	spec *agentos.Spec,
	status *agentos.Status,
	idempotencyKey string,
) (agentos.Status, error) {
	statusJSON, err := marshalProcessPlatformJSON("AgentOSProcessRepo - UpdateProcessStatus status", status)
	if err != nil {
		return agentos.Status{}, err
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.Status{}, fmt.Errorf("AgentOSProcessRepo - UpdateProcessStatus - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	queries := r.queries.WithTx(tx)

	updated, err := queries.UpdateProcessProjection(ctx, sqlcgen.UpdateProcessProjectionParams{
		ProcessID:      spec.ProcessID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		LifecycleState: status.LifecycleState,
		Reason:         status.Reason,
		StatusJson:     statusJSON,
		UpdatedAt:      status.UpdatedAt,
	})
	if err != nil {
		return agentos.Status{}, fmt.Errorf("AgentOSProcessRepo - UpdateProcessStatus - exec: %w", err)
	}

	if updated == 0 {
		return agentos.Status{}, fmt.Errorf("%w: process %q not found", agentoscore.ErrProcessRouteNotFound, spec.ProcessID)
	}

	if err := queries.InsertProcessStatusUpdate(ctx, sqlcgen.InsertProcessStatusUpdateParams{
		ProcessID:      status.ProcessID,
		AccountID:      status.AccountID,
		ProjectID:      status.ProjectID,
		IdempotencyKey: idempotencyKey,
		StatusJson:     statusJSON,
	}); err != nil {
		return agentos.Status{}, fmt.Errorf("AgentOSProcessRepo - UpdateProcessStatus - status key: %w", err)
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

	rows, err := r.queries.ListProcessEvents(ctx, sqlcgen.ListProcessEventsParams{
		ProcessID: scope.ProcessID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
		Sequence:  scope.AfterSequence,
		RowLimit:  optionalInt8(scope.Limit),
	})

	return listRecords("AgentOSProcessRepo - ListProcessEvents", rows, err, func(row *[]byte) (agentos.Event, error) {
		return decodeProcessPlatformJSON[agentos.Event]("AgentOSProcessRepo - ListProcessEvents", *row)
	})
}

func (r *AgentOSProcessRepo) processForCreate(ctx context.Context, spec *agentos.Spec) (agentos.Spec, agentos.Status, bool, error) {
	existingSpec, existingStatus, exists, err := r.loadProcessByIdempotencyKey(ctx, "processForCreateByKey", spec.AccountID, spec.ProjectID, spec.IdempotencyKey)
	if err != nil || exists {
		return existingSpec, existingStatus, exists, err
	}

	return r.loadProcessByRef(ctx, "processForCreateByRef", agentos.Ref{
		ProcessID: spec.ProcessID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	})
}

func (r *AgentOSProcessRepo) insertProcess(ctx context.Context, spec *agentos.Spec, status *agentos.Status) error {
	specJSON, err := marshalProcessPlatformJSON("AgentOSProcessRepo - insertProcess spec", spec)
	if err != nil {
		return err
	}

	statusJSON, err := marshalProcessPlatformJSON("AgentOSProcessRepo - insertProcess status", status)
	if err != nil {
		return err
	}

	err = r.queries.InsertProcess(ctx, sqlcgen.InsertProcessParams{
		ProcessID:      spec.ProcessID,
		Kind:           string(spec.Kind),
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		ResourceKind:   string(spec.Resource.Kind),
		ResourceID:     spec.Resource.ResourceID,
		IdempotencyKey: spec.IdempotencyKey,
		LifecycleState: status.LifecycleState,
		Reason:         status.Reason,
		SpecJson:       specJSON,
		StatusJson:     statusJSON,
		RequestedAt:    optionalTimestamptz(spec.RequestedAt),
		UpdatedAt:      status.UpdatedAt,
	})
	if err != nil {
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

func (r *AgentOSProcessRepo) loadProcessByRef(ctx context.Context, op string, ref agentos.Ref) (agentos.Spec, agentos.Status, bool, error) {
	row, err := r.queries.GetProcessByRef(ctx, sqlcgen.GetProcessByRefParams{
		ProcessID: ref.ProcessID,
		AccountID: ref.AccountID,
		ProjectID: ref.ProjectID,
	})
	if missingRow(err) {
		return agentos.Spec{}, agentos.Status{}, false, nil
	}

	if err != nil {
		return agentos.Spec{}, agentos.Status{}, false, fmt.Errorf("AgentOSProcessRepo - %s - query: %w", op, err)
	}

	return decodeProcessSpecStatus(op, row.SpecJson, row.StatusJson)
}

func (r *AgentOSProcessRepo) loadProcessByIdempotencyKey(ctx context.Context, op, accountID, projectID, idempotencyKey string) (agentos.Spec, agentos.Status, bool, error) {
	row, err := r.queries.GetProcessByIdempotencyKey(ctx, sqlcgen.GetProcessByIdempotencyKeyParams{
		AccountID:      accountID,
		ProjectID:      projectID,
		IdempotencyKey: idempotencyKey,
	})
	if missingRow(err) {
		return agentos.Spec{}, agentos.Status{}, false, nil
	}

	if err != nil {
		return agentos.Spec{}, agentos.Status{}, false, fmt.Errorf("AgentOSProcessRepo - %s - query: %w", op, err)
	}

	return decodeProcessSpecStatus(op, row.SpecJson, row.StatusJson)
}

// decodeProcessSpecStatus turns the two stored documents of a process row into
// the domain pair both lookup paths return.
func decodeProcessSpecStatus(op string, specJSON, statusJSON []byte) (agentos.Spec, agentos.Status, bool, error) {
	spec, err := decodeProcessPlatformJSON[agentos.Spec]("AgentOSProcessRepo - "+op+" - spec", specJSON)
	if err != nil {
		return agentos.Spec{}, agentos.Status{}, false, err
	}

	status, err := decodeProcessPlatformJSON[agentos.Status]("AgentOSProcessRepo - "+op+" - status", statusJSON)
	if err != nil {
		return agentos.Spec{}, agentos.Status{}, false, err
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
	queries := r.queries.WithTx(tx)

	locked, err := queries.LockProcessEventScope(ctx, sqlcgen.LockProcessEventScopeParams{
		ProcessID: event.ProcessID,
		AccountID: event.AccountID,
		ProjectID: event.ProjectID,
	})
	if missingRow(err) {
		return processEventScope{}, fmt.Errorf("%w: process %q not found", agentoscore.ErrProcessRouteNotFound, event.ProcessID)
	}

	if err != nil {
		return processEventScope{}, fmt.Errorf("AgentOSProcessRepo - AppendProcessEvent - lock query: %w", err)
	}

	scope := processEventScope{
		ProcessID:    locked.ProcessID,
		AccountID:    locked.AccountID,
		ProjectID:    locked.ProjectID,
		ResourceKind: locked.ResourceKind,
		ResourceID:   locked.ResourceID,
		Sequence:     locked.EventSequence + 1,
	}

	if err := queries.AdvanceProcessEventSequence(ctx, sqlcgen.AdvanceProcessEventSequenceParams{
		ProcessID:     scope.ProcessID,
		AccountID:     scope.AccountID,
		ProjectID:     scope.ProjectID,
		EventSequence: scope.Sequence,
	}); err != nil {
		return processEventScope{}, fmt.Errorf("AgentOSProcessRepo - AppendProcessEvent - sequence update: %w", err)
	}

	return scope, nil
}

func (r *AgentOSProcessRepo) insertProcessEvent(ctx context.Context, tx pgx.Tx, event *agentos.Event, idempotencyKey string) error {
	payloadJSON, err := marshalProcessPlatformJSON("AgentOSProcessRepo - insertProcessEvent payload", event.Payload)
	if err != nil {
		return err
	}

	eventJSON, err := marshalProcessPlatformJSON("AgentOSProcessRepo - insertProcessEvent event", event)
	if err != nil {
		return err
	}

	err = r.queries.WithTx(tx).InsertProcessEvent(ctx, sqlcgen.InsertProcessEventParams{
		EventID:        event.EventID,
		ProcessID:      event.ProcessID,
		AccountID:      event.AccountID,
		ProjectID:      event.ProjectID,
		ResourceKind:   string(event.Resource.Kind),
		ResourceID:     event.Resource.ResourceID,
		EventType:      string(event.EventType),
		Sequence:       event.Sequence,
		IdempotencyKey: idempotencyKey,
		PayloadJson:    payloadJSON,
		EventJson:      eventJSON,
		Timestamp:      event.Timestamp,
	})
	if err != nil {
		return fmt.Errorf("AgentOSProcessRepo - insertProcessEvent - exec: %w", err)
	}

	return nil
}

func (r *AgentOSProcessRepo) processEventByIdempotencyKey(ctx context.Context, processID, accountID, projectID, idempotencyKey string) (agentos.Event, bool, error) {
	eventJSON, err := r.queries.GetProcessEventByIdempotencyKey(ctx, sqlcgen.GetProcessEventByIdempotencyKeyParams{
		ProcessID:      processID,
		AccountID:      accountID,
		ProjectID:      projectID,
		IdempotencyKey: idempotencyKey,
	})

	return getJSONRecord[agentos.Event]("AgentOSProcessRepo - processEventByIdempotencyKey", eventJSON, err)
}

func (r *AgentOSProcessRepo) processStatusByIdempotencyKey(ctx context.Context, processID, accountID, projectID, idempotencyKey string) (agentos.Status, bool, error) {
	statusJSON, err := r.queries.GetProcessStatusByIdempotencyKey(ctx, sqlcgen.GetProcessStatusByIdempotencyKeyParams{
		ProcessID:      processID,
		AccountID:      accountID,
		ProjectID:      projectID,
		IdempotencyKey: idempotencyKey,
	})

	return getJSONRecord[agentos.Status]("AgentOSProcessRepo - processStatusByIdempotencyKey", statusJSON, err)
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

	if out.Timestamp.IsZero() {
		out.Timestamp = existing.Timestamp
	}

	return out
}

var (
	_ agentosprocess.ProcessIndex      = (*AgentOSProcessRepo)(nil)
	_ agentosprocess.ProcessEventStore = (*AgentOSProcessRepo)(nil)
)
