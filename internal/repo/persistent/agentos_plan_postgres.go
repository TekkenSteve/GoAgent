package persistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent/sqlcgen"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/jackc/pgx/v5"
)

var errInsertedAuditRecordNotFound = errors.New("inserted audit record not found")

// errcheckIgnore is a helper for intentional error discards.
func errcheckIgnore(_ error) {}

// AgentOSPlanRepo persists RunPlan aggregate state and durable plan events.
// Plan, plan node, plan event, audit, command and metric statements and
// bindings come from queries/plan.sql; this file owns the
// create/append/transition/claim protocols around them.
type AgentOSPlanRepo struct {
	*postgres.Postgres

	queries *sqlcgen.Queries
}

// NewAgentOSPlanRepo creates a Postgres-backed RunPlan repository.
func NewAgentOSPlanRepo(pg *postgres.Postgres) *AgentOSPlanRepo {
	return &AgentOSPlanRepo{Postgres: pg, queries: sqlcgen.New(pg.Pool)}
}

// CreatePlan persists a new RunPlan from its spec and status, returning the stored status and whether the plan was newly created.
func (r *AgentOSPlanRepo) CreatePlan(ctx context.Context, spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus) (agentos.RunPlanStatus, bool, error) {
	if spec.PlanID == "" {
		return agentos.RunPlanStatus{}, false, fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidRunPlan)
	}

	if spec.IdempotencyKey == "" {
		return agentos.RunPlanStatus{}, false, fmt.Errorf("%w: plan idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	normalizedStatus := *status
	if normalizedStatus.PlanID == "" {
		normalizedStatus.PlanID = spec.PlanID
	}

	existing, exists, err := r.existingPlanForCreate(ctx, spec)
	if err != nil {
		return agentos.RunPlanStatus{}, false, err
	}

	if exists {
		return existing, false, nil
	}

	createdStatus, err := r.createPlanState(ctx, &agentosplan.PlanStateSnapshot{
		Spec:   *spec,
		Status: normalizedStatus,
	})
	if err != nil {
		existing, lookupErr := r.resolvePlanCreateConflict(ctx, spec, err)
		if lookupErr != nil {
			return agentos.RunPlanStatus{}, false, lookupErr
		}

		if existing != nil {
			return *existing, false, nil
		}

		return agentos.RunPlanStatus{}, false, err
	}

	return createdStatus, true, nil
}

func (r *AgentOSPlanRepo) existingPlanForCreate(ctx context.Context, spec *agentos.RunPlanSpec) (agentos.RunPlanStatus, bool, error) {
	existingSpec, existing, exists, err := r.planByIdempotencyKey(ctx, spec.AccountID, spec.ProjectID, spec.IdempotencyKey)
	if err != nil || exists {
		return existing, exists, validateExistingPlanStart(&existingSpec, spec, exists)
	}

	existingSpec, existing, exists, err = r.GetPlan(ctx, spec.PlanID)
	if err != nil || exists {
		return existing, exists, validateExistingPlanStart(&existingSpec, spec, exists)
	}

	return agentos.RunPlanStatus{}, false, nil
}

func validateExistingPlanStart(existingSpec, spec *agentos.RunPlanSpec, exists bool) error {
	if !exists {
		return nil
	}

	return agentosplan.ValidatePlanStartIdempotency(existingSpec, spec)
}

func (r *AgentOSPlanRepo) resolvePlanCreateConflict(ctx context.Context, spec *agentos.RunPlanSpec, err error) (*agentos.RunPlanStatus, error) {
	if !isPostgresUniqueViolation(err) {
		return nil, nil
	}

	existingSpec, existing, exists, lookupErr := r.planByIdempotencyKey(ctx, spec.AccountID, spec.ProjectID, spec.IdempotencyKey)
	if lookupErr != nil {
		return nil, lookupErr
	}

	if !exists {
		return nil, nil
	}

	if validateErr := agentosplan.ValidatePlanStartIdempotency(&existingSpec, spec); validateErr != nil {
		return nil, validateErr
	}

	return &existing, nil
}

func (r *AgentOSPlanRepo) createPlanState(ctx context.Context, snapshot *agentosplan.PlanStateSnapshot) (agentos.RunPlanStatus, error) {
	normalized, err := normalizePlanStateSnapshotForPostgres(snapshot)
	if err != nil {
		return agentos.RunPlanStatus{}, err
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.RunPlanStatus{}, fmt.Errorf("AgentOSPlanRepo - createPlanState - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	if err := r.savePlanStateWithTx(ctx, tx, &normalized, true); err != nil {
		return agentos.RunPlanStatus{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return agentos.RunPlanStatus{}, fmt.Errorf("AgentOSPlanRepo - createPlanState - commit: %w", err)
	}

	return normalized.Status, nil
}

// GetPlan loads the spec and status of the plan with the given ID.
func (r *AgentOSPlanRepo) GetPlan(ctx context.Context, planID string) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error) {
	snapshot, exists, err := r.LoadPlanState(ctx, planID)
	if err != nil || !exists {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, exists, err
	}

	return snapshot.Spec, snapshot.Status, true, nil
}

// GetPlanByRef loads a plan's spec and status within the given tenant-scoped reference.
func (r *AgentOSPlanRepo) GetPlanByRef(ctx context.Context, ref agentos.PlanRef) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error) {
	if err := agentosplan.ValidatePlanRef(ref); err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, err
	}

	row, err := r.queries.GetPlanStateByRef(ctx, sqlcgen.GetPlanStateByRefParams{
		PlanID:    ref.PlanID,
		AccountID: ref.AccountID,
		ProjectID: ref.ProjectID,
	})
	if missingRow(err) {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, nil
	}

	if err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, fmt.Errorf("AgentOSPlanRepo - GetPlanByRef - query: %w", err)
	}

	snapshot, err := r.planStateFromDocuments(ctx, "AgentOSPlanRepo - GetPlanByRef", row.SpecJson, row.StatusJson)
	if err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, err
	}

	if err := agentosplan.ValidatePlanTenantAccess(ref, &snapshot.Spec); err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, err
	}

	return snapshot.Spec, snapshot.Status, true, nil
}

// ListPlanRefs returns plan references matching the given scope filters.
func (r *AgentOSPlanRepo) ListPlanRefs(ctx context.Context, scope *agentosplan.PlanRefScope) ([]agentos.PlanRef, error) {
	if scope.Limit < 0 {
		return nil, fmt.Errorf("%w: plan ref limit must be non-negative", agentoscore.ErrInvalidPlanScope)
	}

	if slices.Contains(scope.LifecycleStates, "") {
		return nil, fmt.Errorf("%w: lifecycle state is required", agentoscore.ErrInvalidPlanScope)
	}

	rows, err := r.queries.ListPlanRefs(ctx, sqlcgen.ListPlanRefsParams{
		AccountID:       scope.AccountID,
		ProjectID:       scope.ProjectID,
		LifecycleStates: scope.LifecycleStates,
		UpdatedAfter:    optionalTimestamptz(scope.UpdatedAfter),
		RowLimit:        optionalInt8(scope.Limit),
	})

	return listRecords("AgentOSPlanRepo - ListPlanRefs", rows, err, func(row *sqlcgen.ListPlanRefsRow) (agentos.PlanRef, error) {
		return agentos.PlanRef{PlanID: row.PlanID, AccountID: row.AccountID, ProjectID: row.ProjectID}, nil
	})
}

// SavePlanState persists a full plan state snapshot, upserting the plan row and its node statuses.
func (r *AgentOSPlanRepo) SavePlanState(ctx context.Context, snapshot *agentosplan.PlanStateSnapshot) error {
	normalized, err := normalizePlanStateSnapshotForPostgres(snapshot)
	if err != nil {
		return err
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	if err := r.savePlanStateWithTx(ctx, tx, &normalized, false); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - commit: %w", err)
	}

	return nil
}

func normalizePlanStateSnapshotForPostgres(snapshot *agentosplan.PlanStateSnapshot) (agentosplan.PlanStateSnapshot, error) {
	if err := agentosplan.ValidateRunPlanScope(&snapshot.Spec); err != nil {
		return agentosplan.PlanStateSnapshot{}, err
	}

	normalized := *snapshot
	if snapshot.Spec.IdempotencyKey == "" {
		return agentosplan.PlanStateSnapshot{}, fmt.Errorf("%w: plan idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	normalized.Spec.RequestedAt = agentosplan.NormalizeDurableTimestamp(normalized.Spec.RequestedAt)
	if normalized.Status.PlanID == "" {
		normalized.Status.PlanID = normalized.Spec.PlanID
	}

	if normalized.Status.LifecycleState == "" {
		normalized.Status.LifecycleState = agentos.PlanLifecyclePending
	}

	if normalized.Status.UpdatedAt.IsZero() {
		normalized.Status.UpdatedAt = time.Now().UTC()
	}

	state, err := agentosplan.NewStateFromStatus(&normalized.Spec, &normalized.Status)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, err
	}

	normalized.Status = state.Status

	return normalized, nil
}

func (r *AgentOSPlanRepo) savePlanStateWithTx(ctx context.Context, tx pgx.Tx, snapshot *agentosplan.PlanStateSnapshot, allowCreate bool) error {
	specJSON, err := marshalProcessPlatformJSON("AgentOSPlanRepo - SavePlanState spec", snapshot.Spec)
	if err != nil {
		return err
	}

	statusJSON, err := marshalProcessPlatformJSON("AgentOSPlanRepo - SavePlanState status", snapshot.Status)
	if err != nil {
		return err
	}

	queries := r.queries.WithTx(tx)

	existingSpec, exists, err := r.planSpecForUpdate(ctx, queries, snapshot.Spec.PlanID)
	if err != nil {
		return err
	}

	if err := validatePlanStateSaveTarget(&existingSpec, &snapshot.Spec, exists, allowCreate); err != nil {
		return err
	}

	if err := queries.UpsertPlan(ctx, sqlcgen.UpsertPlanParams{
		PlanID:         snapshot.Spec.PlanID,
		ThreadID:       snapshot.Spec.ThreadID,
		AccountID:      snapshot.Spec.AccountID,
		ProjectID:      snapshot.Spec.ProjectID,
		IdempotencyKey: snapshot.Spec.IdempotencyKey,
		LifecycleState: snapshot.Status.LifecycleState,
		Reason:         snapshot.Status.Reason,
		SpecJson:       specJSON,
		StatusJson:     statusJSON,
		RequestedAt:    optionalTimestamptz(snapshot.Spec.RequestedAt),
	}); err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanState - plan exec: %w", err)
	}

	capabilityByNode := r.buildNodeCapabilityMap(snapshot.Spec.Nodes)
	for i := range snapshot.Status.Nodes {
		node := &snapshot.Status.Nodes[i]
		if err := r.upsertPlanNode(ctx, queries, snapshot.Spec.PlanID, capabilityByNode[node.NodeID], node); err != nil {
			return err
		}
	}

	return r.deleteStalePlanNodes(ctx, queries, snapshot.Spec.PlanID, snapshot.Status.Nodes)
}

func validatePlanStateSaveTarget(existingSpec, spec *agentos.RunPlanSpec, exists, allowCreate bool) error {
	if exists {
		return agentosplan.ValidatePlanStateIdentity(existingSpec, spec)
	}

	if !allowCreate {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, spec.PlanID)
	}

	return nil
}

func (r *AgentOSPlanRepo) buildNodeCapabilityMap(nodes []agentos.PlanNodeSpec) map[string]string {
	capabilityByNode := make(map[string]string, len(nodes))
	for i := range nodes {
		node := &nodes[i]
		capabilityByNode[node.NodeID] = node.Capability
	}

	return capabilityByNode
}

func (r *AgentOSPlanRepo) planSpecForUpdate(ctx context.Context, queries *sqlcgen.Queries, planID string) (agentos.RunPlanSpec, bool, error) {
	specJSON, err := queries.GetPlanSpecForUpdate(ctx, planID)

	return getJSONRecord[agentos.RunPlanSpec]("AgentOSPlanRepo - planSpecForUpdate", specJSON, err)
}

func (r *AgentOSPlanRepo) upsertPlanNode(ctx context.Context, queries *sqlcgen.Queries, planID, capability string, node *agentos.PlanNodeStatus) error {
	statusJSON, err := marshalProcessPlatformJSON("AgentOSPlanRepo - upsertPlanNode", node)
	if err != nil {
		return err
	}

	if err := queries.UpsertPlanNode(ctx, sqlcgen.UpsertPlanNodeParams{
		PlanID:         planID,
		NodeID:         node.NodeID,
		RunID:          node.RunID,
		BackendKind:    string(node.Backend.Kind),
		BackendName:    node.Backend.Name,
		Capability:     capability,
		LifecycleState: node.LifecycleState,
		Attempts:       node.Attempts,
		Reason:         node.Reason,
		StatusJson:     statusJSON,
	}); err != nil {
		return fmt.Errorf("AgentOSPlanRepo - upsertPlanNode - exec: %w", err)
	}

	return nil
}

func (r *AgentOSPlanRepo) deleteStalePlanNodes(ctx context.Context, queries *sqlcgen.Queries, planID string, nodes []agentos.PlanNodeStatus) error {
	nodeIDs := make([]string, 0, len(nodes))
	for i := range nodes {
		node := nodes[i]
		nodeIDs = append(nodeIDs, node.NodeID)
	}

	if len(nodeIDs) == 0 {
		if err := queries.DeletePlanNodesByPlanID(ctx, planID); err != nil {
			return fmt.Errorf("AgentOSPlanRepo - deleteStalePlanNodes - delete all: %w", err)
		}

		return nil
	}

	if err := queries.DeletePlanNodesNotIn(ctx, sqlcgen.DeletePlanNodesNotInParams{
		PlanID:  planID,
		NodeIds: nodeIDs,
	}); err != nil {
		return fmt.Errorf("AgentOSPlanRepo - deleteStalePlanNodes - delete: %w", err)
	}

	return nil
}

// LoadPlanState loads the plan state snapshot for the given plan ID, including node statuses.
func (r *AgentOSPlanRepo) LoadPlanState(ctx context.Context, planID string) (agentosplan.PlanStateSnapshot, bool, error) {
	if planID == "" {
		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidRunPlan)
	}

	row, err := r.queries.GetPlanStateByID(ctx, planID)
	if missingRow(err) {
		return agentosplan.PlanStateSnapshot{}, false, nil
	}

	if err != nil {
		return agentosplan.PlanStateSnapshot{}, false, fmt.Errorf("AgentOSPlanRepo - LoadPlanState - query: %w", err)
	}

	snapshot, err := r.planStateFromDocuments(ctx, "AgentOSPlanRepo - LoadPlanState", row.SpecJson, row.StatusJson)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, false, err
	}

	return snapshot, true, nil
}

// planStateFromDocuments turns a stored spec/status pair into the aggregate
// snapshot both plan lookups return, loading the node projections under it.
func (r *AgentOSPlanRepo) planStateFromDocuments(ctx context.Context, name string, specJSON, statusJSON []byte) (agentosplan.PlanStateSnapshot, error) {
	spec, status, err := planSpecStatusFromJSON(name, specJSON, statusJSON)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, err
	}

	nodes, err := r.loadPlanNodeStatuses(ctx, spec.PlanID)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, err
	}

	status.Nodes = nodes

	state, err := agentosplan.NewStateFromStatus(&spec, &status)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, err
	}

	return agentosplan.PlanStateSnapshot{
		Spec:   spec,
		Status: state.Status,
	}, nil
}

// planSpecStatusFromJSON decodes the two JSON documents a plan row carries.
func planSpecStatusFromJSON(name string, specJSON, statusJSON []byte) (agentos.RunPlanSpec, agentos.RunPlanStatus, error) {
	spec, err := decodeProcessPlatformJSON[agentos.RunPlanSpec](name+" - spec", specJSON)
	if err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, err
	}

	status, err := decodeProcessPlatformJSON[agentos.RunPlanStatus](name+" - status", statusJSON)
	if err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, err
	}

	return spec, status, nil
}

func (r *AgentOSPlanRepo) loadPlanNodeStatuses(ctx context.Context, planID string) ([]agentos.PlanNodeStatus, error) {
	rows, err := r.queries.ListPlanNodesByPlanID(ctx, planID)

	return listRecords("AgentOSPlanRepo - loadPlanNodeStatuses", rows, err, func(row *[]byte) (agentos.PlanNodeStatus, error) {
		return decodeProcessPlatformJSON[agentos.PlanNodeStatus]("AgentOSPlanRepo - loadPlanNodeStatuses - status", *row)
	})
}

func (r *AgentOSPlanRepo) planByIdempotencyKey(ctx context.Context, accountID, projectID, idempotencyKey string) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error) {
	if idempotencyKey == "" {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, fmt.Errorf("%w: plan idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	row, err := r.queries.GetPlanByIdempotencyKey(ctx, sqlcgen.GetPlanByIdempotencyKeyParams{
		AccountID:      accountID,
		ProjectID:      projectID,
		IdempotencyKey: idempotencyKey,
	})
	if missingRow(err) {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, nil
	}

	if err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, fmt.Errorf("AgentOSPlanRepo - planByIdempotencyKey - query: %w", err)
	}

	spec, status, err := planSpecStatusFromJSON("AgentOSPlanRepo - planByIdempotencyKey", row.SpecJson, row.StatusJson)
	if err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, err
	}

	return spec, status, true, nil
}

type planTenantScope struct {
	AccountID string
	ProjectID string
}

func planTenantScopeByPlanID(ctx context.Context, queries *sqlcgen.Queries, planID string) (planTenantScope, bool, error) {
	row, err := queries.GetPlanTenantScope(ctx, planID)
	if missingRow(err) {
		return planTenantScope{}, false, nil
	}

	if err != nil {
		return planTenantScope{}, false, fmt.Errorf("planTenantScopeByPlanID - query: %w", err)
	}

	return planTenantScope{AccountID: row.AccountID, ProjectID: row.ProjectID}, true, nil
}

// PersistPlanTransition atomically saves the given plan state snapshot and appends its transition event idempotently.
func (r *AgentOSPlanRepo) PersistPlanTransition(ctx context.Context, snapshot *agentosplan.PlanStateSnapshot, event *agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	normalizedSnapshot, normalizedEvent, transitionIdentity, err := normalizePlanTransitionForPostgres(snapshot, event, idempotencyKey)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - PersistPlanTransition - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	queries := r.queries.WithTx(tx)

	scope, planFound, err := r.lockPlanRow(ctx, queries, normalizedEvent.PlanID)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	existing, exists, err := r.existingPlanTransitionEvent(ctx, queries, scope, planFound, &normalizedEvent, idempotencyKey, transitionIdentity)
	if err != nil || exists {
		return existing, err
	}

	if err := r.savePlanStateWithTx(ctx, tx, &normalizedSnapshot, false); err != nil {
		return agentos.PlanEvent{}, err
	}

	stored, err := r.appendPlanEventWithTx(ctx, tx, &normalizedEvent, idempotencyKey, transitionIdentity)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - PersistPlanTransition - commit: %w", err)
	}

	return stored, nil
}

func normalizePlanTransitionForPostgres(snapshot *agentosplan.PlanStateSnapshot, event *agentos.PlanEvent, idempotencyKey string) (agentosplan.PlanStateSnapshot, agentos.PlanEvent, agentosplan.PlanTransitionSnapshotIdentity, error) {
	normalizedSnapshot, err := normalizePlanStateSnapshotForPostgres(snapshot)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, agentos.PlanEvent{}, agentosplan.PlanTransitionSnapshotIdentity{}, err
	}

	normalizedEvent, err := normalizePlanEventAppendForPostgres(event, idempotencyKey)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, agentos.PlanEvent{}, agentosplan.PlanTransitionSnapshotIdentity{}, err
	}

	if normalizedEvent.PlanID != normalizedSnapshot.Spec.PlanID {
		return agentosplan.PlanStateSnapshot{}, agentos.PlanEvent{}, agentosplan.PlanTransitionSnapshotIdentity{}, fmt.Errorf("%w: event plan %q does not match snapshot plan %q", agentoscore.ErrInvalidPlanEvent, normalizedEvent.PlanID, normalizedSnapshot.Spec.PlanID)
	}

	transitionIdentity, err := agentosplan.NewPlanTransitionSnapshotIdentity(&normalizedSnapshot, idempotencyKey)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, agentos.PlanEvent{}, agentosplan.PlanTransitionSnapshotIdentity{}, err
	}

	normalizedEvent, err = agentosplan.ScopePlanEventToSpec(&normalizedEvent, &normalizedSnapshot.Spec)
	if err != nil {
		return agentosplan.PlanStateSnapshot{}, agentos.PlanEvent{}, agentosplan.PlanTransitionSnapshotIdentity{}, err
	}

	return normalizedSnapshot, normalizedEvent, transitionIdentity, nil
}

func (r *AgentOSPlanRepo) existingPlanTransitionEvent(
	ctx context.Context,
	queries *sqlcgen.Queries,
	scope planTenantScope,
	planFound bool,
	event *agentos.PlanEvent,
	idempotencyKey string,
	transitionIdentity agentosplan.PlanTransitionSnapshotIdentity,
) (agentos.PlanEvent, bool, error) {
	if !planFound {
		return agentos.PlanEvent{}, false, nil
	}

	existing, exists, err := r.planEventByIdempotencyKeyWith(ctx, queries, scope, event.PlanID, idempotencyKey)
	if err != nil || !exists {
		return agentos.PlanEvent{}, false, err
	}

	err = r.validatePlanEventIdempotency(&existing.Event, existing.TransitionSnapshotDigest, event, transitionIdentity)

	return existing.Event, true, err
}

func (r *AgentOSPlanRepo) lockPlanRow(ctx context.Context, queries *sqlcgen.Queries, planID string) (planTenantScope, bool, error) {
	row, err := queries.LockPlanRow(ctx, planID)
	if missingRow(err) {
		return planTenantScope{}, false, nil
	}

	if err != nil {
		return planTenantScope{}, false, fmt.Errorf("AgentOSPlanRepo - PersistPlanTransition - lock plan: %w", err)
	}

	return planTenantScope{AccountID: row.AccountID, ProjectID: row.ProjectID}, true, nil
}

// AppendPlanEvent appends a durable plan event with a monotonically increasing sequence number.
func (r *AgentOSPlanRepo) AppendPlanEvent(ctx context.Context, event *agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	normalizedEvent, err := normalizePlanEventAppendForPostgres(event, idempotencyKey)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - begin: %w", err)
	}

	defer func() {
		errcheckIgnore(tx.Rollback(ctx))
	}()

	stored, err := r.appendPlanEventWithTx(ctx, tx, &normalizedEvent, idempotencyKey, agentosplan.PlanTransitionSnapshotIdentity{})
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - commit: %w", err)
	}

	return stored, nil
}

func normalizePlanEventAppendForPostgres(event *agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	if event.PlanID == "" {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidPlanEvent)
	}

	if idempotencyKey == "" {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan event idempotency key is required", agentoscore.ErrInvalidPlanEvent)
	}

	return agentosplan.NormalizePlanEventAppendRequest(event), nil
}

func (r *AgentOSPlanRepo) appendPlanEventWithTx(ctx context.Context, tx pgx.Tx, requestedEvent *agentos.PlanEvent, idempotencyKey string, transitionIdentity agentosplan.PlanTransitionSnapshotIdentity) (agentos.PlanEvent, error) {
	event := *requestedEvent
	queries := r.queries.WithTx(tx)

	locked, err := queries.LockPlanEventSequence(ctx, event.PlanID)
	if missingRow(err) {
		return agentos.PlanEvent{}, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, event.PlanID)
	}

	if err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - lock plan: %w", err)
	}

	scope := planTenantScope{AccountID: locked.AccountID, ProjectID: locked.ProjectID}

	spec := agentos.RunPlanSpec{
		PlanID:    event.PlanID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	}

	event, err = agentosplan.ScopePlanEventToSpec(&event, &spec)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	existing, exists, err := r.planEventByIdempotencyKeyWith(ctx, queries, scope, event.PlanID, idempotencyKey)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	if exists {
		if err := r.validatePlanEventIdempotency(&existing.Event, existing.TransitionSnapshotDigest, &event, transitionIdentity); err != nil {
			return agentos.PlanEvent{}, err
		}

		return existing.Event, nil
	}

	stored, err := r.buildAndInsertPlanEvent(ctx, queries, &event, locked.EventSequence, scope, idempotencyKey, transitionIdentity)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	return stored, nil
}

func (r *AgentOSPlanRepo) buildAndInsertPlanEvent(ctx context.Context, queries *sqlcgen.Queries, event *agentos.PlanEvent, currentSequence int64, scope planTenantScope, idempotencyKey string, transitionIdentity agentosplan.PlanTransitionSnapshotIdentity) (agentos.PlanEvent, error) {
	event.Sequence = currentSequence + 1
	if err := queries.AdvancePlanEventSequence(ctx, sqlcgen.AdvancePlanEventSequenceParams{
		PlanID:        event.PlanID,
		EventSequence: event.Sequence,
	}); err != nil {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - update sequence: %w", err)
	}

	event.EventID = fmt.Sprintf("%s:%d", event.PlanID, event.Sequence)
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	event.Timestamp = agentosplan.NormalizeDurableTimestamp(event.Timestamp)
	if event.Payload == nil {
		event.Payload = map[string]any{}
	}

	payloadJSON, err := marshalProcessPlatformJSON("AgentOSPlanRepo - AppendPlanEvent payload", event.Payload)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	eventJSON, err := agentos.MarshalPlanEvent(event)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	var transitionSnapshotJSON []byte
	if transitionIdentity.Digest != "" {
		transitionSnapshotJSON = transitionIdentity.JSON
	}

	storedJSON, err := queries.InsertPlanEvent(ctx, sqlcgen.InsertPlanEventParams{
		EventID:                  event.EventID,
		PlanID:                   event.PlanID,
		AccountID:                scope.AccountID,
		ProjectID:                scope.ProjectID,
		NodeID:                   event.NodeID,
		RunID:                    event.RunID,
		EventType:                string(event.EventType),
		Sequence:                 event.Sequence,
		IdempotencyKey:           idempotencyKey,
		TransitionSnapshotDigest: transitionIdentity.Digest,
		TransitionSnapshotJson:   transitionSnapshotJSON,
		PayloadJson:              payloadJSON,
		EventJson:                eventJSON,
		Timestamp:                event.Timestamp,
	})
	if err != nil {
		return r.resolvePlanEventInsertConflict(ctx, queries, scope, event.PlanID, idempotencyKey, event, transitionIdentity, err)
	}

	return agentos.UnmarshalPlanEvent(storedJSON)
}

func (r *AgentOSPlanRepo) validatePlanEventIdempotency(existing *agentos.PlanEvent, existingDigest string, requested *agentos.PlanEvent, transitionIdentity agentosplan.PlanTransitionSnapshotIdentity) error {
	if err := agentosplan.ValidatePlanEventIdempotency(existing, requested); err != nil {
		return err
	}

	if transitionIdentity.Digest != "" {
		if err := agentosplan.ValidatePlanTransitionIdempotency(existingDigest, transitionIdentity); err != nil {
			return err
		}
	}

	return nil
}

func (r *AgentOSPlanRepo) resolvePlanEventInsertConflict(ctx context.Context, queries *sqlcgen.Queries, scope planTenantScope, planID, idempotencyKey string, requestedEvent *agentos.PlanEvent, transitionIdentity agentosplan.PlanTransitionSnapshotIdentity, err error) (agentos.PlanEvent, error) {
	if !isPostgresUniqueViolation(err) {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - insert: %w", err)
	}

	existing, exists, lookupErr := r.planEventByIdempotencyKeyWith(ctx, queries, scope, planID, idempotencyKey)
	if lookupErr != nil {
		return agentos.PlanEvent{}, lookupErr
	}

	if !exists {
		return agentos.PlanEvent{}, fmt.Errorf("AgentOSPlanRepo - AppendPlanEvent - insert: %w", err)
	}

	if err := r.validatePlanEventIdempotency(&existing.Event, existing.TransitionSnapshotDigest, requestedEvent, transitionIdentity); err != nil {
		return agentos.PlanEvent{}, err
	}

	return existing.Event, nil
}

type planEventIdempotencyRecord struct {
	Event                    agentos.PlanEvent
	TransitionSnapshotDigest string
}

func (r *AgentOSPlanRepo) planEventByIdempotencyKeyWith(ctx context.Context, queries *sqlcgen.Queries, scope planTenantScope, planID, idempotencyKey string) (planEventIdempotencyRecord, bool, error) {
	row, err := queries.GetPlanEventByIdempotencyKey(ctx, sqlcgen.GetPlanEventByIdempotencyKeyParams{
		AccountID:      scope.AccountID,
		ProjectID:      scope.ProjectID,
		PlanID:         planID,
		IdempotencyKey: idempotencyKey,
	})
	if missingRow(err) {
		return planEventIdempotencyRecord{}, false, nil
	}

	if err != nil {
		return planEventIdempotencyRecord{}, false, fmt.Errorf("AgentOSPlanRepo - planEventByIdempotencyKey - query: %w", err)
	}

	event, err := agentos.UnmarshalPlanEvent(row.EventJson)
	if err != nil {
		return planEventIdempotencyRecord{}, false, err
	}

	return planEventIdempotencyRecord{
		Event:                    event,
		TransitionSnapshotDigest: row.TransitionSnapshotDigest,
	}, true, nil
}

// ListPlanEvents returns plan events matching the given stream scope, ordered by sequence.
func (r *AgentOSPlanRepo) ListPlanEvents(ctx context.Context, scope *agentos.PlanStreamScope, limit int) ([]agentos.PlanEvent, error) {
	if err := r.authorizePlanStreamScope(ctx, scope); err != nil {
		return nil, err
	}

	rows, err := r.queries.ListPlanEvents(ctx, sqlcgen.ListPlanEventsParams{
		PlanID:    scope.PlanID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
		Sequence:  scope.AfterSequence,
		NodeID:    scope.NodeID,
		RunID:     scope.RunID,
		RowLimit:  optionalInt8(limit),
	})

	return listRecords("AgentOSPlanRepo - ListPlanEvents", rows, err, func(row *[]byte) (agentos.PlanEvent, error) {
		return agentos.UnmarshalPlanEvent(*row)
	})
}

func (r *AgentOSPlanRepo) authorizePlanStreamScope(ctx context.Context, scope *agentos.PlanStreamScope) error {
	if err := agentosplan.ValidatePlanStreamScope(scope); err != nil {
		return err
	}

	spec, _, exists, err := r.GetPlan(ctx, scope.PlanID)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, scope.PlanID)
	}

	return agentosplan.ValidatePlanTenantAccess(agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}, &spec)
}

// GetPlanMetricCheckpoint loads the metrics checkpoint stored by an exporter for a plan.
func (r *AgentOSPlanRepo) GetPlanMetricCheckpoint(ctx context.Context, exporterID string, ref agentos.PlanRef) (agentosplan.PlanMetricCheckpoint, bool, error) {
	if exporterID == "" {
		return agentosplan.PlanMetricCheckpoint{}, false, fmt.Errorf("%w: metrics exporter id is required", agentoscore.ErrInvalidRunPlan)
	}

	if err := agentosplan.ValidatePlanRef(ref); err != nil {
		return agentosplan.PlanMetricCheckpoint{}, false, err
	}

	row, err := r.queries.GetPlanMetricCheckpoint(ctx, sqlcgen.GetPlanMetricCheckpointParams{
		ExporterID: exporterID,
		PlanID:     ref.PlanID,
		AccountID:  ref.AccountID,
		ProjectID:  ref.ProjectID,
	})
	if missingRow(err) {
		return agentosplan.PlanMetricCheckpoint{}, false, nil
	}

	if err != nil {
		return agentosplan.PlanMetricCheckpoint{}, false, fmt.Errorf("AgentOSPlanRepo - GetPlanMetricCheckpoint - query: %w", err)
	}

	checkpoint := agentosplan.PlanMetricCheckpoint{
		ExporterID: row.ExporterID,
		PlanID:     row.PlanID,
		AccountID:  row.AccountID,
		ProjectID:  row.ProjectID,
		Sequence:   row.Sequence,
		UpdatedAt:  row.UpdatedAt,
	}

	if len(row.ProjectionJson) > 0 {
		if err := json.Unmarshal(row.ProjectionJson, &checkpoint.Projection); err != nil {
			return agentosplan.PlanMetricCheckpoint{}, false, fmt.Errorf("AgentOSPlanRepo - GetPlanMetricCheckpoint - decode projection: %w", err)
		}
	}

	if err := agentosplan.ValidatePlanMetricCheckpointRef(&checkpoint, exporterID, ref); err != nil {
		return agentosplan.PlanMetricCheckpoint{}, false, err
	}

	return checkpoint, true, nil
}

// SavePlanMetricCheckpoint upserts a metrics checkpoint for a plan, rejecting backward sequence moves.
func (r *AgentOSPlanRepo) SavePlanMetricCheckpoint(ctx context.Context, checkpoint *agentosplan.PlanMetricCheckpoint) error {
	ref := agentos.PlanRef{
		PlanID:    checkpoint.PlanID,
		AccountID: checkpoint.AccountID,
		ProjectID: checkpoint.ProjectID,
	}
	if err := agentosplan.ValidatePlanMetricCheckpointRef(checkpoint, checkpoint.ExporterID, ref); err != nil {
		return err
	}

	scope, exists, err := planTenantScopeByPlanID(ctx, r.queries, checkpoint.PlanID)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, checkpoint.PlanID)
	}

	if scope.AccountID != checkpoint.AccountID || scope.ProjectID != checkpoint.ProjectID {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, checkpoint.PlanID)
	}

	projectionJSON, err := json.Marshal(checkpoint.Projection)
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanMetricCheckpoint - marshal projection: %w", err)
	}

	updated, err := r.queries.SavePlanMetricCheckpoint(ctx, sqlcgen.SavePlanMetricCheckpointParams{
		ExporterID:     checkpoint.ExporterID,
		PlanID:         checkpoint.PlanID,
		AccountID:      checkpoint.AccountID,
		ProjectID:      checkpoint.ProjectID,
		Sequence:       checkpoint.Sequence,
		ProjectionJson: projectionJSON,
	})
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - SavePlanMetricCheckpoint - upsert: %w", err)
	}

	if updated == 0 {
		return fmt.Errorf("%w: metrics checkpoint sequence moved backward for plan %q", agentoscore.ErrInvalidRunPlan, checkpoint.PlanID)
	}

	return nil
}

// RecordPlanMetric persists a plan metric sample idempotently.
func (r *AgentOSPlanRepo) RecordPlanMetric(ctx context.Context, sample *agentosplan.PlanMetricSample) error {
	normalized := agentosplan.NormalizePlanMetricSample(sample)

	sample = &normalized
	if err := agentosplan.ValidatePlanMetricSample(sample); err != nil {
		return err
	}

	scope, exists, err := planTenantScopeByPlanID(ctx, r.queries, sample.PlanID)
	if err != nil {
		return err
	}

	if err := validatePlanMetricScope(sample, scope, exists); err != nil {
		return err
	}

	labelsJSON, err := json.Marshal(planMetricLabelsForStorage(sample.Labels))
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - RecordPlanMetric - marshal labels: %w", err)
	}

	inserted, err := r.queries.RecordPlanMetric(ctx, sqlcgen.RecordPlanMetricParams{
		MetricName:      string(sample.Name),
		PlanID:          sample.PlanID,
		AccountID:       sample.AccountID,
		ProjectID:       sample.ProjectID,
		NodeID:          sample.NodeID,
		RunID:           sample.RunID,
		EventID:         sample.EventID,
		Sequence:        sample.Sequence,
		Value:           sample.Value,
		Unit:            sample.Unit,
		LabelsJson:      labelsJSON,
		SampleTimestamp: sample.Timestamp,
	})
	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - RecordPlanMetric - insert: %w", err)
	}

	if inserted == 1 {
		return nil
	}

	return r.validateMetricSampleConflict(ctx, sample)
}

func validatePlanMetricScope(sample *agentosplan.PlanMetricSample, scope planTenantScope, exists bool) error {
	if !exists {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, sample.PlanID)
	}

	if scope.AccountID != sample.AccountID || scope.ProjectID != sample.ProjectID {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, sample.PlanID)
	}

	return nil
}

func (r *AgentOSPlanRepo) validateMetricSampleConflict(ctx context.Context, sample *agentosplan.PlanMetricSample) error {
	key := sample.Key()

	existing, exists, err := r.planMetricSampleByKey(ctx, &key)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("%w: metric sample identity conflict for plan %q event %q", agentoscore.ErrInvalidRunPlan, sample.PlanID, sample.EventID)
	}

	return agentosplan.ValidatePlanMetricSampleIdempotency(&existing, sample)
}

func (r *AgentOSPlanRepo) planMetricSampleByKey(ctx context.Context, key *agentosplan.PlanMetricSampleKey) (agentosplan.PlanMetricSample, bool, error) {
	row, err := r.queries.GetPlanMetricSampleByKey(ctx, sqlcgen.GetPlanMetricSampleByKeyParams{
		MetricName: string(key.Name),
		PlanID:     key.PlanID,
		NodeID:     key.NodeID,
		RunID:      key.RunID,
		EventID:    key.EventID,
		Sequence:   key.Sequence,
	})
	if missingRow(err) {
		return agentosplan.PlanMetricSample{}, false, nil
	}

	if err != nil {
		return agentosplan.PlanMetricSample{}, false, fmt.Errorf("AgentOSPlanRepo - planMetricSampleByKey - query: %w", err)
	}

	sample := agentosplan.PlanMetricSample{
		Name:      agentosplan.PlanMetricName(row.MetricName),
		PlanID:    row.PlanID,
		AccountID: row.AccountID,
		ProjectID: row.ProjectID,
		NodeID:    row.NodeID,
		RunID:     row.RunID,
		EventID:   row.EventID,
		Sequence:  row.Sequence,
		Value:     row.Value,
		Unit:      row.Unit,
		Timestamp: row.SampleTimestamp,
	}

	if len(row.LabelsJson) > 0 {
		if err := json.Unmarshal(row.LabelsJson, &sample.Labels); err != nil {
			return agentosplan.PlanMetricSample{}, false, fmt.Errorf("AgentOSPlanRepo - planMetricSampleByKey - decode labels: %w", err)
		}
	}

	return sample, true, nil
}

func planMetricLabelsForStorage(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return map[string]string{}
	}

	return labels
}

// RecordAudit persists an audit record for a plan, returning the stored record and whether it was newly created.
func (r *AgentOSPlanRepo) RecordAudit(ctx context.Context, record *agentosplan.AuditRecord) (agentosplan.AuditRecord, bool, error) {
	ref, scope, err := r.prepareAuditRecord(ctx, record)
	if err != nil {
		return agentosplan.AuditRecord{}, false, err
	}

	existing, exists, err := r.existingAuditRecord(ctx, ref, record)
	if err != nil || exists {
		return existing, false, err
	}

	if err := r.validateAuditOwnership(ctx, record, scope); err != nil {
		return agentosplan.AuditRecord{}, false, err
	}

	setAuditRecordDefaults(record, ref)

	stored, err := r.insertAuditRecord(ctx, record, ref)
	if err != nil {
		return agentosplan.AuditRecord{}, false, err
	}

	return stored, true, nil
}

func (r *AgentOSPlanRepo) prepareAuditRecord(ctx context.Context, record *agentosplan.AuditRecord) (agentosplan.AuditRef, planTenantScope, error) {
	if record.PlanID == "" {
		return agentosplan.AuditRef{}, planTenantScope{}, fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidRunPlan)
	}

	if record.Action == "" {
		return agentosplan.AuditRef{}, planTenantScope{}, fmt.Errorf("%w: audit action is required", agentoscore.ErrInvalidRunPlan)
	}

	if record.IdempotencyKey == "" {
		return agentosplan.AuditRef{}, planTenantScope{}, fmt.Errorf("%w: audit idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	scope, exists, err := planTenantScopeByPlanID(ctx, r.queries, record.PlanID)
	if err != nil {
		return agentosplan.AuditRef{}, planTenantScope{}, err
	}

	if !exists {
		return agentosplan.AuditRef{}, planTenantScope{}, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, record.PlanID)
	}

	record.AccountID = scope.AccountID
	record.ProjectID = scope.ProjectID

	return agentosplan.AuditRefFromRecord(record), scope, nil
}

func (r *AgentOSPlanRepo) existingAuditRecord(ctx context.Context, ref agentosplan.AuditRef, record *agentosplan.AuditRecord) (agentosplan.AuditRecord, bool, error) {
	existing, exists, err := r.GetAuditRecord(ctx, ref)
	if err != nil || !exists {
		return agentosplan.AuditRecord{}, false, err
	}

	if err := agentosplan.ValidateAuditIdempotency(&existing, record); err != nil {
		return agentosplan.AuditRecord{}, false, err
	}

	return existing, true, nil
}

func setAuditRecordDefaults(record *agentosplan.AuditRecord, ref agentosplan.AuditRef) {
	if record.AuditID == "" {
		record.AuditID = agentosplan.AuditIDFromRef(ref)
	}

	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}

	if record.Payload == nil {
		record.Payload = map[string]any{}
	}
}

func (r *AgentOSPlanRepo) insertAuditRecord(ctx context.Context, record *agentosplan.AuditRecord, ref agentosplan.AuditRef) (agentosplan.AuditRecord, error) {
	// Stored and hashed in the same canonical form, so the verifier hashes
	// exactly what the writer hashed after jsonb has had its way with it.
	payloadJSON, err := canonicalAuditPayload(record.Payload)
	if err != nil {
		return agentosplan.AuditRecord{}, fmt.Errorf("AgentOSPlanRepo - RecordAudit - marshal payload: %w", err)
	}

	// The row is appended to its tenant's hash chain, in the same transaction
	// that reads the predecessor: an audit row written outside the chain would
	// be an audit row that cannot be proven unedited.
	err = r.insertAuditRecordChained(ctx, record, payloadJSON)
	if err != nil {
		return resolveUniqueInsertConflict(
			err,
			"AgentOSPlanRepo - RecordAudit - insert",
			func() (agentosplan.AuditRecord, bool, error) { return r.GetAuditRecord(ctx, ref) },
			func(existing *agentosplan.AuditRecord) error {
				return agentosplan.ValidateAuditIdempotency(existing, record)
			},
		)
	}

	stored, exists, err := r.GetAuditRecord(ctx, ref)
	if err != nil {
		return agentosplan.AuditRecord{}, err
	}

	if !exists {
		return agentosplan.AuditRecord{}, fmt.Errorf(
			"AgentOSPlanRepo - RecordAudit - insert: %w",
			errInsertedAuditRecordNotFound,
		)
	}

	return stored, nil
}

func resolveUniqueInsertConflict[T any](err error, operation string, lookup func() (T, bool, error), validate func(*T) error) (T, error) {
	var zero T

	if !isPostgresUniqueViolation(err) {
		return zero, fmt.Errorf("%s: %w", operation, err)
	}

	existing, exists, lookupErr := lookup()
	if lookupErr != nil {
		return zero, lookupErr
	}

	if !exists {
		return zero, fmt.Errorf("%s: %w", operation, err)
	}

	if err := validate(&existing); err != nil {
		return zero, err
	}

	return existing, nil
}

// GetAuditRecord loads the audit record identified by the given reference.
func (r *AgentOSPlanRepo) GetAuditRecord(ctx context.Context, ref agentosplan.AuditRef) (agentosplan.AuditRecord, bool, error) {
	if err := agentosplan.ValidateAuditRef(ref); err != nil {
		return agentosplan.AuditRecord{}, false, err
	}

	row, err := r.queries.GetAuditRecord(ctx, sqlcgen.GetAuditRecordParams{
		PlanID:         ref.PlanID,
		AccountID:      ref.AccountID,
		ProjectID:      ref.ProjectID,
		IdempotencyKey: ref.IdempotencyKey,
	})

	return getRecord("AgentOSPlanRepo - GetAuditRecord - query", row, err, planAuditRecordFromGetRow)
}

// ListAuditRecords returns audit records matching the given plan audit scope.
func (r *AgentOSPlanRepo) ListAuditRecords(ctx context.Context, scope *agentos.PlanAuditScope) ([]agentos.PlanAuditRecord, error) {
	if err := agentosplan.ValidatePlanAuditScope(scope); err != nil {
		return nil, err
	}

	spec, _, exists, err := r.GetPlan(ctx, scope.PlanID)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, scope.PlanID)
	}

	if err := agentosplan.ValidatePlanTenantAccess(agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}, &spec); err != nil {
		return nil, err
	}

	rows, err := r.queries.ListAuditRecords(ctx, sqlcgen.ListAuditRecordsParams{
		PlanID:    scope.PlanID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
		NodeID:    scope.NodeID,
		RunID:     scope.RunID,
		Action:    string(scope.Action),
		RowLimit:  optionalInt8(scope.Limit),
	})

	return listRecords("AgentOSPlanRepo - ListAuditRecords", rows, err, planAuditRecordFromListRow)
}

// planAuditRecordFields is the one projection every audit_logs query returns,
// so every row mapper shapes it through this single constructor.
type planAuditRecordFields struct {
	AuditID        string
	PlanID         string
	AccountID      string
	ProjectID      string
	RunID          string
	NodeID         string
	ActorID        string
	Action         string
	IdempotencyKey string
	Payload        []byte
	CreatedAt      time.Time
}

func planAuditRecordFromGetRow(row *sqlcgen.GetAuditRecordRow) (agentosplan.AuditRecord, error) {
	return planAuditRecordFromFields("GetAuditRecord", &planAuditRecordFields{
		AuditID: row.AuditID, PlanID: row.PlanID, AccountID: row.AccountID, ProjectID: row.ProjectID,
		RunID: row.RunID, NodeID: row.NodeID, ActorID: row.ActorID, Action: row.Action,
		IdempotencyKey: row.IdempotencyKey, Payload: row.PayloadJson, CreatedAt: row.CreatedAt,
	})
}

func planAuditRecordFromListRow(row *sqlcgen.ListAuditRecordsRow) (agentos.PlanAuditRecord, error) {
	record, err := planAuditRecordFromFields("scanPlanAuditRecord", &planAuditRecordFields{
		AuditID: row.AuditID, PlanID: row.PlanID, AccountID: row.AccountID, ProjectID: row.ProjectID,
		RunID: row.RunID, NodeID: row.NodeID, ActorID: row.ActorID, Action: row.Action,
		IdempotencyKey: row.IdempotencyKey, Payload: row.PayloadJson, CreatedAt: row.CreatedAt,
	})
	if err != nil {
		return agentos.PlanAuditRecord{}, err
	}

	return agentos.PlanAuditRecord{
		AuditID:        record.AuditID,
		PlanID:         record.PlanID,
		AccountID:      record.AccountID,
		ProjectID:      record.ProjectID,
		RunID:          record.RunID,
		NodeID:         record.NodeID,
		ActorID:        record.ActorID,
		Action:         agentos.PlanAuditAction(record.Action),
		IdempotencyKey: record.IdempotencyKey,
		Payload:        record.Payload,
		CreatedAt:      record.CreatedAt,
	}, nil
}

func planAuditRecordFromFields(name string, fields *planAuditRecordFields) (agentosplan.AuditRecord, error) {
	record := agentosplan.AuditRecord{
		AuditID:        fields.AuditID,
		PlanID:         fields.PlanID,
		AccountID:      fields.AccountID,
		ProjectID:      fields.ProjectID,
		RunID:          fields.RunID,
		NodeID:         fields.NodeID,
		ActorID:        fields.ActorID,
		Action:         agentosplan.AuditAction(fields.Action),
		IdempotencyKey: fields.IdempotencyKey,
		CreatedAt:      fields.CreatedAt,
	}

	if len(fields.Payload) > 0 {
		if err := json.Unmarshal(fields.Payload, &record.Payload); err != nil {
			return agentosplan.AuditRecord{}, fmt.Errorf("AgentOSPlanRepo - %s - decode payload: %w", name, err)
		}
	}

	return record, nil
}

func (r *AgentOSPlanRepo) validateAuditOwnership(ctx context.Context, record *agentosplan.AuditRecord, scope planTenantScope) error {
	if err := agentosplan.ValidateAuditNodeRunPair(record); err != nil {
		return err
	}

	if record.NodeID == "" {
		return nil
	}

	owner, err := r.queries.GetPlanNodeRunOwnership(ctx, sqlcgen.GetPlanNodeRunOwnershipParams{
		PlanID:    record.PlanID,
		NodeID:    record.NodeID,
		RunID:     record.RunID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	})
	if missingRow(err) {
		return fmt.Errorf("%w: audit node %q is not durable", agentoscore.ErrInvalidRunPlan, record.NodeID)
	}

	if err != nil {
		return fmt.Errorf("AgentOSPlanRepo - validateAuditOwnership - query: %w", err)
	}

	if owner.NodeRunID != record.RunID {
		return fmt.Errorf("%w: audit node %q has durable run id %q, got %q", agentoscore.ErrInvalidRunPlan, record.NodeID, owner.NodeRunID, record.RunID)
	}

	if !owner.OwnedRunID.Valid || owner.OwnedRunID.String == "" {
		return fmt.Errorf("%w: %s", agentoscore.ErrRunRouteNotFound, record.RunID)
	}

	return nil
}

// RecordPlanCommand persists a plan command, returning the stored record and whether it was newly created.
func (r *AgentOSPlanRepo) RecordPlanCommand(ctx context.Context, command *agentosplan.PlanCommandRecord) (agentosplan.PlanCommandRecord, bool, error) {
	ref, err := r.preparePlanCommandRecord(ctx, command)
	if err != nil {
		return agentosplan.PlanCommandRecord{}, false, err
	}

	existing, exists, err := r.existingPlanCommand(ctx, ref, command)
	if err != nil || exists {
		return existing, false, err
	}

	if err := setPlanCommandRecordDefaults(command, ref); err != nil {
		return agentosplan.PlanCommandRecord{}, false, err
	}

	stored, err := r.insertPlanCommand(ctx, command, ref)
	if err != nil {
		return agentosplan.PlanCommandRecord{}, false, err
	}

	return stored, true, nil
}

func (r *AgentOSPlanRepo) preparePlanCommandRecord(ctx context.Context, command *agentosplan.PlanCommandRecord) (agentosplan.PlanCommandRef, error) {
	if command.PlanID == "" {
		return agentosplan.PlanCommandRef{}, fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidRunPlan)
	}

	if command.Action == "" {
		return agentosplan.PlanCommandRef{}, fmt.Errorf("%w: command action is required", agentoscore.ErrInvalidRunPlan)
	}

	if command.IdempotencyKey == "" {
		return agentosplan.PlanCommandRef{}, fmt.Errorf("%w: command idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	scope, exists, err := planTenantScopeByPlanID(ctx, r.queries, command.PlanID)
	if err != nil {
		return agentosplan.PlanCommandRef{}, err
	}

	if !exists {
		return agentosplan.PlanCommandRef{}, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, command.PlanID)
	}

	command.AccountID = scope.AccountID
	command.ProjectID = scope.ProjectID

	return agentosplan.PlanCommandRefFromRecord(command), nil
}

func (r *AgentOSPlanRepo) existingPlanCommand(ctx context.Context, ref agentosplan.PlanCommandRef, command *agentosplan.PlanCommandRecord) (agentosplan.PlanCommandRecord, bool, error) {
	existing, exists, err := r.GetPlanCommand(ctx, ref)
	if err != nil || !exists {
		return agentosplan.PlanCommandRecord{}, false, err
	}

	if err := agentosplan.ValidatePlanCommandIdempotency(&existing, command); err != nil {
		return agentosplan.PlanCommandRecord{}, false, err
	}

	return existing, true, nil
}

func setPlanCommandRecordDefaults(command *agentosplan.PlanCommandRecord, ref agentosplan.PlanCommandRef) error {
	if command.CommandID == "" {
		command.CommandID = agentosplan.PlanCommandIDFromRef(ref)
	}

	status, err := agentosplan.NormalizeNewPlanCommandStatus(command.Status)
	if err != nil {
		return err
	}

	command.Status = status
	if command.CreatedAt.IsZero() {
		command.CreatedAt = time.Now().UTC()
	}

	if command.UpdatedAt.IsZero() {
		command.UpdatedAt = command.CreatedAt
	}

	if command.Payload == nil {
		command.Payload = map[string]any{}
	}

	return nil
}

func (r *AgentOSPlanRepo) insertPlanCommand(ctx context.Context, command *agentosplan.PlanCommandRecord, ref agentosplan.PlanCommandRef) (agentosplan.PlanCommandRecord, error) {
	payloadJSON, err := json.Marshal(command.Payload)
	if err != nil {
		return agentosplan.PlanCommandRecord{}, fmt.Errorf("AgentOSPlanRepo - RecordPlanCommand - marshal payload: %w", err)
	}

	err = r.queries.InsertPlanCommand(ctx, sqlcgen.InsertPlanCommandParams{
		CommandID:      command.CommandID,
		PlanID:         command.PlanID,
		ActorID:        command.ActorID,
		Action:         string(command.Action),
		IdempotencyKey: command.IdempotencyKey,
		PayloadJson:    payloadJSON,
		Status:         string(command.Status),
		FailureReason:  command.FailureReason,
		CreatedAt:      command.CreatedAt,
		UpdatedAt:      command.UpdatedAt,
		AccountID:      command.AccountID,
		ProjectID:      command.ProjectID,
	})
	if err != nil {
		return resolveUniqueInsertConflict(
			err,
			"AgentOSPlanRepo - RecordPlanCommand - insert",
			func() (agentosplan.PlanCommandRecord, bool, error) { return r.GetPlanCommand(ctx, ref) },
			func(existing *agentosplan.PlanCommandRecord) error {
				return agentosplan.ValidatePlanCommandIdempotency(existing, command)
			},
		)
	}

	return *command, nil
}

// GetPlanCommand loads the plan command identified by the given reference.
func (r *AgentOSPlanRepo) GetPlanCommand(ctx context.Context, ref agentosplan.PlanCommandRef) (agentosplan.PlanCommandRecord, bool, error) {
	if err := agentosplan.ValidatePlanCommandRef(ref); err != nil {
		return agentosplan.PlanCommandRecord{}, false, err
	}

	row, err := r.queries.GetPlanCommand(ctx, sqlcgen.GetPlanCommandParams{
		PlanID:         ref.PlanID,
		AccountID:      ref.AccountID,
		ProjectID:      ref.ProjectID,
		IdempotencyKey: ref.IdempotencyKey,
	})

	return getRecord("AgentOSPlanRepo - scanPlanCommand", row, err, planCommandRecordFromRow)
}

// ListRecoverablePlanCommands returns plan commands in recoverable states matching the given scope.
func (r *AgentOSPlanRepo) ListRecoverablePlanCommands(ctx context.Context, scope *agentosplan.PlanCommandScope) ([]agentosplan.PlanCommandRecord, error) {
	statuses, err := agentosplan.RecoverablePlanCommandStatuses(scope)
	if err != nil {
		return nil, err
	}

	statusValues := make([]string, 0, len(statuses))
	for i := range statuses {
		statusValues = append(statusValues, string(statuses[i]))
	}

	rows, err := r.queries.ListRecoverablePlanCommands(ctx, sqlcgen.ListRecoverablePlanCommandsParams{
		Statuses:  statusValues,
		PlanID:    scope.PlanID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
		Action:    string(scope.Action),
		RowLimit:  optionalInt8(scope.Limit),
	})

	return listRecords("AgentOSPlanRepo - ListRecoverablePlanCommands", rows, err, planCommandRecordFromRow)
}

// planCommandRecordFromRow maps the one plan_commands projection every command
// statement returns onto the domain record.
func planCommandRecordFromRow(row *sqlcgen.PlanCommand) (agentosplan.PlanCommandRecord, error) {
	command := agentosplan.PlanCommandRecord{
		CommandID:      row.CommandID,
		PlanID:         row.PlanID,
		AccountID:      row.AccountID,
		ProjectID:      row.ProjectID,
		ActorID:        row.ActorID,
		Action:         agentosplan.AuditAction(row.Action),
		IdempotencyKey: row.IdempotencyKey,
		Status:         agentosplan.PlanCommandStatus(row.Status),
		FailureReason:  row.FailureReason,
		CreatedAt:      row.CreatedAt,
		UpdatedAt:      row.UpdatedAt,
	}

	if len(row.PayloadJson) > 0 {
		if err := json.Unmarshal(row.PayloadJson, &command.Payload); err != nil {
			return agentosplan.PlanCommandRecord{}, fmt.Errorf("AgentOSPlanRepo - scanPlanCommand - decode payload: %w", err)
		}
	}

	return command, nil
}

// MarkPlanCommandDelivered transitions a plan command to the delivered state.
func (r *AgentOSPlanRepo) MarkPlanCommandDelivered(ctx context.Context, ref agentosplan.PlanCommandRef) (agentosplan.PlanCommandRecord, error) {
	return r.updatePlanCommandStatus(ctx, ref, agentosplan.PlanCommandDelivered, "")
}

// MarkPlanCommandFailed transitions a plan command to the failed state with the given reason.
func (r *AgentOSPlanRepo) MarkPlanCommandFailed(ctx context.Context, ref agentosplan.PlanCommandRef, reason string) (agentosplan.PlanCommandRecord, error) {
	return r.updatePlanCommandStatus(ctx, ref, agentosplan.PlanCommandFailed, reason)
}

func (r *AgentOSPlanRepo) updatePlanCommandStatus(ctx context.Context, ref agentosplan.PlanCommandRef, status agentosplan.PlanCommandStatus, reason string) (agentosplan.PlanCommandRecord, error) {
	if err := agentosplan.ValidatePlanCommandRef(ref); err != nil {
		return agentosplan.PlanCommandRecord{}, err
	}

	command, exists, err := r.GetPlanCommand(ctx, ref)
	if err != nil {
		return agentosplan.PlanCommandRecord{}, err
	}

	if !exists {
		return agentosplan.PlanCommandRecord{}, fmt.Errorf("%w: command %q", agentoscore.ErrInvalidRunPlan, ref.IdempotencyKey)
	}

	if err := agentosplan.ValidatePlanCommandStatusTransition(command.Status, status); err != nil {
		return agentosplan.PlanCommandRecord{}, err
	}

	if status == agentosplan.PlanCommandDelivered {
		if err := r.validatePlanCommandDeliveredAudit(ctx, &command); err != nil {
			return agentosplan.PlanCommandRecord{}, err
		}
	}

	command.Status = status
	command.FailureReason = reason
	command.UpdatedAt = time.Now().UTC()

	row, err := r.queries.UpdatePlanCommandStatus(ctx, sqlcgen.UpdatePlanCommandStatusParams{
		Status:         string(command.Status),
		FailureReason:  command.FailureReason,
		UpdatedAt:      command.UpdatedAt,
		PlanID:         ref.PlanID,
		AccountID:      ref.AccountID,
		ProjectID:      ref.ProjectID,
		IdempotencyKey: ref.IdempotencyKey,
	})
	if err != nil {
		return agentosplan.PlanCommandRecord{}, fmt.Errorf("AgentOSPlanRepo - scanPlanCommand: %w", err)
	}

	return planCommandRecordFromRow(&row)
}

func (r *AgentOSPlanRepo) validatePlanCommandDeliveredAudit(ctx context.Context, command *agentosplan.PlanCommandRecord) error {
	commandAudit := agentosplan.AuditRecordFromPlanCommand(command)
	ref := agentosplan.AuditRefFromRecord(&commandAudit)

	audit, exists, err := r.GetAuditRecord(ctx, ref)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("%w: delivered command requires durable audit %q", agentoscore.ErrInvalidRunPlan, command.IdempotencyKey)
	}

	return agentosplan.ValidatePlanCommandDeliveredAudit(command, &audit)
}

var (
	_ agentosplan.PlanIndex                 = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanRefStore              = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanTransitionStore       = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanStateStore            = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanEventStore            = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanMetricCheckpointStore = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanMetricsSink           = (*AgentOSPlanRepo)(nil)
	_ agentosplan.PlanCommandStore          = (*AgentOSPlanRepo)(nil)
	_ agentosplan.AuditStore                = (*AgentOSPlanRepo)(nil)
)
