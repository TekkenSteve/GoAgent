package agentosplan

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// MemoryPlanStore is an explicit in-process PlanIndex, PlanTransitionStore,
// PlanStateStore, and PlanEventStore for unit tests and embedded demos.
type MemoryPlanStore struct {
	mu                  sync.RWMutex
	specs               map[string]agentos.RunPlanSpec
	statuses            map[string]agentos.RunPlanStatus
	planKeys            map[planStartKey]string
	events              map[string][]agentos.PlanEvent
	eventKeys           map[planEventIdempotencyKey]agentos.PlanEvent
	transitionSnapshots map[planEventIdempotencyKey]string
	commands            map[PlanCommandRef]PlanCommandRecord
	auditKeys           map[AuditRef]AuditRecord
	metrics             map[planMetricCheckpointKey]PlanMetricCheckpoint
}

type planEventIdempotencyKey struct {
	PlanID         string
	IdempotencyKey string
}

type planStartKey struct {
	AccountID      string
	ProjectID      string
	IdempotencyKey string
}

type planMetricCheckpointKey struct {
	ExporterID string
	PlanID     string
	AccountID  string
	ProjectID  string
}

func planMetricCheckpointKeyFromRef(exporterID string, ref agentos.PlanRef) planMetricCheckpointKey {
	return planMetricCheckpointKey{
		ExporterID: exporterID,
		PlanID:     ref.PlanID,
		AccountID:  ref.AccountID,
		ProjectID:  ref.ProjectID,
	}
}

// NewMemoryPlanStore creates an empty in-memory plan store.
func NewMemoryPlanStore() *MemoryPlanStore {
	return &MemoryPlanStore{
		specs:               make(map[string]agentos.RunPlanSpec),
		statuses:            make(map[string]agentos.RunPlanStatus),
		planKeys:            make(map[planStartKey]string),
		events:              make(map[string][]agentos.PlanEvent),
		eventKeys:           make(map[planEventIdempotencyKey]agentos.PlanEvent),
		transitionSnapshots: make(map[planEventIdempotencyKey]string),
		commands:            make(map[PlanCommandRef]PlanCommandRecord),
		auditKeys:           make(map[AuditRef]AuditRecord),
		metrics:             make(map[planMetricCheckpointKey]PlanMetricCheckpoint),
	}
}

func (s *MemoryPlanStore) lookupExistingPlan(key planStartKey, spec *agentos.RunPlanSpec) (agentos.RunPlanStatus, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lookupExistingPlanLocked(key, spec)
}

func (s *MemoryPlanStore) lookupExistingPlanLocked(key planStartKey, spec *agentos.RunPlanSpec) (agentos.RunPlanStatus, bool, error) {
	if existingPlanID, exists := s.planKeys[key]; exists {
		existingSpec := s.specs[existingPlanID]
		existingStatus := s.statuses[existingPlanID]

		if err := ValidatePlanStartIdempotency(&existingSpec, spec); err != nil {
			return agentos.RunPlanStatus{}, false, err
		}

		return cloneRunPlanStatus(&existingStatus), true, nil
	}

	return agentos.RunPlanStatus{}, false, nil
}

func (s *MemoryPlanStore) lookupExistingPlanIDLocked(spec *agentos.RunPlanSpec) (agentos.RunPlanStatus, bool, error) {
	existingSpec, exists := s.specs[spec.PlanID]
	if !exists {
		return agentos.RunPlanStatus{}, false, nil
	}

	if err := ValidatePlanStartIdempotency(&existingSpec, spec); err != nil {
		return agentos.RunPlanStatus{}, false, err
	}

	existingStatus := s.statuses[spec.PlanID]

	return cloneRunPlanStatus(&existingStatus), true, nil
}

func (s *MemoryPlanStore) CreatePlan(_ context.Context, spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus) (agentos.RunPlanStatus, bool, error) {
	if err := validateMemoryCreatePlanInput(spec); err != nil {
		return agentos.RunPlanStatus{}, false, err
	}

	key := planStartKey{
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		IdempotencyKey: spec.IdempotencyKey,
	}

	if status, found, err := s.lookupExistingPlan(key, spec); found || err != nil {
		return status, false, err
	}

	if status, found, err := s.lookupExistingPlanByID(spec); found || err != nil {
		return status, false, err
	}

	snapshot, err := prepareMemoryCreatePlanSnapshot(spec, status)
	if err != nil {
		return agentos.RunPlanStatus{}, false, err
	}

	return s.createPlanSnapshot(key, spec, &snapshot)
}

func prepareMemoryCreatePlanSnapshot(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus) (PlanStateSnapshot, error) {
	if status == nil {
		return PlanStateSnapshot{}, fmt.Errorf("%w: plan status is required", agentoscore.ErrInvalidRunPlan)
	}

	snapshot := PlanStateSnapshot{Spec: cloneRunPlanSpec(spec), Status: cloneRunPlanStatus(status)}

	return normalizeMemoryPlanStateSnapshot(&snapshot)
}

func (s *MemoryPlanStore) createPlanSnapshot(key planStartKey, spec *agentos.RunPlanSpec, snapshot *PlanStateSnapshot) (agentos.RunPlanStatus, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if status, found, err := s.lookupExistingPlanLocked(key, spec); found || err != nil {
		return cloneRunPlanStatus(&status), false, err
	}

	if status, found, err := s.lookupExistingPlanIDLocked(spec); found || err != nil {
		return status, false, err
	}

	if err := s.savePlanStateLocked(snapshot, true); err != nil {
		return agentos.RunPlanStatus{}, false, err
	}

	return cloneRunPlanStatus(&snapshot.Status), true, nil
}

func (s *MemoryPlanStore) lookupExistingPlanByID(spec *agentos.RunPlanSpec) (agentos.RunPlanStatus, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lookupExistingPlanIDLocked(spec)
}

func validateMemoryCreatePlanInput(spec *agentos.RunPlanSpec) error {
	if spec == nil {
		return fmt.Errorf("%w: run plan spec is required", agentoscore.ErrInvalidRunPlan)
	}

	if spec.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidRunPlan)
	}

	if spec.IdempotencyKey == "" {
		return fmt.Errorf("%w: plan idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	return nil
}

func (s *MemoryPlanStore) GetPlan(_ context.Context, planID string) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, ok := s.specs[planID]
	if !ok {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, nil
	}

	status := s.statuses[planID]

	return cloneRunPlanSpec(&spec), cloneRunPlanStatus(&status), true, nil
}

func (s *MemoryPlanStore) GetPlanByRef(_ context.Context, ref agentos.PlanRef) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error) {
	if err := ValidatePlanRef(ref); err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, ok := s.specs[ref.PlanID]
	if !ok {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, nil
	}

	if err := ValidatePlanTenantAccess(ref, &spec); err != nil {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, err
	}

	status := s.statuses[ref.PlanID]

	return cloneRunPlanSpec(&spec), cloneRunPlanStatus(&status), true, nil
}

func (s *MemoryPlanStore) ListPlanRefs(_ context.Context, scope *PlanRefScope) ([]agentos.PlanRef, error) {
	if scope == nil {
		scope = &PlanRefScope{}
	}

	if scope.Limit < 0 {
		return nil, fmt.Errorf("%w: plan ref limit must be non-negative", agentoscore.ErrInvalidPlanScope)
	}

	lifecycleStates, err := planLifecycleStateSet(scope.LifecycleStates)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	refs := make([]agentos.PlanRef, 0, len(s.specs))
	for planID := range s.specs {
		spec := s.specs[planID]
		status := s.statuses[planID]

		if !memoryPlanRefMatchesScope(&spec, &status, scope, lifecycleStates) {
			continue
		}

		refs = append(refs, agentos.PlanRef{
			PlanID:    spec.PlanID,
			AccountID: spec.AccountID,
			ProjectID: spec.ProjectID,
		})
	}

	sortMemoryPlanRefs(refs, s.statuses)

	if scope.Limit > 0 && len(refs) > scope.Limit {
		refs = refs[:scope.Limit]
	}

	return refs, nil
}

func planLifecycleStateSet(states []string) (map[string]struct{}, error) {
	lifecycleStates := make(map[string]struct{}, len(states))
	for _, state := range states {
		if state == "" {
			return nil, fmt.Errorf("%w: lifecycle state is required", agentoscore.ErrInvalidPlanScope)
		}

		lifecycleStates[state] = struct{}{}
	}

	return lifecycleStates, nil
}

func memoryPlanRefMatchesScope(spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus, scope *PlanRefScope, lifecycleStates map[string]struct{}) bool {
	if scope.AccountID != "" && spec.AccountID != scope.AccountID {
		return false
	}

	if scope.ProjectID != "" && spec.ProjectID != scope.ProjectID {
		return false
	}

	if len(lifecycleStates) > 0 {
		if _, ok := lifecycleStates[status.LifecycleState]; !ok {
			return false
		}
	}

	return scope.UpdatedAfter.IsZero() || status.UpdatedAt.After(scope.UpdatedAfter)
}

func sortMemoryPlanRefs(refs []agentos.PlanRef, statuses map[string]agentos.RunPlanStatus) {
	sort.SliceStable(refs, func(i, j int) bool {
		left := statuses[refs[i].PlanID]
		right := statuses[refs[j].PlanID]

		if !left.UpdatedAt.Equal(right.UpdatedAt) {
			return left.UpdatedAt.Before(right.UpdatedAt)
		}

		return refs[i].PlanID < refs[j].PlanID
	})
}

func (s *MemoryPlanStore) SavePlanState(_ context.Context, snapshot *PlanStateSnapshot) error {
	normalized, err := normalizeMemoryPlanStateSnapshot(snapshot)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.savePlanStateLocked(&normalized, false)
}

func normalizeMemoryPlanStateSnapshot(snapshot *PlanStateSnapshot) (PlanStateSnapshot, error) {
	if snapshot == nil {
		return PlanStateSnapshot{}, fmt.Errorf("%w: plan state snapshot is required", agentoscore.ErrInvalidRunPlan)
	}

	if err := ValidateRunPlanScope(&snapshot.Spec); err != nil {
		return PlanStateSnapshot{}, err
	}

	normalized := clonePlanStateSnapshot(snapshot)
	if snapshot.Spec.IdempotencyKey == "" {
		return PlanStateSnapshot{}, fmt.Errorf("%w: plan idempotency key is required", agentoscore.ErrInvalidRunPlan)
	}

	normalized.Spec.RequestedAt = NormalizeDurableTimestamp(normalized.Spec.RequestedAt)
	if normalized.Status.PlanID == "" {
		normalized.Status.PlanID = normalized.Spec.PlanID
	}

	if normalized.Status.UpdatedAt.IsZero() {
		normalized.Status.UpdatedAt = time.Now().UTC()
	}

	return normalized, nil
}

func (s *MemoryPlanStore) savePlanStateLocked(snapshot *PlanStateSnapshot, allowCreate bool) error {
	if existingSpec, ok := s.specs[snapshot.Spec.PlanID]; ok {
		if err := ValidatePlanStateIdentity(&existingSpec, &snapshot.Spec); err != nil {
			return err
		}
	} else if !allowCreate {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, snapshot.Spec.PlanID)
	}

	key := planStartKey{
		AccountID:      snapshot.Spec.AccountID,
		ProjectID:      snapshot.Spec.ProjectID,
		IdempotencyKey: snapshot.Spec.IdempotencyKey,
	}
	if existingPlanID, ok := s.planKeys[key]; ok && existingPlanID != snapshot.Spec.PlanID {
		return fmt.Errorf("%w: plan idempotency key belongs to plan %q", agentoscore.ErrInvalidRunPlan, existingPlanID)
	}

	s.specs[snapshot.Spec.PlanID] = cloneRunPlanSpec(&snapshot.Spec)
	s.statuses[snapshot.Spec.PlanID] = cloneRunPlanStatus(&snapshot.Status)
	s.planKeys[key] = snapshot.Spec.PlanID

	return nil
}

func (s *MemoryPlanStore) PersistPlanTransition(_ context.Context, snapshot *PlanStateSnapshot, event *agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	normalizedSnapshot, err := normalizeMemoryPlanStateSnapshot(snapshot)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	if err := validateMemoryPlanTransitionInput(&normalizedSnapshot, event, idempotencyKey); err != nil {
		return agentos.PlanEvent{}, err
	}

	transitionIdentity, err := NewPlanTransitionSnapshotIdentity(&normalizedSnapshot, idempotencyKey)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	requestedEvent := NormalizePlanEventAppendRequest(event)

	s.mu.Lock()
	defer s.mu.Unlock()

	requestedEvent, err = ScopePlanEventToSpec(&requestedEvent, &normalizedSnapshot.Spec)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	key := planEventIdempotencyKey{PlanID: requestedEvent.PlanID, IdempotencyKey: idempotencyKey}
	if existing, ok := s.eventKeys[key]; ok {
		if err := validateMemoryPlanTransitionReplay(&existing, &requestedEvent, s.transitionSnapshots[key], transitionIdentity); err != nil {
			return agentos.PlanEvent{}, err
		}

		return clonePlanEvent(&existing), nil
	}

	if err := s.savePlanStateLocked(&normalizedSnapshot, false); err != nil {
		return agentos.PlanEvent{}, err
	}

	stored, err := s.appendPlanEventLocked(&requestedEvent, idempotencyKey)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	s.transitionSnapshots[key] = transitionIdentity.Digest

	return stored, nil
}

func validateMemoryPlanTransitionInput(snapshot *PlanStateSnapshot, event *agentos.PlanEvent, idempotencyKey string) error {
	if event == nil {
		return fmt.Errorf("%w: plan event is required", agentoscore.ErrInvalidPlanEvent)
	}

	if event.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidPlanEvent)
	}

	if event.PlanID != snapshot.Spec.PlanID {
		return fmt.Errorf("%w: event plan %q does not match snapshot plan %q", agentoscore.ErrInvalidPlanEvent, event.PlanID, snapshot.Spec.PlanID)
	}

	if idempotencyKey == "" {
		return fmt.Errorf("%w: plan event idempotency key is required", agentoscore.ErrInvalidPlanEvent)
	}

	return nil
}

func validateMemoryPlanTransitionReplay(existing, requested *agentos.PlanEvent, existingDigest string, requestedIdentity PlanTransitionSnapshotIdentity) error {
	if err := ValidatePlanEventIdempotency(existing, requested); err != nil {
		return err
	}

	return ValidatePlanTransitionIdempotency(existingDigest, requestedIdentity)
}

func (s *MemoryPlanStore) LoadPlanState(_ context.Context, planID string) (PlanStateSnapshot, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, ok := s.specs[planID]
	if !ok {
		return PlanStateSnapshot{}, false, nil
	}

	return clonePlanStateSnapshot(&PlanStateSnapshot{
		Spec:   spec,
		Status: s.statuses[planID],
	}), true, nil
}

func (s *MemoryPlanStore) AppendPlanEvent(_ context.Context, event *agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	if event == nil {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan event is required", agentoscore.ErrInvalidPlanEvent)
	}

	if event.PlanID == "" {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan id is required", agentoscore.ErrInvalidPlanEvent)
	}

	if idempotencyKey == "" {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan event idempotency key is required", agentoscore.ErrInvalidPlanEvent)
	}

	requestedEvent := NormalizePlanEventAppendRequest(event)

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.appendPlanEventLocked(&requestedEvent, idempotencyKey)
}

func (s *MemoryPlanStore) appendPlanEventLocked(event *agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	spec, ok := s.specs[event.PlanID]
	if !ok {
		return agentos.PlanEvent{}, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, event.PlanID)
	}

	scoped, err := ScopePlanEventToSpec(event, &spec)
	if err != nil {
		return agentos.PlanEvent{}, err
	}

	key := planEventIdempotencyKey{PlanID: scoped.PlanID, IdempotencyKey: idempotencyKey}
	if existing, ok := s.eventKeys[key]; ok {
		if err := ValidatePlanEventIdempotency(&existing, &scoped); err != nil {
			return agentos.PlanEvent{}, err
		}

		return clonePlanEvent(&existing), nil
	}

	scoped.Sequence = int64(len(s.events[scoped.PlanID]) + 1)

	scoped.EventID = fmt.Sprintf("%s:%d", scoped.PlanID, scoped.Sequence)
	if scoped.Timestamp.IsZero() {
		scoped.Timestamp = time.Now().UTC()
	}

	scoped.Timestamp = NormalizeDurableTimestamp(scoped.Timestamp)
	stored := clonePlanEvent(&scoped)
	s.events[scoped.PlanID] = append(s.events[scoped.PlanID], stored)
	s.eventKeys[key] = stored

	return clonePlanEvent(&stored), nil
}

func (s *MemoryPlanStore) ListPlanEvents(_ context.Context, scope *agentos.PlanStreamScope, limit int) ([]agentos.PlanEvent, error) {
	if err := ValidatePlanStreamScope(scope); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, ok := s.specs[scope.PlanID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, scope.PlanID)
	}

	if err := ValidatePlanTenantAccess(agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}, &spec); err != nil {
		return nil, err
	}

	events := clonePlanEvents(s.events[scope.PlanID])
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Sequence < events[j].Sequence
	})

	return filterMemoryPlanEvents(events, scope, limit), nil
}

func filterMemoryPlanEvents(events []agentos.PlanEvent, scope *agentos.PlanStreamScope, limit int) []agentos.PlanEvent {
	filtered := make([]agentos.PlanEvent, 0, len(events))

	for i := range events {
		event := events[i]
		if !memoryPlanEventMatchesScope(&event, scope) {
			continue
		}

		filtered = append(filtered, clonePlanEvent(&event))
		if limit > 0 && len(filtered) >= limit {
			break
		}
	}

	return filtered
}

func memoryPlanEventMatchesScope(event *agentos.PlanEvent, scope *agentos.PlanStreamScope) bool {
	if event.Sequence <= scope.AfterSequence {
		return false
	}

	if scope.NodeID != "" && event.NodeID != scope.NodeID {
		return false
	}

	return scope.RunID == "" || event.RunID == scope.RunID
}

func (s *MemoryPlanStore) GetPlanMetricCheckpoint(_ context.Context, exporterID string, ref agentos.PlanRef) (PlanMetricCheckpoint, bool, error) {
	if exporterID == "" {
		return PlanMetricCheckpoint{}, false, fmt.Errorf("%w: metrics exporter id is required", agentoscore.ErrInvalidRunPlan)
	}

	if err := ValidatePlanRef(ref); err != nil {
		return PlanMetricCheckpoint{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	checkpoint, ok := s.metrics[planMetricCheckpointKeyFromRef(exporterID, ref)]

	return checkpoint, ok, nil
}

func (s *MemoryPlanStore) SavePlanMetricCheckpoint(_ context.Context, checkpoint *PlanMetricCheckpoint) error {
	if checkpoint == nil {
		return fmt.Errorf("%w: metrics checkpoint is required", agentoscore.ErrInvalidRunPlan)
	}

	ref := agentos.PlanRef{
		PlanID:    checkpoint.PlanID,
		AccountID: checkpoint.AccountID,
		ProjectID: checkpoint.ProjectID,
	}
	if err := ValidatePlanMetricCheckpointRef(checkpoint, checkpoint.ExporterID, ref); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	spec, ok := s.specs[checkpoint.PlanID]
	if !ok {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, checkpoint.PlanID)
	}

	if spec.AccountID != checkpoint.AccountID || spec.ProjectID != checkpoint.ProjectID {
		return fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, checkpoint.PlanID)
	}

	key := planMetricCheckpointKeyFromRef(checkpoint.ExporterID, ref)
	if existing, ok := s.metrics[key]; ok {
		if checkpoint.Sequence < existing.Sequence {
			return fmt.Errorf("%w: metrics checkpoint sequence moved backward from %d to %d", agentoscore.ErrInvalidRunPlan, existing.Sequence, checkpoint.Sequence)
		}
	}

	stored := *checkpoint
	if stored.UpdatedAt.IsZero() {
		stored.UpdatedAt = time.Now().UTC()
	}

	s.metrics[key] = stored

	return nil
}

func (s *MemoryPlanStore) RecordAudit(_ context.Context, record *AuditRecord) (AuditRecord, bool, error) {
	if err := validateMemoryAuditRecord(record); err != nil {
		return AuditRecord{}, false, err
	}

	stored := cloneAuditRecord(record)

	s.mu.Lock()
	defer s.mu.Unlock()

	spec, ok := s.specs[stored.PlanID]
	if !ok {
		return AuditRecord{}, false, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, stored.PlanID)
	}

	stored.AccountID = spec.AccountID
	stored.ProjectID = spec.ProjectID

	ref := AuditRefFromRecord(&stored)
	if existing, ok := s.auditKeys[ref]; ok {
		if err := ValidateAuditIdempotency(&existing, &stored); err != nil {
			return AuditRecord{}, false, err
		}

		return cloneAuditRecord(&existing), false, nil
	}

	status := s.statuses[stored.PlanID]
	if err := ValidateAuditNodeRunInPlan(&stored, &spec, &status); err != nil {
		return AuditRecord{}, false, err
	}

	prepareMemoryAuditRecord(&stored, ref)

	stored = cloneAuditRecord(&stored)
	s.auditKeys[ref] = stored

	return cloneAuditRecord(&stored), true, nil
}

func validateMemoryAuditRecord(record *AuditRecord) error {
	if record == nil {
		return fmt.Errorf("%w: audit record is required", agentoscore.ErrInvalidRunPlan)
	}

	required := []struct {
		value string
		label string
	}{
		{value: record.PlanID, label: "plan id"},
		{value: string(record.Action), label: "audit action"},
		{value: record.IdempotencyKey, label: "audit idempotency key"},
	}

	for _, field := range required {
		if field.value == "" {
			return fmt.Errorf("%w: %s is required", agentoscore.ErrInvalidRunPlan, field.label)
		}
	}

	return nil
}

func prepareMemoryAuditRecord(record *AuditRecord, ref AuditRef) {
	if record.AuditID == "" {
		record.AuditID = AuditIDFromRef(ref)
	}

	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}

	if record.Payload == nil {
		record.Payload = map[string]any{}
	}
}

func (s *MemoryPlanStore) RecordPlanCommand(_ context.Context, command *PlanCommandRecord) (PlanCommandRecord, bool, error) {
	if err := validateMemoryPlanCommandRecord(command); err != nil {
		return PlanCommandRecord{}, false, err
	}

	stored := clonePlanCommandRecord(command)

	s.mu.Lock()
	defer s.mu.Unlock()

	spec, ok := s.specs[stored.PlanID]
	if !ok {
		return PlanCommandRecord{}, false, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, stored.PlanID)
	}

	stored.AccountID = spec.AccountID
	stored.ProjectID = spec.ProjectID

	ref := PlanCommandRefFromRecord(&stored)
	if existing, ok := s.commands[ref]; ok {
		if err := ValidatePlanCommandIdempotency(&existing, &stored); err != nil {
			return PlanCommandRecord{}, false, err
		}

		return clonePlanCommandRecord(&existing), false, nil
	}

	if err := prepareMemoryPlanCommandRecord(&stored, ref); err != nil {
		return PlanCommandRecord{}, false, err
	}

	stored = clonePlanCommandRecord(&stored)
	s.commands[ref] = stored

	return clonePlanCommandRecord(&stored), true, nil
}

func validateMemoryPlanCommandRecord(command *PlanCommandRecord) error {
	if command == nil {
		return fmt.Errorf("%w: command record is required", agentoscore.ErrInvalidRunPlan)
	}

	required := []struct {
		value string
		label string
	}{
		{value: command.PlanID, label: "plan id"},
		{value: string(command.Action), label: "command action"},
		{value: command.IdempotencyKey, label: "command idempotency key"},
	}

	for _, field := range required {
		if field.value == "" {
			return fmt.Errorf("%w: %s is required", agentoscore.ErrInvalidRunPlan, field.label)
		}
	}

	return nil
}

func prepareMemoryPlanCommandRecord(command *PlanCommandRecord, ref PlanCommandRef) error {
	if command.CommandID == "" {
		command.CommandID = PlanCommandIDFromRef(ref)
	}

	status, err := NormalizeNewPlanCommandStatus(command.Status)
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

func (s *MemoryPlanStore) GetPlanCommand(_ context.Context, ref PlanCommandRef) (PlanCommandRecord, bool, error) {
	if err := ValidatePlanCommandRef(ref); err != nil {
		return PlanCommandRecord{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	command, ok := s.commands[ref]

	return clonePlanCommandRecord(&command), ok, nil
}

func (s *MemoryPlanStore) ListRecoverablePlanCommands(_ context.Context, scope *PlanCommandScope) ([]PlanCommandRecord, error) {
	statuses, err := RecoverablePlanCommandStatuses(scope)
	if err != nil {
		return nil, err
	}

	statusSet := make(map[PlanCommandStatus]struct{}, len(statuses))
	for i := range statuses {
		status := statuses[i]
		statusSet[status] = struct{}{}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	commands := make([]PlanCommandRecord, 0, len(s.commands))
	for key := range s.commands {
		command := s.commands[key]

		if !memoryPlanCommandMatchesScope(&command, scope, statusSet) {
			continue
		}

		commands = append(commands, clonePlanCommandRecord(&command))
	}

	sortPlanCommandRecords(commands)

	if scope.Limit > 0 && len(commands) > scope.Limit {
		commands = commands[:scope.Limit]
	}

	return commands, nil
}

func memoryPlanCommandMatchesScope(command *PlanCommandRecord, scope *PlanCommandScope, statusSet map[PlanCommandStatus]struct{}) bool {
	if scope.PlanID != "" && command.PlanID != scope.PlanID {
		return false
	}

	if scope.AccountID != "" && command.AccountID != scope.AccountID {
		return false
	}

	if scope.ProjectID != "" && command.ProjectID != scope.ProjectID {
		return false
	}

	if scope.Action != "" && command.Action != scope.Action {
		return false
	}

	_, ok := statusSet[command.Status]

	return ok
}

func sortPlanCommandRecords(commands []PlanCommandRecord) {
	sort.SliceStable(commands, func(i, j int) bool {
		if !commands[i].UpdatedAt.Equal(commands[j].UpdatedAt) {
			return commands[i].UpdatedAt.Before(commands[j].UpdatedAt)
		}

		return commands[i].CommandID < commands[j].CommandID
	})
}

func (s *MemoryPlanStore) MarkPlanCommandDelivered(_ context.Context, ref PlanCommandRef) (PlanCommandRecord, error) {
	return s.updatePlanCommandStatus(ref, PlanCommandDelivered, "")
}

func (s *MemoryPlanStore) MarkPlanCommandFailed(_ context.Context, ref PlanCommandRef, reason string) (PlanCommandRecord, error) {
	return s.updatePlanCommandStatus(ref, PlanCommandFailed, reason)
}

func (s *MemoryPlanStore) updatePlanCommandStatus(ref PlanCommandRef, status PlanCommandStatus, reason string) (PlanCommandRecord, error) {
	if err := ValidatePlanCommandRef(ref); err != nil {
		return PlanCommandRecord{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	command, ok := s.commands[ref]
	if !ok {
		return PlanCommandRecord{}, fmt.Errorf("%w: command %q", agentoscore.ErrInvalidRunPlan, ref.IdempotencyKey)
	}

	if err := ValidatePlanCommandStatusTransition(command.Status, status); err != nil {
		return PlanCommandRecord{}, err
	}

	if status == PlanCommandDelivered {
		commandAudit := AuditRecordFromPlanCommand(&command)
		audit, ok := s.auditKeys[AuditRefFromRecord(&commandAudit)]

		if !ok {
			return PlanCommandRecord{}, fmt.Errorf("%w: delivered command requires durable audit %q", agentoscore.ErrInvalidRunPlan, ref.IdempotencyKey)
		}

		if err := ValidatePlanCommandDeliveredAudit(&command, &audit); err != nil {
			return PlanCommandRecord{}, err
		}
	}

	command.Status = status
	command.FailureReason = reason
	command.UpdatedAt = time.Now().UTC()
	command = clonePlanCommandRecord(&command)
	s.commands[ref] = command

	return clonePlanCommandRecord(&command), nil
}

func (s *MemoryPlanStore) GetAuditRecord(_ context.Context, ref AuditRef) (AuditRecord, bool, error) {
	if err := ValidateAuditRef(ref); err != nil {
		return AuditRecord{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	record, ok := s.auditKeys[ref]

	return cloneAuditRecord(&record), ok, nil
}

func (s *MemoryPlanStore) ListAuditRecords(_ context.Context, scope *agentos.PlanAuditScope) ([]agentos.PlanAuditRecord, error) {
	if err := ValidatePlanAuditScope(scope); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, ok := s.specs[scope.PlanID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", agentoscore.ErrPlanRouteNotFound, scope.PlanID)
	}

	if err := ValidatePlanTenantAccess(agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}, &spec); err != nil {
		return nil, err
	}

	records := make([]AuditRecord, 0, len(s.auditKeys))
	for key := range s.auditKeys {
		record := s.auditKeys[key]

		if !memoryAuditRecordMatchesScope(&record, scope) {
			continue
		}

		records = append(records, cloneAuditRecord(&record))
	}

	sortAuditRecords(records)

	if scope.Limit > 0 && len(records) > scope.Limit {
		records = records[:scope.Limit]
	}

	audits := make([]agentos.PlanAuditRecord, 0, len(records))
	for i := range records {
		record := &records[i]
		audits = append(audits, PlanAuditRecordFromAuditRecord(record))
	}

	return audits, nil
}

func memoryAuditRecordMatchesScope(record *AuditRecord, scope *agentos.PlanAuditScope) bool {
	if record.PlanID != scope.PlanID {
		return false
	}

	if scope.NodeID != "" && record.NodeID != scope.NodeID {
		return false
	}

	if scope.RunID != "" && record.RunID != scope.RunID {
		return false
	}

	return scope.Action == "" || string(record.Action) == string(scope.Action)
}

func sortAuditRecords(records []AuditRecord) {
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].AuditID < records[j].AuditID
		}

		return records[i].CreatedAt.Before(records[j].CreatedAt)
	})
}

func clonePlanStateSnapshot(snapshot *PlanStateSnapshot) PlanStateSnapshot {
	if snapshot == nil {
		return PlanStateSnapshot{}
	}

	return PlanStateSnapshot{
		Spec:   cloneRunPlanSpec(&snapshot.Spec),
		Status: cloneRunPlanStatus(&snapshot.Status),
	}
}

func cloneRunPlanSpec(spec *agentos.RunPlanSpec) agentos.RunPlanSpec {
	if spec == nil {
		return agentos.RunPlanSpec{}
	}

	clone := *spec
	clone.Inputs = cloneMemoryAnyMap(spec.Inputs)
	clone.Metadata = cloneMemoryStringMap(spec.Metadata)
	clone.Nodes = clonePlanNodeSpecs(spec.Nodes)
	clone.Edges = clonePlanEdgeSpecs(spec.Edges)

	return clone
}

func clonePlanNodeSpecs(nodes []agentos.PlanNodeSpec) []agentos.PlanNodeSpec {
	if nodes == nil {
		return nil
	}

	clone := make([]agentos.PlanNodeSpec, len(nodes))
	for i := range nodes {
		node := nodes[i]
		node.Run = cloneRunSpec(&node.Run)
		node.Inputs = append([]agentos.InputMapping(nil), node.Inputs...)
		node.Outputs = append([]agentos.ArtifactSpec(nil), node.Outputs...)
		node.Conditions = append([]string(nil), node.Conditions...)
		clone[i] = node
	}

	return clone
}

func clonePlanEdgeSpecs(edges []agentos.PlanEdgeSpec) []agentos.PlanEdgeSpec {
	if edges == nil {
		return nil
	}

	clone := make([]agentos.PlanEdgeSpec, len(edges))
	for i := range edges {
		edge := edges[i]
		edge.InputMapping = append([]agentos.InputMapping(nil), edge.InputMapping...)
		clone[i] = edge
	}

	return clone
}

func cloneRunPlanStatus(status *agentos.RunPlanStatus) agentos.RunPlanStatus {
	if status == nil {
		return agentos.RunPlanStatus{}
	}

	clone := *status
	clone.Nodes = clonePlanNodeStatuses(status.Nodes)
	clone.ActiveRunIDs = append([]string(nil), status.ActiveRunIDs...)
	clone.Artifacts = cloneArtifactRefs(status.Artifacts)
	clone.Metadata = cloneMemoryStringMap(status.Metadata)

	return clone
}

func clonePlanNodeStatuses(nodes []agentos.PlanNodeStatus) []agentos.PlanNodeStatus {
	if nodes == nil {
		return nil
	}

	clone := make([]agentos.PlanNodeStatus, len(nodes))
	for i := range nodes {
		node := nodes[i]
		node.Artifacts = cloneArtifactRefs(node.Artifacts)
		clone[i] = node
	}

	return clone
}

func cloneRunSpec(spec *agentos.RunSpec) agentos.RunSpec {
	if spec == nil {
		return agentos.RunSpec{}
	}

	clone := *spec
	clone.Metadata = cloneMemoryStringMap(spec.Metadata)
	clone.Input = cloneMemoryAnyMap(spec.Input)

	return clone
}

func clonePlanEvents(events []agentos.PlanEvent) []agentos.PlanEvent {
	if events == nil {
		return nil
	}

	clone := make([]agentos.PlanEvent, len(events))
	for i := range events {
		clone[i] = clonePlanEvent(&events[i])
	}

	return clone
}

func clonePlanEvent(event *agentos.PlanEvent) agentos.PlanEvent {
	if event == nil {
		return agentos.PlanEvent{}
	}

	clone := *event
	clone.Payload = cloneMemoryAnyMap(event.Payload)

	return clone
}

func cloneAuditRecord(record *AuditRecord) AuditRecord {
	if record == nil {
		return AuditRecord{}
	}

	clone := *record
	clone.Payload = cloneMemoryAnyMap(record.Payload)

	return clone
}

func clonePlanCommandRecord(command *PlanCommandRecord) PlanCommandRecord {
	if command == nil {
		return PlanCommandRecord{}
	}

	clone := *command
	clone.Payload = cloneMemoryAnyMap(command.Payload)

	return clone
}

func cloneArtifactRefs(artifacts []agentoscore.ArtifactRef) []agentoscore.ArtifactRef {
	if artifacts == nil {
		return nil
	}

	clone := make([]agentoscore.ArtifactRef, len(artifacts))
	for i := range artifacts {
		artifact := artifacts[i]
		artifact.Metadata = cloneMemoryStringMap(artifact.Metadata)
		clone[i] = artifact
	}

	return clone
}

func cloneMemoryStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}

	clone := make(map[string]string, len(input))
	maps.Copy(clone, input)

	return clone
}

func cloneMemoryAnyMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}

	clone := make(map[string]any, len(input))
	for key, value := range input {
		clone[key] = cloneMemoryAnyValue(value)
	}

	return clone
}

func cloneMemoryAnyValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMemoryAnyMap(typed)
	case []any:
		return cloneMemoryAnySlice(typed)
	case map[string]string:
		return cloneMemoryStringMap(typed)
	case []string:
		return append([]string(nil), typed...)
	case []map[string]any:
		clone := make([]map[string]any, len(typed))
		for i := range typed {
			clone[i] = cloneMemoryAnyMap(typed[i])
		}

		return clone
	case []map[string]string:
		clone := make([]map[string]string, len(typed))
		for i := range typed {
			clone[i] = cloneMemoryStringMap(typed[i])
		}

		return clone
	default:
		return value
	}
}

func cloneMemoryAnySlice(input []any) []any {
	if input == nil {
		return nil
	}

	clone := make([]any, len(input))
	for i := range input {
		clone[i] = cloneMemoryAnyValue(input[i])
	}

	return clone
}

var (
	_ PlanIndex                 = (*MemoryPlanStore)(nil)
	_ PlanRefStore              = (*MemoryPlanStore)(nil)
	_ PlanTransitionStore       = (*MemoryPlanStore)(nil)
	_ PlanStateStore            = (*MemoryPlanStore)(nil)
	_ PlanEventStore            = (*MemoryPlanStore)(nil)
	_ PlanMetricCheckpointStore = (*MemoryPlanStore)(nil)
	_ PlanCommandStore          = (*MemoryPlanStore)(nil)
	_ AuditStore                = (*MemoryPlanStore)(nil)
)
