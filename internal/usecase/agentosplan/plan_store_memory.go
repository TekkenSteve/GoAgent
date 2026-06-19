package agentosplan

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// MemoryPlanStore is an explicit in-process PlanIndex, PlanStateStore, and
// PlanEventStore for unit tests and embedded demos.
type MemoryPlanStore struct {
	mu        sync.RWMutex
	specs     map[string]agentos.RunPlanSpec
	statuses  map[string]agentos.RunPlanStatus
	planKeys  map[string]string
	events    map[string][]agentos.PlanEvent
	eventKeys map[planEventIdempotencyKey]agentos.PlanEvent
	commands  map[string]PlanCommandRecord
	auditKeys map[string]AuditRecord
	metrics   map[planMetricCheckpointKey]PlanMetricCheckpoint
}

type planEventIdempotencyKey struct {
	PlanID         string
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
		specs:     make(map[string]agentos.RunPlanSpec),
		statuses:  make(map[string]agentos.RunPlanStatus),
		planKeys:  make(map[string]string),
		events:    make(map[string][]agentos.PlanEvent),
		eventKeys: make(map[planEventIdempotencyKey]agentos.PlanEvent),
		commands:  make(map[string]PlanCommandRecord),
		auditKeys: make(map[string]AuditRecord),
		metrics:   make(map[planMetricCheckpointKey]PlanMetricCheckpoint),
	}
}

func (s *MemoryPlanStore) CreatePlan(ctx context.Context, spec agentos.RunPlanSpec, status agentos.RunPlanStatus) (agentos.RunPlanStatus, bool, error) {
	if spec.PlanID == "" {
		return agentos.RunPlanStatus{}, false, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if spec.IdempotencyKey == "" {
		return agentos.RunPlanStatus{}, false, fmt.Errorf("%w: plan idempotency key is required", agentos.ErrInvalidRunPlan)
	}
	s.mu.RLock()
	if existingPlanID, exists := s.planKeys[spec.IdempotencyKey]; exists {
		existingSpec := s.specs[existingPlanID]
		existingStatus := s.statuses[existingPlanID]
		s.mu.RUnlock()
		if err := ValidatePlanStartIdempotency(existingSpec, spec); err != nil {
			return agentos.RunPlanStatus{}, false, err
		}

		return existingStatus, false, nil
	}
	existingSpec, exists := s.specs[spec.PlanID]
	existingStatus := s.statuses[spec.PlanID]
	s.mu.RUnlock()
	if exists {
		if err := ValidatePlanStartIdempotency(existingSpec, spec); err != nil {
			return agentos.RunPlanStatus{}, false, err
		}

		return existingStatus, false, nil
	}

	snapshot := PlanStateSnapshot{
		Spec:           spec,
		Status:         status,
		IdempotencyKey: spec.IdempotencyKey,
	}
	if err := s.SavePlanState(ctx, snapshot); err != nil {
		return agentos.RunPlanStatus{}, false, err
	}

	return status, true, nil
}

func (s *MemoryPlanStore) GetPlan(_ context.Context, planID string) (agentos.RunPlanSpec, agentos.RunPlanStatus, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, ok := s.specs[planID]
	if !ok {
		return agentos.RunPlanSpec{}, agentos.RunPlanStatus{}, false, nil
	}

	return spec, s.statuses[planID], true, nil
}

func (s *MemoryPlanStore) ListPlanRefs(_ context.Context, scope PlanRefScope) ([]agentos.PlanRef, error) {
	if scope.Limit < 0 {
		return nil, fmt.Errorf("%w: plan ref limit must be non-negative", agentos.ErrInvalidPlanScope)
	}
	lifecycleStates := make(map[string]struct{}, len(scope.LifecycleStates))
	for _, state := range scope.LifecycleStates {
		if state == "" {
			return nil, fmt.Errorf("%w: lifecycle state is required", agentos.ErrInvalidPlanScope)
		}
		lifecycleStates[state] = struct{}{}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	refs := make([]agentos.PlanRef, 0, len(s.specs))
	for planID, spec := range s.specs {
		status := s.statuses[planID]
		if scope.AccountID != "" && spec.AccountID != scope.AccountID {
			continue
		}
		if scope.ProjectID != "" && spec.ProjectID != scope.ProjectID {
			continue
		}
		if len(lifecycleStates) > 0 {
			if _, ok := lifecycleStates[status.LifecycleState]; !ok {
				continue
			}
		}
		if !scope.UpdatedAfter.IsZero() && !status.UpdatedAt.After(scope.UpdatedAfter) {
			continue
		}
		refs = append(refs, agentos.PlanRef{
			PlanID:    spec.PlanID,
			AccountID: spec.AccountID,
			ProjectID: spec.ProjectID,
		})
	}
	sort.SliceStable(refs, func(i, j int) bool {
		left := s.statuses[refs[i].PlanID]
		right := s.statuses[refs[j].PlanID]
		if !left.UpdatedAt.Equal(right.UpdatedAt) {
			return left.UpdatedAt.Before(right.UpdatedAt)
		}

		return refs[i].PlanID < refs[j].PlanID
	})
	if scope.Limit > 0 && len(refs) > scope.Limit {
		refs = refs[:scope.Limit]
	}

	return refs, nil
}

func (s *MemoryPlanStore) UpdatePlanStatus(_ context.Context, status agentos.RunPlanStatus, _ string) error {
	if status.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.specs[status.PlanID]; !ok {
		return fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, status.PlanID)
	}
	s.statuses[status.PlanID] = status

	return nil
}

func (s *MemoryPlanStore) SavePlanState(_ context.Context, snapshot PlanStateSnapshot) error {
	if snapshot.Spec.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if snapshot.Spec.IdempotencyKey == "" {
		return fmt.Errorf("%w: plan idempotency key is required", agentos.ErrInvalidRunPlan)
	}
	if snapshot.Status.PlanID == "" {
		snapshot.Status.PlanID = snapshot.Spec.PlanID
	}
	if snapshot.Status.UpdatedAt.IsZero() {
		snapshot.Status.UpdatedAt = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existingSpec, ok := s.specs[snapshot.Spec.PlanID]; ok {
		if err := ValidatePlanStateIdentity(existingSpec, snapshot.Spec); err != nil {
			return err
		}
	}
	if existingPlanID, ok := s.planKeys[snapshot.Spec.IdempotencyKey]; ok && existingPlanID != snapshot.Spec.PlanID {
		return fmt.Errorf("%w: plan idempotency key belongs to plan %q", agentos.ErrInvalidRunPlan, existingPlanID)
	}
	s.specs[snapshot.Spec.PlanID] = snapshot.Spec
	s.statuses[snapshot.Spec.PlanID] = snapshot.Status
	s.planKeys[snapshot.Spec.IdempotencyKey] = snapshot.Spec.PlanID

	return nil
}

func (s *MemoryPlanStore) LoadPlanState(_ context.Context, planID string) (PlanStateSnapshot, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, ok := s.specs[planID]
	if !ok {
		return PlanStateSnapshot{}, false, nil
	}

	return PlanStateSnapshot{
		Spec:           spec,
		Status:         s.statuses[planID],
		IdempotencyKey: spec.IdempotencyKey,
	}, true, nil
}

func (s *MemoryPlanStore) AppendPlanEvent(_ context.Context, event agentos.PlanEvent, idempotencyKey string) (agentos.PlanEvent, error) {
	if event.PlanID == "" {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidPlanEvent)
	}
	if idempotencyKey == "" {
		return agentos.PlanEvent{}, fmt.Errorf("%w: plan event idempotency key is required", agentos.ErrInvalidPlanEvent)
	}
	requestedEvent := NormalizePlanEventAppendRequest(event)
	event = requestedEvent

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.specs[event.PlanID]; !ok {
		return agentos.PlanEvent{}, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, event.PlanID)
	}
	key := planEventIdempotencyKey{PlanID: event.PlanID, IdempotencyKey: idempotencyKey}
	if existing, ok := s.eventKeys[key]; ok {
		if err := ValidatePlanEventIdempotency(existing, requestedEvent); err != nil {
			return agentos.PlanEvent{}, err
		}

		return existing, nil
	}
	event.Sequence = int64(len(s.events[event.PlanID]) + 1)
	event.EventID = fmt.Sprintf("%s:%d", event.PlanID, event.Sequence)
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	s.events[event.PlanID] = append(s.events[event.PlanID], event)
	s.eventKeys[key] = event

	return event, nil
}

func (s *MemoryPlanStore) ListPlanEvents(_ context.Context, scope agentos.PlanStreamScope, limit int) ([]agentos.PlanEvent, error) {
	if scope.PlanID == "" {
		return nil, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidStreamScope)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if scope.AccountID != "" {
		spec, ok := s.specs[scope.PlanID]
		if !ok {
			return nil, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, scope.PlanID)
		}
		if err := ValidatePlanTenantAccess(agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}, spec); err != nil {
			return nil, err
		}
	}
	events := append([]agentos.PlanEvent(nil), s.events[scope.PlanID]...)
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Sequence < events[j].Sequence
	})

	filtered := make([]agentos.PlanEvent, 0, len(events))
	for _, event := range events {
		if event.Sequence <= scope.AfterSequence {
			continue
		}
		if scope.NodeID != "" && event.NodeID != scope.NodeID {
			continue
		}
		if scope.RunID != "" && event.RunID != scope.RunID {
			continue
		}
		filtered = append(filtered, event)
		if limit > 0 && len(filtered) >= limit {
			break
		}
	}

	return filtered, nil
}

func (s *MemoryPlanStore) GetPlanMetricCheckpoint(_ context.Context, exporterID string, ref agentos.PlanRef) (PlanMetricCheckpoint, bool, error) {
	if exporterID == "" {
		return PlanMetricCheckpoint{}, false, fmt.Errorf("%w: metrics exporter id is required", agentos.ErrInvalidRunPlan)
	}
	if err := ValidatePlanRef(ref); err != nil {
		return PlanMetricCheckpoint{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	checkpoint, ok := s.metrics[planMetricCheckpointKeyFromRef(exporterID, ref)]

	return checkpoint, ok, nil
}

func (s *MemoryPlanStore) SavePlanMetricCheckpoint(_ context.Context, checkpoint PlanMetricCheckpoint) error {
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
		return fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, checkpoint.PlanID)
	}
	if spec.AccountID != checkpoint.AccountID || spec.ProjectID != checkpoint.ProjectID {
		return fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, checkpoint.PlanID)
	}
	key := planMetricCheckpointKeyFromRef(checkpoint.ExporterID, ref)
	if existing, ok := s.metrics[key]; ok {
		if checkpoint.Sequence < existing.Sequence {
			return fmt.Errorf("%w: metrics checkpoint sequence moved backward from %d to %d", agentos.ErrInvalidRunPlan, existing.Sequence, checkpoint.Sequence)
		}
	}
	if checkpoint.UpdatedAt.IsZero() {
		checkpoint.UpdatedAt = time.Now().UTC()
	}
	s.metrics[key] = checkpoint

	return nil
}

func (s *MemoryPlanStore) RecordAudit(_ context.Context, record AuditRecord) (AuditRecord, bool, error) {
	if record.PlanID == "" {
		return AuditRecord{}, false, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if record.Action == "" {
		return AuditRecord{}, false, fmt.Errorf("%w: audit action is required", agentos.ErrInvalidRunPlan)
	}
	if record.IdempotencyKey == "" {
		return AuditRecord{}, false, fmt.Errorf("%w: audit idempotency key is required", agentos.ErrInvalidRunPlan)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	spec, ok := s.specs[record.PlanID]
	if !ok {
		return AuditRecord{}, false, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, record.PlanID)
	}
	record.AccountID = spec.AccountID
	record.ProjectID = spec.ProjectID
	if existing, ok := s.auditKeys[record.IdempotencyKey]; ok {
		if err := ValidateAuditIdempotency(existing, record); err != nil {
			return AuditRecord{}, false, err
		}

		return existing, false, nil
	}
	if record.AuditID == "" {
		record.AuditID = AuditIDFromIdempotencyKey(record.IdempotencyKey)
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	if record.Payload == nil {
		record.Payload = map[string]any{}
	}
	s.auditKeys[record.IdempotencyKey] = record

	return record, true, nil
}

func (s *MemoryPlanStore) RecordPlanCommand(_ context.Context, command PlanCommandRecord) (PlanCommandRecord, bool, error) {
	if command.PlanID == "" {
		return PlanCommandRecord{}, false, fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if command.Action == "" {
		return PlanCommandRecord{}, false, fmt.Errorf("%w: command action is required", agentos.ErrInvalidRunPlan)
	}
	if command.IdempotencyKey == "" {
		return PlanCommandRecord{}, false, fmt.Errorf("%w: command idempotency key is required", agentos.ErrInvalidRunPlan)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	spec, ok := s.specs[command.PlanID]
	if !ok {
		return PlanCommandRecord{}, false, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, command.PlanID)
	}
	command.AccountID = spec.AccountID
	command.ProjectID = spec.ProjectID
	if existing, ok := s.commands[command.IdempotencyKey]; ok {
		if err := ValidatePlanCommandIdempotency(existing, command); err != nil {
			return PlanCommandRecord{}, false, err
		}

		return existing, false, nil
	}
	if command.CommandID == "" {
		command.CommandID = PlanCommandIDFromIdempotencyKey(command.IdempotencyKey)
	}
	if command.Status == "" {
		command.Status = PlanCommandPending
	}
	if command.CreatedAt.IsZero() {
		command.CreatedAt = time.Now().UTC()
	}
	if command.UpdatedAt.IsZero() {
		command.UpdatedAt = command.CreatedAt
	}
	if command.Payload == nil {
		command.Payload = map[string]any{}
	}
	s.commands[command.IdempotencyKey] = command

	return command, true, nil
}

func (s *MemoryPlanStore) GetPlanCommand(_ context.Context, idempotencyKey string) (PlanCommandRecord, bool, error) {
	if idempotencyKey == "" {
		return PlanCommandRecord{}, false, fmt.Errorf("%w: command idempotency key is required", agentos.ErrInvalidRunPlan)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	command, ok := s.commands[idempotencyKey]

	return command, ok, nil
}

func (s *MemoryPlanStore) ListRecoverablePlanCommands(_ context.Context, scope PlanCommandScope) ([]PlanCommandRecord, error) {
	statuses, err := RecoverablePlanCommandStatuses(scope)
	if err != nil {
		return nil, err
	}
	statusSet := make(map[PlanCommandStatus]struct{}, len(statuses))
	for _, status := range statuses {
		statusSet[status] = struct{}{}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	commands := make([]PlanCommandRecord, 0, len(s.commands))
	for _, command := range s.commands {
		if scope.PlanID != "" && command.PlanID != scope.PlanID {
			continue
		}
		if scope.Action != "" && command.Action != scope.Action {
			continue
		}
		if _, ok := statusSet[command.Status]; !ok {
			continue
		}
		commands = append(commands, command)
	}
	sort.SliceStable(commands, func(i, j int) bool {
		if !commands[i].UpdatedAt.Equal(commands[j].UpdatedAt) {
			return commands[i].UpdatedAt.Before(commands[j].UpdatedAt)
		}

		return commands[i].CommandID < commands[j].CommandID
	})
	if scope.Limit > 0 && len(commands) > scope.Limit {
		commands = commands[:scope.Limit]
	}

	return commands, nil
}

func (s *MemoryPlanStore) MarkPlanCommandDelivered(_ context.Context, idempotencyKey string) (PlanCommandRecord, error) {
	return s.updatePlanCommandStatus(idempotencyKey, PlanCommandDelivered, "")
}

func (s *MemoryPlanStore) MarkPlanCommandFailed(_ context.Context, idempotencyKey string, reason string) (PlanCommandRecord, error) {
	return s.updatePlanCommandStatus(idempotencyKey, PlanCommandFailed, reason)
}

func (s *MemoryPlanStore) updatePlanCommandStatus(idempotencyKey string, status PlanCommandStatus, reason string) (PlanCommandRecord, error) {
	if idempotencyKey == "" {
		return PlanCommandRecord{}, fmt.Errorf("%w: command idempotency key is required", agentos.ErrInvalidRunPlan)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	command, ok := s.commands[idempotencyKey]
	if !ok {
		return PlanCommandRecord{}, fmt.Errorf("%w: command %q", agentos.ErrInvalidRunPlan, idempotencyKey)
	}
	command.Status = status
	command.FailureReason = reason
	command.UpdatedAt = time.Now().UTC()
	s.commands[idempotencyKey] = command

	return command, nil
}

func (s *MemoryPlanStore) GetAuditRecord(_ context.Context, idempotencyKey string) (AuditRecord, bool, error) {
	if idempotencyKey == "" {
		return AuditRecord{}, false, fmt.Errorf("%w: audit idempotency key is required", agentos.ErrInvalidRunPlan)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.auditKeys[idempotencyKey]

	return record, ok, nil
}

func (s *MemoryPlanStore) ListAuditRecords(_ context.Context, scope agentos.PlanAuditScope) ([]agentos.PlanAuditRecord, error) {
	if err := ValidatePlanAuditScope(scope); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	spec, ok := s.specs[scope.PlanID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", agentos.ErrPlanRouteNotFound, scope.PlanID)
	}
	if err := ValidatePlanTenantAccess(agentos.PlanRef{PlanID: scope.PlanID, AccountID: scope.AccountID, ProjectID: scope.ProjectID}, spec); err != nil {
		return nil, err
	}

	records := make([]AuditRecord, 0, len(s.auditKeys))
	for _, record := range s.auditKeys {
		if record.PlanID != scope.PlanID {
			continue
		}
		if scope.NodeID != "" && record.NodeID != scope.NodeID {
			continue
		}
		if scope.RunID != "" && record.RunID != scope.RunID {
			continue
		}
		if scope.Action != "" && string(record.Action) != string(scope.Action) {
			continue
		}
		records = append(records, record)
	}
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].AuditID < records[j].AuditID
		}

		return records[i].CreatedAt.Before(records[j].CreatedAt)
	})
	if scope.Limit > 0 && len(records) > scope.Limit {
		records = records[:scope.Limit]
	}

	audits := make([]agentos.PlanAuditRecord, 0, len(records))
	for _, record := range records {
		audits = append(audits, PlanAuditRecordFromAuditRecord(record))
	}

	return audits, nil
}

var (
	_ PlanIndex                 = (*MemoryPlanStore)(nil)
	_ PlanRefStore              = (*MemoryPlanStore)(nil)
	_ PlanStateStore            = (*MemoryPlanStore)(nil)
	_ PlanEventStore            = (*MemoryPlanStore)(nil)
	_ PlanMetricCheckpointStore = (*MemoryPlanStore)(nil)
	_ PlanCommandStore          = (*MemoryPlanStore)(nil)
	_ AuditStore                = (*MemoryPlanStore)(nil)
)
