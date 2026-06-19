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
	eventKeys map[string]agentos.PlanEvent
	auditKeys map[string]AuditRecord
}

// NewMemoryPlanStore creates an empty in-memory plan store.
func NewMemoryPlanStore() *MemoryPlanStore {
	return &MemoryPlanStore{
		specs:     make(map[string]agentos.RunPlanSpec),
		statuses:  make(map[string]agentos.RunPlanStatus),
		planKeys:  make(map[string]string),
		events:    make(map[string][]agentos.PlanEvent),
		eventKeys: make(map[string]agentos.PlanEvent),
		auditKeys: make(map[string]AuditRecord),
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

func (s *MemoryPlanStore) UpdatePlanStatus(_ context.Context, status agentos.RunPlanStatus, _ string) error {
	if status.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[status.PlanID] = status

	return nil
}

func (s *MemoryPlanStore) SavePlanState(_ context.Context, snapshot PlanStateSnapshot) error {
	if snapshot.Spec.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", agentos.ErrInvalidRunPlan)
	}
	if snapshot.Status.PlanID == "" {
		snapshot.Status.PlanID = snapshot.Spec.PlanID
	}
	if snapshot.Status.UpdatedAt.IsZero() {
		snapshot.Status.UpdatedAt = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existingSpec, ok := s.specs[snapshot.Spec.PlanID]; ok && existingSpec.IdempotencyKey != "" && snapshot.Spec.IdempotencyKey != "" {
		if err := ValidatePlanStartIdempotency(existingSpec, snapshot.Spec); err != nil {
			return err
		}
	}
	if existingPlanID, ok := s.planKeys[snapshot.Spec.IdempotencyKey]; snapshot.Spec.IdempotencyKey != "" && ok && existingPlanID != snapshot.Spec.PlanID {
		return fmt.Errorf("%w: plan idempotency key belongs to plan %q", agentos.ErrInvalidRunPlan, existingPlanID)
	}
	s.specs[snapshot.Spec.PlanID] = snapshot.Spec
	s.statuses[snapshot.Spec.PlanID] = snapshot.Status
	if snapshot.Spec.IdempotencyKey != "" {
		s.planKeys[snapshot.Spec.IdempotencyKey] = snapshot.Spec.PlanID
	}

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

	s.mu.Lock()
	defer s.mu.Unlock()
	if idempotencyKey != "" {
		if existing, ok := s.eventKeys[idempotencyKey]; ok {
			return existing, nil
		}
	}
	if event.Sequence == 0 {
		event.Sequence = int64(len(s.events[event.PlanID]) + 1)
	}
	if event.EventID == "" {
		event.EventID = fmt.Sprintf("%s:%d", event.PlanID, event.Sequence)
	}
	s.events[event.PlanID] = append(s.events[event.PlanID], event)
	if idempotencyKey != "" {
		s.eventKeys[idempotencyKey] = event
	}

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
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	if record.AuditID == "" {
		record.AuditID = record.PlanID + ":" + record.IdempotencyKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.auditKeys[record.IdempotencyKey]; ok {
		if err := ValidateAuditIdempotency(existing, record); err != nil {
			return AuditRecord{}, false, err
		}

		return existing, false, nil
	}
	s.auditKeys[record.IdempotencyKey] = record

	return record, true, nil
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

var (
	_ PlanIndex      = (*MemoryPlanStore)(nil)
	_ PlanStateStore = (*MemoryPlanStore)(nil)
	_ PlanEventStore = (*MemoryPlanStore)(nil)
	_ AuditStore     = (*MemoryPlanStore)(nil)
)
