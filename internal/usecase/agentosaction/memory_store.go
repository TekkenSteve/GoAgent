package agentosaction

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"sync"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
)

// MemoryStore is an explicit in-process action store for unit tests and
// embedded demos.
type MemoryStore struct {
	mu         sync.RWMutex
	specs      map[string]agentos.GovernedActionSpec
	statuses   map[string]agentos.GovernedActionStatus
	startKeys  map[actionStartKey]string
	statusKeys map[actionStatusKey]agentos.GovernedActionStatus
}

type actionStartKey struct {
	AccountID      string
	ProjectID      string
	IdempotencyKey string
}

type actionStatusKey struct {
	ActionID       string
	IdempotencyKey string
}

// NewMemoryStore creates an empty in-memory action store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		specs:      make(map[string]agentos.GovernedActionSpec),
		statuses:   make(map[string]agentos.GovernedActionStatus),
		startKeys:  make(map[actionStartKey]string),
		statusKeys: make(map[actionStatusKey]agentos.GovernedActionStatus),
	}
}

// CreateAction creates a governed action identity or returns an idempotent
// replay of the original status.
func (s *MemoryStore) CreateAction(
	_ context.Context,
	spec *agentos.GovernedActionSpec,
	status *agentos.GovernedActionStatus,
) (agentos.GovernedActionStatus, bool, error) {
	if err := validateCreateActionInput(spec, status); err != nil {
		return agentos.GovernedActionStatus{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := actionStartKeyFromSpec(spec)
	if existing, found, err := s.lookupExistingActionStartLocked(key, spec); found || err != nil {
		return existing, false, err
	}

	if existing, found, err := s.lookupExistingActionIDLocked(spec); found || err != nil {
		return existing, false, err
	}

	preparedStatus := normalizeActionStatus(spec, status)
	s.specs[spec.ActionID] = cloneActionSpec(spec)
	s.statuses[spec.ActionID] = preparedStatus
	s.startKeys[key] = spec.ActionID

	return cloneActionStatus(&preparedStatus), true, nil
}

// GetAction returns one tenant-scoped action projection.
func (s *MemoryStore) GetAction(_ context.Context, ref agentos.ActionRef) (agentos.GovernedActionSpec, agentos.GovernedActionStatus, bool, error) {
	if err := agentos.ValidateActionRef(ref); err != nil {
		return agentos.GovernedActionSpec{}, agentos.GovernedActionStatus{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, exists := s.specs[ref.ActionID]
	if !exists {
		return agentos.GovernedActionSpec{}, agentos.GovernedActionStatus{}, false, nil
	}

	if err := validateActionTenantAccess(ref, &spec); err != nil {
		return agentos.GovernedActionSpec{}, agentos.GovernedActionStatus{}, false, err
	}

	status := s.statuses[ref.ActionID]

	return cloneActionSpec(&spec), cloneActionStatus(&status), true, nil
}

// ListActions returns tenant-scoped action projections.
func (s *MemoryStore) ListActions(_ context.Context, scope *agentos.ActionScope) ([]agentos.GovernedActionStatus, error) {
	if err := agentos.ValidateActionScope(scope); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	statuses := make([]agentos.GovernedActionStatus, 0, len(s.statuses))
	for actionID := range s.statuses {
		status := s.statuses[actionID]
		if !actionStatusMatchesScope(&status, scope) {
			continue
		}

		statuses = append(statuses, cloneActionStatus(&status))
	}

	sort.SliceStable(statuses, func(i, j int) bool {
		return statuses[i].ActionID < statuses[j].ActionID
	})

	if scope.Limit > 0 && len(statuses) > scope.Limit {
		statuses = statuses[:scope.Limit]
	}

	return statuses, nil
}

// UpdateActionStatus updates the latest action projection idempotently.
func (s *MemoryStore) UpdateActionStatus(
	_ context.Context,
	status *agentos.GovernedActionStatus,
	idempotencyKey string,
) (agentos.GovernedActionStatus, error) {
	if err := validateUpdateActionStatusInput(status, idempotencyKey); err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := actionStatusKey{ActionID: status.ActionID, IdempotencyKey: idempotencyKey}
	if existing, exists := s.statusKeys[key]; exists {
		return cloneActionStatus(&existing), nil
	}

	spec, exists := s.specs[status.ActionID]
	if !exists {
		return agentos.GovernedActionStatus{}, fmt.Errorf("%w: action %q not found", agentoscore.ErrInvalidGovernedActionScope, status.ActionID)
	}

	if err := validateActionStatusScope(status, &spec); err != nil {
		return agentos.GovernedActionStatus{}, err
	}

	prepared := normalizeActionStatus(&spec, status)
	s.statuses[status.ActionID] = prepared
	s.statusKeys[key] = prepared

	return cloneActionStatus(&prepared), nil
}

func validateCreateActionInput(spec *agentos.GovernedActionSpec, status *agentos.GovernedActionStatus) error {
	if err := agentos.ValidateGovernedActionSpec(spec); err != nil {
		return err
	}

	if status == nil {
		return fmt.Errorf("%w: action status is required", agentoscore.ErrInvalidGovernedAction)
	}

	return nil
}

func validateUpdateActionStatusInput(status *agentos.GovernedActionStatus, idempotencyKey string) error {
	if status == nil {
		return fmt.Errorf("%w: action status is required", agentoscore.ErrInvalidGovernedAction)
	}

	if idempotencyKey == "" {
		return fmt.Errorf("%w: status idempotency key is required", agentoscore.ErrInvalidGovernedAction)
	}

	if status.ActionID == "" || status.AccountID == "" || status.ProjectID == "" {
		return fmt.Errorf("%w: action status scope is required", agentoscore.ErrInvalidGovernedAction)
	}

	return nil
}

func (s *MemoryStore) lookupExistingActionStartLocked(
	key actionStartKey,
	spec *agentos.GovernedActionSpec,
) (agentos.GovernedActionStatus, bool, error) {
	existingActionID, exists := s.startKeys[key]
	if !exists {
		return agentos.GovernedActionStatus{}, false, nil
	}

	existingSpec := s.specs[existingActionID]
	if err := ValidateActionStartIdempotency(&existingSpec, spec); err != nil {
		return agentos.GovernedActionStatus{}, true, err
	}

	status := s.statuses[existingActionID]

	return cloneActionStatus(&status), true, nil
}

func (s *MemoryStore) lookupExistingActionIDLocked(spec *agentos.GovernedActionSpec) (agentos.GovernedActionStatus, bool, error) {
	existingSpec, exists := s.specs[spec.ActionID]
	if !exists {
		return agentos.GovernedActionStatus{}, false, nil
	}

	if err := ValidateActionStartIdempotency(&existingSpec, spec); err != nil {
		return agentos.GovernedActionStatus{}, true, err
	}

	status := s.statuses[spec.ActionID]

	return cloneActionStatus(&status), true, nil
}

func actionStartKeyFromSpec(spec *agentos.GovernedActionSpec) actionStartKey {
	return actionStartKey{
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		IdempotencyKey: spec.IdempotencyKey,
	}
}

func validateActionTenantAccess(ref agentos.ActionRef, spec *agentos.GovernedActionSpec) error {
	if ref.AccountID != spec.AccountID || ref.ProjectID != spec.ProjectID {
		return fmt.Errorf("%w: action %q is outside tenant scope", agentoscore.ErrInvalidGovernedActionScope, ref.ActionID)
	}

	return nil
}

func validateActionStatusScope(status *agentos.GovernedActionStatus, spec *agentos.GovernedActionSpec) error {
	if status.AccountID != spec.AccountID || status.ProjectID != spec.ProjectID {
		return fmt.Errorf("%w: action %q status is outside tenant scope", agentoscore.ErrInvalidGovernedActionScope, status.ActionID)
	}

	return nil
}

func normalizeActionStatus(
	spec *agentos.GovernedActionSpec,
	status *agentos.GovernedActionStatus,
) agentos.GovernedActionStatus {
	normalized := cloneActionStatus(status)
	normalized.ActionID = spec.ActionID
	normalized.AccountID = spec.AccountID
	normalized.ProjectID = spec.ProjectID
	normalized.ProcessID = spec.ProcessID
	normalized.Resource = spec.Resource
	normalized.Kind = spec.Kind

	return normalized
}

func actionStatusMatchesScope(status *agentos.GovernedActionStatus, scope *agentos.ActionScope) bool {
	if status.AccountID != scope.AccountID || status.ProjectID != scope.ProjectID {
		return false
	}

	if !actionStatusMatchesIdentityScope(status, scope) {
		return false
	}

	return actionStatusMatchesStateScope(status, scope)
}

func actionStatusMatchesIdentityScope(status *agentos.GovernedActionStatus, scope *agentos.ActionScope) bool {
	if scope.ProcessID != "" && status.ProcessID != scope.ProcessID {
		return false
	}

	if scope.Kind != "" && status.Kind != scope.Kind {
		return false
	}

	if scope.Resource.Kind != "" && status.Resource != scope.Resource {
		return false
	}

	return true
}

func actionStatusMatchesStateScope(status *agentos.GovernedActionStatus, scope *agentos.ActionScope) bool {
	if scope.LifecycleState != "" && status.LifecycleState != scope.LifecycleState {
		return false
	}

	return true
}

func cloneActionSpec(spec *agentos.GovernedActionSpec) agentos.GovernedActionSpec {
	if spec == nil {
		return agentos.GovernedActionSpec{}
	}

	clone := *spec
	clone.InputRefs = cloneLedgerDataRefs(spec.InputRefs)
	clone.Risk.EvidenceRefs = cloneLedgerDataRefs(spec.Risk.EvidenceRefs)
	clone.Metadata = maps.Clone(spec.Metadata)

	if spec.CompensationRef != nil {
		compensation := *spec.CompensationRef
		compensation.Metadata = maps.Clone(spec.CompensationRef.Metadata)
		clone.CompensationRef = &compensation
	}

	return clone
}

func cloneActionStatus(status *agentos.GovernedActionStatus) agentos.GovernedActionStatus {
	if status == nil {
		return agentos.GovernedActionStatus{}
	}

	clone := *status
	clone.Risk.EvidenceRefs = cloneLedgerDataRefs(status.Risk.EvidenceRefs)

	return clone
}

func cloneLedgerDataRefs(refs []agentos.LedgerDataRef) []agentos.LedgerDataRef {
	if len(refs) == 0 {
		return nil
	}

	clone := make([]agentos.LedgerDataRef, len(refs))
	for i := range refs {
		clone[i] = refs[i]
		clone[i].Metadata = maps.Clone(refs[i].Metadata)
	}

	return clone
}
