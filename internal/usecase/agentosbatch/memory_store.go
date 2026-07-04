package agentosbatch

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"sync"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// MemoryStore is an explicit in-process workset store for unit tests and
// embedded demos.
type MemoryStore struct {
	mu         sync.RWMutex
	specs      map[string]agentos.WorksetSpec
	statuses   map[string]agentos.WorksetStatus
	startKeys  map[worksetStartKey]string
	statusKeys map[worksetStatusKey]agentos.WorksetStatus
	chunkKeys  map[worksetChunkKey]agentos.WorksetStatus
}

type worksetStartKey struct {
	AccountID      string
	ProjectID      string
	IdempotencyKey string
}

type worksetStatusKey struct {
	WorksetID      string
	IdempotencyKey string
}

type worksetChunkKey struct {
	WorksetID      string
	ChunkID        string
	IdempotencyKey string
}

// NewMemoryStore creates an empty in-memory workset store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		specs:      make(map[string]agentos.WorksetSpec),
		statuses:   make(map[string]agentos.WorksetStatus),
		startKeys:  make(map[worksetStartKey]string),
		statusKeys: make(map[worksetStatusKey]agentos.WorksetStatus),
		chunkKeys:  make(map[worksetChunkKey]agentos.WorksetStatus),
	}
}

// CreateWorkset creates a workset identity or returns an idempotent replay.
func (s *MemoryStore) CreateWorkset(
	_ context.Context,
	spec *agentos.WorksetSpec,
	status *agentos.WorksetStatus,
) (agentos.WorksetStatus, bool, error) {
	if err := validateCreateWorksetInput(spec, status); err != nil {
		return agentos.WorksetStatus{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := worksetStartKeyFromSpec(spec)
	if existing, found, err := s.lookupExistingWorksetStartLocked(key, spec); found || err != nil {
		return existing, false, err
	}

	if existing, found, err := s.lookupExistingWorksetIDLocked(spec); found || err != nil {
		return existing, false, err
	}

	preparedStatus := normalizeWorksetStatus(spec, status)
	s.specs[spec.WorksetID] = cloneWorksetSpec(spec)
	s.statuses[spec.WorksetID] = preparedStatus
	s.startKeys[key] = spec.WorksetID

	return cloneWorksetStatus(&preparedStatus), true, nil
}

// GetWorkset returns one tenant-scoped workset projection.
func (s *MemoryStore) GetWorkset(_ context.Context, ref agentos.WorksetRef) (agentos.WorksetSpec, agentos.WorksetStatus, bool, error) {
	if err := agentos.ValidateWorksetRef(ref); err != nil {
		return agentos.WorksetSpec{}, agentos.WorksetStatus{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, exists := s.specs[ref.WorksetID]
	if !exists {
		return agentos.WorksetSpec{}, agentos.WorksetStatus{}, false, nil
	}

	if err := validateWorksetTenantAccess(ref, &spec); err != nil {
		return agentos.WorksetSpec{}, agentos.WorksetStatus{}, false, err
	}

	status := s.statuses[ref.WorksetID]

	return cloneWorksetSpec(&spec), cloneWorksetStatus(&status), true, nil
}

// ListWorksets returns tenant-scoped workset projections.
func (s *MemoryStore) ListWorksets(_ context.Context, scope *agentos.WorksetScope) ([]agentos.WorksetStatus, error) {
	if err := agentos.ValidateWorksetScope(scope); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	statuses := make([]agentos.WorksetStatus, 0, len(s.statuses))
	for worksetID := range s.statuses {
		status := s.statuses[worksetID]
		if !worksetStatusMatchesScope(&status, scope) {
			continue
		}

		statuses = append(statuses, cloneWorksetStatus(&status))
	}

	sort.SliceStable(statuses, func(i, j int) bool {
		return statuses[i].WorksetID < statuses[j].WorksetID
	})

	if scope.Limit > 0 && len(statuses) > scope.Limit {
		statuses = statuses[:scope.Limit]
	}

	return statuses, nil
}

// ApplyChunkResult applies one chunk progress result idempotently.
func (s *MemoryStore) ApplyChunkResult(
	_ context.Context,
	ref agentos.WorksetRef,
	result *agentos.WorksetChunkResult,
	status *agentos.WorksetStatus,
) (agentos.WorksetStatus, error) {
	if err := validateApplyChunkInput(ref, result, status); err != nil {
		return agentos.WorksetStatus{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := worksetChunkKey{
		WorksetID:      ref.WorksetID,
		ChunkID:        result.ChunkID,
		IdempotencyKey: result.IdempotencyKey,
	}
	if existing, exists := s.chunkKeys[key]; exists {
		return cloneWorksetStatus(&existing), nil
	}

	spec, exists := s.specs[ref.WorksetID]
	if !exists {
		return agentos.WorksetStatus{}, fmt.Errorf("%w: workset %q not found", agentos.ErrInvalidWorksetScope, ref.WorksetID)
	}

	if err := validateWorksetTenantAccess(ref, &spec); err != nil {
		return agentos.WorksetStatus{}, err
	}

	prepared := normalizeWorksetStatus(&spec, status)
	s.statuses[ref.WorksetID] = prepared
	s.chunkKeys[key] = prepared

	return cloneWorksetStatus(&prepared), nil
}

// UpdateWorksetStatus updates latest workset projection idempotently.
func (s *MemoryStore) UpdateWorksetStatus(
	_ context.Context,
	status *agentos.WorksetStatus,
	idempotencyKey string,
) (agentos.WorksetStatus, error) {
	if err := validateUpdateWorksetStatusInput(status, idempotencyKey); err != nil {
		return agentos.WorksetStatus{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := worksetStatusKey{WorksetID: status.WorksetID, IdempotencyKey: idempotencyKey}
	if existing, exists := s.statusKeys[key]; exists {
		return cloneWorksetStatus(&existing), nil
	}

	spec, exists := s.specs[status.WorksetID]
	if !exists {
		return agentos.WorksetStatus{}, fmt.Errorf("%w: workset %q not found", agentos.ErrInvalidWorksetScope, status.WorksetID)
	}

	prepared := normalizeWorksetStatus(&spec, status)
	s.statuses[status.WorksetID] = prepared
	s.statusKeys[key] = prepared

	return cloneWorksetStatus(&prepared), nil
}

func validateCreateWorksetInput(spec *agentos.WorksetSpec, status *agentos.WorksetStatus) error {
	if err := agentos.ValidateWorksetSpec(spec); err != nil {
		return err
	}

	if status == nil {
		return fmt.Errorf("%w: workset status is required", agentos.ErrInvalidWorkset)
	}

	return nil
}

func validateApplyChunkInput(
	ref agentos.WorksetRef,
	result *agentos.WorksetChunkResult,
	status *agentos.WorksetStatus,
) error {
	if err := agentos.ValidateWorksetRef(ref); err != nil {
		return err
	}

	if err := validateChunkResult(result); err != nil {
		return err
	}

	return validateUpdateWorksetStatusInput(status, result.IdempotencyKey)
}

func validateUpdateWorksetStatusInput(status *agentos.WorksetStatus, idempotencyKey string) error {
	if status == nil {
		return fmt.Errorf("%w: workset status is required", agentos.ErrInvalidWorkset)
	}

	if idempotencyKey == "" {
		return fmt.Errorf("%w: status idempotency key is required", agentos.ErrInvalidWorkset)
	}

	if status.WorksetID == "" || status.AccountID == "" || status.ProjectID == "" {
		return fmt.Errorf("%w: workset status scope is required", agentos.ErrInvalidWorkset)
	}

	return nil
}

func (s *MemoryStore) lookupExistingWorksetStartLocked(
	key worksetStartKey,
	spec *agentos.WorksetSpec,
) (agentos.WorksetStatus, bool, error) {
	existingWorksetID, exists := s.startKeys[key]
	if !exists {
		return agentos.WorksetStatus{}, false, nil
	}

	existingSpec := s.specs[existingWorksetID]
	if err := ValidateWorksetStartIdempotency(&existingSpec, spec); err != nil {
		return agentos.WorksetStatus{}, true, err
	}

	status := s.statuses[existingWorksetID]

	return cloneWorksetStatus(&status), true, nil
}

func (s *MemoryStore) lookupExistingWorksetIDLocked(spec *agentos.WorksetSpec) (agentos.WorksetStatus, bool, error) {
	existingSpec, exists := s.specs[spec.WorksetID]
	if !exists {
		return agentos.WorksetStatus{}, false, nil
	}

	if err := ValidateWorksetStartIdempotency(&existingSpec, spec); err != nil {
		return agentos.WorksetStatus{}, true, err
	}

	status := s.statuses[spec.WorksetID]

	return cloneWorksetStatus(&status), true, nil
}

func worksetStartKeyFromSpec(spec *agentos.WorksetSpec) worksetStartKey {
	return worksetStartKey{
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		IdempotencyKey: spec.IdempotencyKey,
	}
}

func validateWorksetTenantAccess(ref agentos.WorksetRef, spec *agentos.WorksetSpec) error {
	if ref.AccountID != spec.AccountID || ref.ProjectID != spec.ProjectID {
		return fmt.Errorf("%w: workset %q is outside tenant scope", agentos.ErrInvalidWorksetScope, ref.WorksetID)
	}

	return nil
}

func normalizeWorksetStatus(spec *agentos.WorksetSpec, status *agentos.WorksetStatus) agentos.WorksetStatus {
	normalized := cloneWorksetStatus(status)
	normalized.WorksetID = spec.WorksetID
	normalized.AccountID = spec.AccountID
	normalized.ProjectID = spec.ProjectID
	normalized.ProcessID = spec.ProcessID
	normalized.Resource = spec.Resource
	normalized.Kind = spec.Kind

	return normalized
}

func worksetStatusMatchesScope(status *agentos.WorksetStatus, scope *agentos.WorksetScope) bool {
	if status.AccountID != scope.AccountID || status.ProjectID != scope.ProjectID {
		return false
	}

	if !worksetStatusMatchesIdentityScope(status, scope) {
		return false
	}

	return worksetStatusMatchesStateScope(status, scope)
}

func worksetStatusMatchesIdentityScope(status *agentos.WorksetStatus, scope *agentos.WorksetScope) bool {
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

func worksetStatusMatchesStateScope(status *agentos.WorksetStatus, scope *agentos.WorksetScope) bool {
	if scope.LifecycleState != "" && status.LifecycleState != scope.LifecycleState {
		return false
	}

	return true
}

func cloneWorksetSpec(spec *agentos.WorksetSpec) agentos.WorksetSpec {
	if spec == nil {
		return agentos.WorksetSpec{}
	}

	clone := *spec
	clone.ItemsRef = cloneWorksetItemsRef(&spec.ItemsRef)
	clone.Chunks = cloneWorksetChunks(spec.Chunks)
	clone.Metadata = maps.Clone(spec.Metadata)

	return clone
}

func cloneWorksetStatus(status *agentos.WorksetStatus) agentos.WorksetStatus {
	if status == nil {
		return agentos.WorksetStatus{}
	}

	clone := *status
	clone.Metadata = maps.Clone(status.Metadata)

	return clone
}

func cloneWorksetChunks(chunks []agentos.WorksetChunkSpec) []agentos.WorksetChunkSpec {
	if len(chunks) == 0 {
		return nil
	}

	clone := make([]agentos.WorksetChunkSpec, len(chunks))
	for i := range chunks {
		clone[i] = chunks[i]
		clone[i].ItemsRef = cloneWorksetItemsRef(&chunks[i].ItemsRef)
	}

	return clone
}

func cloneWorksetItemsRef(ref *agentos.WorksetItemsRef) agentos.WorksetItemsRef {
	if ref == nil {
		return agentos.WorksetItemsRef{}
	}

	clone := *ref
	clone.Metadata = maps.Clone(ref.Metadata)

	return clone
}
