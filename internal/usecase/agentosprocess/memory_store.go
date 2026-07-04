package agentosprocess

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// MemoryStore is an explicit in-process ProcessIndex and ProcessEventStore for
// unit tests and embedded demos.
type MemoryStore struct {
	mu         sync.RWMutex
	specs      map[string]agentos.ProcessSpec
	statuses   map[string]agentos.ProcessStatus
	startKeys  map[processStartKey]string
	statusKeys map[processEventIdempotencyKey]agentos.ProcessStatus
	events     map[string][]agentos.ProcessEvent
	eventKeys  map[processEventIdempotencyKey]agentos.ProcessEvent
}

type processStartKey struct {
	AccountID      string
	ProjectID      string
	IdempotencyKey string
}

type processEventIdempotencyKey struct {
	ProcessID      string
	IdempotencyKey string
}

// NewMemoryStore creates an empty in-memory process store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		specs:      make(map[string]agentos.ProcessSpec),
		statuses:   make(map[string]agentos.ProcessStatus),
		startKeys:  make(map[processStartKey]string),
		statusKeys: make(map[processEventIdempotencyKey]agentos.ProcessStatus),
		events:     make(map[string][]agentos.ProcessEvent),
		eventKeys:  make(map[processEventIdempotencyKey]agentos.ProcessEvent),
	}
}

// CreateProcess creates a durable process identity or returns an idempotent
// replay of the original status.
func (s *MemoryStore) CreateProcess(
	_ context.Context,
	spec *agentos.ProcessSpec,
	status *agentos.ProcessStatus,
) (agentos.ProcessStatus, bool, error) {
	if err := validateCreateProcessInput(spec, status); err != nil {
		return agentos.ProcessStatus{}, false, err
	}

	key := processStartKeyFromSpec(spec)

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, found, err := s.lookupExistingProcessLocked(key, spec); found || err != nil {
		return existing, false, err
	}

	if existing, found, err := s.lookupExistingProcessIDLocked(spec); found || err != nil {
		return existing, false, err
	}

	preparedStatus := normalizeProcessStatus(spec, status)
	s.specs[spec.ProcessID] = cloneProcessSpec(spec)
	s.statuses[spec.ProcessID] = preparedStatus
	s.startKeys[key] = spec.ProcessID

	return cloneProcessStatus(&preparedStatus), true, nil
}

// GetProcessByRef returns a tenant-scoped process projection.
func (s *MemoryStore) GetProcessByRef(_ context.Context, ref agentos.ProcessRef) (agentos.ProcessSpec, agentos.ProcessStatus, bool, error) {
	if err := agentos.ValidateProcessRef(ref); err != nil {
		return agentos.ProcessSpec{}, agentos.ProcessStatus{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, ok := s.specs[ref.ProcessID]
	if !ok {
		return agentos.ProcessSpec{}, agentos.ProcessStatus{}, false, nil
	}

	if err := validateProcessTenantAccess(ref, &spec); err != nil {
		return agentos.ProcessSpec{}, agentos.ProcessStatus{}, false, err
	}

	status := s.statuses[ref.ProcessID]

	return cloneProcessSpec(&spec), cloneProcessStatus(&status), true, nil
}

// UpdateProcessStatus updates the latest durable process projection.
func (s *MemoryStore) UpdateProcessStatus(
	_ context.Context,
	status *agentos.ProcessStatus,
	idempotencyKey string,
) (agentos.ProcessStatus, error) {
	if err := validateUpdateProcessStatusInput(status, idempotencyKey); err != nil {
		return agentos.ProcessStatus{}, err
	}

	key := processEventIdempotencyKey{
		ProcessID:      status.ProcessID,
		IdempotencyKey: idempotencyKey,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, exists := s.statusKeys[key]; exists {
		return cloneProcessStatus(&existing), nil
	}

	spec, exists := s.specs[status.ProcessID]
	if !exists {
		return agentos.ProcessStatus{}, fmt.Errorf("%w: process %q not found", agentos.ErrProcessRouteNotFound, status.ProcessID)
	}

	if err := validateProcessStatusScope(status, &spec); err != nil {
		return agentos.ProcessStatus{}, err
	}

	prepared := normalizeProcessStatus(&spec, status)
	s.statuses[status.ProcessID] = prepared
	s.statusKeys[key] = prepared

	return cloneProcessStatus(&prepared), nil
}

// AppendProcessEvent appends a durable process event and assigns store-owned
// identity fields.
func (s *MemoryStore) AppendProcessEvent(
	_ context.Context,
	event *agentos.ProcessEvent,
	idempotencyKey string,
) (agentos.ProcessEvent, error) {
	if err := validateAppendProcessEventInput(event, idempotencyKey); err != nil {
		return agentos.ProcessEvent{}, err
	}

	key := processEventIdempotencyKey{
		ProcessID:      event.ProcessID,
		IdempotencyKey: idempotencyKey,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, exists := s.eventKeys[key]; exists {
		requested := prepareProcessEvent(event, existing.EventID, existing.Sequence)
		if err := ValidateProcessEventIdempotency(&existing, &requested); err != nil {
			return agentos.ProcessEvent{}, err
		}

		return cloneProcessEvent(&existing), nil
	}

	if _, exists := s.specs[event.ProcessID]; !exists {
		return agentos.ProcessEvent{}, fmt.Errorf("%w: process %q not found", agentos.ErrProcessRouteNotFound, event.ProcessID)
	}

	sequence := int64(len(s.events[event.ProcessID]) + 1)
	stored := prepareProcessEvent(event, fmt.Sprintf("%s-event-%d", event.ProcessID, sequence), sequence)
	s.events[event.ProcessID] = append(s.events[event.ProcessID], stored)
	s.eventKeys[key] = stored

	return cloneProcessEvent(&stored), nil
}

// ListProcessEvents returns tenant-scoped durable events in sequence order.
func (s *MemoryStore) ListProcessEvents(_ context.Context, scope *agentos.ProcessEventScope) ([]agentos.ProcessEvent, error) {
	if err := agentos.ValidateProcessEventScope(scope); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, ok := s.specs[scope.ProcessID]
	if !ok {
		return nil, fmt.Errorf("%w: process %q not found", agentos.ErrProcessRouteNotFound, scope.ProcessID)
	}

	if err := validateProcessTenantAccess(processRefFromEventScope(scope), &spec); err != nil {
		return nil, err
	}

	events := s.events[scope.ProcessID]

	filtered := make([]agentos.ProcessEvent, 0, len(events))
	for i := range events {
		event := events[i]
		if event.Sequence <= scope.AfterSequence {
			continue
		}

		filtered = append(filtered, cloneProcessEvent(&event))
	}

	sort.SliceStable(filtered, func(i, j int) bool {
		return filtered[i].Sequence < filtered[j].Sequence
	})

	if scope.Limit > 0 && len(filtered) > scope.Limit {
		filtered = filtered[:scope.Limit]
	}

	return filtered, nil
}

func validateCreateProcessInput(spec *agentos.ProcessSpec, status *agentos.ProcessStatus) error {
	if err := agentos.ValidateProcessSpec(spec); err != nil {
		return err
	}

	if status == nil {
		return fmt.Errorf("%w: process status is required", agentos.ErrInvalidProcess)
	}

	if status.ProcessID != "" && status.ProcessID != spec.ProcessID {
		return fmt.Errorf("%w: process status belongs to process %q", agentos.ErrInvalidProcess, status.ProcessID)
	}

	return nil
}

func validateUpdateProcessStatusInput(status *agentos.ProcessStatus, idempotencyKey string) error {
	if status == nil {
		return fmt.Errorf("%w: process status is required", agentos.ErrInvalidProcess)
	}

	if idempotencyKey == "" {
		return fmt.Errorf("%w: process status idempotency key is required", agentos.ErrInvalidProcess)
	}

	if status.ProcessID == "" {
		return fmt.Errorf("%w: process id is required", agentos.ErrInvalidProcess)
	}

	if status.AccountID == "" {
		return fmt.Errorf("%w: account id is required", agentos.ErrInvalidProcess)
	}

	if status.ProjectID == "" {
		return fmt.Errorf("%w: project id is required", agentos.ErrInvalidProcess)
	}

	if status.LifecycleState == "" {
		return fmt.Errorf("%w: lifecycle state is required", agentos.ErrInvalidProcess)
	}

	if err := agentos.ValidateResourceRef(status.Resource); err != nil {
		return err
	}

	return nil
}

func validateAppendProcessEventInput(event *agentos.ProcessEvent, idempotencyKey string) error {
	if event == nil {
		return fmt.Errorf("%w: process event is required", agentos.ErrInvalidProcess)
	}

	if idempotencyKey == "" {
		return fmt.Errorf("%w: process event idempotency key is required", agentos.ErrInvalidProcess)
	}

	if err := agentos.ValidateResourceRef(event.Resource); err != nil {
		return err
	}

	ref := agentos.ProcessRef{
		ProcessID: event.ProcessID,
		AccountID: event.AccountID,
		ProjectID: event.ProjectID,
	}
	if err := agentos.ValidateProcessRef(ref); err != nil {
		return err
	}

	if event.Resource.AccountID != event.AccountID {
		return fmt.Errorf("%w: event resource account %q does not match event account %q", agentos.ErrInvalidProcess, event.Resource.AccountID, event.AccountID)
	}

	if event.Resource.ProjectID != event.ProjectID {
		return fmt.Errorf("%w: event resource project %q does not match event project %q", agentos.ErrInvalidProcess, event.Resource.ProjectID, event.ProjectID)
	}

	if event.EventType == "" {
		return fmt.Errorf("%w: process event type is required", agentos.ErrInvalidProcess)
	}

	return nil
}

func processStartKeyFromSpec(spec *agentos.ProcessSpec) processStartKey {
	return processStartKey{
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		IdempotencyKey: spec.IdempotencyKey,
	}
}

func processRefFromEventScope(scope *agentos.ProcessEventScope) agentos.ProcessRef {
	return agentos.ProcessRef{
		ProcessID: scope.ProcessID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	}
}

func (s *MemoryStore) lookupExistingProcessLocked(
	key processStartKey,
	spec *agentos.ProcessSpec,
) (agentos.ProcessStatus, bool, error) {
	existingProcessID, exists := s.startKeys[key]
	if !exists {
		return agentos.ProcessStatus{}, false, nil
	}

	existingSpec := s.specs[existingProcessID]
	if err := ValidateProcessStartIdempotency(&existingSpec, spec); err != nil {
		return agentos.ProcessStatus{}, false, err
	}

	existingStatus := s.statuses[existingProcessID]

	return cloneProcessStatus(&existingStatus), true, nil
}

func (s *MemoryStore) lookupExistingProcessIDLocked(spec *agentos.ProcessSpec) (agentos.ProcessStatus, bool, error) {
	existingSpec, exists := s.specs[spec.ProcessID]
	if !exists {
		return agentos.ProcessStatus{}, false, nil
	}

	if err := ValidateProcessStartIdempotency(&existingSpec, spec); err != nil {
		return agentos.ProcessStatus{}, false, err
	}

	existingStatus := s.statuses[spec.ProcessID]

	return cloneProcessStatus(&existingStatus), true, nil
}

func validateProcessTenantAccess(ref agentos.ProcessRef, spec *agentos.ProcessSpec) error {
	if ref.AccountID != spec.AccountID {
		return fmt.Errorf("%w: process not found", agentos.ErrProcessRouteNotFound)
	}

	if ref.ProjectID != spec.ProjectID {
		return fmt.Errorf("%w: process not found", agentos.ErrProcessRouteNotFound)
	}

	return nil
}

func validateProcessStatusScope(status *agentos.ProcessStatus, spec *agentos.ProcessSpec) error {
	if status.AccountID != spec.AccountID {
		return fmt.Errorf("%w: process not found", agentos.ErrProcessRouteNotFound)
	}

	if status.ProjectID != spec.ProjectID {
		return fmt.Errorf("%w: process not found", agentos.ErrProcessRouteNotFound)
	}

	if status.Resource != spec.Resource {
		return fmt.Errorf("%w: process resource scope changed", agentos.ErrInvalidProcess)
	}

	return nil
}

func normalizeProcessStatus(spec *agentos.ProcessSpec, status *agentos.ProcessStatus) agentos.ProcessStatus {
	out := cloneProcessStatus(status)
	out.ProcessID = spec.ProcessID
	out.Kind = spec.Kind
	out.AccountID = spec.AccountID
	out.ProjectID = spec.ProjectID

	out.Resource = spec.Resource

	if out.LifecycleState == "" {
		out.LifecycleState = agentos.ProcessPending
	}

	if out.StartedAt.IsZero() {
		out.StartedAt = spec.RequestedAt
	}

	if out.UpdatedAt.IsZero() {
		out.UpdatedAt = out.StartedAt
	}

	return out
}

func prepareProcessEvent(event *agentos.ProcessEvent, eventID string, sequence int64) agentos.ProcessEvent {
	out := cloneProcessEvent(event)
	out.EventID = eventID
	out.Sequence = sequence
	out.ProcessID = event.ProcessID
	out.Event.ProcessID = event.ProcessID

	if out.Timestamp.IsZero() {
		out.Timestamp = time.Now().UTC()
	}

	return out
}

func cloneProcessSpec(spec *agentos.ProcessSpec) agentos.ProcessSpec {
	if spec == nil {
		return agentos.ProcessSpec{}
	}

	out := *spec
	out.Inputs = cloneAnyMap(spec.Inputs)
	out.Metadata = cloneStringMap(spec.Metadata)
	out.Timers = append([]agentos.ProcessTimerSpec(nil), spec.Timers...)

	return out
}

func cloneProcessStatus(status *agentos.ProcessStatus) agentos.ProcessStatus {
	if status == nil {
		return agentos.ProcessStatus{}
	}

	out := *status

	out.Metadata = cloneStringMap(status.Metadata)

	if status.Progress != nil {
		progress := *status.Progress
		out.Progress = &progress
	}

	return out
}

func cloneProcessEvent(event *agentos.ProcessEvent) agentos.ProcessEvent {
	if event == nil {
		return agentos.ProcessEvent{}
	}

	out := *event
	out.Payload = cloneAnyMap(event.Payload)

	return out
}

func cloneAnyMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}

	output := make(map[string]any, len(input))
	maps.Copy(output, input)

	return output
}

func cloneStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}

	output := make(map[string]string, len(input))
	maps.Copy(output, input)

	return output
}
