package agentosprocess

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
)

// MemoryStore is an explicit in-process ProcessIndex and ProcessEventStore for
// unit tests and embedded demos.
type MemoryStore struct {
	mu         sync.RWMutex
	specs      map[string]agentos.Spec
	statuses   map[string]agentos.Status
	startKeys  map[processStartKey]string
	statusKeys map[processEventIdempotencyKey]agentos.Status
	events     map[string][]agentos.Event
	eventKeys  map[processEventIdempotencyKey]agentos.Event
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
		specs:      make(map[string]agentos.Spec),
		statuses:   make(map[string]agentos.Status),
		startKeys:  make(map[processStartKey]string),
		statusKeys: make(map[processEventIdempotencyKey]agentos.Status),
		events:     make(map[string][]agentos.Event),
		eventKeys:  make(map[processEventIdempotencyKey]agentos.Event),
	}
}

// CreateProcess creates a durable process identity or returns an idempotent
// replay of the original status.
func (s *MemoryStore) CreateProcess(
	_ context.Context,
	spec *agentos.Spec,
	status *agentos.Status,
) (agentos.Status, bool, error) {
	if err := validateCreateProcessInput(spec, status); err != nil {
		return agentos.Status{}, false, err
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
func (s *MemoryStore) GetProcessByRef(_ context.Context, ref agentos.Ref) (agentos.Spec, agentos.Status, bool, error) {
	if err := agentos.ValidateProcessRef(ref); err != nil {
		return agentos.Spec{}, agentos.Status{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, ok := s.specs[ref.ProcessID]
	if !ok {
		return agentos.Spec{}, agentos.Status{}, false, nil
	}

	if err := validateProcessTenantAccess(ref, &spec); err != nil {
		return agentos.Spec{}, agentos.Status{}, false, err
	}

	status := s.statuses[ref.ProcessID]

	return cloneProcessSpec(&spec), cloneProcessStatus(&status), true, nil
}

// UpdateProcessStatus updates the latest durable process projection.
func (s *MemoryStore) UpdateProcessStatus(
	_ context.Context,
	status *agentos.Status,
	idempotencyKey string,
) (agentos.Status, error) {
	if err := validateUpdateProcessStatusInput(status, idempotencyKey); err != nil {
		return agentos.Status{}, err
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
		return agentos.Status{}, fmt.Errorf("%w: process %q not found", agentoscore.ErrProcessRouteNotFound, status.ProcessID)
	}

	if err := validateProcessStatusScope(status, &spec); err != nil {
		return agentos.Status{}, err
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
	event *agentos.Event,
	idempotencyKey string,
) (agentos.Event, error) {
	if err := validateAppendProcessEventInput(event, idempotencyKey); err != nil {
		return agentos.Event{}, err
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
			return agentos.Event{}, err
		}

		return cloneProcessEvent(&existing), nil
	}

	if _, exists := s.specs[event.ProcessID]; !exists {
		return agentos.Event{}, fmt.Errorf("%w: process %q not found", agentoscore.ErrProcessRouteNotFound, event.ProcessID)
	}

	sequence := int64(len(s.events[event.ProcessID]) + 1)
	stored := prepareProcessEvent(event, fmt.Sprintf("%s-event-%d", event.ProcessID, sequence), sequence)
	s.events[event.ProcessID] = append(s.events[event.ProcessID], stored)
	s.eventKeys[key] = stored

	return cloneProcessEvent(&stored), nil
}

// ListProcessEvents returns tenant-scoped durable events in sequence order.
func (s *MemoryStore) ListProcessEvents(_ context.Context, scope *agentos.EventScope) ([]agentos.Event, error) {
	if err := agentos.ValidateProcessEventScope(scope); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	spec, ok := s.specs[scope.ProcessID]
	if !ok {
		return nil, fmt.Errorf("%w: process %q not found", agentoscore.ErrProcessRouteNotFound, scope.ProcessID)
	}

	if err := validateProcessTenantAccess(processRefFromEventScope(scope), &spec); err != nil {
		return nil, err
	}

	events := s.events[scope.ProcessID]

	filtered := make([]agentos.Event, 0, len(events))
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

func validateCreateProcessInput(spec *agentos.Spec, status *agentos.Status) error {
	if err := agentos.ValidateProcessSpec(spec); err != nil {
		return err
	}

	if status == nil {
		return fmt.Errorf("%w: process status is required", agentoscore.ErrInvalidProcess)
	}

	if status.ProcessID != "" && status.ProcessID != spec.ProcessID {
		return fmt.Errorf("%w: process status belongs to process %q", agentoscore.ErrInvalidProcess, status.ProcessID)
	}

	return nil
}

func validateUpdateProcessStatusInput(status *agentos.Status, idempotencyKey string) error {
	if status == nil {
		return fmt.Errorf("%w: process status is required", agentoscore.ErrInvalidProcess)
	}

	if idempotencyKey == "" {
		return fmt.Errorf("%w: process status idempotency key is required", agentoscore.ErrInvalidProcess)
	}

	if status.ProcessID == "" {
		return fmt.Errorf("%w: process id is required", agentoscore.ErrInvalidProcess)
	}

	if status.AccountID == "" {
		return fmt.Errorf("%w: account id is required", agentoscore.ErrInvalidProcess)
	}

	if status.ProjectID == "" {
		return fmt.Errorf("%w: project id is required", agentoscore.ErrInvalidProcess)
	}

	if status.LifecycleState == "" {
		return fmt.Errorf("%w: lifecycle state is required", agentoscore.ErrInvalidProcess)
	}

	if err := agentos.ValidateResourceRef(status.Resource); err != nil {
		return err
	}

	return nil
}

func validateAppendProcessEventInput(event *agentos.Event, idempotencyKey string) error {
	if event == nil {
		return fmt.Errorf("%w: process event is required", agentoscore.ErrInvalidProcess)
	}

	if idempotencyKey == "" {
		return fmt.Errorf("%w: process event idempotency key is required", agentoscore.ErrInvalidProcess)
	}

	if err := agentos.ValidateResourceRef(event.Resource); err != nil {
		return err
	}

	ref := agentos.Ref{
		ProcessID: event.ProcessID,
		AccountID: event.AccountID,
		ProjectID: event.ProjectID,
	}
	if err := agentos.ValidateProcessRef(ref); err != nil {
		return err
	}

	if event.Resource.AccountID != event.AccountID {
		return fmt.Errorf("%w: event resource account %q does not match event account %q", agentoscore.ErrInvalidProcess, event.Resource.AccountID, event.AccountID)
	}

	if event.Resource.ProjectID != event.ProjectID {
		return fmt.Errorf("%w: event resource project %q does not match event project %q", agentoscore.ErrInvalidProcess, event.Resource.ProjectID, event.ProjectID)
	}

	if event.EventType == "" {
		return fmt.Errorf("%w: process event type is required", agentoscore.ErrInvalidProcess)
	}

	return nil
}

func processStartKeyFromSpec(spec *agentos.Spec) processStartKey {
	return processStartKey{
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		IdempotencyKey: spec.IdempotencyKey,
	}
}

func processRefFromEventScope(scope *agentos.EventScope) agentos.Ref {
	return agentos.Ref{
		ProcessID: scope.ProcessID,
		AccountID: scope.AccountID,
		ProjectID: scope.ProjectID,
	}
}

func (s *MemoryStore) lookupExistingProcessLocked(
	key processStartKey,
	spec *agentos.Spec,
) (agentos.Status, bool, error) {
	existingProcessID, exists := s.startKeys[key]
	if !exists {
		return agentos.Status{}, false, nil
	}

	existingSpec := s.specs[existingProcessID]
	if err := ValidateProcessStartIdempotency(&existingSpec, spec); err != nil {
		return agentos.Status{}, false, err
	}

	existingStatus := s.statuses[existingProcessID]

	return cloneProcessStatus(&existingStatus), true, nil
}

func (s *MemoryStore) lookupExistingProcessIDLocked(spec *agentos.Spec) (agentos.Status, bool, error) {
	existingSpec, exists := s.specs[spec.ProcessID]
	if !exists {
		return agentos.Status{}, false, nil
	}

	if err := ValidateProcessStartIdempotency(&existingSpec, spec); err != nil {
		return agentos.Status{}, false, err
	}

	existingStatus := s.statuses[spec.ProcessID]

	return cloneProcessStatus(&existingStatus), true, nil
}

func validateProcessTenantAccess(ref agentos.Ref, spec *agentos.Spec) error {
	if ref.AccountID != spec.AccountID {
		return fmt.Errorf("%w: process not found", agentoscore.ErrProcessRouteNotFound)
	}

	if ref.ProjectID != spec.ProjectID {
		return fmt.Errorf("%w: process not found", agentoscore.ErrProcessRouteNotFound)
	}

	return nil
}

func validateProcessStatusScope(status *agentos.Status, spec *agentos.Spec) error {
	if status.AccountID != spec.AccountID {
		return fmt.Errorf("%w: process not found", agentoscore.ErrProcessRouteNotFound)
	}

	if status.ProjectID != spec.ProjectID {
		return fmt.Errorf("%w: process not found", agentoscore.ErrProcessRouteNotFound)
	}

	if status.Resource != spec.Resource {
		return fmt.Errorf("%w: process resource scope changed", agentoscore.ErrInvalidProcess)
	}

	return nil
}

func normalizeProcessStatus(spec *agentos.Spec, status *agentos.Status) agentos.Status {
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

func prepareProcessEvent(event *agentos.Event, eventID string, sequence int64) agentos.Event {
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

func cloneProcessSpec(spec *agentos.Spec) agentos.Spec {
	if spec == nil {
		return agentos.Spec{}
	}

	out := *spec
	out.Inputs = cloneAnyMap(spec.Inputs)
	out.Metadata = cloneStringMap(spec.Metadata)
	out.Timers = append([]agentos.TimerSpec(nil), spec.Timers...)

	return out
}

func cloneProcessStatus(status *agentos.Status) agentos.Status {
	if status == nil {
		return agentos.Status{}
	}

	out := *status

	out.Metadata = cloneStringMap(status.Metadata)

	if status.Progress != nil {
		progress := *status.Progress
		out.Progress = &progress
	}

	return out
}

func cloneProcessEvent(event *agentos.Event) agentos.Event {
	if event == nil {
		return agentos.Event{}
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
