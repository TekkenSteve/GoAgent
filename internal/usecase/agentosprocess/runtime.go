package agentosprocess

import (
	"context"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// Runtime coordinates durable process starts and read projections.
type Runtime struct {
	index  ProcessIndex
	events ProcessEventStore
}

// NewRuntime creates a generic durable process use case.
func NewRuntime(index ProcessIndex, events ProcessEventStore) (*Runtime, error) {
	if index == nil {
		return nil, fmt.Errorf("%w: process index is required", agentos.ErrProcessRouteNotFound)
	}

	if events == nil {
		return nil, fmt.Errorf("%w: process event store is required", agentos.ErrProcessRouteNotFound)
	}

	return &Runtime{index: index, events: events}, nil
}

// StartProcess claims a coarse-grained durable process for an application-owned
// resource.
func (r *Runtime) StartProcess(ctx context.Context, spec *agentos.ProcessSpec) (agentos.ProcessStatus, error) {
	if err := agentos.ValidateProcessSpec(spec); err != nil {
		return agentos.ProcessStatus{}, err
	}

	status := initialProcessStatus(spec)

	status, created, err := r.index.CreateProcess(ctx, spec, &status)
	if err != nil {
		return agentos.ProcessStatus{}, err
	}

	if !created {
		return status, nil
	}

	_, err = r.events.AppendProcessEvent(ctx, processStartedEvent(spec, &status), spec.IdempotencyKey)
	if err != nil {
		return agentos.ProcessStatus{}, err
	}

	return status, nil
}

// StatusProcess returns the latest durable process projection.
func (r *Runtime) StatusProcess(ctx context.Context, ref agentos.ProcessRef) (agentos.ProcessStatus, error) {
	_, status, exists, err := r.index.GetProcessByRef(ctx, ref)
	if err != nil {
		return agentos.ProcessStatus{}, err
	}

	if !exists {
		return agentos.ProcessStatus{}, fmt.Errorf("%w: process %q not found", agentos.ErrProcessRouteNotFound, ref.ProcessID)
	}

	return status, nil
}

// DescribeProcess returns the durable process projection with its immutable
// start spec.
func (r *Runtime) DescribeProcess(ctx context.Context, ref agentos.ProcessRef) (agentos.ProcessDescription, error) {
	spec, status, exists, err := r.index.GetProcessByRef(ctx, ref)
	if err != nil {
		return agentos.ProcessDescription{}, err
	}

	if !exists {
		return agentos.ProcessDescription{}, fmt.Errorf("%w: process %q not found", agentos.ErrProcessRouteNotFound, ref.ProcessID)
	}

	return agentos.ProcessDescription{
		ProcessID: spec.ProcessID,
		Kind:      spec.Kind,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		Resource:  spec.Resource,
		Status:    status,
		Policy:    spec.Policy,
		Timers:    append([]agentos.ProcessTimerSpec(nil), spec.Timers...),
		Metadata:  cloneStringMap(spec.Metadata),
		UpdatedAt: status.UpdatedAt,
	}, nil
}

// SignalProcess records external input for a durable process.
func (r *Runtime) SignalProcess(ctx context.Context, ref agentos.ProcessRef, signal *agentos.Signal) error {
	spec, status, err := r.requireProcess(ctx, ref)
	if err != nil {
		return err
	}

	if err := validateProcessSignal(signal); err != nil {
		return err
	}

	event := processSignalEvent(&spec, signal)

	_, err = r.events.AppendProcessEvent(ctx, event, signal.IdempotencyKey)
	if err != nil {
		return err
	}

	next := status
	next.LifecycleState = processLifecycleAfterSignal(status.LifecycleState)

	next.UpdatedAt = signal.SentAt
	if next.UpdatedAt.IsZero() {
		next.UpdatedAt = time.Now().UTC()
	}

	_, err = r.index.UpdateProcessStatus(ctx, &next, signal.IdempotencyKey)

	return err
}

// ControlProcess records and applies a lifecycle control operation.
func (r *Runtime) ControlProcess(ctx context.Context, ref agentos.ProcessRef, control *agentos.ControlRequest) error {
	spec, status, err := r.requireProcess(ctx, ref)
	if err != nil {
		return err
	}

	if err := agentos.ValidateControlRequest(control); err != nil {
		return err
	}

	if control.IdempotencyKey == "" {
		return fmt.Errorf("%w: process control idempotency key is required", agentos.ErrInvalidProcess)
	}

	event := processControlEvent(&spec, control)

	_, err = r.events.AppendProcessEvent(ctx, event, control.IdempotencyKey)
	if err != nil {
		return err
	}

	next := status
	next.LifecycleState = processLifecycleAfterControl(status.LifecycleState, control.Operation)

	next.UpdatedAt = control.RequestedAt
	if next.UpdatedAt.IsZero() {
		next.UpdatedAt = time.Now().UTC()
	}

	_, err = r.index.UpdateProcessStatus(ctx, &next, control.IdempotencyKey)

	return err
}

// SubscribeProcess replays durable process events through the public
// subscription boundary.
func (r *Runtime) SubscribeProcess(ctx context.Context, scope *agentos.ProcessStreamScope) (agentos.Subscription, error) {
	if err := agentos.ValidateProcessStreamScope(scope); err != nil {
		return nil, err
	}

	events, err := r.events.ListProcessEvents(ctx, &agentos.ProcessEventScope{
		ProcessID:     scope.ProcessID,
		AccountID:     scope.AccountID,
		ProjectID:     scope.ProjectID,
		AfterSequence: scope.AfterSequence,
	})
	if err != nil {
		return nil, err
	}

	return newReplaySubscription(events), nil
}

// ListProcessEvents returns durable process events from the event store.
func (r *Runtime) ListProcessEvents(ctx context.Context, scope *agentos.ProcessEventScope) ([]agentos.ProcessEvent, error) {
	return r.events.ListProcessEvents(ctx, scope)
}

func (r *Runtime) requireProcess(ctx context.Context, ref agentos.ProcessRef) (agentos.ProcessSpec, agentos.ProcessStatus, error) {
	spec, status, exists, err := r.index.GetProcessByRef(ctx, ref)
	if err != nil {
		return agentos.ProcessSpec{}, agentos.ProcessStatus{}, err
	}

	if !exists {
		return agentos.ProcessSpec{}, agentos.ProcessStatus{}, fmt.Errorf("%w: process %q not found", agentos.ErrProcessRouteNotFound, ref.ProcessID)
	}

	return spec, status, nil
}

func initialProcessStatus(spec *agentos.ProcessSpec) agentos.ProcessStatus {
	now := spec.RequestedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}

	return agentos.ProcessStatus{
		ProcessID:      spec.ProcessID,
		Kind:           spec.Kind,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		Resource:       spec.Resource,
		LifecycleState: agentos.ProcessRunning,
		Metadata:       cloneStringMap(spec.Metadata),
		StartedAt:      now,
		UpdatedAt:      now,
	}
}

func processStartedEvent(spec *agentos.ProcessSpec, status *agentos.ProcessStatus) *agentos.ProcessEvent {
	timestamp := status.StartedAt
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}

	return &agentos.ProcessEvent{
		Event: agentos.Event{
			EventType: agentos.EventProcessStarted,
			ProcessID: spec.ProcessID,
			Timestamp: timestamp,
			Payload: map[string]any{
				"lifecycle_state": status.LifecycleState,
				"resource_kind":   string(spec.Resource.Kind),
				"resource_id":     spec.Resource.ResourceID,
			},
		},
		ProcessID: spec.ProcessID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		Resource:  spec.Resource,
	}
}

func validateProcessSignal(signal *agentos.Signal) error {
	if signal == nil {
		return fmt.Errorf("%w: process signal is required", agentos.ErrInvalidSignal)
	}

	if signal.Type == "" {
		return fmt.Errorf("%w: process signal type is required", agentos.ErrInvalidSignal)
	}

	if signal.IdempotencyKey == "" {
		return fmt.Errorf("%w: process signal idempotency key is required", agentos.ErrInvalidSignal)
	}

	return nil
}

func processSignalEvent(spec *agentos.ProcessSpec, signal *agentos.Signal) *agentos.ProcessEvent {
	timestamp := signal.SentAt
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}

	return processEvent(spec, agentos.EventProcessSignalReceived, timestamp, map[string]any{
		"signal_type": string(signal.Type),
		"actor_id":    signal.ActorID,
		"payload":     cloneAnyMap(signal.Payload),
	})
}

func processEvent(spec *agentos.ProcessSpec, eventType agentos.EventType, timestamp time.Time, payload map[string]any) *agentos.ProcessEvent {
	return &agentos.ProcessEvent{
		Event: agentos.Event{
			EventType: eventType,
			ProcessID: spec.ProcessID,
			Timestamp: timestamp,
			Payload:   payload,
		},
		ProcessID: spec.ProcessID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		Resource:  spec.Resource,
	}
}

func processControlEvent(spec *agentos.ProcessSpec, control *agentos.ControlRequest) *agentos.ProcessEvent {
	timestamp := control.RequestedAt
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}

	return processEvent(spec, agentos.EventProcessControlReceived, timestamp, map[string]any{
		"operation": string(control.Operation),
		"actor_id":  control.ActorID,
		"metadata":  cloneStringMap(control.Metadata),
	})
}

func processLifecycleAfterSignal(current string) string {
	if current == agentos.ProcessWaiting || current == agentos.ProcessBlocked {
		return agentos.ProcessRunning
	}

	return current
}

func processLifecycleAfterControl(current string, operation agentos.ControlOperation) string {
	switch operation {
	case agentos.ControlPause:
		return agentos.ProcessWaiting
	case agentos.ControlResume:
		return agentos.ProcessRunning
	case agentos.ControlCancel:
		return agentos.ProcessCanceled
	default:
		return current
	}
}
