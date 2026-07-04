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

// ListProcessEvents returns durable process events from the event store.
func (r *Runtime) ListProcessEvents(ctx context.Context, scope *agentos.ProcessEventScope) ([]agentos.ProcessEvent, error) {
	return r.events.ListProcessEvents(ctx, scope)
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
