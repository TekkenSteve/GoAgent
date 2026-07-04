package agentosbatch

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// Runtime coordinates generic batch workset progress.
type Runtime struct {
	store Store
}

// NewRuntime creates a generic batch use case.
func NewRuntime(store Store) (*Runtime, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: workset store is required", agentos.ErrInvalidWorksetScope)
	}

	return &Runtime{store: store}, nil
}

// StartWorkset claims one batch workset.
func (r *Runtime) StartWorkset(ctx context.Context, spec *agentos.WorksetSpec) (agentos.WorksetStatus, error) {
	if err := agentos.ValidateWorksetSpec(spec); err != nil {
		return agentos.WorksetStatus{}, err
	}

	status := initialWorksetStatus(spec)

	status, _, err := r.store.CreateWorkset(ctx, spec, &status)

	return status, err
}

// StatusWorkset returns the latest workset projection.
func (r *Runtime) StatusWorkset(ctx context.Context, ref agentos.WorksetRef) (agentos.WorksetStatus, error) {
	_, status, exists, err := r.store.GetWorkset(ctx, ref)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	if !exists {
		return agentos.WorksetStatus{}, fmt.Errorf("%w: workset %q not found", agentos.ErrInvalidWorksetScope, ref.WorksetID)
	}

	return status, nil
}

// ListWorksets returns tenant-scoped workset projections.
func (r *Runtime) ListWorksets(ctx context.Context, scope *agentos.WorksetScope) ([]agentos.WorksetStatus, error) {
	if err := agentos.ValidateWorksetScope(scope); err != nil {
		return nil, err
	}

	return r.store.ListWorksets(ctx, scope)
}

// RecordWorksetChunk applies aggregate progress for one coarse chunk.
func (r *Runtime) RecordWorksetChunk(
	ctx context.Context,
	ref agentos.WorksetRef,
	result *agentos.WorksetChunkResult,
) (agentos.WorksetStatus, error) {
	spec, status, err := r.requireWorkset(ctx, ref)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	if err := validateChunkResult(result); err != nil {
		return agentos.WorksetStatus{}, err
	}

	if !worksetHasChunk(&spec, result.ChunkID) {
		return agentos.WorksetStatus{}, fmt.Errorf("%w: chunk %q is not part of workset %q", agentos.ErrInvalidWorkset, result.ChunkID, ref.WorksetID)
	}

	next := status
	next.UpdatedAt = worksetTimestamp(result.RecordedAt)

	next.Progress.CompletedChunks++
	if result.Succeeded {
		next.Progress.CompletedItems += result.CompletedItems
	} else {
		next.Progress.FailedChunks++
		next.Progress.FailedItems += result.FailedItems
		next.Reason = result.Reason
	}

	next.LifecycleState = worksetLifecycleAfterChunk(next.Progress)

	return r.store.ApplyChunkResult(ctx, ref, result, &next)
}

// CancelWorkset records a workset cancellation.
func (r *Runtime) CancelWorkset(
	ctx context.Context,
	ref agentos.WorksetRef,
	control *agentos.ControlRequest,
) (agentos.WorksetStatus, error) {
	_, status, err := r.requireWorkset(ctx, ref)
	if err != nil {
		return agentos.WorksetStatus{}, err
	}

	if err := validateCancelControl(control); err != nil {
		return agentos.WorksetStatus{}, err
	}

	next := status
	next.LifecycleState = agentos.WorksetCanceled
	next.Reason = control.Metadata["reason"]
	next.UpdatedAt = worksetTimestamp(control.RequestedAt)

	return r.store.UpdateWorksetStatus(ctx, &next, control.IdempotencyKey)
}

func (r *Runtime) requireWorkset(
	ctx context.Context,
	ref agentos.WorksetRef,
) (agentos.WorksetSpec, agentos.WorksetStatus, error) {
	if err := agentos.ValidateWorksetRef(ref); err != nil {
		return agentos.WorksetSpec{}, agentos.WorksetStatus{}, err
	}

	spec, status, exists, err := r.store.GetWorkset(ctx, ref)
	if err != nil {
		return agentos.WorksetSpec{}, agentos.WorksetStatus{}, err
	}

	if !exists {
		return agentos.WorksetSpec{}, agentos.WorksetStatus{}, fmt.Errorf("%w: workset %q not found", agentos.ErrInvalidWorksetScope, ref.WorksetID)
	}

	return spec, status, nil
}

func initialWorksetStatus(spec *agentos.WorksetSpec) agentos.WorksetStatus {
	now := worksetTimestamp(spec.RequestedAt)
	progress := agentos.WorksetProgress{
		TotalItems:  spec.ItemsRef.Count,
		TotalChunks: worksetChunkCount(spec.Chunks),
	}

	if progress.TotalChunks == 0 {
		progress.TotalChunks = 1
	}

	return agentos.WorksetStatus{
		WorksetID:      spec.WorksetID,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		ProcessID:      spec.ProcessID,
		Resource:       spec.Resource,
		Kind:           spec.Kind,
		LifecycleState: agentos.WorksetRunning,
		Progress:       progress,
		Metadata:       cloneStringMap(spec.Metadata),
		UpdatedAt:      now,
	}
}

func worksetChunkCount(chunks []agentos.WorksetChunkSpec) int32 {
	var count int32
	for range chunks {
		count++
	}

	return count
}

func worksetLifecycleAfterChunk(progress agentos.WorksetProgress) string {
	if progress.FailedChunks > 0 {
		return agentos.WorksetFailed
	}

	if progress.TotalChunks > 0 && progress.CompletedChunks >= progress.TotalChunks {
		return agentos.WorksetSucceeded
	}

	return agentos.WorksetRunning
}

func worksetHasChunk(spec *agentos.WorksetSpec, chunkID string) bool {
	if len(spec.Chunks) == 0 {
		return chunkID == spec.WorksetID
	}

	for i := range spec.Chunks {
		if spec.Chunks[i].ChunkID == chunkID {
			return true
		}
	}

	return false
}

func validateChunkResult(result *agentos.WorksetChunkResult) error {
	if result == nil {
		return fmt.Errorf("%w: chunk result is required", agentos.ErrInvalidWorkset)
	}

	switch {
	case result.ChunkID == "":
		return fmt.Errorf("%w: chunk id is required", agentos.ErrInvalidWorkset)
	case result.IdempotencyKey == "":
		return fmt.Errorf("%w: chunk idempotency key is required", agentos.ErrInvalidWorkset)
	case result.CompletedItems < 0 || result.FailedItems < 0:
		return fmt.Errorf("%w: chunk item counts must be non-negative", agentos.ErrInvalidWorkset)
	default:
		return nil
	}
}

func validateCancelControl(control *agentos.ControlRequest) error {
	if err := agentos.ValidateControlRequest(control); err != nil {
		return err
	}

	if control.Operation != agentos.ControlCancel {
		return fmt.Errorf("%w: workset only accepts cancel control", agentos.ErrInvalidControlOperation)
	}

	if control.IdempotencyKey == "" {
		return fmt.Errorf("%w: cancel idempotency key is required", agentos.ErrInvalidWorkset)
	}

	return nil
}

func worksetTimestamp(ts time.Time) time.Time {
	if ts.IsZero() {
		return time.Now().UTC()
	}

	return ts
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}

	clone := make(map[string]string, len(values))
	maps.Copy(clone, values)

	return clone
}
