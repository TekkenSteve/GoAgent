package agentosbatch

import (
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestRuntimeImplementsBatchRuntime(t *testing.T) {
	t.Parallel()

	var _ agentos.BatchRuntime = (*Runtime)(nil)
}

func TestRuntimeStartWorksetInitializesAggregateProgress(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleWorksetSpec()

	status, err := runtime.StartWorkset(t.Context(), &spec)
	if err != nil {
		t.Fatalf("StartWorkset: %v", err)
	}

	if status.LifecycleState != agentos.WorksetRunning ||
		status.Progress.TotalItems != 10 ||
		status.Progress.TotalChunks != 2 {
		t.Fatalf("status = %#v, want running aggregate progress", status)
	}
}

func TestRuntimeRecordsChunkProgressIdempotently(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleWorksetSpec()
	ref := worksetRefFromSpec(&spec)

	if _, err := runtime.StartWorkset(t.Context(), &spec); err != nil {
		t.Fatalf("StartWorkset: %v", err)
	}

	result := agentos.WorksetChunkResult{
		ChunkID:        "chunk-1",
		IdempotencyKey: "chunk-key-1",
		CompletedItems: 5,
		Succeeded:      true,
		RecordedAt:     spec.RequestedAt.Add(time.Minute),
	}

	first, err := runtime.RecordWorksetChunk(t.Context(), ref, &result)
	if err != nil {
		t.Fatalf("RecordWorksetChunk: %v", err)
	}

	second, err := runtime.RecordWorksetChunk(t.Context(), ref, &result)
	if err != nil {
		t.Fatalf("RecordWorksetChunk replay: %v", err)
	}

	if first.Progress.CompletedItems != 5 || second.Progress.CompletedItems != 5 {
		t.Fatalf("progress first=%#v second=%#v, want idempotent completed items", first.Progress, second.Progress)
	}
}

func TestRuntimeMarksWorksetSucceededAfterAllChunks(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleWorksetSpec()
	ref := worksetRefFromSpec(&spec)

	if _, err := runtime.StartWorkset(t.Context(), &spec); err != nil {
		t.Fatalf("StartWorkset: %v", err)
	}

	recordChunk(t, runtime, ref, "chunk-1", "chunk-key-1", true)
	status := recordChunk(t, runtime, ref, "chunk-2", "chunk-key-2", true)

	if status.LifecycleState != agentos.WorksetSucceeded {
		t.Fatalf("lifecycle = %q, want succeeded", status.LifecycleState)
	}
}

func TestRuntimeMarksWorksetFailedOnChunkFailure(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleWorksetSpec()
	ref := worksetRefFromSpec(&spec)

	if _, err := runtime.StartWorkset(t.Context(), &spec); err != nil {
		t.Fatalf("StartWorkset: %v", err)
	}

	status := recordChunk(t, runtime, ref, "chunk-1", "chunk-key-1", false)
	if status.LifecycleState != agentos.WorksetFailed || status.Progress.FailedChunks != 1 {
		t.Fatalf("status = %#v, want failed chunk progress", status)
	}
}

func TestRuntimeRejectsUnknownChunk(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleWorksetSpec()
	ref := worksetRefFromSpec(&spec)

	if _, err := runtime.StartWorkset(t.Context(), &spec); err != nil {
		t.Fatalf("StartWorkset: %v", err)
	}

	_, err := runtime.RecordWorksetChunk(t.Context(), ref, &agentos.WorksetChunkResult{
		ChunkID:        "missing",
		IdempotencyKey: "chunk-key-1",
		Succeeded:      true,
	})
	if !errors.Is(err, agentos.ErrInvalidWorkset) {
		t.Fatalf("RecordWorksetChunk error = %v, want ErrInvalidWorkset", err)
	}
}

func TestRuntimeCancelWorkset(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleWorksetSpec()
	ref := worksetRefFromSpec(&spec)

	if _, err := runtime.StartWorkset(t.Context(), &spec); err != nil {
		t.Fatalf("StartWorkset: %v", err)
	}

	status, err := runtime.CancelWorkset(t.Context(), ref, &agentos.ControlRequest{
		Operation:      agentos.ControlCancel,
		IdempotencyKey: "cancel-1",
		RequestedAt:    spec.RequestedAt.Add(time.Minute),
		Metadata:       map[string]string{"reason": "operator canceled"},
	})
	if err != nil {
		t.Fatalf("CancelWorkset: %v", err)
	}

	if status.LifecycleState != agentos.WorksetCanceled {
		t.Fatalf("lifecycle = %q, want canceled", status.LifecycleState)
	}
}

func TestRuntimeListWorksetsFiltersByLifecycle(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	running := sampleWorksetSpec()
	done := sampleWorksetSpec()
	done.WorksetID = "workset-2"
	done.IdempotencyKey = "workset-key-2"

	if _, err := runtime.StartWorkset(t.Context(), &running); err != nil {
		t.Fatalf("StartWorkset running: %v", err)
	}

	if _, err := runtime.StartWorkset(t.Context(), &done); err != nil {
		t.Fatalf("StartWorkset done: %v", err)
	}

	ref := worksetRefFromSpec(&done)
	recordChunk(t, runtime, ref, "chunk-1", "chunk-key-1", true)
	recordChunk(t, runtime, ref, "chunk-2", "chunk-key-2", true)

	statuses, err := runtime.ListWorksets(t.Context(), &agentos.WorksetScope{
		AccountID:      running.AccountID,
		ProjectID:      running.ProjectID,
		LifecycleState: agentos.WorksetSucceeded,
	})
	if err != nil {
		t.Fatalf("ListWorksets: %v", err)
	}

	if len(statuses) != 1 || statuses[0].WorksetID != done.WorksetID {
		t.Fatalf("statuses = %#v, want succeeded workset only", statuses)
	}
}

func newSampleRuntime(t *testing.T) *Runtime {
	t.Helper()

	runtime, err := NewRuntime(NewMemoryStore())
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	return runtime
}

func recordChunk(
	t *testing.T,
	runtime *Runtime,
	ref agentos.WorksetRef,
	chunkID string,
	key string,
	succeeded bool,
) agentos.WorksetStatus {
	t.Helper()

	status, err := runtime.RecordWorksetChunk(t.Context(), ref, &agentos.WorksetChunkResult{
		ChunkID:        chunkID,
		IdempotencyKey: key,
		CompletedItems: 5,
		FailedItems:    failedItems(succeeded),
		Succeeded:      succeeded,
	})
	if err != nil {
		t.Fatalf("RecordWorksetChunk %s: %v", chunkID, err)
	}

	return status
}

func failedItems(succeeded bool) int64 {
	if succeeded {
		return 0
	}

	return 5
}

func sampleWorksetSpec() agentos.WorksetSpec {
	return agentos.WorksetSpec{
		WorksetID:      "workset-1",
		IdempotencyKey: "workset-key-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		ProcessID:      "process-1",
		Resource: agentos.ResourceRef{
			Kind:       "resource-kind",
			ResourceID: "resource-1",
			AccountID:  "acct-1",
			ProjectID:  "proj-1",
		},
		Kind:        "batch-operation",
		RequestedAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
		ItemsRef: agentos.WorksetItemsRef{
			Kind:  "dataset",
			URI:   "s3://bucket/items.jsonl",
			Count: 10,
		},
		Chunks: []agentos.WorksetChunkSpec{
			sampleChunk("chunk-1", 5),
			sampleChunk("chunk-2", 5),
		},
		Policy: agentos.WorksetPolicy{MaxItems: 10, MaxChunkSize: 5, MaxChunks: 2, MaxConcurrency: 2},
	}
}

func sampleChunk(chunkID string, count int64) agentos.WorksetChunkSpec {
	return agentos.WorksetChunkSpec{
		ChunkID:   chunkID,
		ItemCount: count,
		ItemsRef: agentos.WorksetItemsRef{
			Kind:  "chunk",
			URI:   "s3://bucket/" + chunkID + ".jsonl",
			Count: count,
		},
		Concurrency: 1,
	}
}

func worksetRefFromSpec(spec *agentos.WorksetSpec) agentos.WorksetRef {
	return agentos.WorksetRef{
		WorksetID: spec.WorksetID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	}
}
