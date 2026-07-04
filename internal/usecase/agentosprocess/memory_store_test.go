package agentosprocess

import (
	"errors"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
)

func TestMemoryStoreCreateProcessIsIdempotent(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := sampleProcessSpec()
	status := sampleProcessStatus(&spec)

	created, fresh, err := store.CreateProcess(t.Context(), &spec, &status)
	if err != nil {
		t.Fatalf("CreateProcess: %v", err)
	}

	if !fresh {
		t.Fatal("CreateProcess fresh = false, want true")
	}

	replayed, fresh, err := store.CreateProcess(t.Context(), &spec, &status)
	if err != nil {
		t.Fatalf("CreateProcess replay: %v", err)
	}

	if fresh {
		t.Fatal("CreateProcess replay fresh = true, want false")
	}

	if replayed.LifecycleState != created.LifecycleState {
		t.Fatalf("replayed lifecycle = %q, want %q", replayed.LifecycleState, created.LifecycleState)
	}
}

func TestMemoryStoreRejectsProcessStartKeyReuseWithDifferentRequest(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := createSampleProcess(t, store)

	changed := spec
	changed.ProcessID = "process-2"
	status := sampleProcessStatus(&changed)

	_, _, err := store.CreateProcess(t.Context(), &changed, &status)
	if !errors.Is(err, agentoscore.ErrInvalidProcess) {
		t.Fatalf("CreateProcess changed error = %v, want ErrInvalidProcess", err)
	}
}

func TestMemoryStoreGetProcessByRefEnforcesTenantScope(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := createSampleProcess(t, store)

	_, err := getProcessStatusByRef(t, store, agentos.Ref{
		ProcessID: spec.ProcessID,
		AccountID: "acct-other",
		ProjectID: spec.ProjectID,
	})
	if !errors.Is(err, agentoscore.ErrProcessRouteNotFound) {
		t.Fatalf("GetProcessByRef tenant error = %v, want ErrProcessRouteNotFound", err)
	}
}

func TestMemoryStoreAppendProcessEventIsIdempotent(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := createSampleProcess(t, store)

	event := sampleProcessEvent(&spec, agentoscore.EventProcessWaiting)

	first, err := store.AppendProcessEvent(t.Context(), &event, "event-key-1")
	if err != nil {
		t.Fatalf("AppendProcessEvent: %v", err)
	}

	second, err := store.AppendProcessEvent(t.Context(), &event, "event-key-1")
	if err != nil {
		t.Fatalf("AppendProcessEvent replay: %v", err)
	}

	if first.EventID == "" || first.Sequence != 1 {
		t.Fatalf("first event identity = (%q, %d), want assigned sequence 1", first.EventID, first.Sequence)
	}

	if second.EventID != first.EventID || second.Sequence != first.Sequence {
		t.Fatalf("replayed event identity = (%q, %d), want (%q, %d)", second.EventID, second.Sequence, first.EventID, first.Sequence)
	}
}

func TestMemoryStoreRejectsProcessEventKeyReuseWithDifferentEvent(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := createSampleProcess(t, store)

	event := sampleProcessEvent(&spec, agentoscore.EventProcessWaiting)
	if _, err := store.AppendProcessEvent(t.Context(), &event, "event-key-1"); err != nil {
		t.Fatalf("AppendProcessEvent: %v", err)
	}

	changed := sampleProcessEvent(&spec, agentoscore.EventProcessBlocked)

	_, err := store.AppendProcessEvent(t.Context(), &changed, "event-key-1")
	if !errors.Is(err, agentoscore.ErrInvalidProcess) {
		t.Fatalf("AppendProcessEvent changed error = %v, want ErrInvalidProcess", err)
	}
}

func TestMemoryStoreListProcessEventsFiltersAndLimits(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	spec := createSampleProcess(t, store)

	events := []agentos.Event{
		sampleProcessEvent(&spec, agentoscore.EventProcessWaiting),
		sampleProcessEvent(&spec, agentoscore.EventProcessBlocked),
		sampleProcessEvent(&spec, agentoscore.EventProcessSucceeded),
	}
	for i := range events {
		if _, err := store.AppendProcessEvent(t.Context(), &events[i], string(events[i].EventType)); err != nil {
			t.Fatalf("AppendProcessEvent %d: %v", i, err)
		}
	}

	got, err := store.ListProcessEvents(t.Context(), &agentos.EventScope{
		ProcessID:     spec.ProcessID,
		AccountID:     spec.AccountID,
		ProjectID:     spec.ProjectID,
		AfterSequence: 1,
		Limit:         1,
	})
	if err != nil {
		t.Fatalf("ListProcessEvents: %v", err)
	}

	if len(got) != 1 || got[0].Sequence != 2 {
		t.Fatalf("events = %#v, want one event at sequence 2", got)
	}
}

func sampleProcessSpec() agentos.Spec {
	return agentos.Spec{
		ProcessID:      "process-1",
		Kind:           "resource-lifecycle",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "process-key-1",
		Resource: agentos.ResourceRef{
			Kind:       "resource-kind",
			ResourceID: "resource-1",
			AccountID:  "acct-1",
			ProjectID:  "proj-1",
		},
		RequestedAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
	}
}

func createSampleProcess(t *testing.T, store *MemoryStore) agentos.Spec {
	t.Helper()

	spec := sampleProcessSpec()
	status := sampleProcessStatus(&spec)

	if _, _, err := store.CreateProcess(t.Context(), &spec, &status); err != nil {
		t.Fatalf("CreateProcess: %v", err)
	}

	return spec
}

func getProcessStatusByRef(t *testing.T, store *MemoryStore, ref agentos.Ref) (agentos.Status, error) {
	t.Helper()

	_, status, _, err := store.GetProcessByRef(t.Context(), ref)

	return status, err
}

func sampleProcessStatus(spec *agentos.Spec) agentos.Status {
	return agentos.Status{
		ProcessID:      spec.ProcessID,
		LifecycleState: agentos.ProcessRunning,
		StartedAt:      spec.RequestedAt,
		UpdatedAt:      spec.RequestedAt,
	}
}

func sampleProcessEvent(spec *agentos.Spec, eventType agentoscore.EventType) agentos.Event {
	return agentos.Event{
		Event: agentoscore.Event{
			EventType: eventType,
			ProcessID: spec.ProcessID,
			Timestamp: spec.RequestedAt,
		},
		ProcessID: spec.ProcessID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		Resource:  spec.Resource,
	}
}
