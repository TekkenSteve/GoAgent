package agentosprocess

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestRuntimeStartProcessCreatesStatusAndStartedEvent(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleProcessSpec()

	status, err := runtime.StartProcess(t.Context(), &spec)
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}

	if status.LifecycleState != agentos.ProcessRunning {
		t.Fatalf("status lifecycle = %q, want running", status.LifecycleState)
	}

	events, err := runtime.ListProcessEvents(t.Context(), &agentos.ProcessEventScope{
		ProcessID: spec.ProcessID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	})
	if err != nil {
		t.Fatalf("ListProcessEvents: %v", err)
	}

	if len(events) != 1 || events[0].EventType != agentos.EventProcessStarted {
		t.Fatalf("events = %#v, want one process.started event", events)
	}
}

func TestRuntimeStartProcessIsIdempotent(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleProcessSpec()

	first, err := runtime.StartProcess(t.Context(), &spec)
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}

	second, err := runtime.StartProcess(t.Context(), &spec)
	if err != nil {
		t.Fatalf("StartProcess replay: %v", err)
	}

	if second.UpdatedAt != first.UpdatedAt {
		t.Fatalf("replayed updated_at = %v, want %v", second.UpdatedAt, first.UpdatedAt)
	}

	events, err := runtime.ListProcessEvents(t.Context(), &agentos.ProcessEventScope{
		ProcessID: spec.ProcessID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	})
	if err != nil {
		t.Fatalf("ListProcessEvents: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
}

func TestRuntimeDescribeProcessReturnsSpecProjection(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleProcessSpec()

	spec.Metadata = map[string]string{"owner": "platform"}
	if _, err := runtime.StartProcess(t.Context(), &spec); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}

	description, err := runtime.DescribeProcess(t.Context(), agentos.ProcessRef{
		ProcessID: spec.ProcessID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	})
	if err != nil {
		t.Fatalf("DescribeProcess: %v", err)
	}

	if description.Resource.ResourceID != spec.Resource.ResourceID {
		t.Fatalf("description resource = %q, want %q", description.Resource.ResourceID, spec.Resource.ResourceID)
	}

	if description.Metadata["owner"] != "platform" {
		t.Fatalf("description metadata = %#v, want owner metadata", description.Metadata)
	}
}

func TestRuntimeStatusProcessReportsMissingRoute(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)

	_, err := runtime.StatusProcess(t.Context(), agentos.ProcessRef{
		ProcessID: "missing",
		AccountID: "acct-1",
		ProjectID: "proj-1",
	})
	if !errors.Is(err, agentos.ErrProcessRouteNotFound) {
		t.Fatalf("StatusProcess error = %v, want ErrProcessRouteNotFound", err)
	}
}

func newSampleRuntime(t *testing.T) *Runtime {
	t.Helper()

	store := NewMemoryStore()

	runtime, err := NewRuntime(store, store)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	return runtime
}
