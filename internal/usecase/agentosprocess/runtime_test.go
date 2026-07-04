package agentosprocess

import (
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestRuntimeImplementsProcessRuntime(t *testing.T) {
	t.Parallel()

	var _ agentos.ProcessRuntime = (*Runtime)(nil)
}

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

func TestRuntimeSignalProcessRecordsEventAndResumesWaitingProcess(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleProcessSpec()

	if _, err := runtime.StartProcess(t.Context(), &spec); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}

	control := agentos.ControlRequest{
		Operation:      agentos.ControlPause,
		IdempotencyKey: "pause-1",
		RequestedAt:    spec.RequestedAt.Add(time.Minute),
	}
	if err := runtime.ControlProcess(t.Context(), processRefFromSpec(&spec), &control); err != nil {
		t.Fatalf("ControlProcess pause: %v", err)
	}

	signal := agentos.Signal{
		Type:           "external.update",
		IdempotencyKey: "signal-1",
		SentAt:         spec.RequestedAt.Add(2 * time.Minute),
		Payload:        map[string]any{"state": "ready"},
	}
	if err := runtime.SignalProcess(t.Context(), processRefFromSpec(&spec), &signal); err != nil {
		t.Fatalf("SignalProcess: %v", err)
	}

	status, err := runtime.StatusProcess(t.Context(), processRefFromSpec(&spec))
	if err != nil {
		t.Fatalf("StatusProcess: %v", err)
	}

	if status.LifecycleState != agentos.ProcessRunning {
		t.Fatalf("lifecycle = %q, want running", status.LifecycleState)
	}

	events := listProcessEvents(t, runtime, &spec)
	if len(events) != 3 || events[2].EventType != agentos.EventProcessSignalReceived {
		t.Fatalf("events = %#v, want signal as third event", events)
	}
}

func TestRuntimeControlProcessCancelsProcess(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleProcessSpec()

	if _, err := runtime.StartProcess(t.Context(), &spec); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}

	control := agentos.ControlRequest{
		Operation:      agentos.ControlCancel,
		IdempotencyKey: "cancel-1",
		RequestedAt:    spec.RequestedAt.Add(time.Minute),
		ActorID:        "operator-1",
	}
	if err := runtime.ControlProcess(t.Context(), processRefFromSpec(&spec), &control); err != nil {
		t.Fatalf("ControlProcess: %v", err)
	}

	status, err := runtime.StatusProcess(t.Context(), processRefFromSpec(&spec))
	if err != nil {
		t.Fatalf("StatusProcess: %v", err)
	}

	if status.LifecycleState != agentos.ProcessCanceled {
		t.Fatalf("lifecycle = %q, want canceled", status.LifecycleState)
	}
}

func TestRuntimeSubscribeProcessReplaysDurableEvents(t *testing.T) {
	t.Parallel()

	runtime := newSampleRuntime(t)
	spec := sampleProcessSpec()

	if _, err := runtime.StartProcess(t.Context(), &spec); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}

	sub, err := runtime.SubscribeProcess(t.Context(), &agentos.ProcessStreamScope{
		ProcessID: spec.ProcessID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	})
	if err != nil {
		t.Fatalf("SubscribeProcess: %v", err)
	}
	defer sub.Close()

	event, ok := <-sub.Events()
	if !ok {
		t.Fatal("subscription closed before replay event")
	}

	if event.ProcessID != spec.ProcessID || event.EventType != agentos.EventProcessStarted {
		t.Fatalf("event = %#v, want process.started for %q", event, spec.ProcessID)
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

func listProcessEvents(t *testing.T, runtime *Runtime, spec *agentos.ProcessSpec) []agentos.ProcessEvent {
	t.Helper()

	events, err := runtime.ListProcessEvents(t.Context(), &agentos.ProcessEventScope{
		ProcessID: spec.ProcessID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	})
	if err != nil {
		t.Fatalf("ListProcessEvents: %v", err)
	}

	return events
}

func processRefFromSpec(spec *agentos.ProcessSpec) agentos.ProcessRef {
	return agentos.ProcessRef{
		ProcessID: spec.ProcessID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
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
