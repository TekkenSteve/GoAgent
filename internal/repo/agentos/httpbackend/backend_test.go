package httpbackend

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/memstream"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime/agentosruntimetest"
	"github.com/stretchr/testify/require"
)

const (
	_RUNS = "/runs"
	Run1  = "run-1"
)

func TestBackendConformance(t *testing.T) {
	t.Parallel()

	probe := &agentosruntimetest.SubscriberProbe{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleConformanceRequest(t, w, r)
	}))
	defer server.Close()

	backend, err := NewBackend(server.Client(), probe, nil, Config{
		Name:     "http-conformance",
		Endpoint: server.URL,
	})
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}

	agentosruntimetest.RunBackendConformance(t, &agentosruntimetest.BackendConformanceCase{
		Name:            "http",
		Backend:         backend,
		Ref:             backend.config.Ref(),
		StatusState:     "running",
		SubscriberProbe: probe,
	})
}

func handleConformanceRequest(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()

	switch {
	case r.Method == http.MethodPost && r.URL.Path == _RUNS:
		handleConformanceStart(t, w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/signals"):
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/control"):
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/status"):
		writeConformanceStatus(w)
	default:
		http.NotFound(w, r)
	}
}

func handleConformanceStart(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()

	var req startRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		t.Fatalf("decode start: %v", err)
	}

	status := agentos.RunStatus{
		RunID:          req.RunID,
		LifecycleState: "created",
		UpdatedAt:      time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC),
	}
	writeJSONStatus(w, &status)
}

func writeConformanceStatus(w http.ResponseWriter) {
	writeJSONStatus(w, &agentos.RunStatus{
		RunID:          "agentos-conformance-run",
		LifecycleState: "running",
		UpdatedAt:      time.Date(2026, 6, 16, 12, 1, 0, 0, time.UTC),
	})
}

func writeJSONStatus(w http.ResponseWriter, status *agentos.RunStatus) {
	if err := json.NewEncoder(w).Encode(status); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func TestBackendStartPostsRunEnvelope(t *testing.T) {
	t.Parallel()

	var got startRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleStartEnvelopeTestRequest(t, w, r, &got)
	}))
	defer server.Close()

	backend := newTestBackend(t, server)

	spec := agentos.RunSpec{
		RunID:    Run1,
		ThreadID: "thread-1",
		Backend:  backend.config.Ref(),
		Input: map[string]any{
			"task": "code",
		},
	}

	status, err := backend.Start(context.Background(), &spec)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	assertStartEnvelopeRequest(t, &got)
	assertCreatedRunStatus(t, &status)
}

func handleStartEnvelopeTestRequest(t *testing.T, w http.ResponseWriter, r *http.Request, got *startRequest) {
	t.Helper()

	if r.Method != http.MethodPost || r.URL.Path != _RUNS {
		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
	}

	if r.Header.Get("X-AgentOS-Test") != "yes" {
		t.Fatalf("missing configured header")
	}

	if err := json.NewDecoder(r.Body).Decode(got); err != nil {
		t.Fatalf("decode request: %v", err)
	}

	if err := json.NewEncoder(w).Encode(agentos.RunStatus{
		RunID:          got.RunID,
		LifecycleState: "created",
		UpdatedAt:      time.Date(2026, 6, 16, 14, 0, 0, 0, time.UTC),
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func assertStartEnvelopeRequest(t *testing.T, got *startRequest) {
	t.Helper()

	if got.RunID != Run1 || got.ThreadID != "thread-1" || got.Input["task"] != "code" {
		t.Fatalf("unexpected start request: %#v", got)
	}
}

func assertCreatedRunStatus(t *testing.T, status *agentos.RunStatus) {
	t.Helper()

	if status.RunID != Run1 || status.LifecycleState != "created" {
		t.Fatalf("unexpected status: %#v", status)
	}
}

func TestBackendSignalPostsSignal(t *testing.T) {
	t.Parallel()

	var got signalRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/runs/run-1/signals" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}

		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	backend := newTestBackend(t, server)

	signal := agentoscore.Signal{
		Type: agentoscore.SignalUserMessage,
		Payload: map[string]any{
			"content": "continue",
		},
	}

	err := backend.Signal(context.Background(), Run1, &signal)
	if err != nil {
		t.Fatalf("Signal: %v", err)
	}

	if got.Type != agentoscore.SignalUserMessage || got.Payload["content"] != "continue" {
		t.Fatalf("unexpected signal: %#v", got)
	}
}

func TestBackendControlPostsOperation(t *testing.T) {
	t.Parallel()

	var got controlRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/runs/run-1/control" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}

		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	backend := newTestBackend(t, server)
	control := agentoscore.ControlRequest{Operation: agentoscore.ControlCancel, IdempotencyKey: "control-1"}

	if err := backend.Control(context.Background(), Run1, &control); err != nil {
		t.Fatalf("Control: %v", err)
	}

	if got.Operation != agentoscore.ControlCancel {
		t.Fatalf("operation = %q", got.Operation)
	}

	if got.IdempotencyKey != "control-1" {
		t.Fatalf("idempotency_key = %q", got.IdempotencyKey)
	}
}

func TestBackendRejectsNilRunInputs(t *testing.T) {
	t.Parallel()

	backend := &Backend{config: Config{Name: "http-test"}}

	if _, err := backend.Start(context.Background(), nil); !errors.Is(err, agentoscore.ErrInvalidRunSpec) {
		t.Fatalf("Start nil error = %v, want ErrInvalidRunSpec", err)
	}

	if err := backend.Signal(context.Background(), Run1, nil); !errors.Is(err, agentoscore.ErrInvalidSignal) {
		t.Fatalf("Signal nil error = %v, want ErrInvalidSignal", err)
	}
}

func TestBackendStatusGetsRunStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/runs/run-1/status" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}

		if err := json.NewEncoder(w).Encode(agentos.RunStatus{
			RunID:          Run1,
			LifecycleState: "running",
			Progress:       &agentoscore.RunProgress{Current: 3, Total: 10, Label: "draft"},
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}
	}))
	defer server.Close()

	backend := newTestBackend(t, server)

	status, err := backend.Status(context.Background(), Run1)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}

	if status.RunID != Run1 || status.LifecycleState != "running" || status.Progress == nil || status.Progress.Current != 3 {
		t.Fatalf("unexpected status: %#v", status)
	}
}

func TestBackendRejectsErrorStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "failed", http.StatusBadGateway)
	}))
	defer server.Close()

	backend := newTestBackend(t, server)
	if _, err := backend.Status(context.Background(), Run1); err == nil {
		t.Fatal("expected error")
	}
}

func newTestBackend(t *testing.T, server *httptest.Server) *Backend {
	t.Helper()

	backend, err := NewBackend(server.Client(), nil, nil, Config{
		Name:     "claude-code",
		Endpoint: server.URL,
		Headers: map[string]string{
			"X-AgentOS-Test": "yes",
		},
	})
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}

	return backend
}

// newRunTimelineServer serves the run lifecycle a data-plane test drives: Start
// returns a running state, the status endpoint reports the run completed.
func newRunTimelineServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == _RUNS:
			writeJSONStatus(w, &agentos.RunStatus{RunID: Run1, LifecycleState: "running"})
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/status"):
			writeJSONStatus(w, &agentos.RunStatus{RunID: Run1, LifecycleState: "completed"})
		default:
			http.NotFound(w, r)
		}
	}))
}

// TestBackendPublishesLifecycleToDataPlane verifies Start and Status forward
// their observations to the data-plane lifecycle adapter: a successful Start
// opens the run and the terminal Status the remote reports closes it.
func TestBackendPublishesLifecycleToDataPlane(t *testing.T) {
	t.Parallel()

	probe := &agentosruntimetest.LifecycleProbe{}

	server := newRunTimelineServer()
	defer server.Close()

	backend, err := NewBackend(server.Client(), nil, probe, Config{
		Name:     "http-lifecycle",
		Endpoint: server.URL,
	})
	require.NoError(t, err)

	spec := agentos.RunSpec{RunID: Run1, ThreadID: "thread-1", AccountID: "acme", Backend: backend.config.Ref()}

	started, err := backend.Start(context.Background(), &spec)
	require.NoError(t, err)
	require.Equal(t, "running", started.LifecycleState)
	require.Equal(t, spec, probe.LastStartedSpec())
	require.Equal(t, started, probe.LastStartedStatus())

	status, err := backend.Status(context.Background(), Run1)
	require.NoError(t, err)
	require.Equal(t, "completed", status.LifecycleState)
	require.Equal(t, []agentos.RunStatus{status}, probe.Statuses())
}

// TestBackendPublishesRunTimelineToBus is the reference-implementation
// acceptance demo: a real backend, a real RunLifecycle adapter, and a real bus.
// Start publishes RUN_STARTED; the terminal Status the remote reports publishes
// RUN_FINISHED — the full §7 映射→发布 contract end to end.
func TestBackendPublishesRunTimelineToBus(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	lifecycle := streamadapter.NewRunLifecycle(bus, nil)

	server := newRunTimelineServer()
	defer server.Close()

	backend, err := NewBackend(server.Client(), nil, lifecycle, Config{
		Name:     "http-timeline",
		Endpoint: server.URL,
	})
	require.NoError(t, err)

	sub, err := bus.Subscribe(t.Context(), streamadapter.HandleForRun("acme", Run1), 0)
	require.NoError(t, err)

	t.Cleanup(sub.Close)

	spec := agentos.RunSpec{RunID: Run1, ThreadID: "thread-1", AccountID: "acme", Backend: backend.config.Ref()}

	_, err = backend.Start(t.Context(), &spec)
	require.NoError(t, err)
	requireBusEvent(t, sub, stream.EventRunStarted)

	_, err = backend.Status(t.Context(), Run1)
	require.NoError(t, err)
	requireBusEvent(t, sub, stream.EventRunFinished)
}

func requireBusEvent(t *testing.T, sub *stream.Subscription, want stream.EventType) {
	t.Helper()

	select {
	case stored := <-sub.C:
		require.Equal(t, want, stored.Event.Type)
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", want)
	}
}
