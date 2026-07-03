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

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime/agentosruntimetest"
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

	backend, err := NewBackend(http.DefaultClient, probe, Config{
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

	backend := newTestBackend(t, server.URL)

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

	backend := newTestBackend(t, server.URL)

	signal := agentos.Signal{
		Type: agentos.SignalUserMessage,
		Payload: map[string]any{
			"content": "continue",
		},
	}

	err := backend.Signal(context.Background(), Run1, &signal)
	if err != nil {
		t.Fatalf("Signal: %v", err)
	}

	if got.Type != agentos.SignalUserMessage || got.Payload["content"] != "continue" {
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

	backend := newTestBackend(t, server.URL)
	control := agentos.ControlRequest{Operation: agentos.ControlCancel, IdempotencyKey: "control-1"}

	if err := backend.Control(context.Background(), Run1, &control); err != nil {
		t.Fatalf("Control: %v", err)
	}

	if got.Operation != agentos.ControlCancel {
		t.Fatalf("operation = %q", got.Operation)
	}

	if got.IdempotencyKey != "control-1" {
		t.Fatalf("idempotency_key = %q", got.IdempotencyKey)
	}
}

func TestBackendRejectsNilRunInputs(t *testing.T) {
	t.Parallel()

	backend := &Backend{config: Config{Name: "http-test"}}

	if _, err := backend.Start(context.Background(), nil); !errors.Is(err, agentos.ErrInvalidRunSpec) {
		t.Fatalf("Start nil error = %v, want ErrInvalidRunSpec", err)
	}

	if err := backend.Signal(context.Background(), Run1, nil); !errors.Is(err, agentos.ErrInvalidSignal) {
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
			Progress:       &agentos.RunProgress{Current: 3, Total: 10, Label: "draft"},
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}
	}))
	defer server.Close()

	backend := newTestBackend(t, server.URL)

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

	backend := newTestBackend(t, server.URL)
	if _, err := backend.Status(context.Background(), Run1); err == nil {
		t.Fatal("expected error")
	}
}

func newTestBackend(t *testing.T, endpoint string) *Backend {
	t.Helper()

	backend, err := NewBackend(http.DefaultClient, nil, Config{
		Name:     "claude-code",
		Endpoint: endpoint,
		Headers: map[string]string{
			"X-AgentOS-Test": "yes",
		},
	})
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}

	return backend
}
