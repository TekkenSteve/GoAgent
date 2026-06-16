package httpbackend

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime/agentosruntimetest"
)

func TestBackendConformance(t *testing.T) {
	probe := &agentosruntimetest.SubscriberProbe{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/runs":
			var req startRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode start: %v", err)
			}
			_ = json.NewEncoder(w).Encode(agentos.RunStatus{
				RunID:          req.RunID,
				LifecycleState: "created",
				UpdatedAt:      time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC),
			})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/signals"):
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/control"):
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/status"):
			_ = json.NewEncoder(w).Encode(agentos.RunStatus{
				RunID:          "agentos-conformance-run",
				LifecycleState: "running",
				UpdatedAt:      time.Date(2026, 6, 16, 12, 1, 0, 0, time.UTC),
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	backend, err := NewBackend(http.DefaultClient, probe, Config{
		Name:     "http-conformance",
		Endpoint: server.URL,
	})
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}

	agentosruntimetest.RunBackendConformance(t, agentosruntimetest.BackendConformanceCase{
		Name:            "http",
		Backend:         backend,
		Ref:             backend.config.Ref(),
		StatusState:     "running",
		SubscriberProbe: probe,
	})
}

func TestBackendStartPostsRunEnvelope(t *testing.T) {
	var got startRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/runs" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("X-AgentOS-Test") != "yes" {
			t.Fatalf("missing configured header")
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(agentos.RunStatus{
			RunID:          got.RunID,
			LifecycleState: "created",
			UpdatedAt:      time.Date(2026, 6, 16, 14, 0, 0, 0, time.UTC),
		})
	}))
	defer server.Close()

	backend := newTestBackend(t, server.URL)
	status, err := backend.Start(context.Background(), agentos.RunSpec{
		RunID:    "run-1",
		ThreadID: "thread-1",
		Backend:  backend.config.Ref(),
		Input: map[string]any{
			"task": "code",
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if got.RunID != "run-1" || got.ThreadID != "thread-1" || got.Input["task"] != "code" {
		t.Fatalf("unexpected start request: %#v", got)
	}
	if status.RunID != "run-1" || status.LifecycleState != "created" {
		t.Fatalf("unexpected status: %#v", status)
	}
}

func TestBackendSignalPostsSignal(t *testing.T) {
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
	err := backend.Signal(context.Background(), "run-1", agentos.Signal{
		Type: agentos.SignalUserMessage,
		Payload: map[string]any{
			"content": "continue",
		},
	})
	if err != nil {
		t.Fatalf("Signal: %v", err)
	}

	if got.Type != agentos.SignalUserMessage || got.Payload["content"] != "continue" {
		t.Fatalf("unexpected signal: %#v", got)
	}
}

func TestBackendControlPostsOperation(t *testing.T) {
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
	if err := backend.Control(context.Background(), "run-1", agentos.ControlCancel); err != nil {
		t.Fatalf("Control: %v", err)
	}

	if got.Operation != agentos.ControlCancel {
		t.Fatalf("operation = %q", got.Operation)
	}
}

func TestBackendStatusGetsRunStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/runs/run-1/status" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(agentos.RunStatus{
			RunID:          "run-1",
			LifecycleState: "running",
			Step:           3,
		})
	}))
	defer server.Close()

	backend := newTestBackend(t, server.URL)
	status, err := backend.Status(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}

	if status.RunID != "run-1" || status.LifecycleState != "running" || status.Step != 3 {
		t.Fatalf("unexpected status: %#v", status)
	}
}

func TestBackendRejectsErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "failed", http.StatusBadGateway)
	}))
	defer server.Close()

	backend := newTestBackend(t, server.URL)
	if _, err := backend.Status(context.Background(), "run-1"); err == nil {
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
