package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/gofiber/fiber/v2"
)

func TestAgentOSRunRoutesUseRuntimeControlPlane(t *testing.T) {
	runtime := &fakeAgentOSRuntime{}
	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, runtime, nil)

	startBody := `{
		"run_id": "run-1",
		"thread_id": "thread-1",
		"account_id": "acct-1",
		"backend": {"kind": "temporal_external", "name": "langgraph-main"},
		"input": {"task": "plan"}
	}`
	resp := doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/runs", startBody)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("start status = %d", resp.StatusCode)
	}
	if runtime.started.RunID != "run-1" ||
		runtime.started.ThreadID != "thread-1" ||
		runtime.started.Backend.Kind != agentos.BackendKindTemporalExternal ||
		runtime.started.Backend.Name != "langgraph-main" ||
		runtime.started.Input["task"] != "plan" {
		t.Fatalf("unexpected RunSpec: %#v", runtime.started)
	}

	signalBody := `{
		"type": "user.message",
		"idempotency_key": "msg-1",
		"payload": {"content": "continue"}
	}`
	resp = doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/runs/run-1/signals", signalBody)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("signal status = %d", resp.StatusCode)
	}
	if runtime.signalRunID != "run-1" ||
		runtime.signal.Type != agentos.SignalUserMessage ||
		runtime.signal.Payload["content"] != "continue" {
		t.Fatalf("unexpected signal: run=%q signal=%#v", runtime.signalRunID, runtime.signal)
	}

	resp = doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/runs/run-1/control", `{"operation":"cancel"}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("control status = %d", resp.StatusCode)
	}
	if runtime.controlRunID != "run-1" || runtime.control != agentos.ControlCancel {
		t.Fatalf("unexpected control: run=%q op=%q", runtime.controlRunID, runtime.control)
	}

	resp = doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/runs/run-1/status", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d", resp.StatusCode)
	}
	var status agentos.RunStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.RunID != "run-1" || status.LifecycleState != "running" {
		t.Fatalf("unexpected status: %#v", status)
	}
}

func doAgentOSRouteRequest(t *testing.T, app *fiber.App, method, target, body string) *http.Response {
	t.Helper()

	var requestBody *bytes.Reader
	if body == "" {
		requestBody = bytes.NewReader(nil)
	} else {
		requestBody = bytes.NewReader([]byte(body))
	}
	req := httptest.NewRequestWithContext(context.Background(), method, target, requestBody)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, target, err)
	}

	return resp
}

type fakeAgentOSRuntime struct {
	started      agentos.RunSpec
	signalRunID  string
	signal       agentos.Signal
	controlRunID string
	control      agentos.ControlOperation
}

func (r *fakeAgentOSRuntime) Start(_ context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	r.started = spec

	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: "created", UpdatedAt: time.Now()}, nil
}

func (r *fakeAgentOSRuntime) Signal(_ context.Context, runID string, signal agentos.Signal) error {
	r.signalRunID = runID
	r.signal = signal

	return nil
}

func (r *fakeAgentOSRuntime) Status(_ context.Context, runID string) (agentos.RunStatus, error) {
	return agentos.RunStatus{RunID: runID, LifecycleState: "running", UpdatedAt: time.Now()}, nil
}

func (r *fakeAgentOSRuntime) Control(_ context.Context, runID string, control agentos.ControlRequest) error {
	r.controlRunID = runID
	r.control = control.Operation

	return nil
}

func (r *fakeAgentOSRuntime) Subscribe(context.Context, agentos.StreamScope) (agentos.Subscription, error) {
	return nil, nil
}

func (r *fakeAgentOSRuntime) Close() error {
	return nil
}
