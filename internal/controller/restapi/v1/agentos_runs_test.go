package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/gofiber/fiber/v2"
)

const Run1 = "run-1"

func TestAgentOSRunRoutesUseRuntimeControlPlane(t *testing.T) {
	t.Parallel()

	runtime := &fakeAgentOSRuntime{}
	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, runtime, nil, nil, nil)

	startBody := `{"run_id": "run-1", "thread_id": "thread-1", "account_id": "acct-1", "backend": {"kind": "temporal_external", "name": "langgraph-main"}, "input": {"task": "plan"}}`

	resp := doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/runs", startBody)
	assertRunStartRoute(t, resp, runtime)

	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}

	signalBody := `{"type": "user.message", "idempotency_key": "msg-1", "payload": {"content": "continue"}}`

	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}

	resp = doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/runs/run-1/signals", signalBody)
	assertRunSignalRoute(t, resp, runtime)

	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}

	resp = doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/runs/run-1/control", `{"operation":"cancel"}`)
	assertRunControlRoute(t, resp, runtime)

	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}

	resp = doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/runs/run-1/status", "")
	assertRunStatusRoute(t, resp)

	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}
}

func assertRunStartRoute(t *testing.T, resp *http.Response, runtime *fakeAgentOSRuntime) {
	t.Helper()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("start status = %d", resp.StatusCode)
	}

	if runtime.started.RunID != Run1 ||
		runtime.started.ThreadID != "thread-1" ||
		runtime.started.Backend.Kind != agentos.BackendKindTemporalExternal ||
		runtime.started.Backend.Name != "langgraph-main" ||
		runtime.started.Input["task"] != "plan" {
		t.Fatalf("unexpected RunSpec: %#v", runtime.started)
	}
}

func assertRunSignalRoute(t *testing.T, resp *http.Response, runtime *fakeAgentOSRuntime) {
	t.Helper()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("signal status = %d", resp.StatusCode)
	}

	if runtime.signalRunID != Run1 ||
		runtime.signal.Type != agentoscore.SignalUserMessage ||
		runtime.signal.Payload["content"] != "continue" {
		t.Fatalf("unexpected signal: run=%q signal=%#v", runtime.signalRunID, runtime.signal)
	}
}

func assertRunControlRoute(t *testing.T, resp *http.Response, runtime *fakeAgentOSRuntime) {
	t.Helper()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("control status = %d", resp.StatusCode)
	}

	if runtime.controlRunID != Run1 || runtime.control != agentoscore.ControlCancel {
		t.Fatalf("unexpected control: run=%q op=%q", runtime.controlRunID, runtime.control)
	}
}

func assertRunStatusRoute(t *testing.T, resp *http.Response) {
	t.Helper()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d", resp.StatusCode)
	}

	var status agentos.RunStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}

	if status.RunID != Run1 || status.LifecycleState != "running" {
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
	signal       agentoscore.Signal
	controlRunID string
	control      agentoscore.ControlOperation
}

func (r *fakeAgentOSRuntime) Start(_ context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	r.started = *spec

	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: "created", UpdatedAt: time.Now()}, nil
}

func (r *fakeAgentOSRuntime) Signal(_ context.Context, runID string, signal *agentoscore.Signal) error {
	r.signalRunID = runID
	r.signal = *signal

	return nil
}

func (r *fakeAgentOSRuntime) Status(_ context.Context, runID string) (agentos.RunStatus, error) {
	return agentos.RunStatus{RunID: runID, LifecycleState: "running", UpdatedAt: time.Now()}, nil
}

func (r *fakeAgentOSRuntime) Control(_ context.Context, runID string, control *agentoscore.ControlRequest) error {
	r.controlRunID = runID
	r.control = control.Operation

	return nil
}

func (r *fakeAgentOSRuntime) Subscribe(context.Context, agentoscore.StreamScope) (agentoscore.Subscription, error) {
	return nil, nil
}

func (r *fakeAgentOSRuntime) Close() error {
	return nil
}
