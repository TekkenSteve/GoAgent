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
	"github.com/gofiber/fiber/v2"
)

const Run1 = "run-1"

func TestAgentOSRunRoutesUseRuntimeControlPlane(t *testing.T) {
	t.Parallel()

	runtime := &fakeAgentOSRuntime{}
	app := fiber.New()
	newTestRoutes(t, app, runtime, nil, nil, nil)

	// The account is not in the body: it comes from the authenticated principal.
	startBody := `{"run_id": "run-1", "thread_id": "thread-1", "project_id": "proj-1", "backend": {"kind": "temporal_external", "name": "langgraph-main"}, "input": {"task": "plan"}}`

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

	if runtime.signalAccountID != testAccountID {
		t.Fatalf("signal reached the runtime as account %q, want the authenticated %q", runtime.signalAccountID, testAccountID)
	}

	if runtime.signal.ActorID != testAccountID {
		t.Fatalf("signal actor = %q, want the credential's account", runtime.signal.ActorID)
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

	if runtime.controlAccountID != testAccountID {
		t.Fatalf("control reached the runtime as account %q, want the authenticated %q", runtime.controlAccountID, testAccountID)
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
	// The tenant each by-id operation was addressed with, so tests can assert
	// the handler took it from the credential.
	signalAccountID  string
	controlAccountID string
	controlActorID   string
	statusAccountID  string
}

func (r *fakeAgentOSRuntime) Start(_ context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	r.started = *spec

	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: "created", UpdatedAt: time.Now()}, nil
}

func (r *fakeAgentOSRuntime) Signal(_ context.Context, ref agentos.RunRef, signal *agentoscore.Signal) error {
	r.signalRunID = ref.RunID
	r.signalAccountID = ref.AccountID
	r.signal = *signal

	return nil
}

func (r *fakeAgentOSRuntime) Status(_ context.Context, ref agentos.RunRef) (agentos.RunStatus, error) {
	r.statusAccountID = ref.AccountID

	return agentos.RunStatus{RunID: ref.RunID, LifecycleState: "running", UpdatedAt: time.Now()}, nil
}

func (r *fakeAgentOSRuntime) Control(_ context.Context, ref agentos.RunRef, control *agentoscore.ControlRequest) error {
	r.controlRunID = ref.RunID
	r.controlAccountID = ref.AccountID
	r.controlActorID = control.ActorID
	r.control = control.Operation

	return nil
}

func (r *fakeAgentOSRuntime) Subscribe(context.Context, agentoscore.StreamScope) (agentoscore.Subscription, error) {
	return nil, nil
}

func (r *fakeAgentOSRuntime) Close() error {
	return nil
}

// TestAgentOSRunStartIgnoresAReportedAccount is the regression guard for
// self-reported tenancy: a caller that names an account in the body must not be
// able to start work in it. The account comes from the credential, so the
// reported value is inert data the decoder does not even bind.
func TestAgentOSRunStartIgnoresAReportedAccount(t *testing.T) {
	t.Parallel()

	runtime := &fakeAgentOSRuntime{}
	app := fiber.New()
	newTestRoutes(t, app, runtime, nil, nil, nil)

	body := `{"run_id": "run-1", "project_id": "proj-1", "account_id": "acct-2", "backend": {"kind": "temporal_external", "name": "langgraph-main"}}`

	resp := doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/runs", body)
	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("start status = %d, want 202", resp.StatusCode)
	}

	if runtime.started.AccountID != testAccountID {
		t.Fatalf("RunSpec.AccountID = %q, want the authenticated account %q", runtime.started.AccountID, testAccountID)
	}

	if runtime.started.ProjectID != "proj-1" {
		t.Fatalf("RunSpec.ProjectID = %q, want the requested project", runtime.started.ProjectID)
	}
}

// TestAgentOSRunStartRequiresAProject pins the addressing rule: a run must say
// which project it belongs to, because every store keys rows by
// (account_id, project_id).
func TestAgentOSRunStartRequiresAProject(t *testing.T) {
	t.Parallel()

	runtime := &fakeAgentOSRuntime{}
	app := fiber.New()
	newTestRoutes(t, app, runtime, nil, nil, nil)

	body := `{"run_id": "run-1", "backend": {"kind": "temporal_external", "name": "langgraph-main"}}`

	resp := doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/runs", body)
	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("start status = %d, want 400", resp.StatusCode)
	}
}

// TestAgentOSRunStartWithoutAPrincipalIsUnauthenticated covers the wiring bug
// this surface used to ship: a route reachable without an identity must fail
// closed rather than run with an empty tenant.
func TestAgentOSRunStartWithoutAPrincipalIsUnauthenticated(t *testing.T) {
	t.Parallel()

	runtime := &fakeAgentOSRuntime{}
	app := fiber.New()
	newTestRoutesUnauthenticated(app, runtime, nil, nil, nil)

	body := `{"run_id": "run-1", "project_id": "proj-1", "backend": {"kind": "temporal_external", "name": "langgraph-main"}}`

	resp := doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/runs", body)
	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("start status = %d, want 401", resp.StatusCode)
	}

	if runtime.started.RunID != "" {
		t.Fatalf("an unauthenticated request must not reach the runtime, got %#v", runtime.started)
	}
}

// TestAgentOSRunStatusCarriesTheCredentialedAccount covers the read half of
// the by-id surface: the body cannot name another account, because the
// reference is built from the credential and enforced by the runtime.
func TestAgentOSRunStatusCarriesTheCredentialedAccount(t *testing.T) {
	t.Parallel()

	runtime := &fakeAgentOSRuntime{}
	app := fiber.New()
	newTestRoutes(t, app, runtime, nil, nil, nil)

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/runs/"+Run1+"/status", "")
	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	if runtime.statusAccountID != testAccountID {
		t.Fatalf("status used account %q, want the authenticated %q", runtime.statusAccountID, testAccountID)
	}
}

// TestAgentOSRunSignalCarriesTheCredentialedAccountAndActor covers the write
// half: the account comes from the credential even though the body names
// another, and the audit actor is the credential's rather than the caller's
// claim.
func TestAgentOSRunSignalCarriesTheCredentialedAccountAndActor(t *testing.T) {
	t.Parallel()

	runtime := &fakeAgentOSRuntime{}
	app := fiber.New()
	newTestRoutes(t, app, runtime, nil, nil, nil)

	body := `{"type": "user_message", "account_id": "acct-2", "actor_id": "someone-else", "payload": {"content": "continue"}}`

	resp := doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/runs/"+Run1+"/signals", body)
	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	if runtime.signalAccountID != testAccountID {
		t.Fatalf("signal used account %q, want the authenticated %q", runtime.signalAccountID, testAccountID)
	}

	if runtime.signal.ActorID != testAccountID {
		t.Fatalf("signal actor = %q, want the credential's account %q", runtime.signal.ActorID, testAccountID)
	}
}

// TestAgentOSRunControlCarriesTheCredentialedAccountAndActor is the control
// half of the same rule.
func TestAgentOSRunControlCarriesTheCredentialedAccountAndActor(t *testing.T) {
	t.Parallel()

	runtime := &fakeAgentOSRuntime{}
	app := fiber.New()
	newTestRoutes(t, app, runtime, nil, nil, nil)

	body := `{"operation": "cancel", "account_id": "acct-2", "actor_id": "someone-else"}`

	resp := doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/runs/"+Run1+"/control", body)
	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	if runtime.controlAccountID != testAccountID {
		t.Fatalf("control used account %q, want the authenticated %q", runtime.controlAccountID, testAccountID)
	}

	if runtime.controlActorID != testAccountID {
		t.Fatalf("control actor = %q, want the credential's account %q", runtime.controlActorID, testAccountID)
	}
}
