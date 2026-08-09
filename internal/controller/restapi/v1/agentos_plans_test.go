package v1

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/TekkenSteve/GoAgent/pkg/sse"
	"github.com/gofiber/fiber/v2"
)

const (
	plan1      = "plan-1"
	thread1    = "thread-1"
	account1   = "acct-1"
	project1   = "proj-1"
	operator1  = "operator-1"
	research   = "research"
	artifact1  = "artifact-1"
	planEvent1 = "plan-event-1"
)

func TestAgentOSPlanRoutesUsePlanRuntime(t *testing.T) {
	t.Parallel()

	planRuntime := newFakePlanRuntime()
	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, planRuntime, nil)

	testPlanStartRoute(t, app, planRuntime)
	testPlanSignalRoute(t, app, planRuntime)
	testPlanControlRoute(t, app, planRuntime)
	testPlanStatusRoute(t, app, planRuntime)
	testPlanDescriptionRoute(t, app, planRuntime)
	testPlanAuditsRoute(t, app, planRuntime)
	testPlanArtifactsRoute(t, app, planRuntime)
	testPlanArtifactGetRoute(t, app, planRuntime)
	testPlanEventHistoryRoute(t, app, planRuntime)
	testPlanDebugTracesRoute(t, app, planRuntime)
}

func testPlanStartRoute(t *testing.T, app *fiber.App, planRuntime *fakePlanRuntime) {
	t.Helper()

	startBody := `{
		"plan_id": "plan-1", "thread_id": "thread-1", "account_id": "acct-1", "project_id": "proj-1",
		"idempotency_key": "plan-start-1", "inputs": {"topic": "durable coordination"},
		"nodes": [{"node_id": "research", "run": {"run_id": "run-research", "account_id": "acct-1", "project_id": "proj-1", "backend": {"kind": "http", "name": "research-http"}, "input": {"task": "research"}}}]
	}`

	resp := doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/plans", startBody)
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("start status = %d", resp.StatusCode)
	}

	if planRuntime.started.PlanID != plan1 ||
		planRuntime.started.ThreadID != thread1 ||
		planRuntime.started.Inputs["topic"] != "durable coordination" ||
		planRuntime.started.Nodes[0].Run.Backend.Name != "research-http" {
		t.Fatalf("unexpected RunPlanSpec: %#v", planRuntime.started)
	}
}

func testPlanSignalRoute(t *testing.T, app *fiber.App, planRuntime *fakePlanRuntime) {
	t.Helper()

	signalBody := `{"type": "plan.node.retry", "account_id": "acct-1", "project_id": "proj-1", "idempotency_key": "retry-1", "actor_id": "operator-1", "payload": {"node_id": "research"}}`

	resp := doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/plans/plan-1/signals", signalBody)
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("signal status = %d", resp.StatusCode)
	}

	if planRuntime.signalRef.PlanID != plan1 ||
		planRuntime.signalRef.AccountID != account1 ||
		planRuntime.signalRef.ProjectID != project1 ||
		planRuntime.signal.Type != agentoscore.SignalPlanNodeRetry ||
		planRuntime.signal.ActorID != operator1 ||
		planRuntime.signal.Payload["node_id"] != research {
		t.Fatalf("unexpected signal: ref=%#v signal=%#v", planRuntime.signalRef, planRuntime.signal)
	}
}

func testPlanControlRoute(t *testing.T, app *fiber.App, planRuntime *fakePlanRuntime) {
	t.Helper()

	controlBody := `{"operation": "pause", "account_id": "acct-1", "project_id": "proj-1", "idempotency_key": "pause-1", "actor_id": "operator-1"}`

	resp := doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/plans/plan-1/control", controlBody)
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("control status = %d", resp.StatusCode)
	}

	if planRuntime.controlRef.PlanID != plan1 ||
		planRuntime.controlRef.AccountID != account1 ||
		planRuntime.controlRef.ProjectID != project1 ||
		planRuntime.control.Operation != agentoscore.ControlPause ||
		planRuntime.control.ActorID != operator1 {
		t.Fatalf("unexpected control: ref=%#v control=%#v", planRuntime.controlRef, planRuntime.control)
	}
}

func testPlanStatusRoute(t *testing.T, app *fiber.App, _ *fakePlanRuntime) {
	t.Helper()

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/status?account_id=acct-1&project_id=proj-1", "")
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d", resp.StatusCode)
	}

	var status agentos.RunPlanStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}

	if status.PlanID != plan1 || status.LifecycleState != agentos.PlanLifecycleRunning {
		t.Fatalf("unexpected status: %#v", status)
	}
}

func testPlanDescriptionRoute(t *testing.T, app *fiber.App, _ *fakePlanRuntime) {
	t.Helper()

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/description?account_id=acct-1&project_id=proj-1", "")
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("description status = %d", resp.StatusCode)
	}

	var description agentos.RunPlanDescription
	if err := json.NewDecoder(resp.Body).Decode(&description); err != nil {
		t.Fatalf("decode description: %v", err)
	}

	if description.PlanID != plan1 ||
		len(description.Topology.Nodes) != 2 ||
		len(description.Topology.Edges) != 1 ||
		description.Topology.Edges[0].From != research ||
		description.Topology.Edges[0].To != "write" {
		t.Fatalf("unexpected description: %#v", description)
	}
}

func testPlanAuditsRoute(t *testing.T, app *fiber.App, planRuntime *fakePlanRuntime) {
	t.Helper()

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/audits?account_id=acct-1&project_id=proj-1&action=plan.control&limit=25", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("audits status = %d", resp.StatusCode)
	}

	assertPlanAuditScope(t, &planRuntime.auditScope)

	var audits []agentos.PlanAuditRecord
	if err := json.NewDecoder(resp.Body).Decode(&audits); err != nil {
		t.Fatalf("decode audits: %v", err)
	}

	if len(audits) != 1 || audits[0].Action != agentos.PlanAuditActionControl {
		t.Fatalf("unexpected audits: %#v", audits)
	}

	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}
}

func testPlanArtifactsRoute(t *testing.T, app *fiber.App, planRuntime *fakePlanRuntime) {
	t.Helper()

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/artifacts?account_id=acct-1&project_id=proj-1&node_id=research&limit=10", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("artifacts status = %d", resp.StatusCode)
	}

	assertPlanArtifactScope(t, &planRuntime.artifactScope)

	var refs []agentoscore.ArtifactRef
	if err := json.NewDecoder(resp.Body).Decode(&refs); err != nil {
		t.Fatalf("decode artifacts: %v", err)
	}

	if len(refs) != 1 || refs[0].ArtifactID != artifact1 {
		t.Fatalf("unexpected artifacts: %#v", refs)
	}

	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}
}

func testPlanArtifactGetRoute(t *testing.T, app *fiber.App, planRuntime *fakePlanRuntime) {
	t.Helper()

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/artifacts/artifact-1?account_id=acct-1&project_id=proj-1", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("artifact status = %d", resp.StatusCode)
	}

	assertPlanArtifactGetScope(t, &planRuntime.artifactGetScope)

	var artifact agentoscore.Artifact
	if err := json.NewDecoder(resp.Body).Decode(&artifact); err != nil {
		t.Fatalf("decode artifact: %v", err)
	}

	payload, ok := artifact.Payload.(map[string]any)
	if !ok {
		t.Fatalf("artifact payload type = %T", artifact.Payload)
	}

	if artifact.Ref.ArtifactID != artifact1 || payload["summary"] != "ok" {
		t.Fatalf("unexpected artifact: %#v", artifact)
	}

	if err := resp.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}
}

func assertPlanAuditScope(t *testing.T, scope *agentos.PlanAuditScope) {
	t.Helper()

	if scope.PlanID != plan1 ||
		scope.AccountID != account1 ||
		scope.ProjectID != project1 ||
		scope.Action != agentos.PlanAuditActionControl ||
		scope.Limit != 25 {
		t.Fatalf("unexpected audit scope: %#v", scope)
	}
}

func assertPlanArtifactScope(t *testing.T, scope *agentos.PlanArtifactScope) {
	t.Helper()

	if scope.PlanID != plan1 ||
		scope.AccountID != account1 ||
		scope.ProjectID != project1 ||
		scope.NodeID != research ||
		scope.Limit != 10 {
		t.Fatalf("unexpected artifact scope: %#v", scope)
	}
}

func assertPlanArtifactGetScope(t *testing.T, scope *agentos.PlanArtifactScope) {
	t.Helper()

	if scope.PlanID != plan1 ||
		scope.AccountID != account1 ||
		scope.ProjectID != project1 ||
		scope.ArtifactID != artifact1 {
		t.Fatalf("unexpected artifact get scope: %#v", scope)
	}
}

func testPlanEventHistoryRoute(t *testing.T, app *fiber.App, planRuntime *fakePlanRuntime) {
	t.Helper()

	planEvents := getPlanRouteJSON[[]agentos.PlanEvent](t, app, "/v1/agentos/plans/plan-1/events/history?account_id=acct-1&project_id=proj-1&node_id=research&run_id=run-research&after_sequence=7&limit=3", "event history")
	assertPlanEventHistoryScope(t, &planRuntime.eventScope)
	assertPlanEventHistory(t, planEvents)
}

func getPlanRouteJSON[T any](t *testing.T, app *fiber.App, target, label string) T {
	t.Helper()

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, target, "")
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status = %d", label, resp.StatusCode)
	}

	var value T
	if err := json.NewDecoder(resp.Body).Decode(&value); err != nil {
		t.Fatalf("decode %s: %v", label, err)
	}

	return value
}

func assertPlanEventHistoryScope(t *testing.T, scope *agentos.PlanEventScope) {
	t.Helper()

	if scope.PlanID != plan1 ||
		scope.AccountID != account1 ||
		scope.ProjectID != project1 ||
		scope.NodeID != research ||
		scope.RunID != "run-research" ||
		scope.AfterSequence != 7 ||
		scope.Limit != 3 {
		t.Fatalf("unexpected event scope: %#v", scope)
	}
}

func assertPlanEventHistory(t *testing.T, planEvents []agentos.PlanEvent) {
	t.Helper()

	if len(planEvents) != 1 || planEvents[0].PlanID != plan1 || planEvents[0].NodeID != research {
		t.Fatalf("unexpected event history: %#v", planEvents)
	}
}

func testPlanDebugTracesRoute(t *testing.T, app *fiber.App, planRuntime *fakePlanRuntime) {
	t.Helper()

	traces := getPlanRouteJSON[[]agentos.PlanDebugTrace](t, app, "/v1/agentos/plans/plan-1/debug/traces?account_id=acct-1&project_id=proj-1&node_id=research&after_sequence=7&limit=3", "debug traces")
	assertPlanDebugScope(t, &planRuntime.debugScope)
	assertPlanDebugTraces(t, traces)
}

func assertPlanDebugScope(t *testing.T, scope *agentos.PlanDebugTraceScope) {
	t.Helper()

	if scope.PlanID != plan1 ||
		scope.AccountID != account1 ||
		scope.ProjectID != project1 ||
		scope.NodeID != research ||
		scope.AfterSequence != 7 ||
		scope.Limit != 3 {
		t.Fatalf("unexpected debug scope: %#v", scope)
	}
}

func assertPlanDebugTraces(t *testing.T, traces []agentos.PlanDebugTrace) {
	t.Helper()

	if len(traces) != 1 ||
		traces[0].EventType != agentoscore.EventNodeInputResolved ||
		traces[0].InputResolution == nil ||
		traces[0].InputResolution.MappingCount != 1 {
		t.Fatalf("unexpected debug traces: %#v", traces)
	}
}

func TestAgentOSPlanControlAndSignalRoutesCoverConsoleActions(t *testing.T) {
	t.Parallel()
	runControlOrSignalConsoleCase(t, []controlOrSignalCase{
		{name: "pause", route: "/v1/agentos/plans/plan-1/control", body: `{"operation":"pause","account_id":"acct-1","project_id":"proj-1","idempotency_key":"pause-1","actor_id":"operator-1"}`, wantOp: agentoscore.ControlPause, wantKey: "pause-1", wantActor: "operator-1"},
		{name: "resume", route: "/v1/agentos/plans/plan-1/control", body: `{"operation":"resume","account_id":"acct-1","project_id":"proj-1","idempotency_key":"resume-1","actor_id":"operator-1"}`, wantOp: agentoscore.ControlResume, wantKey: "resume-1", wantActor: operator1},
		{name: "cancel", route: "/v1/agentos/plans/plan-1/control", body: `{"operation":"cancel","account_id":"acct-1","project_id":"proj-1","idempotency_key":"cancel-1","actor_id":"operator-1"}`, wantOp: agentoscore.ControlCancel, wantKey: "cancel-1", wantActor: "operator-1"},
		{name: "retry", route: "/v1/agentos/plans/plan-1/signals", body: `{"type":"plan.node.retry","account_id":"acct-1","project_id":"proj-1","idempotency_key":"retry-1","actor_id":"operator-1","payload":{"node_id":"research"}}`, wantType: agentoscore.SignalPlanNodeRetry, wantKey: "retry-1", wantActor: operator1},
		{name: "approve", route: "/v1/agentos/plans/plan-1/signals", body: `{"type":"plan.approve","account_id":"acct-1","project_id":"proj-1","idempotency_key":"approve-1","actor_id":"operator-1"}`, wantType: agentoscore.SignalPlanApprove, wantKey: "approve-1", wantActor: "operator-1"},
		{name: "reject", route: "/v1/agentos/plans/plan-1/signals", body: `{"type":"plan.reject","account_id":"acct-1","project_id":"proj-1","idempotency_key":"reject-1","actor_id":"operator-1","payload":{"reason":"operator rejected"}}`, wantType: agentoscore.SignalPlanReject, wantKey: "reject-1", wantActor: operator1},
	})
}

type controlOrSignalCase struct {
	name      string
	route     string
	body      string
	wantOp    agentoscore.ControlOperation
	wantType  agentoscore.SignalType
	wantKey   string
	wantActor string
}

func runControlOrSignalConsoleCase(t *testing.T, cases []controlOrSignalCase) {
	t.Helper()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			planRuntime := newFakePlanRuntime()
			app := fiber.New()
			NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, planRuntime, nil)

			resp := doAgentOSRouteRequest(t, app, http.MethodPost, tc.route, tc.body)
			t.Cleanup(func() {
				if err := resp.Body.Close(); err != nil {
					t.Errorf("close response body: %v", err)
				}
			})

			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("%s status = %d", tc.name, resp.StatusCode)
			}

			assertControlOrSignalCase(t, planRuntime, &tc)
		})
	}
}

func assertControlOrSignalCase(t *testing.T, planRuntime *fakePlanRuntime, tc *controlOrSignalCase) {
	t.Helper()

	if tc.wantOp != "" {
		assertConsoleControl(t, planRuntime, tc)

		return
	}

	assertConsoleSignal(t, planRuntime, tc)
}

func assertConsoleControl(t *testing.T, planRuntime *fakePlanRuntime, tc *controlOrSignalCase) {
	t.Helper()

	if planRuntime.controlRef.PlanID != plan1 ||
		planRuntime.controlRef.AccountID != account1 ||
		planRuntime.controlRef.ProjectID != project1 ||
		planRuntime.control.Operation != tc.wantOp ||
		planRuntime.control.IdempotencyKey != tc.wantKey ||
		planRuntime.control.ActorID != tc.wantActor {
		t.Fatalf("unexpected control: ref=%#v control=%#v", planRuntime.controlRef, planRuntime.control)
	}
}

func assertConsoleSignal(t *testing.T, planRuntime *fakePlanRuntime, tc *controlOrSignalCase) {
	t.Helper()

	if planRuntime.signalRef.PlanID != plan1 ||
		planRuntime.signalRef.AccountID != account1 ||
		planRuntime.signalRef.ProjectID != project1 ||
		planRuntime.signal.Type != tc.wantType ||
		planRuntime.signal.IdempotencyKey != tc.wantKey ||
		planRuntime.signal.ActorID != tc.wantActor {
		t.Fatalf("unexpected signal: ref=%#v signal=%#v", planRuntime.signalRef, planRuntime.signal)
	}
}

func TestAgentOSPlanSchemaRouteDoesNotRequirePlanRuntime(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, nil, nil)

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/schemas/run-plan", "")
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("schema status = %d", resp.StatusCode)
	}

	if contentType := resp.Header.Get("Content-Type"); !strings.Contains(contentType, "application/json") {
		t.Fatalf("content type = %q", contentType)
	}

	var schema map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&schema); err != nil {
		t.Fatalf("decode schema: %v", err)
	}

	if len(schema) == 0 {
		t.Fatal("schema is empty")
	}

	resp = doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/schemas/unknown", "")
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown schema status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestAgentOSPlanAuthorRouteDoesNotRequirePlanRuntime(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, nil, nil)

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/author", "")
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("author status = %d", resp.StatusCode)
	}

	if contentType := resp.Header.Get("Content-Type"); !strings.Contains(contentType, "text/html") {
		t.Fatalf("content type = %q", contentType)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read author: %v", err)
	}

	html := string(body)
	for _, want := range []string{
		"AgentOS Plan Author",
		"RunPlanSpec",
		`data-schema-endpoint="/v1/agentos/plans/schemas/run-plan"`,
		`data-start-enabled="false"`,
		`data-default-backend-name="goagent-native"`,
		`data-default-capability="run"`,
		`<option value="native">native</option>`,
		`<option value="temporal_external">temporal_external</option>`,
		`<option value="http">http</option>`,
		`<option value="grpc">grpc</option>`,
		`<option value="success">success</option>`,
		`<option value="error">error</option>`,
		`<option value="complete">complete</option>`,
		`<option value="always">always</option>`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("author body missing %q:\n%s", want, html)
		}
	}
}

func TestAgentOSPlanAuthorRouteEnablesStartWithPlanRuntime(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, newFakePlanRuntime(), nil)

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/author", "")
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("author status = %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read author: %v", err)
	}

	html := string(body)
	for _, want := range []string{
		`data-start-enabled="true"`,
		`data-start-endpoint="/v1/agentos/plans"`,
		`id="start-plan" class="button primary"`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("author body missing %q:\n%s", want, html)
		}
	}
}

func TestAgentOSPlanRoutesDoNotMutateRunningTopology(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, newFakePlanRuntime(), nil)

	planRoutes := 0

	for _, methodRoutes := range app.Stack() {
		for _, route := range methodRoutes {
			if !strings.HasPrefix(route.Path, "/v1/agentos/plans") {
				continue
			}

			planRoutes++

			switch route.Method {
			case http.MethodGet, http.MethodHead, http.MethodPost:
			case http.MethodPut, http.MethodPatch, http.MethodDelete:
				t.Fatalf("RunPlan route %s %s would mutate running topology outside PlanRuntime signal/control", route.Method, route.Path)
			default:
				t.Fatalf("RunPlan route %s %s is not part of the public read/start/signal/control surface", route.Method, route.Path)
			}
		}
	}

	if planRoutes == 0 {
		t.Fatal("no AgentOS RunPlan routes registered")
	}
}

func TestAgentOSPlanRoutesRequireProjectScope(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	planRuntime := newFakePlanRuntime()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, planRuntime, nil)

	for _, tt := range []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"status", http.MethodGet, "/v1/agentos/plans/plan-1/status?account_id=acct-1", ""},
		{"description", http.MethodGet, "/v1/agentos/plans/plan-1/description?account_id=acct-1", ""},
		{"stream events", http.MethodGet, "/v1/agentos/plans/plan-1/events?account_id=acct-1", ""},
		{"event history", http.MethodGet, "/v1/agentos/plans/plan-1/events/history?account_id=acct-1", ""},
		{"debug traces", http.MethodGet, "/v1/agentos/plans/plan-1/debug/traces?account_id=acct-1", ""},
		{"audits", http.MethodGet, "/v1/agentos/plans/plan-1/audits?account_id=acct-1", ""},
		{"artifacts", http.MethodGet, "/v1/agentos/plans/plan-1/artifacts?account_id=acct-1", ""},
		{"artifact", http.MethodGet, "/v1/agentos/plans/plan-1/artifacts/artifact-1?account_id=acct-1", ""},
		{"console", http.MethodGet, "/v1/agentos/plans/plan-1/console?account_id=acct-1", ""},
		{"signal", http.MethodPost, "/v1/agentos/plans/plan-1/signals", `{"type":"plan.approve","account_id":ACCT_1}`},
		{"control", http.MethodPost, "/v1/agentos/plans/plan-1/control", `{"operation":"pause","account_id":"acct-1"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := doAgentOSRouteRequest(t, app, tt.method, tt.path, tt.body)
			t.Cleanup(func() {
				if err := resp.Body.Close(); err != nil {
					t.Errorf("close response body: %v", err)
				}
			})

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
		})
	}

	assertPlanRuntimeNotCalled(t, planRuntime)
}

func assertPlanRuntimeNotCalled(t *testing.T, planRuntime *fakePlanRuntime) {
	t.Helper()

	for _, planID := range []string{
		planRuntime.statusRef.PlanID,
		planRuntime.descriptionRef.PlanID,
		planRuntime.signalRef.PlanID,
		planRuntime.controlRef.PlanID,
		planRuntime.scope.PlanID,
		planRuntime.eventScope.PlanID,
		planRuntime.debugScope.PlanID,
		planRuntime.auditScope.PlanID,
		planRuntime.artifactScope.PlanID,
		planRuntime.artifactGetScope.PlanID,
	} {
		if planID != "" {
			t.Fatalf("runtime was called despite missing project scope: %#v", planRuntime)
		}
	}
}

func TestAgentOSPlanEventRouteStreamsSSE(t *testing.T) {
	t.Parallel()

	planRuntime := newFakePlanRuntime()
	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, planRuntime, nil)

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/events?account_id=acct-1&project_id=proj-1&node_id=research&after_sequence=7", "")
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("events status = %d", resp.StatusCode)
	}

	if contentType := resp.Header.Get("Content-Type"); !strings.Contains(contentType, "text/event-stream") {
		t.Fatalf("content type = %q", contentType)
	}

	assertPlanEventStreamScope(t, &planRuntime.scope)

	var events []sse.Event

	for event, err := range sse.Read(resp.Body, nil) {
		if err != nil {
			t.Fatalf("read sse: %v", err)
		}

		events = append(events, event)
	}

	assertPlanSSEEvent(t, events)

	var event agentoscore.Event
	if err := json.Unmarshal([]byte(events[0].Data), &event); err != nil {
		t.Fatalf("decode event data: %v", err)
	}

	assertPlanSSEEventData(t, &event)
}

func assertPlanEventStreamScope(t *testing.T, scope *agentos.PlanStreamScope) {
	t.Helper()

	if scope.PlanID != plan1 ||
		scope.AccountID != "acct-1" ||
		scope.ProjectID != "proj-1" ||
		scope.NodeID != "research" ||
		scope.AfterSequence != 7 {
		t.Fatalf("unexpected scope: %#v", scope)
	}
}

func assertPlanSSEEvent(t *testing.T, events []sse.Event) {
	t.Helper()

	if len(events) != 1 {
		t.Fatalf("event count = %d", len(events))
	}

	if events[0].Type != string(agentoscore.EventPlanStarted) || events[0].LastEventID != planEvent1 {
		t.Fatalf("unexpected sse event: %#v", events[0])
	}
}

func assertPlanSSEEventData(t *testing.T, event *agentoscore.Event) {
	t.Helper()

	if event.EventID != "plan-event-1" || event.Payload["plan_id"] != plan1 {
		t.Fatalf("unexpected event data: %#v", event)
	}
}

func TestAgentOSPlanConsoleRendersRuntimeData(t *testing.T) {
	t.Parallel()

	planRuntime := newFakePlanRuntime()
	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, planRuntime, nil)

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/console?account_id=acct-1&project_id=proj-1", "")
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("console status = %d", resp.StatusCode)
	}

	if contentType := resp.Header.Get("Content-Type"); !strings.Contains(contentType, "text/html") {
		t.Fatalf("content type = %q", contentType)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read console: %v", err)
	}

	assertPlanConsoleHTML(t, string(body))
	assertPlanConsoleRuntimeCalls(t, planRuntime)
}

func assertPlanConsoleHTML(t *testing.T, html string) {
	t.Helper()

	for _, want := range []string{
		"AgentOS Plan Console", plan1, "research", "http:research-http", "run-research",
		"Plan Graph", "research -&gt; write", "Debug Traces", "digest:digest-1", "artifact-1",
		"plan.node.started", "plan.control",
		`data-control-endpoint="control"`, `data-signal-endpoint="signals"`,
		`data-control="pause"`, `data-control="resume"`, `data-control="cancel"`,
		`data-signal="plan.approve"`, `data-signal="plan.reject"`, `data-signal="plan.node.retry"`,
		`data-node-id="research"`, `id="actor-id"`, `id="signal-reason"`,
		`actor_id: actorID()`, `payload.idempotency_key = commandKey("control", payload.operation, "")`,
		`payload.idempotency_key = commandKey("signal", payload.type, button.dataset.nodeId || "")`,
		`payload.payload[root.payloadNodeIdKey] = button.dataset.nodeId`, `payload.payload[root.payloadReasonKey] = reason`,
		`await postJSON(root.controlEndpoint, payload)`, `await postJSON(root.signalEndpoint, payload)`, `href="/v1/agentos/plans/author"`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("console body missing %q:\n%s", want, html)
		}
	}
}

func assertPlanConsoleRuntimeCalls(t *testing.T, planRuntime *fakePlanRuntime) {
	t.Helper()

	if planRuntime.descriptionRef.PlanID != plan1 || planRuntime.descriptionRef.AccountID != account1 || planRuntime.descriptionRef.ProjectID != project1 {
		t.Fatalf("unexpected description ref: %#v", planRuntime.descriptionRef)
	}

	if planRuntime.eventScope.Limit != agentOSPlanConsoleDefaultEventLimit || planRuntime.debugScope.Limit != agentOSPlanConsoleDefaultEventLimit || planRuntime.artifactScope.Limit != agentOSPlanConsoleDefaultArtifactLimit || planRuntime.auditScope.Limit != agentOSPlanConsoleDefaultAuditLimit {
		t.Fatalf("unexpected console limits: events=%d debug=%d artifacts=%d audits=%d", planRuntime.eventScope.Limit, planRuntime.debugScope.Limit, planRuntime.artifactScope.Limit, planRuntime.auditScope.Limit)
	}
}

func TestAgentOSPlanConsoleRequiresTenantScope(t *testing.T) {
	t.Parallel()

	planRuntime := newFakePlanRuntime()
	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, planRuntime, nil)

	for _, path := range []string{
		"/v1/agentos/plans/plan-1/console",
		"/v1/agentos/plans/plan-1/console?account_id=acct-1",
		"/v1/agentos/plans/plan-1/console?project_id=proj-1",
	} {
		resp := doAgentOSRouteRequest(t, app, http.MethodGet, path, "")
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("console path %q status = %d", path, resp.StatusCode)
		}
	}
}

type fakePlanRuntime struct {
	started          agentos.RunPlanSpec
	statusRef        agentos.PlanRef
	descriptionRef   agentos.PlanRef
	signalRef        agentos.PlanRef
	signal           agentoscore.Signal
	controlRef       agentos.PlanRef
	control          agentoscore.ControlRequest
	scope            agentos.PlanStreamScope
	eventScope       agentos.PlanEventScope
	debugScope       agentos.PlanDebugTraceScope
	auditScope       agentos.PlanAuditScope
	artifactScope    agentos.PlanArtifactScope
	artifactGetScope agentos.PlanArtifactScope
}

func newFakePlanRuntime() *fakePlanRuntime {
	return &fakePlanRuntime{}
}

func (r *fakePlanRuntime) StartPlan(_ context.Context, spec *agentos.RunPlanSpec) (agentos.RunPlanStatus, error) {
	r.started = *spec

	return agentos.RunPlanStatus{PlanID: spec.PlanID, LifecycleState: agentos.PlanLifecycleRunning, UpdatedAt: time.Now()}, nil
}

func (r *fakePlanRuntime) StatusPlan(_ context.Context, ref agentos.PlanRef) (agentos.RunPlanStatus, error) {
	r.statusRef = ref

	return fakePlanStatus(ref.PlanID), nil
}

func (r *fakePlanRuntime) DescribePlan(_ context.Context, ref agentos.PlanRef) (agentos.RunPlanDescription, error) {
	r.descriptionRef = ref
	status := fakePlanStatus(ref.PlanID)

	return agentos.RunPlanDescription{
		PlanID:    ref.PlanID,
		AccountID: ref.AccountID,
		ProjectID: ref.ProjectID,
		Status:    status,
		Topology: agentos.PlanTopology{
			Nodes: []agentos.PlanTopologyNode{
				{
					NodeID:     research,
					RunID:      "run-research",
					Backend:    agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research-http"},
					Capability: "run",
					Status:     status.Nodes[0],
				},
				{
					NodeID:     "write",
					RunID:      "run-write",
					Backend:    agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "writer-http"},
					Capability: "run",
					Status:     status.Nodes[1],
				},
			},
			Edges: []agentos.PlanTopologyEdge{
				{EdgeID: "research-write", From: research, To: "write", On: agentos.EdgeOnSuccess},
			},
			Order: []string{research, "write"},
		},
		UpdatedAt: status.UpdatedAt,
	}, nil
}

func fakePlanStatus(planID string) agentos.RunPlanStatus {
	return agentos.RunPlanStatus{
		PlanID:         planID,
		LifecycleState: agentos.PlanLifecycleRunning,
		Nodes: []agentos.PlanNodeStatus{
			{
				NodeID:         research,
				RunID:          "run-research",
				Backend:        agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research-http"},
				LifecycleState: agentos.PlanNodeFailed,
				Attempts:       1,
				Reason:         "needs manual retry",
				UpdatedAt:      time.Now(),
			},
			{
				NodeID:         "write",
				RunID:          "run-write",
				Backend:        agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "writer-http"},
				LifecycleState: agentos.PlanNodePending,
				UpdatedAt:      time.Now(),
			},
		},
		ActiveRunIDs: []string{"run-research"},
		Artifacts: []agentoscore.ArtifactRef{
			{ArtifactID: artifact1, PlanID: planID, NodeID: research, RunID: "run-research", Name: "summary", Kind: agentoscore.ArtifactKindObject},
		},
		BudgetUsage: agentos.PlanBudgetUsage{SpentCents: 7},
		UpdatedAt:   time.Now(),
	}
}

func (r *fakePlanRuntime) SignalPlan(_ context.Context, ref agentos.PlanRef, signal *agentoscore.Signal) error {
	r.signalRef = ref
	r.signal = *signal

	return nil
}

func (r *fakePlanRuntime) ControlPlan(_ context.Context, ref agentos.PlanRef, control *agentoscore.ControlRequest) error {
	r.controlRef = ref
	r.control = *control

	return nil
}

func (r *fakePlanRuntime) IngestExternalPlanEvent(_ context.Context, event *agentos.ExternalPlanEvent) (agentos.PlanEvent, error) {
	return agentos.PlanEvent{
		Event:           event.Event,
		PlanID:          event.Plan.PlanID,
		AccountID:       event.Plan.AccountID,
		ProjectID:       event.Plan.ProjectID,
		NodeID:          event.NodeID,
		ExternalEventID: event.Event.EventID,
	}, nil
}

func (r *fakePlanRuntime) SubscribePlan(_ context.Context, scope *agentos.PlanStreamScope) (agentoscore.Subscription, error) {
	r.scope = *scope

	events := make(chan agentoscore.Event, 1)
	events <- agentoscore.Event{
		EventID:   "plan-event-1",
		EventType: agentoscore.EventPlanStarted,
		Sequence:  8,
		Timestamp: time.Now(),
		Payload: map[string]any{
			"plan_id": plan1,
		},
	}

	close(events)

	return fakeSubscription{events: events}, nil
}

func (r *fakePlanRuntime) ListPlanEvents(_ context.Context, scope *agentos.PlanEventScope) ([]agentos.PlanEvent, error) {
	r.eventScope = *scope

	return []agentos.PlanEvent{
		{
			Event: agentoscore.Event{
				EventID:   "plan-event-1",
				EventType: agentoscore.EventPlanNodeStarted,
				RunID:     "run-research",
				Sequence:  8,
				Timestamp: time.Now(),
				Source:    "agentos.plan",
				Payload:   map[string]any{"node_id": research},
			},
			PlanID: scope.PlanID,
			NodeID: research,
		},
	}, nil
}

func (r *fakePlanRuntime) ListPlanDebugTraces(_ context.Context, scope *agentos.PlanDebugTraceScope) ([]agentos.PlanDebugTrace, error) {
	r.debugScope = *scope

	return []agentos.PlanDebugTrace{
		{
			EventID:   "debug-1",
			EventType: agentoscore.EventNodeInputResolved,
			PlanID:    scope.PlanID,
			NodeID:    research,
			RunID:     "run-research",
			Sequence:  9,
			InputResolution: &agentos.PlanInputResolutionTrace{
				InputDigest:  "digest-1",
				InputKeys:    []string{"topic"},
				MappingCount: 1,
				Mappings: []agentos.PlanInputMappingTrace{
					{Target: "topic", SourceArtifact: "summary", Required: true},
				},
			},
		},
	}, nil
}

func (r *fakePlanRuntime) ListPlanAudits(_ context.Context, scope *agentos.PlanAuditScope) ([]agentos.PlanAuditRecord, error) {
	r.auditScope = *scope

	return []agentos.PlanAuditRecord{
		{
			AuditID:        "audit-1",
			PlanID:         scope.PlanID,
			Action:         agentos.PlanAuditActionControl,
			ActorID:        operator1,
			IdempotencyKey: "control-1",
			Payload:        map[string]any{"operation": string(agentoscore.ControlPause)},
			CreatedAt:      time.Now(),
		},
	}, nil
}

func (r *fakePlanRuntime) ListPlanArtifacts(_ context.Context, scope *agentos.PlanArtifactScope) ([]agentoscore.ArtifactRef, error) {
	r.artifactScope = *scope

	return []agentoscore.ArtifactRef{
		{
			ArtifactID: artifact1,
			PlanID:     scope.PlanID,
			NodeID:     research,
			RunID:      "run-research",
			Name:       "summary",
			Kind:       agentoscore.ArtifactKindObject,
			MediaType:  "application/json",
			SizeBytes:  128,
			Digest:     "sha256:artifact",
		},
	}, nil
}

func (r *fakePlanRuntime) GetPlanArtifact(_ context.Context, scope *agentos.PlanArtifactScope) (agentoscore.Artifact, error) {
	r.artifactGetScope = *scope

	return agentoscore.Artifact{
		Ref: agentoscore.ArtifactRef{
			ArtifactID: scope.ArtifactID,
			PlanID:     scope.PlanID,
			Name:       "summary",
			Kind:       agentoscore.ArtifactKindObject,
		},
		Payload: map[string]any{"summary": "ok"},
	}, nil
}

type fakeSubscription struct {
	events <-chan agentoscore.Event
}

func (s fakeSubscription) Events() <-chan agentoscore.Event {
	return s.events
}

func (s fakeSubscription) Close() error {
	return nil
}
