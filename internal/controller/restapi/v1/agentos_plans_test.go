package v1

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/TekkenSteve/GoAgent/pkg/sse"
	"github.com/gofiber/fiber/v2"
)

func TestAgentOSPlanRoutesUsePlanRuntime(t *testing.T) {
	planRuntime := newFakePlanRuntime()
	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, nil, planRuntime)

	startBody := `{
		"plan_id": "plan-1",
		"thread_id": "thread-1",
		"account_id": "acct-1",
		"idempotency_key": "plan-start-1",
		"inputs": {"topic": "durable coordination"},
		"nodes": [
			{
				"node_id": "research",
				"run": {
					"run_id": "run-research",
					"account_id": "acct-1",
					"backend": {"kind": "http", "name": "research-http"},
					"input": {"task": "research"}
				}
			}
		]
	}`
	resp := doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/plans", startBody)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("start status = %d", resp.StatusCode)
	}
	if planRuntime.started.PlanID != "plan-1" ||
		planRuntime.started.ThreadID != "thread-1" ||
		planRuntime.started.Inputs["topic"] != "durable coordination" ||
		planRuntime.started.Nodes[0].Run.Backend.Name != "research-http" {
		t.Fatalf("unexpected RunPlanSpec: %#v", planRuntime.started)
	}

	signalBody := `{
		"type": "plan.node.retry",
		"account_id": "acct-1",
		"idempotency_key": "retry-1",
		"actor_id": "operator-1",
		"payload": {"node_id": "research"}
	}`
	resp = doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/plans/plan-1/signals", signalBody)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("signal status = %d", resp.StatusCode)
	}
	if planRuntime.signalRef.PlanID != "plan-1" ||
		planRuntime.signalRef.AccountID != "acct-1" ||
		planRuntime.signal.Type != agentos.SignalPlanNodeRetry ||
		planRuntime.signal.ActorID != "operator-1" ||
		planRuntime.signal.Payload["node_id"] != "research" {
		t.Fatalf("unexpected signal: ref=%#v signal=%#v", planRuntime.signalRef, planRuntime.signal)
	}

	controlBody := `{
		"operation": "pause",
		"account_id": "acct-1",
		"idempotency_key": "pause-1",
		"actor_id": "operator-1"
	}`
	resp = doAgentOSRouteRequest(t, app, http.MethodPost, "/v1/agentos/plans/plan-1/control", controlBody)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("control status = %d", resp.StatusCode)
	}
	if planRuntime.controlRef.PlanID != "plan-1" ||
		planRuntime.controlRef.AccountID != "acct-1" ||
		planRuntime.control.Operation != agentos.ControlPause ||
		planRuntime.control.ActorID != "operator-1" {
		t.Fatalf("unexpected control: ref=%#v control=%#v", planRuntime.controlRef, planRuntime.control)
	}

	resp = doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/status?account_id=acct-1", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d", resp.StatusCode)
	}
	var status agentos.RunPlanStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.PlanID != "plan-1" || status.LifecycleState != agentos.PlanLifecycleRunning {
		t.Fatalf("unexpected status: %#v", status)
	}

	resp = doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/description?account_id=acct-1", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("description status = %d", resp.StatusCode)
	}
	var description agentos.RunPlanDescription
	if err := json.NewDecoder(resp.Body).Decode(&description); err != nil {
		t.Fatalf("decode description: %v", err)
	}
	if description.PlanID != "plan-1" ||
		len(description.Topology.Nodes) != 2 ||
		len(description.Topology.Edges) != 1 ||
		description.Topology.Edges[0].From != "research" ||
		description.Topology.Edges[0].To != "write" {
		t.Fatalf("unexpected description: %#v", description)
	}

	resp = doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/audits?account_id=acct-1&action=plan.control&limit=25", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("audits status = %d", resp.StatusCode)
	}
	if planRuntime.auditScope.PlanID != "plan-1" ||
		planRuntime.auditScope.AccountID != "acct-1" ||
		planRuntime.auditScope.Action != agentos.PlanAuditActionControl ||
		planRuntime.auditScope.Limit != 25 {
		t.Fatalf("unexpected audit scope: %#v", planRuntime.auditScope)
	}
	var audits []agentos.PlanAuditRecord
	if err := json.NewDecoder(resp.Body).Decode(&audits); err != nil {
		t.Fatalf("decode audits: %v", err)
	}
	if len(audits) != 1 || audits[0].Action != agentos.PlanAuditActionControl {
		t.Fatalf("unexpected audits: %#v", audits)
	}

	resp = doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/artifacts?account_id=acct-1&node_id=research&limit=10", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("artifacts status = %d", resp.StatusCode)
	}
	if planRuntime.artifactScope.PlanID != "plan-1" ||
		planRuntime.artifactScope.AccountID != "acct-1" ||
		planRuntime.artifactScope.NodeID != "research" ||
		planRuntime.artifactScope.Limit != 10 {
		t.Fatalf("unexpected artifact scope: %#v", planRuntime.artifactScope)
	}
	var refs []agentos.ArtifactRef
	if err := json.NewDecoder(resp.Body).Decode(&refs); err != nil {
		t.Fatalf("decode artifacts: %v", err)
	}
	if len(refs) != 1 || refs[0].ArtifactID != "artifact-1" {
		t.Fatalf("unexpected artifacts: %#v", refs)
	}

	resp = doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/artifacts/artifact-1?account_id=acct-1", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("artifact status = %d", resp.StatusCode)
	}
	if planRuntime.artifactGetScope.PlanID != "plan-1" ||
		planRuntime.artifactGetScope.AccountID != "acct-1" ||
		planRuntime.artifactGetScope.ArtifactID != "artifact-1" {
		t.Fatalf("unexpected artifact get scope: %#v", planRuntime.artifactGetScope)
	}
	var artifact agentos.Artifact
	if err := json.NewDecoder(resp.Body).Decode(&artifact); err != nil {
		t.Fatalf("decode artifact: %v", err)
	}
	if artifact.Ref.ArtifactID != "artifact-1" || artifact.Payload.(map[string]any)["summary"] != "ok" {
		t.Fatalf("unexpected artifact: %#v", artifact)
	}

	resp = doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/events/history?account_id=acct-1&node_id=research&run_id=run-research&after_sequence=7&limit=3", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("event history status = %d", resp.StatusCode)
	}
	if planRuntime.eventScope.PlanID != "plan-1" ||
		planRuntime.eventScope.AccountID != "acct-1" ||
		planRuntime.eventScope.NodeID != "research" ||
		planRuntime.eventScope.RunID != "run-research" ||
		planRuntime.eventScope.AfterSequence != 7 ||
		planRuntime.eventScope.Limit != 3 {
		t.Fatalf("unexpected event scope: %#v", planRuntime.eventScope)
	}
	var planEvents []agentos.PlanEvent
	if err := json.NewDecoder(resp.Body).Decode(&planEvents); err != nil {
		t.Fatalf("decode event history: %v", err)
	}
	if len(planEvents) != 1 || planEvents[0].PlanID != "plan-1" || planEvents[0].NodeID != "research" {
		t.Fatalf("unexpected event history: %#v", planEvents)
	}
}

func TestAgentOSPlanEventRouteStreamsSSE(t *testing.T) {
	planRuntime := newFakePlanRuntime()
	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, nil, planRuntime)

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/events?account_id=acct-1&node_id=research&after_sequence=7", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("events status = %d", resp.StatusCode)
	}
	if contentType := resp.Header.Get("Content-Type"); !strings.Contains(contentType, "text/event-stream") {
		t.Fatalf("content type = %q", contentType)
	}
	if planRuntime.scope.PlanID != "plan-1" ||
		planRuntime.scope.AccountID != "acct-1" ||
		planRuntime.scope.NodeID != "research" ||
		planRuntime.scope.AfterSequence != 7 {
		t.Fatalf("unexpected scope: %#v", planRuntime.scope)
	}

	var events []sse.Event
	for event, err := range sse.Read(resp.Body, nil) {
		if err != nil {
			t.Fatalf("read sse: %v", err)
		}
		events = append(events, event)
	}
	if len(events) != 1 {
		t.Fatalf("event count = %d", len(events))
	}
	if events[0].Type != string(agentos.EventPlanStarted) || events[0].LastEventID != "plan-event-1" {
		t.Fatalf("unexpected sse event: %#v", events[0])
	}

	var event agentos.Event
	if err := json.Unmarshal([]byte(events[0].Data), &event); err != nil {
		t.Fatalf("decode event data: %v", err)
	}
	if event.EventID != "plan-event-1" || event.Payload["plan_id"] != "plan-1" {
		t.Fatalf("unexpected event data: %#v", event)
	}
}

func TestAgentOSPlanConsoleRendersRuntimeData(t *testing.T) {
	planRuntime := newFakePlanRuntime()
	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, nil, planRuntime)

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/console?account_id=acct-1&project_id=proj-1", "")
	defer resp.Body.Close()
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
	html := string(body)
	for _, want := range []string{
		"AgentOS Plan Console",
		"plan-1",
		"research",
		"http:research-http",
		"run-research",
		"Plan Graph",
		"research -&gt; write",
		"artifact-1",
		"plan.node.started",
		"plan.control",
		`data-control-endpoint="control"`,
		`data-signal-endpoint="signals"`,
		`data-signal="plan.node.retry"`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("console body missing %q:\n%s", want, html)
		}
	}

	if planRuntime.descriptionRef.PlanID != "plan-1" ||
		planRuntime.descriptionRef.AccountID != "acct-1" ||
		planRuntime.descriptionRef.ProjectID != "proj-1" {
		t.Fatalf("unexpected description ref: %#v", planRuntime.descriptionRef)
	}
	if planRuntime.eventScope.Limit != agentOSPlanConsoleDefaultEventLimit ||
		planRuntime.artifactScope.Limit != agentOSPlanConsoleDefaultArtifactLimit ||
		planRuntime.auditScope.Limit != agentOSPlanConsoleDefaultAuditLimit {
		t.Fatalf("unexpected console limits: events=%d artifacts=%d audits=%d", planRuntime.eventScope.Limit, planRuntime.artifactScope.Limit, planRuntime.auditScope.Limit)
	}
}

func TestAgentOSPlanConsoleRequiresAccountScope(t *testing.T) {
	planRuntime := newFakePlanRuntime()
	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, nil, planRuntime)

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/console", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("console status = %d", resp.StatusCode)
	}
}

type fakePlanRuntime struct {
	started          agentos.RunPlanSpec
	statusRef        agentos.PlanRef
	descriptionRef   agentos.PlanRef
	signalRef        agentos.PlanRef
	signal           agentos.Signal
	controlRef       agentos.PlanRef
	control          agentos.ControlRequest
	scope            agentos.PlanStreamScope
	eventScope       agentos.PlanEventScope
	auditScope       agentos.PlanAuditScope
	artifactScope    agentos.PlanArtifactScope
	artifactGetScope agentos.PlanArtifactScope
}

func newFakePlanRuntime() *fakePlanRuntime {
	return &fakePlanRuntime{}
}

func (r *fakePlanRuntime) StartPlan(_ context.Context, spec agentos.RunPlanSpec) (agentos.RunPlanStatus, error) {
	r.started = spec

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
					NodeID:     "research",
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
				{EdgeID: "research-write", From: "research", To: "write", On: agentos.EdgeOnSuccess},
			},
			Order: []string{"research", "write"},
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
				NodeID:         "research",
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
		Artifacts: []agentos.ArtifactRef{
			{ArtifactID: "artifact-1", PlanID: planID, NodeID: "research", RunID: "run-research", Name: "summary", Kind: agentos.ArtifactKindObject},
		},
		BudgetUsage: agentos.PlanBudgetUsage{SpentCents: 7},
		UpdatedAt:   time.Now(),
	}
}

func (r *fakePlanRuntime) SignalPlan(_ context.Context, ref agentos.PlanRef, signal agentos.Signal) error {
	r.signalRef = ref
	r.signal = signal

	return nil
}

func (r *fakePlanRuntime) ControlPlan(_ context.Context, ref agentos.PlanRef, control agentos.ControlRequest) error {
	r.controlRef = ref
	r.control = control

	return nil
}

func (r *fakePlanRuntime) SubscribePlan(_ context.Context, scope agentos.PlanStreamScope) (agentos.Subscription, error) {
	r.scope = scope
	events := make(chan agentos.Event, 1)
	events <- agentos.Event{
		EventID:   "plan-event-1",
		EventType: agentos.EventPlanStarted,
		Sequence:  8,
		Timestamp: time.Now(),
		Payload: map[string]any{
			"plan_id": "plan-1",
		},
	}
	close(events)

	return fakeSubscription{events: events}, nil
}

func (r *fakePlanRuntime) ListPlanEvents(_ context.Context, scope agentos.PlanEventScope) ([]agentos.PlanEvent, error) {
	r.eventScope = scope

	return []agentos.PlanEvent{
		{
			Event: agentos.Event{
				EventID:   "plan-event-1",
				EventType: agentos.EventPlanNodeStarted,
				RunID:     "run-research",
				Sequence:  8,
				Timestamp: time.Now(),
				Source:    "agentos.plan",
				Payload:   map[string]any{"node_id": "research"},
			},
			PlanID: scope.PlanID,
			NodeID: "research",
		},
	}, nil
}

func (r *fakePlanRuntime) ListPlanAudits(_ context.Context, scope agentos.PlanAuditScope) ([]agentos.PlanAuditRecord, error) {
	r.auditScope = scope

	return []agentos.PlanAuditRecord{
		{
			AuditID:        "audit-1",
			PlanID:         scope.PlanID,
			Action:         agentos.PlanAuditActionControl,
			ActorID:        "operator-1",
			IdempotencyKey: "control-1",
			Payload:        map[string]any{"operation": string(agentos.ControlPause)},
			CreatedAt:      time.Now(),
		},
	}, nil
}

func (r *fakePlanRuntime) ListPlanArtifacts(_ context.Context, scope agentos.PlanArtifactScope) ([]agentos.ArtifactRef, error) {
	r.artifactScope = scope

	return []agentos.ArtifactRef{
		{
			ArtifactID: "artifact-1",
			PlanID:     scope.PlanID,
			NodeID:     "research",
			RunID:      "run-research",
			Name:       "summary",
			Kind:       agentos.ArtifactKindObject,
			MediaType:  "application/json",
			SizeBytes:  128,
			Digest:     "sha256:artifact",
		},
	}, nil
}

func (r *fakePlanRuntime) GetPlanArtifact(_ context.Context, scope agentos.PlanArtifactScope) (agentos.Artifact, error) {
	r.artifactGetScope = scope

	return agentos.Artifact{
		Ref: agentos.ArtifactRef{
			ArtifactID: scope.ArtifactID,
			PlanID:     scope.PlanID,
			Name:       "summary",
			Kind:       agentos.ArtifactKindObject,
		},
		Payload: map[string]any{"summary": "ok"},
	}, nil
}

type fakeSubscription struct {
	events <-chan agentos.Event
}

func (s fakeSubscription) Events() <-chan agentos.Event {
	return s.events
}

func (s fakeSubscription) Close() error {
	return nil
}
