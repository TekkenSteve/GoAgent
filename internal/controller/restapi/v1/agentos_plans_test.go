package v1

import (
	"context"
	"encoding/json"
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

type fakePlanRuntime struct {
	started          agentos.RunPlanSpec
	signalRef        agentos.PlanRef
	signal           agentos.Signal
	controlRef       agentos.PlanRef
	control          agentos.ControlRequest
	scope            agentos.PlanStreamScope
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
	return agentos.RunPlanStatus{PlanID: ref.PlanID, LifecycleState: agentos.PlanLifecycleRunning, UpdatedAt: time.Now()}, nil
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

func (r *fakePlanRuntime) ListPlanAudits(_ context.Context, scope agentos.PlanAuditScope) ([]agentos.PlanAuditRecord, error) {
	r.auditScope = scope

	return []agentos.PlanAuditRecord{
		{
			AuditID:        "audit-1",
			PlanID:         scope.PlanID,
			Action:         agentos.PlanAuditActionControl,
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
