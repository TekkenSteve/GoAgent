package v1

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosprocess "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v2"
)

const (
	process1          = "process-1"
	resource1         = "alert-1"
	action1           = "action-1"
	workset1          = "workset-1"
	ledger1           = "ledger-1"
	alertKind         = "alert"
	investigationKind = "aisoc.investigation"
	isolateHostKind   = "isolate_host"
	huntBackfillKind  = "hunt.backfill"
)

func TestAgentOSProcessPlatformRoutesUseRuntime(t *testing.T) {
	t.Parallel()

	runtime := newFakePlatformRuntime()
	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, runtime, runtime, runtime, nil)

	testProcessStartRoute(t, app, runtime)
	testProcessListRoute(t, app, runtime)
	testProcessDescriptionRoute(t, app, runtime)
	testProcessStatusRoute(t, app, runtime)
	testLedgerAppendRoute(t, app, runtime)
	testLedgerListRoute(t, app, runtime)
	testActionRequestRoute(t, app, runtime)
	testActionListRoute(t, app, runtime)
	testActionStatusRoute(t, app, runtime)
	testActionDryRunRoute(t, app, runtime)
	testActionApprovalRoute(t, app, runtime)
	testActionExecutionRoute(t, app, runtime)
	testActionCancelRoute(t, app, runtime)
	testWorksetStartRoute(t, app, runtime)
	testWorksetListRoute(t, app, runtime)
	testWorksetStatusRoute(t, app, runtime)
	testResourceProjectionRoute(t, app, runtime)
	testResourceProjectionListRoute(t, app, runtime)
}

func TestAgentOSQueryScopeValidationRejectsMissingProject(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/scope", func(ctx *fiber.Ctx) error {
		var req request.AgentOSPlanScope

		return parseRuntimeScopedQuery(&V1{v: validator.New(validator.WithRequiredStructEnabled())}, ctx, "plan scope", true, "", &req)
	})

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/scope?account_id=acct-1", "")
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestAgentOSPlanRouteRejectsMissingProjectScope(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	NewRoutes(app.Group("/v1"), nil, nil, logger.New("error"), nil, nil, nil, nil, nil, newFakePlanRuntime(), nil, nil)

	resp := doAgentOSRouteRequest(t, app, http.MethodGet, "/v1/agentos/plans/plan-1/status?account_id=acct-1", "")
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func testProcessStartRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	body := `{
		"process_id":"process-1","kind":"aisoc.investigation","account_id":"acct-1","project_id":"proj-1","idempotency_key":"process-start-1",
		"resource":{"kind":"alert","resource_id":"alert-1","account_id":"acct-1","project_id":"proj-1"},
		"inputs":{"severity":"high"}
	}`

	status := routeJSON[agentosprocess.Status](t, app, http.MethodPost, "/v1/agentos/processes", body, http.StatusAccepted, "process start")
	if runtime.startedProcess.ProcessID != process1 ||
		runtime.startedProcess.Kind != investigationKind ||
		runtime.startedProcess.Resource.Kind != alertKind ||
		runtime.startedProcess.Inputs["severity"] != "high" ||
		status.ProcessID != process1 {
		t.Fatalf("unexpected process start: spec=%#v status=%#v", runtime.startedProcess, status)
	}
}

func testProcessListRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	statuses := routeJSON[[]agentosprocess.Status](t, app, http.MethodGet, "/v1/agentos/processes?account_id=acct-1&project_id=proj-1&resource_kind=alert&kind=aisoc.investigation&lifecycle_state=running&limit=9", "", http.StatusOK, "process list")
	if runtime.processScope.AccountID != account1 ||
		runtime.processScope.ProjectID != project1 ||
		runtime.processScope.ResourceKind != alertKind ||
		runtime.processScope.Resource.Kind != "" ||
		runtime.processScope.Kind != investigationKind ||
		runtime.processScope.LifecycleState != agentosprocess.ProcessRunning ||
		runtime.processScope.Limit != 9 ||
		len(statuses) != 1 {
		t.Fatalf("unexpected process list: scope=%#v statuses=%#v", runtime.processScope, statuses)
	}
}

func testProcessDescriptionRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	description := routeJSON[agentosprocess.Description](t, app, http.MethodGet, "/v1/agentos/processes/process-1?account_id=acct-1&project_id=proj-1", "", http.StatusOK, "process description")
	if runtime.descriptionProcessRef.ProcessID != process1 ||
		runtime.descriptionProcessRef.AccountID != account1 ||
		runtime.descriptionProcessRef.ProjectID != project1 ||
		description.ProcessID != process1 {
		t.Fatalf("unexpected process description: ref=%#v description=%#v", runtime.descriptionProcessRef, description)
	}
}

func testProcessStatusRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	status := routeJSON[agentosprocess.Status](t, app, http.MethodGet, "/v1/agentos/processes/process-1/status?account_id=acct-1&project_id=proj-1", "", http.StatusOK, "process status")
	if runtime.statusProcessRef.ProcessID != process1 ||
		runtime.statusProcessRef.AccountID != account1 ||
		runtime.statusProcessRef.ProjectID != project1 ||
		status.LifecycleState != agentosprocess.ProcessRunning {
		t.Fatalf("unexpected process status: ref=%#v status=%#v", runtime.statusProcessRef, status)
	}
}

func testLedgerAppendRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	body := `{
		"entry_id":"ledger-1","idempotency_key":"ledger-append-1","account_id":"acct-1","project_id":"proj-1","process_id":"process-1",
		"kind":"evidence","summary":"triaged alert"
	}`

	entry := routeJSON[agentosprocess.LedgerEntry](t, app, http.MethodPost, "/v1/agentos/ledger", body, http.StatusCreated, "ledger append")
	if runtime.ledgerSpec.EntryID != ledger1 ||
		runtime.ledgerSpec.ProcessID != process1 ||
		runtime.ledgerSpec.Kind != agentosprocess.LedgerEntryEvidence ||
		entry.Sequence != 11 {
		t.Fatalf("unexpected ledger append: spec=%#v entry=%#v", runtime.ledgerSpec, entry)
	}
}

func testLedgerListRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	entries := routeJSON[[]agentosprocess.LedgerEntry](t, app, http.MethodGet, "/v1/agentos/ledger?account_id=acct-1&project_id=proj-1&process_id=process-1&resource_kind=alert&resource_id=alert-1&kind=evidence&after_sequence=10&limit=5", "", http.StatusOK, "ledger list")
	if runtime.ledgerScope.ProcessID != process1 ||
		runtime.ledgerScope.Resource.Kind != alertKind ||
		runtime.ledgerScope.Resource.ResourceID != resource1 ||
		runtime.ledgerScope.Kind != agentosprocess.LedgerEntryEvidence ||
		runtime.ledgerScope.AfterSequence != 10 ||
		runtime.ledgerScope.Limit != 5 ||
		len(entries) != 1 {
		t.Fatalf("unexpected ledger list: scope=%#v entries=%#v", runtime.ledgerScope, entries)
	}
}

func testActionRequestRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	body := `{
		"action_id":"action-1","idempotency_key":"action-request-1","account_id":"acct-1","project_id":"proj-1","process_id":"process-1",
		"kind":"isolate_host","intent":"contain compromised host","dry_run_required":true,"approval_required":true
	}`

	status := routeJSON[agentosprocess.GovernedActionStatus](t, app, http.MethodPost, "/v1/agentos/actions", body, http.StatusAccepted, "action request")
	if runtime.actionSpec.ActionID != action1 ||
		runtime.actionSpec.Kind != isolateHostKind ||
		!runtime.actionSpec.DryRunRequired ||
		!runtime.actionSpec.ApprovalRequired ||
		status.ActionID != action1 {
		t.Fatalf("unexpected action request: spec=%#v status=%#v", runtime.actionSpec, status)
	}
}

func testActionListRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	statuses := routeJSON[[]agentosprocess.GovernedActionStatus](t, app, http.MethodGet, "/v1/agentos/actions?account_id=acct-1&project_id=proj-1&process_id=process-1&resource_kind=alert&resource_id=alert-1&kind=isolate_host&lifecycle_state=waiting_approval&limit=6", "", http.StatusOK, "action list")
	if !validProcessResourceListScope(runtime.actionScope.ProcessID, runtime.actionScope.Resource, runtime.actionScope.Kind, runtime.actionScope.LifecycleState, runtime.actionScope.Limit, process1, isolateHostKind, agentosprocess.ActionWaitingApproval, 6, len(statuses)) {
		t.Fatalf("unexpected action list: scope=%#v statuses=%#v", runtime.actionScope, statuses)
	}
}

func testActionStatusRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	status := routeJSON[agentosprocess.GovernedActionStatus](t, app, http.MethodGet, "/v1/agentos/actions/action-1?account_id=acct-1&project_id=proj-1", "", http.StatusOK, "action status")
	if runtime.actionRef.ActionID != action1 ||
		runtime.actionRef.AccountID != account1 ||
		runtime.actionRef.ProjectID != project1 ||
		status.ActionID != action1 {
		t.Fatalf("unexpected action status: ref=%#v status=%#v", runtime.actionRef, status)
	}
}

func testActionDryRunRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	body := `{"idempotency_key":"action-1:dry-run","succeeded":true,"summary":"preview ok"}`

	status := routeJSON[agentosprocess.GovernedActionStatus](t, app, http.MethodPost, "/v1/agentos/actions/action-1/dry-run?account_id=acct-1&project_id=proj-1", body, http.StatusOK, "action dry-run")
	if runtime.actionDryRunRef.ActionID != action1 ||
		runtime.actionDryRunResult.IdempotencyKey != "action-1:dry-run" ||
		!runtime.actionDryRunResult.Succeeded ||
		status.ActionID != action1 {
		t.Fatalf("unexpected action dry-run: ref=%#v result=%#v status=%#v", runtime.actionDryRunRef, runtime.actionDryRunResult, status)
	}
}

func testActionApprovalRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	body := `{"idempotency_key":"action-1:approval","approved":true,"reason":"operator approved"}`

	status := routeJSON[agentosprocess.GovernedActionStatus](t, app, http.MethodPost, "/v1/agentos/actions/action-1/approval?account_id=acct-1&project_id=proj-1", body, http.StatusOK, "action approval")
	if runtime.actionApprovalRef.ActionID != action1 ||
		runtime.actionApprovalDecision.IdempotencyKey != "action-1:approval" ||
		!runtime.actionApprovalDecision.Approved ||
		status.ActionID != action1 {
		t.Fatalf("unexpected action approval: ref=%#v decision=%#v status=%#v", runtime.actionApprovalRef, runtime.actionApprovalDecision, status)
	}
}

func testActionExecutionRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	body := `{"idempotency_key":"action-1:execution","succeeded":true,"summary":"executed"}`

	status := routeJSON[agentosprocess.GovernedActionStatus](t, app, http.MethodPost, "/v1/agentos/actions/action-1/execution?account_id=acct-1&project_id=proj-1", body, http.StatusOK, "action execution")
	if runtime.actionExecutionRef.ActionID != action1 ||
		runtime.actionExecutionResult.IdempotencyKey != "action-1:execution" ||
		!runtime.actionExecutionResult.Succeeded ||
		status.ActionID != action1 {
		t.Fatalf("unexpected action execution: ref=%#v result=%#v status=%#v", runtime.actionExecutionRef, runtime.actionExecutionResult, status)
	}
}

func testActionCancelRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	body := `{"idempotency_key":"action-1:cancel","reason":"operator canceled"}`

	status := routeJSON[agentosprocess.GovernedActionStatus](t, app, http.MethodPost, "/v1/agentos/actions/action-1/cancel?account_id=acct-1&project_id=proj-1", body, http.StatusOK, "action cancel")
	if runtime.actionCancelRef.ActionID != action1 ||
		runtime.actionCancelRequest.IdempotencyKey != "action-1:cancel" ||
		runtime.actionCancelRequest.Reason != "operator canceled" ||
		status.ActionID != action1 {
		t.Fatalf("unexpected action cancel: ref=%#v request=%#v status=%#v", runtime.actionCancelRef, runtime.actionCancelRequest, status)
	}
}

func testWorksetStartRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	body := `{
		"workset_id":"workset-1","idempotency_key":"workset-start-1","account_id":"acct-1","project_id":"proj-1","process_id":"process-1","kind":"hunt.backfill",
		"items_ref":{"kind":"object","uri":"s3://aisoc/worksets/workset-1.jsonl","count":100}
	}`

	status := routeJSON[agentosprocess.WorksetStatus](t, app, http.MethodPost, "/v1/agentos/worksets", body, http.StatusAccepted, "workset start")
	if runtime.worksetSpec.WorksetID != workset1 ||
		runtime.worksetSpec.Kind != huntBackfillKind ||
		runtime.worksetSpec.ItemsRef.Count != 100 ||
		status.WorksetID != workset1 {
		t.Fatalf("unexpected workset start: spec=%#v status=%#v", runtime.worksetSpec, status)
	}
}

func testWorksetListRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	statuses := routeJSON[[]agentosprocess.WorksetStatus](t, app, http.MethodGet, "/v1/agentos/worksets?account_id=acct-1&project_id=proj-1&process_id=process-1&resource_kind=alert&resource_id=alert-1&kind=hunt.backfill&lifecycle_state=running&limit=8", "", http.StatusOK, "workset list")
	if !validProcessResourceListScope(runtime.worksetScope.ProcessID, runtime.worksetScope.Resource, runtime.worksetScope.Kind, runtime.worksetScope.LifecycleState, runtime.worksetScope.Limit, process1, huntBackfillKind, agentosprocess.WorksetRunning, 8, len(statuses)) {
		t.Fatalf("unexpected workset list: scope=%#v statuses=%#v", runtime.worksetScope, statuses)
	}
}

func testWorksetStatusRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	status := routeJSON[agentosprocess.WorksetStatus](t, app, http.MethodGet, "/v1/agentos/worksets/workset-1?account_id=acct-1&project_id=proj-1", "", http.StatusOK, "workset status")
	if runtime.worksetRef.WorksetID != workset1 ||
		runtime.worksetRef.AccountID != account1 ||
		runtime.worksetRef.ProjectID != project1 ||
		status.WorksetID != workset1 {
		t.Fatalf("unexpected workset status: ref=%#v status=%#v", runtime.worksetRef, status)
	}
}

func testResourceProjectionRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	projection := routeJSON[agentosprocess.ResourceProjection](t, app, http.MethodGet, "/v1/agentos/resources?account_id=acct-1&project_id=proj-1&resource_kind=alert&resource_id=alert-1&limit=4", "", http.StatusOK, "resource projection")
	if runtime.resourceScope.Resource.Kind != alertKind ||
		runtime.resourceScope.Resource.ResourceID != resource1 ||
		runtime.resourceScope.Limit != 4 ||
		projection.Resource.ResourceID != resource1 {
		t.Fatalf("unexpected resource projection: scope=%#v projection=%#v", runtime.resourceScope, projection)
	}
}

func testResourceProjectionListRoute(t *testing.T, app *fiber.App, runtime *fakePlatformRuntime) {
	t.Helper()

	summaries := routeJSON[[]agentosprocess.ResourceProjectionSummary](t, app, http.MethodGet, "/v1/agentos/resources?account_id=acct-1&project_id=proj-1&resource_kind=alert&lifecycle_state=running&limit=12", "", http.StatusOK, "resource projection list")
	if runtime.resourceListScope.AccountID != account1 ||
		runtime.resourceListScope.ProjectID != project1 ||
		runtime.resourceListScope.ResourceKind != alertKind ||
		runtime.resourceListScope.LifecycleState != agentosprocess.ProcessRunning ||
		runtime.resourceListScope.Limit != 12 ||
		len(summaries) != 1 {
		t.Fatalf("unexpected resource list: scope=%#v summaries=%#v", runtime.resourceListScope, summaries)
	}
}

func routeJSON[T any](t *testing.T, app *fiber.App, method, target, body string, wantStatus int, label string) T {
	t.Helper()

	resp := doAgentOSRouteRequest(t, app, method, target, body)
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != wantStatus {
		t.Fatalf("%s status = %d, want %d", label, resp.StatusCode, wantStatus)
	}

	var value T
	if err := json.NewDecoder(resp.Body).Decode(&value); err != nil {
		t.Fatalf("decode %s: %v", label, err)
	}

	return value
}

func validProcessResourceListScope[K comparable](processID string, resource agentosprocess.ResourceRef, kind K, lifecycleState string, limit int, wantProcessID string, wantKind K, wantLifecycleState string, wantLimit, rowCount int) bool {
	return processID == wantProcessID &&
		resource.Kind == alertKind &&
		resource.ResourceID == resource1 &&
		kind == wantKind &&
		lifecycleState == wantLifecycleState &&
		limit == wantLimit &&
		rowCount == 1
}

type fakePlatformRuntime struct {
	*fakeAgentOSRuntime
	*fakePlanRuntime

	startedProcess         agentosprocess.Spec
	processScope           agentosprocess.Scope
	statusProcessRef       agentosprocess.Ref
	descriptionProcessRef  agentosprocess.Ref
	ledgerSpec             agentosprocess.LedgerEntrySpec
	ledgerScope            agentosprocess.LedgerScope
	actionSpec             agentosprocess.GovernedActionSpec
	actionScope            agentosprocess.ActionScope
	actionRef              agentosprocess.ActionRef
	actionDryRunRef        agentosprocess.ActionRef
	actionDryRunResult     agentosprocess.ActionDryRunResult
	actionApprovalRef      agentosprocess.ActionRef
	actionApprovalDecision agentosprocess.ActionApprovalDecision
	actionExecutionRef     agentosprocess.ActionRef
	actionExecutionResult  agentosprocess.ActionExecutionResult
	actionCancelRef        agentosprocess.ActionRef
	actionCancelRequest    agentosprocess.ActionCancelRequest
	worksetSpec            agentosprocess.WorksetSpec
	worksetScope           agentosprocess.WorksetScope
	worksetRef             agentosprocess.WorksetRef
	resourceScope          agentosprocess.ResourceProjectionScope
	resourceListScope      agentosprocess.ResourceProjectionListScope
}

func newFakePlatformRuntime() *fakePlatformRuntime {
	return &fakePlatformRuntime{
		fakeAgentOSRuntime: &fakeAgentOSRuntime{},
		fakePlanRuntime:    newFakePlanRuntime(),
	}
}

func (r *fakePlatformRuntime) StartProcess(_ context.Context, spec *agentosprocess.Spec) (agentosprocess.Status, error) {
	r.startedProcess = *spec

	return processStatus(spec.ProcessID), nil
}

func (r *fakePlatformRuntime) StatusProcess(_ context.Context, ref agentosprocess.Ref) (agentosprocess.Status, error) {
	r.statusProcessRef = ref

	return processStatus(ref.ProcessID), nil
}

func (r *fakePlatformRuntime) DescribeProcess(_ context.Context, ref agentosprocess.Ref) (agentosprocess.Description, error) {
	r.descriptionProcessRef = ref

	return agentosprocess.Description{
		ProcessID: ref.ProcessID,
		Kind:      investigationKind,
		AccountID: ref.AccountID,
		ProjectID: ref.ProjectID,
		Resource:  alertResource(),
		Status:    processStatus(ref.ProcessID),
		UpdatedAt: time.Now(),
	}, nil
}

func (r *fakePlatformRuntime) ListProcesses(_ context.Context, scope *agentosprocess.Scope) ([]agentosprocess.Status, error) {
	r.processScope = *scope

	return []agentosprocess.Status{processStatus(process1)}, nil
}

func (r *fakePlatformRuntime) SignalProcess(context.Context, agentosprocess.Ref, *agentoscore.Signal) error {
	return nil
}

func (r *fakePlatformRuntime) ControlProcess(context.Context, agentosprocess.Ref, *agentoscore.ControlRequest) error {
	return nil
}

func (r *fakePlatformRuntime) SubscribeProcess(context.Context, *agentosprocess.StreamScope) (agentoscore.Subscription, error) {
	return nil, nil
}

func (r *fakePlatformRuntime) ListProcessEvents(context.Context, *agentosprocess.EventScope) ([]agentosprocess.Event, error) {
	return nil, nil
}

func (r *fakePlatformRuntime) AppendLedgerEntry(_ context.Context, spec *agentosprocess.LedgerEntrySpec) (agentosprocess.LedgerEntry, error) {
	r.ledgerSpec = *spec

	return ledgerEntry(spec), nil
}

func (r *fakePlatformRuntime) ListLedgerEntries(_ context.Context, scope *agentosprocess.LedgerScope) ([]agentosprocess.LedgerEntry, error) {
	r.ledgerScope = *scope

	return []agentosprocess.LedgerEntry{ledgerEntry(&agentosprocess.LedgerEntrySpec{
		EntryID:   ledger1,
		AccountID: account1,
		ProjectID: project1,
		ProcessID: process1,
		Resource:  alertResource(),
		Kind:      agentosprocess.LedgerEntryEvidence,
	})}, nil
}

func (r *fakePlatformRuntime) RequestAction(_ context.Context, spec *agentosprocess.GovernedActionSpec) (agentosprocess.GovernedActionStatus, error) {
	r.actionSpec = *spec

	return actionStatus(spec.ActionID), nil
}

func (r *fakePlatformRuntime) StatusAction(_ context.Context, ref agentosprocess.ActionRef) (agentosprocess.GovernedActionStatus, error) {
	r.actionRef = ref

	return actionStatus(ref.ActionID), nil
}

func (r *fakePlatformRuntime) ListActions(_ context.Context, scope *agentosprocess.ActionScope) ([]agentosprocess.GovernedActionStatus, error) {
	r.actionScope = *scope

	return []agentosprocess.GovernedActionStatus{actionStatus(action1)}, nil
}

func (r *fakePlatformRuntime) RecordActionDryRun(_ context.Context, ref agentosprocess.ActionRef, result *agentosprocess.ActionDryRunResult) (agentosprocess.GovernedActionStatus, error) {
	r.actionDryRunRef = ref
	r.actionDryRunResult = *result

	return actionStatus(ref.ActionID), nil
}

func (r *fakePlatformRuntime) ResolveActionApproval(_ context.Context, ref agentosprocess.ActionRef, decision *agentosprocess.ActionApprovalDecision) (agentosprocess.GovernedActionStatus, error) {
	r.actionApprovalRef = ref
	r.actionApprovalDecision = *decision

	return actionStatus(ref.ActionID), nil
}

func (r *fakePlatformRuntime) CompleteAction(_ context.Context, ref agentosprocess.ActionRef, result *agentosprocess.ActionExecutionResult) (agentosprocess.GovernedActionStatus, error) {
	r.actionExecutionRef = ref
	r.actionExecutionResult = *result

	return actionStatus(ref.ActionID), nil
}

func (r *fakePlatformRuntime) CancelAction(_ context.Context, ref agentosprocess.ActionRef, req *agentosprocess.ActionCancelRequest) (agentosprocess.GovernedActionStatus, error) {
	r.actionCancelRef = ref
	r.actionCancelRequest = *req

	return actionStatus(ref.ActionID), nil
}

func (r *fakePlatformRuntime) StartWorkset(_ context.Context, spec *agentosprocess.WorksetSpec) (agentosprocess.WorksetStatus, error) {
	r.worksetSpec = *spec

	return worksetStatus(spec.WorksetID), nil
}

func (r *fakePlatformRuntime) StatusWorkset(_ context.Context, ref agentosprocess.WorksetRef) (agentosprocess.WorksetStatus, error) {
	r.worksetRef = ref

	return worksetStatus(ref.WorksetID), nil
}

func (r *fakePlatformRuntime) ListWorksets(_ context.Context, scope *agentosprocess.WorksetScope) ([]agentosprocess.WorksetStatus, error) {
	r.worksetScope = *scope

	return []agentosprocess.WorksetStatus{worksetStatus(workset1)}, nil
}

func (r *fakePlatformRuntime) RecordWorksetChunk(context.Context, agentosprocess.WorksetRef, *agentosprocess.WorksetChunkResult) (agentosprocess.WorksetStatus, error) {
	return agentosprocess.WorksetStatus{}, nil
}

func (r *fakePlatformRuntime) CancelWorkset(context.Context, agentosprocess.WorksetRef, *agentoscore.ControlRequest) (agentosprocess.WorksetStatus, error) {
	return agentosprocess.WorksetStatus{}, nil
}

func (r *fakePlatformRuntime) GetResourceProjection(_ context.Context, scope *agentosprocess.ResourceProjectionScope) (agentosprocess.ResourceProjection, error) {
	r.resourceScope = *scope

	return agentosprocess.ResourceProjection{
		Resource:  scope.Resource,
		Processes: []agentosprocess.Status{processStatus(process1)},
		Actions:   []agentosprocess.GovernedActionStatus{actionStatus(action1)},
		Worksets:  []agentosprocess.WorksetStatus{worksetStatus(workset1)},
		Ledger:    []agentosprocess.LedgerEntry{ledgerEntry(&agentosprocess.LedgerEntrySpec{EntryID: ledger1, AccountID: account1, ProjectID: project1, ProcessID: process1, Resource: alertResource(), Kind: agentosprocess.LedgerEntryEvidence})},
		UpdatedAt: time.Now(),
	}, nil
}

func (r *fakePlatformRuntime) ListResourceProjections(_ context.Context, scope *agentosprocess.ResourceProjectionListScope) ([]agentosprocess.ResourceProjectionSummary, error) {
	r.resourceListScope = *scope

	return []agentosprocess.ResourceProjectionSummary{{
		Resource:             alertResource(),
		LatestProcessID:      process1,
		LatestLifecycleState: agentosprocess.ProcessRunning,
		ProcessCount:         1,
		ActionCount:          1,
		WorksetCount:         1,
		LedgerCount:          1,
		UpdatedAt:            time.Now(),
	}}, nil
}

func processStatus(processID string) agentosprocess.Status {
	return agentosprocess.Status{
		ProcessID:      processID,
		Kind:           investigationKind,
		AccountID:      account1,
		ProjectID:      project1,
		Resource:       alertResource(),
		LifecycleState: agentosprocess.ProcessRunning,
		UpdatedAt:      time.Now(),
	}
}

func ledgerEntry(spec *agentosprocess.LedgerEntrySpec) agentosprocess.LedgerEntry {
	return agentosprocess.LedgerEntry{
		LedgerEntrySpec: *spec,
		Sequence:        11,
		CreatedAt:       time.Now(),
	}
}

func actionStatus(actionID string) agentosprocess.GovernedActionStatus {
	return agentosprocess.GovernedActionStatus{
		ActionID:       actionID,
		AccountID:      account1,
		ProjectID:      project1,
		ProcessID:      process1,
		Resource:       alertResource(),
		Kind:           isolateHostKind,
		LifecycleState: agentosprocess.ActionWaitingApproval,
		UpdatedAt:      time.Now(),
	}
}

func worksetStatus(worksetID string) agentosprocess.WorksetStatus {
	return agentosprocess.WorksetStatus{
		WorksetID:      worksetID,
		AccountID:      account1,
		ProjectID:      project1,
		ProcessID:      process1,
		Resource:       alertResource(),
		Kind:           huntBackfillKind,
		LifecycleState: agentosprocess.WorksetRunning,
		UpdatedAt:      time.Now(),
	}
}

func alertResource() agentosprocess.ResourceRef {
	return agentosprocess.ResourceRef{
		Kind:       alertKind,
		ResourceID: resource1,
		AccountID:  account1,
		ProjectID:  project1,
	}
}

var _ interface {
	agentos.Runtime
	agentos.PlanRuntime
	agentosprocess.Runtime
	agentosprocess.LedgerRuntime
	agentosprocess.GovernedActionRuntime
	agentosprocess.BatchRuntime
	agentosprocess.ProjectionRuntime
} = (*fakePlatformRuntime)(nil)
