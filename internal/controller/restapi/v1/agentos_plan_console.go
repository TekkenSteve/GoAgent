package v1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
	"github.com/gofiber/fiber/v2"
)

const (
	agentOSPlanConsoleDefaultEventLimit    = 100
	agentOSPlanConsoleDefaultAuditLimit    = 50
	agentOSPlanConsoleDefaultArtifactLimit = 50
	agentOSPlanConsoleMaxEventLimit        = 500
	agentOSPlanConsoleMaxAuditLimit        = 200
	agentOSPlanConsoleMaxArtifactLimit     = 200
)

var agentOSPlanConsoleTemplate = template.Must(template.New("agentos_plan_console").Parse(agentOSPlanConsoleHTML))

// @Summary     AgentOS plan console
// @Description Render an operator console for one AgentOS RunPlan.
// @ID          agentos-plan-console
// @Tags        agentos
// @Accept      json
// @Produce     html
// @Param       plan_id path string true "Plan ID"
// @Param       account_id query string true "Account ID"
// @Param       project_id query string true "Project ID"
// @Param       event_limit query int false "Maximum events"
// @Param       audit_limit query int false "Maximum audit records"
// @Param       artifact_limit query int false "Maximum artifact refs"
// @Success     200 {string} string "HTML console"
// @Failure     400 {object} response.Error
// @Failure     404 {object} response.Error
// @Failure     500 {object} response.Error
// @Router      /agentos/plans/{plan_id}/console [get]
func (r *V1) agentOSPlanConsole(ctx *fiber.Ctx) error {
	if r.planRuntime == nil {
		return errorResponse(ctx, http.StatusNotFound, "agentos plan runtime is not configured")
	}

	var req request.AgentOSPlanConsoleScope
	if err := ctx.QueryParser(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "invalid console scope")
	}
	if err := r.v.Struct(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	eventLimit, err := agentOSPlanConsoleLimit("event_limit", req.EventLimit, agentOSPlanConsoleDefaultEventLimit, agentOSPlanConsoleMaxEventLimit)
	if err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}
	auditLimit, err := agentOSPlanConsoleLimit("audit_limit", req.AuditLimit, agentOSPlanConsoleDefaultAuditLimit, agentOSPlanConsoleMaxAuditLimit)
	if err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}
	artifactLimit, err := agentOSPlanConsoleLimit("artifact_limit", req.ArtifactLimit, agentOSPlanConsoleDefaultArtifactLimit, agentOSPlanConsoleMaxArtifactLimit)
	if err != nil {
		return errorResponse(ctx, http.StatusBadRequest, err.Error())
	}

	ref := agentos.PlanRef{
		PlanID:    ctx.Params("plan_id"),
		AccountID: req.AccountID,
		ProjectID: req.ProjectID,
	}
	description, err := r.planRuntime.DescribePlan(ctx.UserContext(), ref)
	if err != nil {
		return agentOSError(ctx, err)
	}
	events, err := r.planRuntime.ListPlanEvents(ctx.UserContext(), agentos.PlanEventScope{
		PlanID:    ref.PlanID,
		AccountID: ref.AccountID,
		ProjectID: ref.ProjectID,
		Limit:     eventLimit,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}
	debugTraces, err := r.planRuntime.ListPlanDebugTraces(ctx.UserContext(), agentos.PlanDebugTraceScope{
		PlanID:    ref.PlanID,
		AccountID: ref.AccountID,
		ProjectID: ref.ProjectID,
		Limit:     eventLimit,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}
	artifacts, err := r.planRuntime.ListPlanArtifacts(ctx.UserContext(), agentos.PlanArtifactScope{
		PlanID:    ref.PlanID,
		AccountID: ref.AccountID,
		ProjectID: ref.ProjectID,
		Limit:     artifactLimit,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}
	audits, err := r.planRuntime.ListPlanAudits(ctx.UserContext(), agentos.PlanAuditScope{
		PlanID:    ref.PlanID,
		AccountID: ref.AccountID,
		ProjectID: ref.ProjectID,
		Limit:     auditLimit,
	})
	if err != nil {
		return agentOSError(ctx, err)
	}

	view := newAgentOSPlanConsoleView(ref, description, events, debugTraces, artifacts, audits)
	var body bytes.Buffer
	if err := agentOSPlanConsoleTemplate.Execute(&body, view); err != nil {
		return errorResponse(ctx, http.StatusInternalServerError, fmt.Sprintf("render plan console: %v", err))
	}

	ctx.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
	return ctx.Status(http.StatusOK).Send(body.Bytes())
}

func agentOSPlanConsoleLimit(name string, requested, defaultValue, maxValue int) (int, error) {
	if requested < 0 {
		return 0, fmt.Errorf("%s must be non-negative", name)
	}
	if requested > maxValue {
		return 0, fmt.Errorf("%s must be less than or equal to %d", name, maxValue)
	}
	if requested == 0 {
		return defaultValue, nil
	}

	return requested, nil
}

type agentOSPlanConsoleView struct {
	PlanID              string
	AccountID           string
	ProjectID           string
	Status              agentOSPlanConsoleStatusView
	Nodes               []agentOSPlanConsoleNodeView
	GraphEdges          []agentOSPlanConsoleEdgeView
	ActiveRunIDs        []string
	Artifacts           []agentOSPlanConsoleArtifactView
	Events              []agentOSPlanConsoleEventView
	DebugTraces         []agentOSPlanConsoleDebugTraceView
	Audits              []agentOSPlanConsoleAuditView
	ControlEndpoint     string
	SignalEndpoint      string
	AuthorEndpoint      string
	DescriptionEndpoint string
	StatusEndpoint      string
	EventsEndpoint      string
	DebugEndpoint       string
	ArtifactsEndpoint   string
	RetrySignalType     agentos.SignalType
	PayloadNodeIDKey    string
	PayloadReasonKey    string
	Controls            []agentOSPlanConsoleControlAction
	PlanSignals         []agentOSPlanConsoleSignalAction
}

type agentOSPlanConsoleStatusView struct {
	LifecycleState string
	StateClass     string
	Reason         string
	SpentCents     string
	UpdatedAt      string
	NodeCount      int
	ArtifactCount  int
	ActiveRunCount int
}

type agentOSPlanConsoleNodeView struct {
	NodeID         string
	RunID          string
	Backend        string
	Capability     string
	LifecycleState string
	StateClass     string
	Attempts       string
	SpentCents     string
	Reason         string
	ArtifactCount  int
	StartedAt      string
	CompletedAt    string
	UpdatedAt      string
	CanRetry       bool
}

type agentOSPlanConsoleEdgeView struct {
	EdgeID       string
	From         string
	To           string
	On           string
	Condition    string
	MappingCount int
}

type agentOSPlanConsoleArtifactView struct {
	ArtifactID string
	NodeID     string
	RunID      string
	Name       string
	Kind       string
	MediaType  string
	Size       string
	Digest     string
	URI        string
	CreatedAt  string
	Endpoint   string
}

type agentOSPlanConsoleEventView struct {
	Sequence  string
	EventID   string
	EventType string
	NodeID    string
	RunID     string
	Source    string
	Timestamp string
	Payload   string
}

type agentOSPlanConsoleDebugTraceView struct {
	Sequence        string
	EventType       string
	NodeID          string
	RunID           string
	Transition      string
	Capability      string
	InputResolution string
	Conditions      string
}

type agentOSPlanConsoleAuditView struct {
	AuditID        string
	Action         string
	ActorID        string
	NodeID         string
	RunID          string
	IdempotencyKey string
	CreatedAt      string
	Payload        string
}

type agentOSPlanConsoleControlAction struct {
	Label     string
	Title     string
	Operation agentos.ControlOperation
	Class     string
}

type agentOSPlanConsoleSignalAction struct {
	Label      string
	Title      string
	SignalType agentos.SignalType
	Class      string
}

func newAgentOSPlanConsoleView(ref agentos.PlanRef, description agentos.RunPlanDescription, events []agentos.PlanEvent, debugTraces []agentos.PlanDebugTrace, artifacts []agentos.ArtifactRef, audits []agentos.PlanAuditRecord) agentOSPlanConsoleView {
	scopeQuery := agentOSPlanConsoleScopeQuery(ref)
	status := description.Status

	return agentOSPlanConsoleView{
		PlanID:              ref.PlanID,
		AccountID:           ref.AccountID,
		ProjectID:           ref.ProjectID,
		Status:              newAgentOSPlanConsoleStatusView(status),
		Nodes:               newAgentOSPlanConsoleNodeViews(description.Topology.Nodes),
		GraphEdges:          newAgentOSPlanConsoleEdgeViews(description.Topology.Edges),
		ActiveRunIDs:        append([]string(nil), status.ActiveRunIDs...),
		Artifacts:           newAgentOSPlanConsoleArtifactViews(ref, artifacts),
		Events:              newAgentOSPlanConsoleEventViews(events),
		DebugTraces:         newAgentOSPlanConsoleDebugTraceViews(debugTraces),
		Audits:              newAgentOSPlanConsoleAuditViews(audits),
		ControlEndpoint:     "control",
		SignalEndpoint:      "signals",
		AuthorEndpoint:      "/v1/agentos/plans/author",
		DescriptionEndpoint: "description?" + scopeQuery,
		StatusEndpoint:      "status?" + scopeQuery,
		EventsEndpoint:      "events/history?" + scopeQuery,
		DebugEndpoint:       "debug/traces?" + scopeQuery,
		ArtifactsEndpoint:   "artifacts?" + scopeQuery,
		RetrySignalType:     agentos.SignalPlanNodeRetry,
		PayloadNodeIDKey:    agentos.SignalPayloadNodeID,
		PayloadReasonKey:    agentos.SignalPayloadReason,
		Controls: []agentOSPlanConsoleControlAction{
			{Label: "Pause", Title: "Pause plan", Operation: agentos.ControlPause, Class: "neutral"},
			{Label: "Resume", Title: "Resume plan", Operation: agentos.ControlResume, Class: "primary"},
			{Label: "Cancel", Title: "Cancel plan", Operation: agentos.ControlCancel, Class: "danger"},
		},
		PlanSignals: []agentOSPlanConsoleSignalAction{
			{Label: "Approve", Title: "Approve blocked plan", SignalType: agentos.SignalPlanApprove, Class: "primary"},
			{Label: "Reject", Title: "Reject blocked plan", SignalType: agentos.SignalPlanReject, Class: "danger"},
		},
	}
}

func newAgentOSPlanConsoleStatusView(status agentos.RunPlanStatus) agentOSPlanConsoleStatusView {
	return agentOSPlanConsoleStatusView{
		LifecycleState: status.LifecycleState,
		StateClass:     agentOSPlanConsoleStateClass(status.LifecycleState),
		Reason:         status.Reason,
		SpentCents:     strconv.FormatInt(status.BudgetUsage.SpentCents, 10),
		UpdatedAt:      agentOSPlanConsoleTime(status.UpdatedAt),
		NodeCount:      len(status.Nodes),
		ArtifactCount:  len(status.Artifacts),
		ActiveRunCount: len(status.ActiveRunIDs),
	}
}

func newAgentOSPlanConsoleNodeViews(nodes []agentos.PlanTopologyNode) []agentOSPlanConsoleNodeView {
	views := make([]agentOSPlanConsoleNodeView, 0, len(nodes))
	for _, node := range nodes {
		status := node.Status
		views = append(views, agentOSPlanConsoleNodeView{
			NodeID:         node.NodeID,
			RunID:          node.RunID,
			Backend:        agentOSPlanConsoleBackend(node.Backend),
			Capability:     node.Capability,
			LifecycleState: status.LifecycleState,
			StateClass:     agentOSPlanConsoleStateClass(status.LifecycleState),
			Attempts:       strconv.FormatInt(int64(status.Attempts), 10),
			SpentCents:     strconv.FormatInt(status.BudgetUsage.SpentCents, 10),
			Reason:         status.Reason,
			ArtifactCount:  len(status.Artifacts),
			StartedAt:      agentOSPlanConsoleTime(status.StartedAt),
			CompletedAt:    agentOSPlanConsoleTime(status.CompletedAt),
			UpdatedAt:      agentOSPlanConsoleTime(status.UpdatedAt),
			CanRetry:       status.LifecycleState == agentos.PlanNodeFailed,
		})
	}

	return views
}

func newAgentOSPlanConsoleEdgeViews(edges []agentos.PlanTopologyEdge) []agentOSPlanConsoleEdgeView {
	views := make([]agentOSPlanConsoleEdgeView, 0, len(edges))
	for _, edge := range edges {
		views = append(views, agentOSPlanConsoleEdgeView{
			EdgeID:       edge.EdgeID,
			From:         edge.From,
			To:           edge.To,
			On:           string(edge.On),
			Condition:    edge.Condition,
			MappingCount: len(edge.InputMapping),
		})
	}

	return views
}

func newAgentOSPlanConsoleArtifactViews(ref agentos.PlanRef, artifacts []agentos.ArtifactRef) []agentOSPlanConsoleArtifactView {
	views := make([]agentOSPlanConsoleArtifactView, 0, len(artifacts))
	scopeQuery := agentOSPlanConsoleScopeQuery(ref)
	for _, artifact := range artifacts {
		views = append(views, agentOSPlanConsoleArtifactView{
			ArtifactID: artifact.ArtifactID,
			NodeID:     artifact.NodeID,
			RunID:      artifact.RunID,
			Name:       artifact.Name,
			Kind:       string(artifact.Kind),
			MediaType:  artifact.MediaType,
			Size:       agentOSPlanConsoleBytes(artifact.SizeBytes),
			Digest:     artifact.Digest,
			URI:        artifact.URI,
			CreatedAt:  agentOSPlanConsoleTime(artifact.CreatedAt),
			Endpoint:   "artifacts/" + url.PathEscape(artifact.ArtifactID) + "?" + scopeQuery,
		})
	}

	return views
}

func newAgentOSPlanConsoleEventViews(events []agentos.PlanEvent) []agentOSPlanConsoleEventView {
	views := make([]agentOSPlanConsoleEventView, 0, len(events))
	for _, event := range events {
		views = append(views, agentOSPlanConsoleEventView{
			Sequence:  strconv.FormatInt(event.Sequence, 10),
			EventID:   event.EventID,
			EventType: string(event.EventType),
			NodeID:    event.NodeID,
			RunID:     event.RunID,
			Source:    event.Source,
			Timestamp: agentOSPlanConsoleTime(event.Timestamp),
			Payload:   agentOSPlanConsoleJSON(event.Payload),
		})
	}

	return views
}

func newAgentOSPlanConsoleDebugTraceViews(traces []agentos.PlanDebugTrace) []agentOSPlanConsoleDebugTraceView {
	views := make([]agentOSPlanConsoleDebugTraceView, 0, len(traces))
	for _, trace := range traces {
		views = append(views, agentOSPlanConsoleDebugTraceView{
			Sequence:        strconv.FormatInt(trace.Sequence, 10),
			EventType:       string(trace.EventType),
			NodeID:          trace.NodeID,
			RunID:           trace.RunID,
			Transition:      agentOSPlanConsoleTransition(trace.Transition),
			Capability:      agentOSPlanConsoleCapability(trace.Capability),
			InputResolution: agentOSPlanConsoleInputResolution(trace.InputResolution),
			Conditions:      agentOSPlanConsoleConditions(trace.Conditions),
		})
	}

	return views
}

func agentOSPlanConsoleTransition(transition *agentos.PlanStateTransition) string {
	if transition == nil {
		return ""
	}
	if transition.PreviousLifecycleState == "" {
		return transition.NextLifecycleState
	}
	if transition.NextLifecycleState == "" {
		return transition.PreviousLifecycleState
	}

	return transition.PreviousLifecycleState + " -> " + transition.NextLifecycleState
}

func agentOSPlanConsoleCapability(capability *agentos.PlanCapabilityTrace) string {
	if capability == nil {
		return ""
	}
	parts := []string{agentOSPlanConsoleBackend(capability.Backend), capability.Capability}
	if len(capability.Controls) > 0 {
		parts = append(parts, "controls:"+strconv.Itoa(len(capability.Controls)))
	}
	if len(capability.Signals) > 0 {
		parts = append(parts, "signals:"+strconv.Itoa(len(capability.Signals)))
	}
	if capability.HasInputSchema {
		parts = append(parts, "input-schema")
	}
	if capability.HasOutputSchema {
		parts = append(parts, "output-schema")
	}

	return strings.Join(parts, " ")
}

func agentOSPlanConsoleInputResolution(input *agentos.PlanInputResolutionTrace) string {
	if input == nil {
		return ""
	}
	parts := make([]string, 0, 3)
	if input.InputDigest != "" {
		parts = append(parts, "digest:"+input.InputDigest)
	}
	if len(input.InputKeys) > 0 {
		parts = append(parts, "keys:"+strings.Join(input.InputKeys, ","))
	}
	parts = append(parts, "mappings:"+strconv.Itoa(input.MappingCount))

	return strings.Join(parts, " ")
}

func agentOSPlanConsoleConditions(conditions []agentos.PlanConditionTrace) string {
	if len(conditions) == 0 {
		return ""
	}
	items := make([]string, 0, len(conditions))
	for _, condition := range conditions {
		items = append(items, condition.Expression+"="+strconv.FormatBool(condition.Result))
	}

	return strings.Join(items, "; ")
}

func newAgentOSPlanConsoleAuditViews(audits []agentos.PlanAuditRecord) []agentOSPlanConsoleAuditView {
	views := make([]agentOSPlanConsoleAuditView, 0, len(audits))
	for _, audit := range audits {
		views = append(views, agentOSPlanConsoleAuditView{
			AuditID:        audit.AuditID,
			Action:         string(audit.Action),
			ActorID:        audit.ActorID,
			NodeID:         audit.NodeID,
			RunID:          audit.RunID,
			IdempotencyKey: audit.IdempotencyKey,
			CreatedAt:      agentOSPlanConsoleTime(audit.CreatedAt),
			Payload:        agentOSPlanConsoleJSON(audit.Payload),
		})
	}

	return views
}

func agentOSPlanConsoleScopeQuery(ref agentos.PlanRef) string {
	values := url.Values{}
	values.Set("account_id", ref.AccountID)
	if ref.ProjectID != "" {
		values.Set("project_id", ref.ProjectID)
	}

	return values.Encode()
}

func agentOSPlanConsoleBackend(ref agentos.BackendRef) string {
	if ref.Name == "" {
		return string(ref.Kind)
	}
	if ref.Kind == "" {
		return ref.Name
	}

	return string(ref.Kind) + ":" + ref.Name
}

func agentOSPlanConsoleStateClass(state string) string {
	switch state {
	case agentos.PlanLifecycleSucceeded:
		return "success"
	case agentos.PlanLifecycleFailed:
		return "danger"
	case agentos.PlanLifecycleCanceled:
		return "muted"
	case agentos.PlanLifecycleBlocked:
		return "warning"
	case agentos.PlanLifecycleRunning, agentos.PlanNodeReady:
		return "active"
	case agentos.PlanLifecyclePending, agentos.PlanNodeSkipped:
		return "neutral"
	default:
		return "neutral"
	}
}

func agentOSPlanConsoleTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}

	return value.UTC().Format(time.RFC3339)
}

func agentOSPlanConsoleJSON(value any) string {
	if value == nil {
		return ""
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", value)
	}

	return string(data)
}

func agentOSPlanConsoleBytes(size int64) string {
	if size <= 0 {
		return ""
	}
	const (
		kib = 1024
		mib = kib * 1024
		gib = mib * 1024
	)
	switch {
	case size >= gib:
		return fmt.Sprintf("%.1f GiB", float64(size)/gib)
	case size >= mib:
		return fmt.Sprintf("%.1f MiB", float64(size)/mib)
	case size >= kib:
		return fmt.Sprintf("%.1f KiB", float64(size)/kib)
	default:
		return strconv.FormatInt(size, 10) + " B"
	}
}

const agentOSPlanConsoleHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AgentOS Plan Console - {{ .PlanID }}</title>
<style>
:root {
  color-scheme: light;
  --bg: #f6f8fb;
  --surface: #ffffff;
  --ink: #172033;
  --muted: #637083;
  --line: #d8dee8;
  --active: #2459d6;
  --success: #1f7a4d;
  --warning: #9a5b00;
  --danger: #b42318;
  --neutral: #57606f;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  font-size: 14px;
  line-height: 1.45;
}
a { color: var(--active); text-decoration: none; }
a:hover { text-decoration: underline; }
button, input {
  font: inherit;
}
.shell {
  width: min(1440px, 100%);
  margin: 0 auto;
  padding: 20px;
}
.topbar {
  display: flex;
  gap: 16px;
  justify-content: space-between;
  align-items: flex-start;
  padding: 12px 0 18px;
  border-bottom: 1px solid var(--line);
}
.eyebrow {
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0;
  text-transform: uppercase;
}
h1, h2 {
  margin: 0;
  letter-spacing: 0;
}
h1 {
  font-size: clamp(22px, 3vw, 30px);
  line-height: 1.15;
  overflow-wrap: anywhere;
}
h2 {
  font-size: 16px;
}
.toolbar {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  align-items: end;
  gap: 8px;
  min-width: min(620px, 100%);
}
.field {
  display: grid;
  gap: 4px;
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
}
.field input {
  width: 180px;
  height: 34px;
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 6px 8px;
  background: var(--surface);
  color: var(--ink);
}
.reason-input {
  width: min(260px, 100%);
}
.button {
  min-height: 34px;
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 6px 10px;
  background: var(--surface);
  color: var(--ink);
  cursor: pointer;
  white-space: nowrap;
}
.button:hover { border-color: #aab4c3; }
.button:disabled { cursor: wait; opacity: .58; }
.button.primary { border-color: #b8c8f4; color: var(--active); }
.button.danger { border-color: #f0b8b3; color: var(--danger); }
.button.neutral { color: var(--neutral); }
.action-status {
  flex-basis: 100%;
  min-height: 20px;
  color: var(--muted);
  text-align: right;
  overflow-wrap: anywhere;
}
.summary {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 12px;
  padding: 16px 0;
  border-bottom: 1px solid var(--line);
}
.metric {
  min-width: 0;
  padding: 0 12px;
  border-left: 3px solid var(--line);
}
.metric strong {
  display: block;
  font-size: 18px;
  overflow-wrap: anywhere;
}
.metric span {
  color: var(--muted);
  font-size: 12px;
}
.section {
  padding: 18px 0;
  border-bottom: 1px solid var(--line);
}
.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 10px;
}
.node-lanes {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 8px;
  margin-bottom: 12px;
}
.graph-grid {
  display: grid;
  grid-template-columns: minmax(0, 2fr) minmax(280px, 1fr);
  gap: 12px;
}
.node-lane {
  min-height: 74px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  padding: 10px;
}
.node-lane .name {
  font-weight: 700;
  overflow-wrap: anywhere;
}
.node-lane .meta {
  margin-top: 4px;
  color: var(--muted);
  font-size: 12px;
  overflow-wrap: anywhere;
}
.edge-list {
  display: grid;
  gap: 8px;
}
.edge-row {
  min-height: 42px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  padding: 8px 10px;
}
.edge-row .route {
  font-weight: 700;
  overflow-wrap: anywhere;
}
.edge-row .meta {
  margin-top: 3px;
  color: var(--muted);
  font-size: 12px;
  overflow-wrap: anywhere;
}
.table-wrap {
  overflow-x: auto;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
}
table {
  width: 100%;
  border-collapse: collapse;
  min-width: 920px;
}
th, td {
  padding: 9px 10px;
  border-bottom: 1px solid var(--line);
  text-align: left;
  vertical-align: top;
}
th {
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
  background: #f9fafc;
}
td {
  overflow-wrap: anywhere;
}
tr:last-child td { border-bottom: 0; }
.pill {
  display: inline-flex;
  align-items: center;
  max-width: 100%;
  min-height: 24px;
  border-radius: 999px;
  padding: 2px 8px;
  border: 1px solid currentColor;
  font-size: 12px;
  font-weight: 700;
  line-height: 1.3;
  overflow-wrap: anywhere;
}
.pill.active { color: var(--active); }
.pill.success { color: var(--success); }
.pill.warning { color: var(--warning); }
.pill.danger { color: var(--danger); }
.pill.neutral, .pill.muted { color: var(--neutral); }
.chips {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}
.muted {
  color: var(--muted);
}
.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
  font-size: 12px;
}
pre {
  max-width: 520px;
  max-height: 220px;
  margin: 0;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-word;
  color: #263244;
}
.empty {
  padding: 12px;
  color: var(--muted);
  border: 1px dashed var(--line);
  border-radius: 6px;
  background: var(--surface);
}
@media (max-width: 900px) {
  .topbar { display: grid; }
  .toolbar { justify-content: flex-start; min-width: 0; }
  .action-status { text-align: left; }
  .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .graph-grid { grid-template-columns: 1fr; }
  .shell { padding: 14px; }
}
</style>
</head>
<body
  data-account-id="{{ .AccountID }}"
  data-project-id="{{ .ProjectID }}"
  data-control-endpoint="{{ .ControlEndpoint }}"
  data-signal-endpoint="{{ .SignalEndpoint }}"
  data-payload-node-id-key="{{ .PayloadNodeIDKey }}"
  data-payload-reason-key="{{ .PayloadReasonKey }}">
<main class="shell">
  <header class="topbar">
    <div>
      <div class="eyebrow">AgentOS Plan Console</div>
      <h1>{{ .PlanID }}</h1>
    </div>
    <div class="toolbar" aria-label="Plan controls">
      <a class="mono" href="{{ .AuthorEndpoint }}">author</a>
      <label class="field">Actor ID
        <input id="actor-id" name="actor_id" autocomplete="off" required>
      </label>
      <label class="field">Reason
        <input id="signal-reason" class="reason-input" name="reason" autocomplete="off">
      </label>
      {{ range .Controls }}
      <button class="button {{ .Class }}" type="button" title="{{ .Title }}" data-control="{{ .Operation }}">{{ .Label }}</button>
      {{ end }}
      {{ range .PlanSignals }}
      <button class="button {{ .Class }}" type="button" title="{{ .Title }}" data-signal="{{ .SignalType }}">{{ .Label }}</button>
      {{ end }}
      <div id="action-status" class="action-status" role="status" aria-live="polite"></div>
    </div>
  </header>

  <section class="summary" aria-label="Plan summary">
    <div class="metric"><strong><span class="pill {{ .Status.StateClass }}">{{ .Status.LifecycleState }}</span></strong><span>Lifecycle</span></div>
    <div class="metric"><strong>{{ .Status.NodeCount }}</strong><span>Nodes</span></div>
    <div class="metric"><strong>{{ .Status.ActiveRunCount }}</strong><span>Active Runs</span></div>
    <div class="metric"><strong>{{ .Status.ArtifactCount }}</strong><span>Status Artifacts</span></div>
    <div class="metric"><strong>{{ .Status.SpentCents }}</strong><span>Spent Cents</span></div>
    <div class="metric"><strong>{{ .Status.UpdatedAt }}</strong><span>Updated</span></div>
  </section>

  {{ if .Status.Reason }}
  <section class="section">
    <div class="section-head"><h2>Failure Reason</h2></div>
    <div class="empty">{{ .Status.Reason }}</div>
  </section>
  {{ end }}

  <section class="section">
    <div class="section-head">
      <h2>Plan Graph</h2>
      <a class="mono" href="{{ .DescriptionEndpoint }}">description json</a>
    </div>
    {{ if .Nodes }}
    <div class="graph-grid">
      <div class="node-lanes" aria-label="Graph nodes">
        {{ range .Nodes }}
        <div class="node-lane">
          <div class="chips"><span class="pill {{ .StateClass }}">{{ .LifecycleState }}</span></div>
          <div class="name">{{ .NodeID }}</div>
          <div class="meta">{{ .Backend }}</div>
          {{ if .Capability }}<div class="meta">capability: {{ .Capability }}</div>{{ end }}
        </div>
        {{ end }}
      </div>
      <div class="edge-list" aria-label="Graph edges">
        {{ if .GraphEdges }}
        {{ range .GraphEdges }}
        <div class="edge-row">
          <div class="route mono">{{ .From }} -&gt; {{ .To }}</div>
          <div class="meta">{{ .On }} {{ if .Condition }}condition: {{ .Condition }}{{ end }} {{ if .MappingCount }}mappings: {{ .MappingCount }}{{ end }}</div>
        </div>
        {{ end }}
        {{ else }}
        <div class="empty">No edges.</div>
        {{ end }}
      </div>
    </div>
    {{ else }}
    <div class="empty">No graph nodes.</div>
    {{ end }}
  </section>

  <section class="section">
    <div class="section-head">
      <h2>Nodes</h2>
      <a class="mono" href="{{ .StatusEndpoint }}">status json</a>
    </div>
    {{ if .Nodes }}
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Node</th>
            <th>State</th>
            <th>Backend</th>
            <th>Capability</th>
            <th>Run</th>
            <th>Attempts</th>
            <th>Budget</th>
            <th>Artifacts</th>
            <th>Updated</th>
            <th>Reason</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          {{ range .Nodes }}
          <tr>
            <td class="mono">{{ .NodeID }}</td>
            <td><span class="pill {{ .StateClass }}">{{ .LifecycleState }}</span></td>
            <td>{{ .Backend }}</td>
            <td>{{ .Capability }}</td>
            <td class="mono">{{ .RunID }}</td>
            <td>{{ .Attempts }}</td>
            <td>{{ .SpentCents }}</td>
            <td>{{ .ArtifactCount }}</td>
            <td>{{ .UpdatedAt }}</td>
            <td>{{ .Reason }}</td>
            <td>
              {{ if .CanRetry }}
              <button class="button primary" type="button" title="Retry failed node" data-signal="{{ $.RetrySignalType }}" data-node-id="{{ .NodeID }}">Retry</button>
              {{ else }}
              <span class="muted">-</span>
              {{ end }}
            </td>
          </tr>
          {{ end }}
        </tbody>
      </table>
    </div>
    {{ else }}
    <div class="empty">No nodes.</div>
    {{ end }}
  </section>

  <section class="section">
    <div class="section-head"><h2>Active Runs</h2></div>
    {{ if .ActiveRunIDs }}
    <div class="chips">
      {{ range .ActiveRunIDs }}<span class="pill active mono">{{ . }}</span>{{ end }}
    </div>
    {{ else }}
    <div class="empty">No active child runs.</div>
    {{ end }}
  </section>

  <section class="section">
    <div class="section-head">
      <h2>Artifacts</h2>
      <a class="mono" href="{{ .ArtifactsEndpoint }}">artifacts json</a>
    </div>
    {{ if .Artifacts }}
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Artifact</th>
            <th>Name</th>
            <th>Kind</th>
            <th>Node</th>
            <th>Run</th>
            <th>Media</th>
            <th>Size</th>
            <th>Created</th>
            <th>Digest</th>
          </tr>
        </thead>
        <tbody>
          {{ range .Artifacts }}
          <tr>
            <td class="mono"><a href="{{ .Endpoint }}">{{ .ArtifactID }}</a></td>
            <td>{{ .Name }}</td>
            <td>{{ .Kind }}</td>
            <td class="mono">{{ .NodeID }}</td>
            <td class="mono">{{ .RunID }}</td>
            <td>{{ .MediaType }}</td>
            <td>{{ .Size }}</td>
            <td>{{ .CreatedAt }}</td>
            <td class="mono">{{ .Digest }}</td>
          </tr>
          {{ end }}
        </tbody>
      </table>
    </div>
    {{ else }}
    <div class="empty">No artifacts.</div>
    {{ end }}
  </section>

  <section class="section">
    <div class="section-head">
      <h2>Debug Traces</h2>
      <a class="mono" href="{{ .DebugEndpoint }}">debug json</a>
    </div>
    {{ if .DebugTraces }}
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Seq</th>
            <th>Type</th>
            <th>Node</th>
            <th>Run</th>
            <th>Transition</th>
            <th>Capability</th>
            <th>Input</th>
            <th>Conditions</th>
          </tr>
        </thead>
        <tbody>
          {{ range .DebugTraces }}
          <tr>
            <td class="mono">{{ .Sequence }}</td>
            <td>{{ .EventType }}</td>
            <td class="mono">{{ .NodeID }}</td>
            <td class="mono">{{ .RunID }}</td>
            <td>{{ .Transition }}</td>
            <td>{{ .Capability }}</td>
            <td>{{ .InputResolution }}</td>
            <td>{{ .Conditions }}</td>
          </tr>
          {{ end }}
        </tbody>
      </table>
    </div>
    {{ else }}
    <div class="empty">No debug traces.</div>
    {{ end }}
  </section>

  <section class="section">
    <div class="section-head">
      <h2>Event Timeline</h2>
      <a class="mono" href="{{ .EventsEndpoint }}">events json</a>
    </div>
    {{ if .Events }}
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Seq</th>
            <th>Type</th>
            <th>Node</th>
            <th>Run</th>
            <th>Time</th>
            <th>Source</th>
            <th>Payload</th>
          </tr>
        </thead>
        <tbody>
          {{ range .Events }}
          <tr>
            <td class="mono">{{ .Sequence }}</td>
            <td>{{ .EventType }}</td>
            <td class="mono">{{ .NodeID }}</td>
            <td class="mono">{{ .RunID }}</td>
            <td>{{ .Timestamp }}</td>
            <td>{{ .Source }}</td>
            <td><pre>{{ .Payload }}</pre></td>
          </tr>
          {{ end }}
        </tbody>
      </table>
    </div>
    {{ else }}
    <div class="empty">No events.</div>
    {{ end }}
  </section>

  <section class="section">
    <div class="section-head"><h2>Audit Log</h2></div>
    {{ if .Audits }}
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Action</th>
            <th>Actor</th>
            <th>Node</th>
            <th>Run</th>
            <th>Idempotency</th>
            <th>Created</th>
            <th>Payload</th>
          </tr>
        </thead>
        <tbody>
          {{ range .Audits }}
          <tr>
            <td>{{ .Action }}</td>
            <td class="mono">{{ .ActorID }}</td>
            <td class="mono">{{ .NodeID }}</td>
            <td class="mono">{{ .RunID }}</td>
            <td class="mono">{{ .IdempotencyKey }}</td>
            <td>{{ .CreatedAt }}</td>
            <td><pre>{{ .Payload }}</pre></td>
          </tr>
          {{ end }}
        </tbody>
      </table>
    </div>
    {{ else }}
    <div class="empty">No audit records.</div>
    {{ end }}
  </section>
</main>
<script>
(() => {
  const root = document.body.dataset;
  const actorInput = document.getElementById("actor-id");
  const reasonInput = document.getElementById("signal-reason");
  const status = document.getElementById("action-status");

  function actorID() {
    const value = actorInput.value.trim();
    if (!value) {
      throw new Error("actor_id is required");
    }
    return value;
  }

  function commandKey(kind, value, nodeID) {
    return ["plan-console", kind, value, nodeID, crypto.randomUUID()].filter(Boolean).join(":");
  }

  async function postJSON(endpoint, payload) {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      throw new Error(await response.text());
    }
  }

  function scopedPayload() {
    const payload = {
      account_id: root.accountId,
      actor_id: actorID(),
    };
    if (root.projectId) {
      payload.project_id = root.projectId;
    }
    return payload;
  }

  document.addEventListener("click", async (event) => {
    const button = event.target.closest("button[data-control],button[data-signal]");
    if (!button) {
      return;
    }

    button.disabled = true;
    status.textContent = "";
    try {
      if (button.dataset.control) {
        const payload = scopedPayload();
        payload.operation = button.dataset.control;
        payload.idempotency_key = commandKey("control", payload.operation, "");
        await postJSON(root.controlEndpoint, payload);
      } else {
        const payload = scopedPayload();
        payload.type = button.dataset.signal;
        payload.idempotency_key = commandKey("signal", payload.type, button.dataset.nodeId || "");
        payload.payload = {};
        if (button.dataset.nodeId) {
          payload.payload[root.payloadNodeIdKey] = button.dataset.nodeId;
        }
        const reason = reasonInput.value.trim();
        if (reason) {
          payload.payload[root.payloadReasonKey] = reason;
        }
        await postJSON(root.signalEndpoint, payload);
      }
      status.textContent = "accepted";
    } catch (error) {
      status.textContent = error.message;
    } finally {
      button.disabled = false;
    }
  });
})();
</script>
</body>
</html>
`
