package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/goccy/go-json"
)

const (
	agentOSPlanAccountID = "e2e-test-account"
	agentOSPlanProjectID = "e2e-test-project"
	agentOSPlanNodeID    = "native"
)

type planStatus struct {
	PlanID         string           `json:"plan_id"`
	LifecycleState string           `json:"lifecycle_state"`
	Nodes          []planNodeStatus `json:"nodes"`
	ActiveRunIDs   []string         `json:"active_run_ids"`
	Reason         string           `json:"reason"`
}

type planNodeStatus struct {
	NodeID         string             `json:"node_id"`
	RunID          string             `json:"run_id"`
	Backend        agentos.BackendRef `json:"backend"`
	LifecycleState string             `json:"lifecycle_state"`
	Reason         string             `json:"reason"`
}

type planDescription struct {
	PlanID   string       `json:"plan_id"`
	Topology planTopology `json:"topology"`
	Status   planStatus   `json:"status"`
}

type planTopology struct {
	Nodes []planTopologyNode `json:"nodes"`
}

type planTopologyNode struct {
	NodeID     string             `json:"node_id"`
	Backend    agentos.BackendRef `json:"backend"`
	Capability string             `json:"capability"`
	Status     planNodeStatus     `json:"status"`
}

type planEvent struct {
	EventID   string            `json:"event_id"`
	EventType agentos.EventType `json:"event_type"`
	PlanID    string            `json:"plan_id"`
	NodeID    string            `json:"node_id"`
	Sequence  int64             `json:"sequence"`
	Payload   map[string]any    `json:"payload"`
}

type planDebugTrace struct {
	EventID    string                    `json:"event_id"`
	EventType  agentos.EventType         `json:"event_type"`
	PlanID     string                    `json:"plan_id"`
	NodeID     string                    `json:"node_id"`
	Capability *planDebugCapabilityTrace `json:"capability"`
}

type planDebugCapabilityTrace struct {
	Backend    agentos.BackendRef `json:"backend"`
	Capability string             `json:"capability"`
}

type planAuditRecord struct {
	AuditID string                  `json:"audit_id"`
	PlanID  string                  `json:"plan_id"`
	Action  agentos.PlanAuditAction `json:"action"`
}

func TestHTTPAgentOSRunPlanNativeCancelV1(t *testing.T) {
	now := time.Now().UnixNano()
	planID := fmt.Sprintf("e2e-plan-%d", now)
	runID := fmt.Sprintf("e2e-plan-run-%d", now)

	started := startAgentOSPlan(t, planID, runID)
	if started.PlanID != planID ||
		(started.LifecycleState != agentos.PlanLifecyclePending && started.LifecycleState != agentos.PlanLifecycleRunning) {
		t.Fatalf("start status = %#v", started)
	}

	running := waitForPlanNodeState(t, planID, agentOSPlanNodeID, agentos.PlanNodeRunning)
	node := requirePlanNode(t, running, agentOSPlanNodeID)
	if node.RunID != runID ||
		node.Backend.Kind != agentos.BackendKindNative ||
		node.Backend.Name != agentos.BackendNameGoAgentNative {
		t.Fatalf("running node = %#v", node)
	}

	description := getAgentOSPlanDescription(t, planID)
	if description.PlanID != planID || len(description.Topology.Nodes) != 1 {
		t.Fatalf("description = %#v", description)
	}
	if description.Topology.Nodes[0].Capability != agentos.CapabilityRun {
		t.Fatalf("topology capability = %q", description.Topology.Nodes[0].Capability)
	}

	events := listAgentOSPlanEvents(t, planID)
	requirePlanEvent(t, events, agentos.EventPlanStarted)
	requirePlanEvent(t, events, agentos.EventCapabilitySelected)
	requirePlanEvent(t, events, agentos.EventPlanNodeStarted)

	traces := listAgentOSPlanDebugTraces(t, planID)
	requireCapabilityTrace(t, traces)

	audits := listAgentOSPlanAudits(t, planID)
	requirePlanAudit(t, audits, agentos.PlanAuditActionStart)

	requireAgentOSPlanConsole(t, planID)

	controlAgentOSPlan(t, planID, agentos.ControlCancel, fmt.Sprintf("%s-cancel", planID))
	canceled := waitForPlanLifecycle(t, planID, agentos.PlanLifecycleCanceled)
	canceledNode := requirePlanNode(t, canceled, agentOSPlanNodeID)
	if canceledNode.LifecycleState != agentos.PlanNodeCanceled {
		t.Fatalf("canceled node = %#v", canceledNode)
	}

	audits = listAgentOSPlanAudits(t, planID)
	requirePlanAudit(t, audits, agentos.PlanAuditActionControl)
}

func TestHTTPAgentOSRunPlanMixedBackendsV1(t *testing.T) {
	now := time.Now().UnixNano()
	planID := fmt.Sprintf("e2e-mixed-plan-%d", now)
	nativeRunID := fmt.Sprintf("e2e-mixed-native-%d", now)
	temporalRunID := fmt.Sprintf("e2e-mixed-temporal-%d", now)
	httpRunID := fmt.Sprintf("e2e-mixed-http-%d", now)
	grpcRunID := fmt.Sprintf("e2e-mixed-grpc-%d", now)

	nativeBackend := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	temporalBackend := agentos.BackendRef{Kind: agentos.BackendKindTemporalExternal, Name: "mock-temporal"}
	httpBackend := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "mock-http"}
	grpcBackend := agentos.BackendRef{Kind: agentos.BackendKindGRPC, Name: "mock-grpc"}

	started := startAgentOSPlanSpec(t, agentos.RunPlanSpec{
		PlanID:         planID,
		ThreadID:       planID + "-thread",
		AccountID:      agentOSPlanAccountID,
		ProjectID:      agentOSPlanProjectID,
		IdempotencyKey: planID + "-start",
		RequestedAt:    time.Now().UTC(),
		Policy:         agentos.PlanPolicy{MaxParallelNodes: 4},
		Nodes: []agentos.PlanNodeSpec{
			mixedPlanNode("native", nativeRunID, nativeBackend, "Start a native AgentOS plan integration test and wait for follow-up input."),
			mixedPlanNode("temporal", temporalRunID, temporalBackend, "temporal external child run"),
			mixedPlanNode("http", httpRunID, httpBackend, "http child run"),
			mixedPlanNode("grpc", grpcRunID, grpcBackend, "grpc child run"),
		},
	})
	if started.PlanID != planID {
		t.Fatalf("start status = %#v", started)
	}

	running := waitForPlanNodeStates(t, planID, map[string]string{
		"native":   agentos.PlanNodeRunning,
		"temporal": agentos.PlanNodeSucceeded,
		"http":     agentos.PlanNodeSucceeded,
		"grpc":     agentos.PlanNodeSucceeded,
	})
	requirePlanNodeBackend(t, running, "native", nativeRunID, nativeBackend)
	requirePlanNodeBackend(t, running, "temporal", temporalRunID, temporalBackend)
	requirePlanNodeBackend(t, running, "http", httpRunID, httpBackend)
	requirePlanNodeBackend(t, running, "grpc", grpcRunID, grpcBackend)

	description := getAgentOSPlanDescription(t, planID)
	if len(description.Topology.Nodes) != 4 {
		t.Fatalf("description topology = %#v", description.Topology)
	}

	events := listAgentOSPlanEvents(t, planID)
	requirePlanEvent(t, events, agentos.EventPlanStarted)
	requirePlanEvent(t, events, agentos.EventPlanNodeSucceeded)
	requirePlanEvent(t, events, agentos.EventUsageReported)

	traces := listAgentOSPlanDebugTraces(t, planID)
	requireCapabilityTraceForBackend(t, traces, nativeBackend)
	requireCapabilityTraceForBackend(t, traces, temporalBackend)
	requireCapabilityTraceForBackend(t, traces, httpBackend)
	requireCapabilityTraceForBackend(t, traces, grpcBackend)

	controlAgentOSPlan(t, planID, agentos.ControlCancel, fmt.Sprintf("%s-cancel", planID))
	canceled := waitForPlanLifecycle(t, planID, agentos.PlanLifecycleCanceled)
	canceledNative := requirePlanNode(t, canceled, "native")
	if canceledNative.LifecycleState != agentos.PlanNodeCanceled {
		t.Fatalf("native node after plan cancel = %#v", canceledNative)
	}

	audits := listAgentOSPlanAudits(t, planID)
	requirePlanAudit(t, audits, agentos.PlanAuditActionStart)
	requirePlanAudit(t, audits, agentos.PlanAuditActionControl)
}

func mixedPlanNode(nodeID, runID string, backend agentos.BackendRef, message string) agentos.PlanNodeSpec {
	return agentos.PlanNodeSpec{
		NodeID:     nodeID,
		Capability: agentos.CapabilityRun,
		Run: agentos.RunSpec{
			RunID:          runID,
			ThreadID:       runID + "-thread",
			AccountID:      agentOSPlanAccountID,
			ProjectID:      agentOSPlanProjectID,
			UserMessage:    message,
			IdempotencyKey: runID + "-start",
			Backend:        backend,
		},
	}
}

func startAgentOSPlan(t *testing.T, planID, runID string) planStatus {
	t.Helper()

	spec := agentos.RunPlanSpec{
		PlanID:         planID,
		ThreadID:       planID + "-thread",
		AccountID:      agentOSPlanAccountID,
		ProjectID:      agentOSPlanProjectID,
		IdempotencyKey: planID + "-start",
		RequestedAt:    time.Now().UTC(),
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID:     agentOSPlanNodeID,
				Capability: agentos.CapabilityRun,
				Run: agentos.RunSpec{
					RunID:          runID,
					ThreadID:       planID + "-thread",
					AccountID:      agentOSPlanAccountID,
					ProjectID:      agentOSPlanProjectID,
					UserMessage:    "Start a native AgentOS plan integration test and wait for follow-up input.",
					IdempotencyKey: runID + "-start",
					Backend: agentos.BackendRef{
						Kind: agentos.BackendKindNative,
						Name: agentos.BackendNameGoAgentNative,
					},
				},
			},
		},
	}
	return startAgentOSPlanSpec(t, spec)
}

func startAgentOSPlanSpec(t *testing.T, spec agentos.RunPlanSpec) planStatus {
	t.Helper()

	body, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("startAgentOSPlan: marshal: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodPost, basePathV1()+"/agentos/plans", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("startAgentOSPlan: request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("startAgentOSPlan: expected 202, got %d: %s", resp.StatusCode, readResponseBody(t, resp.Body))
	}

	var status planStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("startAgentOSPlan: decode failed: %v", err)
	}

	return status
}

func waitForPlanNodeState(t *testing.T, planID, nodeID, lifecycle string) planStatus {
	t.Helper()

	for range 60 {
		status := getAgentOSPlanStatus(t, planID)
		for _, node := range status.Nodes {
			if node.NodeID == nodeID && node.LifecycleState == lifecycle {
				return status
			}
		}
		time.Sleep(time.Second)
	}

	t.Fatalf("timed out waiting for plan %s node %s to reach %s", planID, nodeID, lifecycle)

	return planStatus{}
}

func waitForPlanNodeStates(t *testing.T, planID string, expected map[string]string) planStatus {
	t.Helper()

	for range 60 {
		status := getAgentOSPlanStatus(t, planID)
		matches := 0
		for nodeID, lifecycle := range expected {
			for _, node := range status.Nodes {
				if node.NodeID == nodeID && node.LifecycleState == lifecycle {
					matches++
					break
				}
			}
		}
		if matches == len(expected) {
			return status
		}
		time.Sleep(time.Second)
	}

	t.Fatalf("timed out waiting for plan %s nodes to reach %#v", planID, expected)

	return planStatus{}
}

func waitForPlanLifecycle(t *testing.T, planID, lifecycle string) planStatus {
	t.Helper()

	for range 60 {
		status := getAgentOSPlanStatus(t, planID)
		if status.LifecycleState == lifecycle {
			return status
		}
		time.Sleep(time.Second)
	}

	t.Fatalf("timed out waiting for plan %s to reach %s", planID, lifecycle)

	return planStatus{}
}

func getAgentOSPlanStatus(t *testing.T, planID string) planStatus {
	t.Helper()

	var status planStatus
	getAgentOSPlanJSON(t, planScopedPath(planID, "status"), &status)

	return status
}

func getAgentOSPlanDescription(t *testing.T, planID string) planDescription {
	t.Helper()

	var description planDescription
	getAgentOSPlanJSON(t, planScopedPath(planID, "description"), &description)

	return description
}

func listAgentOSPlanEvents(t *testing.T, planID string) []planEvent {
	t.Helper()

	var events []planEvent
	getAgentOSPlanJSON(t, planScopedPath(planID, "events/history"), &events)

	return events
}

func listAgentOSPlanDebugTraces(t *testing.T, planID string) []planDebugTrace {
	t.Helper()

	var traces []planDebugTrace
	getAgentOSPlanJSON(t, planScopedPath(planID, "debug/traces"), &traces)

	return traces
}

func listAgentOSPlanAudits(t *testing.T, planID string) []planAuditRecord {
	t.Helper()

	var audits []planAuditRecord
	getAgentOSPlanJSON(t, planScopedPath(planID, "audits"), &audits)

	return audits
}

func requireAgentOSPlanConsole(t *testing.T, planID string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodGet, planScopedPath(planID, "console"), nil)
	if err != nil {
		t.Fatalf("requireAgentOSPlanConsole: request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("requireAgentOSPlanConsole: expected 200, got %d: %s", resp.StatusCode, readResponseBody(t, resp.Body))
	}
}

func controlAgentOSPlan(t *testing.T, planID string, operation agentos.ControlOperation, idempotencyKey string) {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"operation":       operation,
		"account_id":      agentOSPlanAccountID,
		"project_id":      agentOSPlanProjectID,
		"idempotency_key": idempotencyKey,
		"actor_id":        "integration-test",
		"requested_at":    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("controlAgentOSPlan: marshal: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodPost, basePathV1()+"/agentos/plans/"+planID+"/control", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("controlAgentOSPlan: request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("controlAgentOSPlan: expected 202, got %d: %s", resp.StatusCode, readResponseBody(t, resp.Body))
	}
}

func getAgentOSPlanJSON(t *testing.T, endpoint string, out any) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatalf("getAgentOSPlanJSON: request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getAgentOSPlanJSON: expected 200, got %d: %s", resp.StatusCode, readResponseBody(t, resp.Body))
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("getAgentOSPlanJSON: decode failed: %v", err)
	}
}

func planScopedPath(planID, suffix string) string {
	values := url.Values{}
	values.Set("account_id", agentOSPlanAccountID)
	values.Set("project_id", agentOSPlanProjectID)

	return fmt.Sprintf("%s/agentos/plans/%s/%s?%s", basePathV1(), planID, suffix, values.Encode())
}

func requirePlanNode(t *testing.T, status planStatus, nodeID string) planNodeStatus {
	t.Helper()

	for _, node := range status.Nodes {
		if node.NodeID == nodeID {
			return node
		}
	}

	t.Fatalf("plan status missing node %s: %#v", nodeID, status)

	return planNodeStatus{}
}

func requirePlanNodeBackend(t *testing.T, status planStatus, nodeID, runID string, backend agentos.BackendRef) {
	t.Helper()

	node := requirePlanNode(t, status, nodeID)
	if node.RunID != runID || node.Backend != backend {
		t.Fatalf("node %s = %#v, want run %s backend %#v", nodeID, node, runID, backend)
	}
}

func requirePlanEvent(t *testing.T, events []planEvent, eventType agentos.EventType) {
	t.Helper()

	for _, event := range events {
		if event.EventType == eventType {
			return
		}
	}

	t.Fatalf("missing plan event %s in %#v", eventType, events)
}

func requireCapabilityTrace(t *testing.T, traces []planDebugTrace) {
	t.Helper()

	for _, trace := range traces {
		if trace.Capability == nil {
			continue
		}
		if trace.Capability.Backend.Kind == agentos.BackendKindNative &&
			trace.Capability.Backend.Name == agentos.BackendNameGoAgentNative &&
			trace.Capability.Capability == agentos.CapabilityRun {
			return
		}
	}

	t.Fatalf("missing native capability trace in %#v", traces)
}

func requireCapabilityTraceForBackend(t *testing.T, traces []planDebugTrace, backend agentos.BackendRef) {
	t.Helper()

	for _, trace := range traces {
		if trace.Capability == nil {
			continue
		}
		if trace.Capability.Backend == backend && trace.Capability.Capability == agentos.CapabilityRun {
			return
		}
	}

	t.Fatalf("missing capability trace for backend %#v in %#v", backend, traces)
}

func requirePlanAudit(t *testing.T, audits []planAuditRecord, action agentos.PlanAuditAction) {
	t.Helper()

	for _, audit := range audits {
		if audit.Action == action {
			return
		}
	}

	t.Fatalf("missing plan audit %s in %#v", action, audits)
}

func readResponseBody(t *testing.T, body io.Reader) string {
	t.Helper()

	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	return string(data)
}
