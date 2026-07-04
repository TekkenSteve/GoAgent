// examples/http/runplan/main.go
//
// Start a durable AgentOS RunPlan through the public REST control plane.
//
//	go run examples/http/runplan/main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/examples/client"
)

var errUnsafeBaseURL = errors.New("unsafe base url")

const (
	requestTimeout             = 30 * time.Second
	exampleContinueAsNewEvents = 100
	exampleMaxHistoryEvents    = 1000
)

func main() {
	ctx := context.Background()

	endpoint, err := newControlPlaneEndpoint(env("BASE_URL", "http://localhost:8080"))
	if err != nil {
		exitf("base url: %v", err)
	}

	accountID := env("ACCOUNT_ID", "demo-account")
	projectID := env("PROJECT_ID", "demo-project")
	planID := fmt.Sprintf("http-runplan-%d", time.Now().UnixMilli())
	native := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID: planID, ThreadID: planID, AccountID: accountID, ProjectID: projectID, IdempotencyKey: planID + ":start", RequestedAt: time.Now().UTC(),
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "research", Capability: agentos.CapabilityRun, Run: agentos.RunSpec{RunID: planID + "-research", ThreadID: planID, AccountID: accountID, ProjectID: projectID, UserMessage: "Summarize why AgentOS RunPlan nodes are backend-owned child runs.", Backend: native, IdempotencyKey: planID + ":research", RequestedAt: time.Now().UTC()}},
			{NodeID: "review", Capability: agentos.CapabilityRun, Run: agentos.RunSpec{RunID: planID + "-review", ThreadID: planID, AccountID: accountID, ProjectID: projectID, UserMessage: "Review the previous AgentOS RunPlan summary for operational gaps.", Backend: native, IdempotencyKey: planID + ":review", RequestedAt: time.Now().UTC()}},
		},
		Edges:  []agentos.PlanEdgeSpec{{EdgeID: "research-review", From: "research", To: "review", On: agentos.EdgeOnSuccess}},
		Policy: agentos.PlanPolicy{MaxParallelNodes: 1, ContinueAsNewEvents: exampleContinueAsNewEvents, MaxHistoryEvents: exampleMaxHistoryEvents},
	}
	c := client.New(endpoint.baseURL(), accountID)

	var status agentos.RunPlanStatus
	if err := c.Do(ctx, "POST", "/v1/agentos/plans", &spec, &status); err != nil {
		exitf("start plan: %v", err)
	}

	fmt.Fprintf(os.Stdout, "plan started: id=%s state=%s\n", status.PlanID, status.LifecycleState)

	if err := c.Do(ctx, "GET", endpoint.planStatusPath(planID, accountID, projectID), nil, &status); err != nil {
		exitf("status plan: %v", err)
	}

	fmt.Fprintf(os.Stdout, "plan status: id=%s state=%s active_runs=%d\n", status.PlanID, status.LifecycleState, len(status.ActiveRunIDs))

	for i := range status.Nodes {
		node := &status.Nodes[i]
		fmt.Fprintf(os.Stdout, "  node=%s backend=%s/%s state=%s run=%s\n", node.NodeID, node.Backend.Kind, node.Backend.Name, node.LifecycleState, node.RunID)
	}
}

type controlPlaneEndpoint struct {
	base url.URL
}

func newControlPlaneEndpoint(raw string) (controlPlaneEndpoint, error) {
	baseURL, err := url.Parse(raw)
	if err != nil {
		return controlPlaneEndpoint{}, err
	}

	if baseURL.Scheme != "http" && baseURL.Scheme != "https" {
		return controlPlaneEndpoint{}, fmt.Errorf("%w: scheme %q", errUnsafeBaseURL, baseURL.Scheme)
	}

	if baseURL.Host == "" {
		return controlPlaneEndpoint{}, fmt.Errorf("%w: host is required", errUnsafeBaseURL)
	}

	if baseURL.User != nil {
		return controlPlaneEndpoint{}, fmt.Errorf("%w: user info is not allowed", errUnsafeBaseURL)
	}

	baseURL.RawQuery = ""
	baseURL.Fragment = ""

	return controlPlaneEndpoint{base: *baseURL}, nil
}

func (endpoint *controlPlaneEndpoint) baseURL() string {
	base := endpoint.base
	base.Path = ""
	base.RawPath = ""
	base.RawQuery = ""
	base.Fragment = ""

	return base.String()
}

func (endpoint *controlPlaneEndpoint) planStatusPath(planID, accountID, projectID string) string {
	values := url.Values{}
	values.Set("account_id", accountID)
	values.Set("project_id", projectID)

	return "/v1/agentos/plans/" + url.PathEscape(planID) + "/status?" + values.Encode()
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
