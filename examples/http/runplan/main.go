// examples/http/runplan/main.go
//
// Start a durable AgentOS RunPlan through the public REST control plane.
//
//	go run examples/http/runplan/main.go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
)

const requestTimeout = 30 * time.Second

func main() {
	ctx := context.Background()
	baseURL := env("BASE_URL", "http://localhost:8080")
	accountID := env("ACCOUNT_ID", "demo-account")
	projectID := env("PROJECT_ID", "demo-project")
	planID := fmt.Sprintf("http-runplan-%d", time.Now().UnixMilli())

	native := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	spec := agentos.RunPlanSpec{
		PlanID:         planID,
		ThreadID:       planID,
		AccountID:      accountID,
		ProjectID:      projectID,
		IdempotencyKey: planID + ":start",
		RequestedAt:    time.Now().UTC(),
		Nodes: []agentos.PlanNodeSpec{
			{
				NodeID:     "research",
				Capability: agentos.CapabilityRun,
				Run: agentos.RunSpec{
					RunID:          planID + "-research",
					ThreadID:       planID,
					AccountID:      accountID,
					ProjectID:      projectID,
					UserMessage:    "Summarize why AgentOS RunPlan nodes are backend-owned child runs.",
					Backend:        native,
					IdempotencyKey: planID + ":research",
					RequestedAt:    time.Now().UTC(),
				},
			},
			{
				NodeID:     "review",
				Capability: agentos.CapabilityRun,
				Run: agentos.RunSpec{
					RunID:          planID + "-review",
					ThreadID:       planID,
					AccountID:      accountID,
					ProjectID:      projectID,
					UserMessage:    "Review the previous AgentOS RunPlan summary for operational gaps.",
					Backend:        native,
					IdempotencyKey: planID + ":review",
					RequestedAt:    time.Now().UTC(),
				},
			},
		},
		Edges: []agentos.PlanEdgeSpec{
			{EdgeID: "research-review", From: "research", To: "review", On: agentos.EdgeOnSuccess},
		},
		Policy: agentos.PlanPolicy{
			MaxParallelNodes:    1,
			ContinueAsNewEvents: 100,
			MaxHistoryEvents:    1000,
		},
	}

	status, err := postJSON[agentos.RunPlanStatus](ctx, baseURL+"/v1/agentos/plans", spec)
	if err != nil {
		exitf("start plan: %v", err)
	}
	fmt.Fprintf(os.Stdout, "plan started: id=%s state=%s\n", status.PlanID, status.LifecycleState)

	status, err = getJSON[agentos.RunPlanStatus](ctx, planStatusURL(baseURL, planID, accountID, projectID))
	if err != nil {
		exitf("status plan: %v", err)
	}
	fmt.Fprintf(os.Stdout, "plan status: id=%s state=%s active_runs=%d\n", status.PlanID, status.LifecycleState, len(status.ActiveRunIDs))
	for _, node := range status.Nodes {
		fmt.Fprintf(os.Stdout, "  node=%s backend=%s/%s state=%s run=%s\n",
			node.NodeID,
			node.Backend.Kind,
			node.Backend.Name,
			node.LifecycleState,
			node.RunID,
		)
	}
}

func postJSON[T any](parent context.Context, endpoint string, body any) (T, error) {
	var zero T
	data, err := json.Marshal(body)
	if err != nil {
		return zero, fmt.Errorf("marshal request: %w", err)
	}

	ctx, cancel := context.WithTimeout(parent, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return zero, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	return doJSON[T](req)
}

func getJSON[T any](parent context.Context, endpoint string) (T, error) {
	var zero T
	ctx, cancel := context.WithTimeout(parent, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return zero, fmt.Errorf("new request: %w", err)
	}

	return doJSON[T](req)
}

func doJSON[T any](req *http.Request) (T, error) {
	var zero T
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return zero, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		data, _ := io.ReadAll(resp.Body)
		return zero, fmt.Errorf("status %d: %s", resp.StatusCode, string(data))
	}

	var result T
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return zero, fmt.Errorf("decode response: %w", err)
	}

	return result, nil
}

func planStatusURL(baseURL, planID, accountID, projectID string) string {
	values := url.Values{}
	values.Set("account_id", accountID)
	values.Set("project_id", projectID)

	return fmt.Sprintf("%s/v1/agentos/plans/%s/status?%s", baseURL, url.PathEscape(planID), values.Encode())
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
