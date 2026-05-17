// examples/research/main.go
//
// # Research Pattern
//
// A research workflow that fans out into parallel exploration
// subtasks and then synthesizes the results.
//
// Flow:
//
//	discover → Split(impacts, solutions, policy) → Join
//	→ synthesize → review (HITL with 10s timeout)
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with Steps (split/join/wait types)
//   - client.WaitForCompletion
//
// Prerequisites: running GoAgent instance, LLM configured.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/TekkenSteve/GoAgent/examples/client"
)

const (
	pollInterval       = 2 * time.Second
	pollTimeout        = 4 * time.Minute
	maxSearchResult    = 5
	reviewTimeoutNanos = 10000000000
	childSearchResult  = 3
)

func main() {
	baseURL := os.Getenv("BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}

	accountID := os.Getenv("ACCOUNT_ID")
	if accountID == "" {
		accountID = "e2e-test-account"
	}

	runID := fmt.Sprintf("research-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	fmt.Fprintf(os.Stdout, "=== Research Pattern ===\nRun ID: %s\n\n", runID)
	fmt.Fprintln(os.Stdout, "Flow: discover → Split(impacts, solutions, policy) → Join → synthesize → review (HITL)")
	fmt.Fprintln(os.Stdout)

	status, err := c.ExecuteOrchestration(ctx, client.OrchestrationRequest{
		RunID: runID,
		Steps: researchSteps(),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)
	fmt.Fprintln(os.Stdout, "Polling for completion...")
	fmt.Fprintln(os.Stdout, "(The 'review' step blocks until 'review-approved' signal or 10s timeout)")

	status, err = c.WaitForOrchestrationCompletion(ctx, runID, pollInterval, pollTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
}

func researchSteps() []map[string]any {
	return []map[string]any{
		{
			"id": "discover", "type": "tool", "tool": "web_search",
			"input": map[string]any{"query": "climate change latest research 2026", "max_results": maxSearchResult},
		},
		{
			"id": "explore", "type": "split",
			"input": map[string]any{
				"children": []map[string]any{
					{
						"id": "explore-impacts", "type": "tool", "tool": "web_search",
						"input": map[string]any{"query": "climate change impacts 2026", "max_results": childSearchResult},
					},
					{
						"id": "explore-solutions", "type": "tool", "tool": "web_search",
						"input": map[string]any{"query": "climate change solutions 2026", "max_results": childSearchResult},
					},
					{
						"id": "explore-policy", "type": "tool", "tool": "web_search",
						"input": map[string]any{"query": "climate policy 2026", "max_results": childSearchResult},
					},
				},
			},
		},
		{
			"id": "gather", "type": "join",
			"input": map[string]any{"_join_group": "explore"},
		},
		{
			"id": "synthesize", "type": "agent", "agent_id": "analyst",
			"input": map[string]any{"message": "Synthesize all research findings into a summary report"},
		},
		{
			"id": "review", "type": "wait",
			"wait_for": map[string]any{
				"signal_name": "review-approved",
				"timeout":     reviewTimeoutNanos, // 10 seconds in nanoseconds
				"on_timeout":  "skip",
			},
		},
	}
}
