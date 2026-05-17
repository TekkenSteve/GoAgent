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

	fmt.Printf("=== Research Pattern ===\nRun ID: %s\n\n", runID)
	fmt.Println("Flow: discover → Split(impacts, solutions, policy) → Join → synthesize → review (HITL)")
	fmt.Println()

	status, err := c.ExecuteOrchestration(ctx, client.OrchestrationRequest{
		RunID: runID,
		Steps: researchSteps(),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)
	fmt.Println("Polling for completion...")
	fmt.Println("(The 'review' step blocks until 'review-approved' signal or 10s timeout)")

	status, err = c.WaitForOrchestrationCompletion(ctx, runID, 2*time.Second, 4*time.Minute)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
}

func researchSteps() []map[string]any {
	return []map[string]any{
		{
			"id": "discover", "type": "tool", "tool": "web_search",
			"input": map[string]any{"query": "climate change latest research 2026", "max_results": 5},
		},
		{
			"id": "explore", "type": "split",
			"input": map[string]any{
				"children": []map[string]any{
					{
						"id": "explore-impacts", "type": "tool", "tool": "web_search",
						"input": map[string]any{"query": "climate change impacts 2026", "max_results": 3},
					},
					{
						"id": "explore-solutions", "type": "tool", "tool": "web_search",
						"input": map[string]any{"query": "climate change solutions 2026", "max_results": 3},
					},
					{
						"id": "explore-policy", "type": "tool", "tool": "web_search",
						"input": map[string]any{"query": "climate policy 2026", "max_results": 3},
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
				"timeout":     10000000000, // 10 seconds in nanoseconds
				"on_timeout":  "skip",
			},
		},
	}
}
