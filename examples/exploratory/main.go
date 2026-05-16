// examples/exploratory/main.go
//
// Exploratory / Self-Modifying Workflow Pattern
//
// After initial exploration, a StepEval evaluates whether enough
// information has been gathered. If not, its OnResult mutation
// injects additional steps into the queue.
//
// Flow:
//
//	plan → Split(automation, jobs, quality) → Join
//	→ evaluate → [inject more steps if needed] → conclude
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with Steps (eval + on_result mutation)
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
	runID := fmt.Sprintf("exploratory-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	fmt.Printf("=== Exploratory Pattern ===\nRun ID: %s\n\n", runID)
	fmt.Println("Key feature: self-modifying step queue")
	fmt.Println("  When 'evaluate' determines more exploration is needed,")
	fmt.Println("  its OnResult injects additional steps after itself.")
	fmt.Println()

	status, err := c.ExecuteOrchestration(ctx, client.OrchestrationRequest{
		RunID: runID,
		Steps: []map[string]any{
			{
				"id": "plan", "type": "agent", "agent_id": "planner",
				"input": map[string]any{"message": "Create an exploration plan for: 'Impact of AI on software development'"},
			},
			{
				"id": "explore", "type": "split",
				"input": map[string]any{
					"children": []map[string]any{
						{
							"id": "explore-automation", "type": "tool", "tool": "web_search",
							"input": map[string]any{"query": "AI code automation tools 2026", "max_results": 3},
						},
						{
							"id": "explore-jobs", "type": "tool", "tool": "web_search",
							"input": map[string]any{"query": "AI impact developer jobs 2026", "max_results": 3},
						},
						{
							"id": "explore-quality", "type": "tool", "tool": "web_search",
							"input": map[string]any{"query": "AI code quality 2026", "max_results": 3},
						},
					},
				},
			},
			{
				"id": "gather", "type": "join",
				"input": map[string]any{"_join_group": "explore"},
			},
			{
				"id": "evaluate", "type": "eval",
				"input": map[string]any{"condition": "enough_information"},
				"on_result": map[string]any{
					"append_after": "evaluate",
					"insert_steps": []map[string]any{
						{
							"id": "explore-future", "type": "tool", "tool": "web_search",
							"input": map[string]any{"query": "AI future predictions software engineering", "max_results": 3},
						},
					},
				},
			},
			{
				"id": "conclude", "type": "agent", "agent_id": "planner",
				"input":      map[string]any{"message": "Summarize all exploration findings"},
				"depends_on": []string{"evaluate"},
			},
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)
	fmt.Println("Polling for completion...")

	status, err = c.WaitForOrchestrationCompletion(ctx, runID, 2*time.Second, 4*time.Minute)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
}
