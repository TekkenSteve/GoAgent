// examples/plan-and-execute/main.go
//
// Plan-and-Execute Pattern
//
// A planning agent first creates a step-by-step plan, then execution
// agents carry out each step in parallel via Split/Join. An eval step
// checks completeness and can inject follow-up tasks.
//
// Flow:
//
//	plan → Split(task-1, task-2, task-3) → Join → evaluate
//	  → [inject more tasks if needed] → report
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with Steps (split/join/eval types + OnResult)
//   - client.WaitForOrchestrationCompletion
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
	runID := fmt.Sprintf("plan-execute-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	fmt.Printf("=== Plan-and-Execute Pattern ===\nRun ID: %s\n\n", runID)
	fmt.Println("Flow: plan → Split(tasks) → Join → evaluate → [inject] → report")
	fmt.Println()

	status, err := c.ExecuteOrchestration(ctx, client.OrchestrationRequest{
		RunID: runID,
		Steps: []map[string]any{
			{
				"id": "plan", "type": "agent", "agent_id": "planner",
				"input": map[string]any{"message": "Create a plan for: 'Set up a CI/CD pipeline with GitHub Actions'. Break it into 3 parallel tasks."},
			},
			{
				"id": "execute", "type": "split",
				"input": map[string]any{
					"children": []map[string]any{
						{
							"id": "task-setup", "type": "agent", "agent_id": "worker",
							"input": map[string]any{"message": "Design the workflow structure: triggers, jobs, and environment configuration"},
						},
						{
							"id": "task-test", "type": "agent", "agent_id": "worker",
							"input": map[string]any{"message": "Design the test automation: unit tests, integration tests, and linting steps"},
						},
						{
							"id": "task-deploy", "type": "agent", "agent_id": "worker",
							"input": map[string]any{"message": "Design the deployment: build, package, and deploy stages with rollback strategy"},
						},
					},
				},
			},
			{
				"id": "gather", "type": "join",
				"input": map[string]any{"_join_group": "execute"},
			},
			{
				"id": "evaluate", "type": "eval",
				"input": map[string]any{"condition": "plan_complete"},
				"on_result": map[string]any{
					"append_after": "evaluate",
					"insert_steps": []map[string]any{
						{
							"id": "task-security", "type": "agent", "agent_id": "worker",
							"input":      map[string]any{"message": "Add security scanning: dependency audit, SAST, and secrets detection"},
							"depends_on": []string{"evaluate"},
						},
					},
				},
			},
			{
				"id": "report", "type": "agent", "agent_id": "planner",
				"input":      map[string]any{"message": "Synthesize all task outputs into a comprehensive CI/CD implementation plan"},
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
