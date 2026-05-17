// examples/supervisor-worker/main.go
//
// # Supervisor-Worker Pattern
//
// A supervisor agent decomposes a task and delegates sub-tasks to
// worker agents running in parallel via Split/Join.
//
// Flow:
//
//	decompose (supervisor) → Split(worker-1, worker-2, worker-3)
//	  → Join → synthesize (supervisor)
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with Steps (split/join types)
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
	runID := fmt.Sprintf("supervisor-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	fmt.Printf("=== Supervisor-Worker Pattern ===\nRun ID: %s\n\n", runID)
	fmt.Println("Flow: decompose → Split(worker tasks) → Join → synthesize")
	fmt.Println()

	status, err := c.ExecuteOrchestration(ctx, client.OrchestrationRequest{
		RunID: runID,
		Steps: supervisorWorkerSteps(),
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

func supervisorWorkerSteps() []map[string]any {
	return []map[string]any{
		{
			"id": "decompose", "type": "agent", "agent_id": "supervisor",
			"input": map[string]any{"message": "Decompose the task 'Build a web application' into 3 parallel work items: frontend, backend, and database design"},
		},
		{
			"id": "execute", "type": "split",
			"input": map[string]any{
				"children": []map[string]any{
					{
						"id": "worker-frontend", "type": "agent", "agent_id": "worker",
						"input": map[string]any{"message": "Design the frontend architecture: React components, state management, and routing"},
					},
					{
						"id": "worker-backend", "type": "agent", "agent_id": "worker",
						"input": map[string]any{"message": "Design the backend architecture: API endpoints, database models, and authentication"},
					},
					{
						"id": "worker-database", "type": "agent", "agent_id": "worker",
						"input": map[string]any{"message": "Design the database schema: tables, indexes, and migration strategy"},
					},
				},
			},
		},
		{
			"id": "gather", "type": "join",
			"input": map[string]any{"_join_group": "execute"},
		},
		{
			"id": "synthesize", "type": "agent", "agent_id": "supervisor",
			"input":      map[string]any{"message": "Synthesize the worker outputs into a cohesive architecture plan"},
			"depends_on": []string{"gather"},
		},
	}
}
