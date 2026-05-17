// examples/pipeline/main.go
//
// # Pipeline Pattern
//
// A sequential processing pipeline where each stage transforms data
// and passes it to the next stage via depends_on.
//
// Flow:
//
//	fetch → validate → transform → analyze → report
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with Steps (agent/tool types)
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
	runID := fmt.Sprintf("pipeline-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	fmt.Printf("=== Pipeline Pattern ===\nRun ID: %s\n\n", runID)
	fmt.Println("Flow: fetch → validate → transform → analyze → report")
	fmt.Println()

	status, err := c.ExecuteOrchestration(ctx, client.OrchestrationRequest{
		RunID: runID,
		Steps: pipelineSteps(),
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

func pipelineSteps() []map[string]any {
	return []map[string]any{
		{
			"id": "fetch", "type": "tool", "tool": "web_search",
			"input": map[string]any{"query": "latest AI research papers 2026", "max_results": 5},
		},
		{
			"id": "validate", "type": "agent", "agent_id": "analyst",
			"input":      map[string]any{"message": "Validate the quality and relevance of the fetched data"},
			"depends_on": []string{"fetch"},
		},
		{
			"id": "transform", "type": "agent", "agent_id": "analyst",
			"input":      map[string]any{"message": "Transform the validated data into structured JSON format"},
			"depends_on": []string{"validate"},
		},
		{
			"id": "analyze", "type": "agent", "agent_id": "analyst",
			"input":      map[string]any{"message": "Analyze the structured data and identify key trends"},
			"depends_on": []string{"transform"},
		},
		{
			"id": "report", "type": "agent", "agent_id": "analyst",
			"input":      map[string]any{"message": "Generate a final summary report from the analysis"},
			"depends_on": []string{"analyze"},
		},
	}
}
