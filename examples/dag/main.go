// examples/dag/main.go
//
// # DAG (Directed Acyclic Graph) Orchestration Pattern
//
// Steps declare explicit dependencies via depends_on. The
// OrchestrationWorkflow resolves execution order automatically.
//
// Step DAG:
//
//	research (agent, no deps)
//	  ├──> calculate (tool, depends: research)
//	  └──> translate (agent, depends: research)
//	        └──> summarize (agent, depends: calculate + translate)
//	              └──> validate (eval, depends: summarize)
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with TeamSpec
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
	pollInterval = 2 * time.Second
	pollTimeout  = 4 * time.Minute
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

	runID := fmt.Sprintf("dag-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	fmt.Fprintf(os.Stdout, "=== DAG Orchestration Pattern ===\nRun ID: %s\n\n", runID)

	status, err := c.ExecuteOrchestration(ctx, client.OrchestrationRequest{
		RunID:    runID,
		TeamSpec: dagTeamSpec(),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)
	fmt.Fprintln(os.Stdout, "Step order (auto-resolved from DependsOn):")
	fmt.Fprintln(os.Stdout, "  1. research (no deps)")
	fmt.Fprintln(os.Stdout, "  2. calculate + translate (parallel, deps: research)")
	fmt.Fprintln(os.Stdout, "  3. summarize (deps: calculate + translate)")
	fmt.Fprintln(os.Stdout, "  4. validate (deps: summarize)")
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Polling for completion...")

	status, err = c.WaitForOrchestrationCompletion(ctx, runID, pollInterval, pollTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
}

func dagTeamSpec() map[string]any {
	return map[string]any{
		"id":   "dag-demo",
		"name": "DAG Pipeline",
		"agents": []map[string]any{
			{
				"id": "researcher", "name": "Researcher",
				"model_ref": "gpt-4.1-mini", "system_prompt": "You are a research assistant.",
				"tools": []map[string]any{{"name": "web_search", "required": true}},
			},
			{
				"id": "translator", "name": "Translator",
				"model_ref": "gpt-4.1-mini", "system_prompt": "You translate text to Japanese.",
			},
			{
				"id": "summarizer", "name": "Summarizer",
				"model_ref": "gpt-4.1-mini", "system_prompt": "You summarize findings.",
			},
		},
		"steps": []map[string]any{
			{
				"id": "research", "type": "agent", "agent_ref": "researcher",
				"input": map[string]any{"message": "Research the latest developments in AI"},
			},
			{
				"id": "calculate", "type": "tool", "tool": "calculator",
				"input":      map[string]any{"expression": "150 * 3 + 42"},
				"depends_on": []string{"research"},
			},
			{
				"id": "translate", "type": "agent", "agent_ref": "translator",
				"input":      map[string]any{"message": "Translate 'Hello world' to Japanese"},
				"depends_on": []string{"research"},
			},
			{
				"id": "summarize", "type": "agent", "agent_ref": "summarizer",
				"input":      map[string]any{"message": "Summarize the research findings"},
				"depends_on": []string{"calculate", "translate"},
			},
			{
				"id": "validate", "type": "eval",
				"input":      map[string]any{"condition": "results_complete"},
				"depends_on": []string{"summarize"},
			},
		},
	}
}
