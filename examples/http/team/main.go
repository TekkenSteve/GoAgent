// examples/team/main.go
//
// # Multi-Agent Team Pattern
//
// A hierarchical software development team with specialized agents
// organized into sub-teams: Research → Development → QA.
//
// Team hierarchy:
//
//	Software Development Team
//	  ├── Research Team (researcher, analyst)
//	  ├── Development (architect, developer, reviewer)
//	  └── QA Team (tester, docs-writer)
//
// Execution order (after team.Expand()):
//
//	research → analyze → design → implement → dev-review → test → document
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with TeamSpec + SubTeams
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

//nolint:gochecknoglobals // config data — long spec map, not complex logic
var teamTeamSpec = map[string]any{
	"id":   "dev-team",
	"name": "Software Development Team",
	"agents": []map[string]any{
		{
			"id": "architect", "name": "Architect",
			"model_ref": "gpt-4.1-mini", "system_prompt": "You design system architecture.",
		},
		{
			"id": "developer", "name": "Developer",
			"model_ref": "gpt-4.1-mini", "system_prompt": "You write Go code.",
		},
		{
			"id": "reviewer", "name": "Reviewer",
			"model_ref": "gpt-4.1-mini", "system_prompt": "You review code.",
		},
	},
	"sub_teams": []map[string]any{
		{
			"id": "research-team", "name": "Research Team",
			"agents": []map[string]any{
				{
					"id": "researcher", "name": "Researcher",
					"model_ref": "gpt-4.1-mini", "system_prompt": "You research requirements.",
					"tools": []map[string]any{{"name": "web_search", "required": true}},
				},
				{
					"id": "analyst", "name": "Analyst",
					"model_ref": "gpt-4.1-mini", "system_prompt": "You analyze feasibility.",
				},
			},
			"steps": []map[string]any{
				{
					"id": "research", "type": "agent", "agent_ref": "researcher",
					"input": map[string]any{"message": "Research requirements for a task management API"},
				},
				{
					"id": "analyze", "type": "agent", "agent_ref": "analyst",
					"input":      map[string]any{"message": "Analyze the research findings"},
					"depends_on": []string{"research"},
				},
			},
		},
		{
			"id": "qa-team", "name": "QA Team",
			"agents": []map[string]any{
				{
					"id": "tester", "name": "Tester",
					"model_ref": "gpt-4.1-mini", "system_prompt": "You write and run tests.",
				},
				{
					"id": "docs-writer", "name": "Docs Writer",
					"model_ref": "gpt-4.1-mini", "system_prompt": "You write documentation.",
				},
			},
			"steps": []map[string]any{
				{
					"id": "test", "type": "agent", "agent_ref": "tester",
					"input":      map[string]any{"message": "Write tests"},
					"depends_on": []string{"dev-review"},
				},
				{
					"id": "document", "type": "agent", "agent_ref": "docs-writer",
					"input":      map[string]any{"message": "Write API documentation"},
					"depends_on": []string{"test"},
				},
			},
		},
	},
	"steps": []map[string]any{
		{
			"id": "design", "type": "agent", "agent_ref": "architect",
			"input":      map[string]any{"message": "Design architecture"},
			"depends_on": []string{"analyze"},
		},
		{
			"id": "implement", "type": "agent", "agent_ref": "developer",
			"input":      map[string]any{"message": "Implement the API"},
			"depends_on": []string{"design"},
		},
		{
			"id": "dev-review", "type": "agent", "agent_ref": "reviewer",
			"input":      map[string]any{"message": "Review the implementation"},
			"depends_on": []string{"implement"},
		},
	},
}

func main() {
	baseURL := os.Getenv("BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}

	accountID := os.Getenv("ACCOUNT_ID")
	if accountID == "" {
		accountID = "e2e-test-account"
	}

	runID := fmt.Sprintf("team-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	fmt.Fprintf(os.Stdout, "=== Multi-Agent Team Pattern ===\nRun ID: %s\n\n", runID)
	fmt.Fprintln(os.Stdout, "Team hierarchy:")
	fmt.Fprintln(os.Stdout, "  Software Development Team")
	fmt.Fprintln(os.Stdout, "    ├── Research Team (researcher, analyst)")
	fmt.Fprintln(os.Stdout, "    ├── Development (architect, developer, reviewer)")
	fmt.Fprintln(os.Stdout, "    └── QA Team (tester, docs-writer)")
	fmt.Fprintln(os.Stdout)

	status, err := c.ExecuteOrchestration(ctx, client.OrchestrationRequest{
		RunID:    runID,
		TeamSpec: teamTeamSpec,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)
	fmt.Fprintln(os.Stdout, "Execution order (after team.Expand()):")
	fmt.Fprintln(os.Stdout, "  research → analyze → design → implement → dev-review → test → document")
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Polling for completion...")

	status, err = c.WaitForOrchestrationCompletion(ctx, runID, pollInterval, pollTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
}
