// examples/hierarchical/main.go
//
// # Hierarchical Team Pattern
//
// A management hierarchy with an executive setting strategy, middle
// managers decomposing into department plans, and teams executing
// in parallel.
//
// Team hierarchy:
//
//	Executive Team
//	  ├── Product Management (product-manager)
//	  ├── Engineering (architect, developer, reviewer)
//	  └── Marketing (marketing-lead)
//
// Execution order:
//
//	strategy → Split(product-plan, eng-plan, marketing-plan) → Join
//	  → consolidate
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with TeamSpec + SubTeams
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
	runID := fmt.Sprintf("hierarchical-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	fmt.Printf("=== Hierarchical Team Pattern ===\nRun ID: %s\n\n", runID)
	fmt.Println("Team hierarchy:")
	fmt.Println("  Executive Team")
	fmt.Println("    ├── Product Management (product-manager)")
	fmt.Println("    ├── Engineering (architect, developer, reviewer)")
	fmt.Println("    └── Marketing (marketing-lead)")
	fmt.Println()

	status, err := c.ExecuteOrchestration(ctx, client.OrchestrationRequest{
		RunID:    runID,
		TeamSpec: hierarchicalTeamSpec,
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

var hierarchicalTeamSpec = map[string]any{
	"id":   "exec-team",
	"name": "Executive Team",
	"agents": []map[string]any{
		{
			"id": "executive", "name": "Executive",
			"model_ref": "gpt-4.1-mini", "system_prompt": "You set strategic direction and consolidate plans.",
		},
	},
	"sub_teams": []map[string]any{
		{
			"id": "product-team", "name": "Product Management",
			"agents": []map[string]any{
				{
					"id": "product-manager", "name": "Product Manager",
					"model_ref": "gpt-4.1-mini", "system_prompt": "You define product requirements and priorities.",
				},
			},
			"steps": []map[string]any{
				{
					"id": "product-plan", "type": "agent", "agent_ref": "product-manager",
					"input": map[string]any{"message": "Create a product roadmap for a new SaaS platform"},
				},
			},
		},
		{
			"id": "eng-team", "name": "Engineering",
			"agents": []map[string]any{
				{
					"id": "architect", "name": "Architect",
					"model_ref": "gpt-4.1-mini", "system_prompt": "You design system architecture.",
				},
				{
					"id": "developer", "name": "Developer",
					"model_ref": "gpt-4.1-mini", "system_prompt": "You write technical specifications.",
				},
			},
			"steps": []map[string]any{
				{
					"id": "eng-plan", "type": "agent", "agent_ref": "architect",
					"input": map[string]any{"message": "Design the high-level system architecture"},
				},
				{
					"id": "eng-spec", "type": "agent", "agent_ref": "developer",
					"input":      map[string]any{"message": "Write detailed technical specs for each component"},
					"depends_on": []string{"eng-plan"},
				},
			},
		},
		{
			"id": "marketing-team", "name": "Marketing",
			"agents": []map[string]any{
				{
					"id": "marketing-lead", "name": "Marketing Lead",
					"model_ref": "gpt-4.1-mini", "system_prompt": "You create go-to-market strategies.",
				},
			},
			"steps": []map[string]any{
				{
					"id": "marketing-plan", "type": "agent", "agent_ref": "marketing-lead",
					"input": map[string]any{"message": "Create a go-to-market strategy for the new platform"},
				},
			},
		},
	},
	"steps": []map[string]any{
		{
			"id": "strategy", "type": "agent", "agent_ref": "executive",
			"input": map[string]any{"message": "Define the strategic vision and goals for the new SaaS platform"},
		},
		{
			"id": "consolidate", "type": "agent", "agent_ref": "executive",
			"input":      map[string]any{"message": "Consolidate all department plans into a unified execution roadmap"},
			"depends_on": []string{"product-plan", "eng-spec", "marketing-plan"},
		},
	},
}
