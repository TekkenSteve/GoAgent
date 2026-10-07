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
	"log"
	"os"
	"time"

	"github.com/TekkenSteve/GoAgent/examples/client"
)

const (
	pollInterval   = 2 * time.Second
	pollTimeout    = 4 * time.Minute
	_kModelRef     = "model_ref"
	_kAgentRef     = "agent_ref"
	_kMessage      = "message"
	_kSystemPrompt = "system_prompt"
	_kSteps        = "steps"
	_kAgents       = "agents"
)

func hierarchicalTeamSpec() map[string]any {
	return map[string]any{
		_kID: "exec-team", _kName: "Executive Team",
		_kAgents: []map[string]any{
			{_kID: "executive", _kName: "Executive", _kModelRef: _kModelMini, _kSystemPrompt: "You set strategic direction and consolidate plans."},
		},
		"sub_teams": []map[string]any{
			{
				_kID: "product-team", _kName: "Product Management",
				_kAgents: []map[string]any{
					{_kID: "product-manager", _kName: "Product Manager", _kModelRef: _kModelMini, _kSystemPrompt: "You define product requirements and priorities."},
				},
				_kSteps: []map[string]any{
					{_kID: "product-plan", _kType: _kAgent, _kAgentRef: "product-manager", _kInput: map[string]any{_kMessage: "Create a product roadmap for a new SaaS platform"}},
				},
			},
			{
				_kID: "eng-team", _kName: "Engineering",
				_kAgents: []map[string]any{
					{_kID: "architect", _kName: "Architect", _kModelRef: _kModelMini, _kSystemPrompt: "You design system architecture."},
					{_kID: "developer", _kName: "Developer", _kModelRef: _kModelMini, _kSystemPrompt: "You write technical specifications."},
				},
				_kSteps: []map[string]any{
					{_kID: "eng-plan", _kType: _kAgent, _kAgentRef: "architect", _kInput: map[string]any{_kMessage: "Design the high-level system architecture"}},
					{_kID: "eng-spec", _kType: _kAgent, _kAgentRef: "developer", _kInput: map[string]any{_kMessage: "Write detailed technical specs for each component"}, "depends_on": []string{"eng-plan"}},
				},
			},
			{
				_kID: "marketing-team", _kName: "Marketing",
				_kAgents: []map[string]any{
					{_kID: "marketing-lead", _kName: "Marketing Lead", _kModelRef: _kModelMini, _kSystemPrompt: "You create go-to-market strategies."},
				},
				_kSteps: []map[string]any{
					{_kID: "marketing-plan", _kType: _kAgent, _kAgentRef: "marketing-lead", _kInput: map[string]any{_kMessage: "Create a go-to-market strategy for the new platform"}},
				},
			},
		},
		_kSteps: []map[string]any{
			{_kID: "strategy", _kType: _kAgent, _kAgentRef: "executive", _kInput: map[string]any{_kMessage: "Define the strategic vision and goals for the new SaaS platform"}},
			{_kID: "consolidate", _kType: _kAgent, _kAgentRef: "executive", _kInput: map[string]any{_kMessage: "Consolidate all department plans into a unified execution roadmap"}, "depends_on": []string{"product-plan", "eng-spec", "marketing-plan"}},
		},
	}
}

const (
	_kID        = "id"
	_kName      = "name"
	_kType      = "type"
	_kInput     = "input"
	_kAgent     = "agent"
	_kModelMini = "gpt-4.1-mini"
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

	writeStdoutf("=== Hierarchical Team Pattern ===\nRun ID: %s\n\n", runID)

	writeStdoutLine("Team hierarchy:")

	writeStdoutLine("  Executive Team")

	writeStdoutLine("    ├── Product Management (product-manager)")

	writeStdoutLine("    ├── Engineering (architect, developer, reviewer)")

	writeStdoutLine("    └── Marketing (marketing-lead)")

	writeStdoutLine()

	status, err := c.ExecuteOrchestration(ctx, &client.OrchestrationRequest{
		RunID:    runID,
		TeamSpec: hierarchicalTeamSpec(),
	})
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)

	writeStdoutLine("Polling for completion...")

	status, err = c.WaitForOrchestrationCompletion(ctx, runID, pollInterval, pollTimeout)
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
}

func writeStdoutf(format string, args ...any) {
	if _, werr := fmt.Fprintf(os.Stdout, format, args...); werr != nil {
		log.Fatalf("write stdout: %v", werr)
	}
}

func writeStderrf(format string, args ...any) {
	if _, werr := fmt.Fprintf(os.Stderr, format, args...); werr != nil {
		log.Fatalf("write stderr: %v", werr)
	}
}

func writeStdoutLine(args ...any) {
	if _, werr := fmt.Fprintln(os.Stdout, args...); werr != nil {
		log.Fatalf("write stdout: %v", werr)
	}
}
