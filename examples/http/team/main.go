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

func teamTeamSpec() map[string]any {
	return map[string]any{
		_kID: "dev-team", _kName: "Software Development Team",
		_kAgents: []map[string]any{
			{_kID: "architect", _kName: "Architect", _kModelRef: _kModelMini, _kSystemPrompt: "You design system architecture."},
			{_kID: "developer", _kName: "Developer", _kModelRef: _kModelMini, _kSystemPrompt: "You write Go code."},
			{_kID: "reviewer", _kName: "Reviewer", _kModelRef: _kModelMini, _kSystemPrompt: "You review code."},
		},
		"sub_teams": []map[string]any{
			{
				_kID: "research-team", _kName: "Research Team",
				_kAgents: []map[string]any{
					{_kID: "researcher", _kName: "Researcher", _kModelRef: _kModelMini, _kSystemPrompt: "You research requirements.", "tools": []map[string]any{{_kName: "web_search", "required": true}}},
					{_kID: "analyst", _kName: "Analyst", _kModelRef: _kModelMini, _kSystemPrompt: "You analyze feasibility."},
				},
				_kSteps: []map[string]any{
					{_kID: "research", _kType: _kAgent, _kAgentRef: "researcher", _kInput: map[string]any{_kMessage: "Research requirements for a task management API"}},
					{_kID: "analyze", _kType: _kAgent, _kAgentRef: "analyst", _kInput: map[string]any{_kMessage: "Analyze the research findings"}, _kDependsOn: []string{"research"}},
				},
			},
			{
				_kID: "qa-team", _kName: "QA Team",
				_kAgents: []map[string]any{
					{_kID: "tester", _kName: "Tester", _kModelRef: _kModelMini, _kSystemPrompt: "You write and run tests."},
					{_kID: "docs-writer", _kName: "Docs Writer", _kModelRef: _kModelMini, _kSystemPrompt: "You write documentation."},
				},
				_kSteps: []map[string]any{
					{_kID: "test", _kType: _kAgent, _kAgentRef: "tester", _kInput: map[string]any{_kMessage: "Write tests"}, _kDependsOn: []string{"dev-review"}},
					{_kID: "document", _kType: _kAgent, _kAgentRef: "docs-writer", _kInput: map[string]any{_kMessage: "Write API documentation"}, _kDependsOn: []string{"test"}},
				},
			},
		},
		_kSteps: []map[string]any{
			{_kID: "design", _kType: _kAgent, _kAgentRef: "architect", _kInput: map[string]any{_kMessage: "Design architecture"}, _kDependsOn: []string{"analyze"}},
			{_kID: "implement", _kType: _kAgent, _kAgentRef: "developer", _kInput: map[string]any{_kMessage: "Implement the API"}, _kDependsOn: []string{"design"}},
			{_kID: "dev-review", _kType: _kAgent, _kAgentRef: "reviewer", _kInput: map[string]any{_kMessage: "Review the implementation"}, _kDependsOn: []string{"implement"}},
		},
	}
}

const (
	_kID        = "id"
	_kName      = "name"
	_kType      = "type"
	_kInput     = "input"
	_kDependsOn = "depends_on"
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

	runID := fmt.Sprintf("team-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	writeStdoutf("=== Multi-Agent Team Pattern ===\nRun ID: %s\n\n", runID)

	writeStdoutLine("Team hierarchy:")

	writeStdoutLine("  Software Development Team")

	writeStdoutLine("    ├── Research Team (researcher, analyst)")

	writeStdoutLine("    ├── Development (architect, developer, reviewer)")

	writeStdoutLine("    └── QA Team (tester, docs-writer)")

	writeStdoutLine()

	status, err := c.ExecuteOrchestration(ctx, &client.OrchestrationRequest{
		RunID:    runID,
		TeamSpec: teamTeamSpec(),
	})
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)

	writeStdoutLine("Execution order (after team.Expand()):")

	writeStdoutLine("  research → analyze → design → implement → dev-review → test → document")

	writeStdoutLine()

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
