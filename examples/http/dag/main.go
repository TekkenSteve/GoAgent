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
)

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

	runID := fmt.Sprintf("dag-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	writeStdoutf("=== DAG Orchestration Pattern ===\nRun ID: %s\n\n", runID)

	status, err := c.ExecuteOrchestration(ctx, &client.OrchestrationRequest{
		RunID:    runID,
		TeamSpec: dagTeamSpec(),
	})
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)

	writeStdoutLine("Step order (auto-resolved from DependsOn):")

	writeStdoutLine("  1. research (no deps)")

	writeStdoutLine("  2. calculate + translate (parallel, deps: research)")

	writeStdoutLine("  3. summarize (deps: calculate + translate)")

	writeStdoutLine("  4. validate (deps: summarize)")

	writeStdoutLine()

	writeStdoutLine("Polling for completion...")

	status, err = c.WaitForOrchestrationCompletion(ctx, runID, pollInterval, pollTimeout)
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("\nFinal status: lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)
}

func dagTeamSpec() map[string]any {
	return map[string]any{
		_kID:   "dag-demo",
		_kName: "DAG Pipeline",
		"agents": []map[string]any{
			{
				_kID: "researcher", _kName: "Researcher",
				_kModelRef: _kModelMini, _kSystemPrompt: "You are a research assistant.",
				"tools": []map[string]any{{_kName: "web_search", "required": true}},
			},
			{
				_kID: "translator", _kName: "Translator",
				_kModelRef: _kModelMini, _kSystemPrompt: "You translate text to Japanese.",
			},
			{
				_kID: "summarizer", _kName: "Summarizer",
				_kModelRef: _kModelMini, _kSystemPrompt: "You summarize findings.",
			},
		},
		"steps": []map[string]any{
			{
				_kID: "research", _kType: _kAgent, _kAgentRef: "researcher",
				_kInput: map[string]any{_kMessage: "Research the latest developments in AI"},
			},
			{
				_kID: "calculate", _kType: "tool", "tool": "calculator",
				_kInput:     map[string]any{"expression": "150 * 3 + 42"},
				_kDependsOn: []string{"research"},
			},
			{
				_kID: "translate", _kType: _kAgent, _kAgentRef: "translator",
				_kInput:     map[string]any{_kMessage: "Translate 'Hello world' to Japanese"},
				_kDependsOn: []string{"research"},
			},
			{
				_kID: "summarize", _kType: _kAgent, _kAgentRef: "summarizer",
				_kInput:     map[string]any{_kMessage: "Summarize the research findings"},
				_kDependsOn: []string{"calculate", "translate"},
			},
			{
				_kID: "validate", _kType: "eval",
				_kInput:     map[string]any{"condition": "results_complete"},
				_kDependsOn: []string{"summarize"},
			},
		},
	}
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
