// examples/delegate/main.go
//
// # Delegation Pattern (delegate_to_agent)
//
// A coordinator agent decomposes a complex task and delegates each sub-task
// to ephemeral sub-agents via the delegate_to_agent tool. Each sub-agent
// runs in an isolated child workflow with its own context, preventing
// context pollution in the coordinator's conversation.
//
// Flow:
//
//	coordinator receives task
//	  → delegate_to_agent(sub-agent-1, task-part-1) → get result
//	  → delegate_to_agent(sub-agent-2, task-part-2) → get result
//	  → synthesize results into a final answer
//
// Key difference from Split/Join orchestration:
//   - Split/Join is pre-planned — the orchestrator fans out work to
//     pre-defined agents at the workflow level.
//   - delegate_to_agent is agent-driven — the agent itself decides during
//     its ReAct loop when and how to create sub-agents, enabling runtime
//     decomposition of tasks that couldn't be anticipated at design time.
//
// Framework APIs demonstrated:
//   - client.OrchestrationRequest with TeamSpec (single coordinator agent
//     instructed to use delegate_to_agent)
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
	pollInterval = 2 * time.Second
	pollTimeout  = 4 * time.Minute
)

func delegateTeamSpec() map[string]any {
	return map[string]any{
		"id":   "research-coordinator-team",
		"name": "Research Coordinator Team",
		"agents": []map[string]any{
			{
				"id":        "coordinator",
				"name":      "Research Coordinator",
				"model_ref": "gpt-4.1-mini",
				"system_prompt": `You are a research coordinator. You have access to the delegate_to_agent tool.

When you receive a complex multi-topic research request, do NOT try to answer everything yourself. Instead:

1. Break the request into focused sub-topics
2. For each sub-topic, call delegate_to_agent with:
   - A system_prompt that defines a specialist role (e.g., "You are an expert in X")
   - A task that contains the specific question to research
   - Optionally a model (use a cheaper model like gpt-4o-mini for simpler sub-tasks)
3. After collecting all sub-agent responses, synthesize them into a comprehensive final answer

Benefits of delegation:
- Each sub-agent works in an isolated context — no context pollution
- Sub-agents can have specialized expertise via their system prompt
- You can use cheaper/faster models for simpler sub-tasks

Always delegate multi-topic research. Do not attempt to cover multiple topics yourself.`,
			},
		},
		"steps": []map[string]any{
			{
				"id":        "coordinate",
				"type":      "agent",
				"agent_ref": "coordinator",
				"input": map[string]any{
					"message": "I need a comparison of Go vs Rust for building web APIs. Cover: performance, ecosystem, learning curve, deployment, and concurrency models.",
				},
			},
		},
	}
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

	runID := fmt.Sprintf("delegate-demo-%d", time.Now().UnixMilli())

	c := client.New(baseURL, accountID)
	ctx := context.Background()

	writeStdoutf("=== Delegation Pattern (delegate_to_agent) ===\nRun ID: %s\n\n", runID)

	writeStdoutLine("How it works:")

	writeStdoutLine("  1. Coordinator receives a complex multi-topic research task")

	writeStdoutLine("  2. For each topic, coordinator calls delegate_to_agent to spawn")

	writeStdoutLine("     an ephemeral sub-agent with specialized instructions")

	writeStdoutLine("  3. Each sub-agent runs in an isolated child workflow")

	writeStdoutLine("  4. Coordinator synthesizes all results into a final answer")

	writeStdoutLine()

	writeStdoutLine("Task: Compare Go vs Rust for building web APIs")

	writeStdoutLine()

	status, err := c.ExecuteOrchestration(ctx, &client.OrchestrationRequest{
		RunID:    runID,
		TeamSpec: delegateTeamSpec(),
	})
	if err != nil {
		writeStderrf("Error: %v\n", err)
		os.Exit(1)
	}

	writeStdoutf("Initial status: lifecycle_state=%s step=%d\n\n", status.LifecycleState, status.Step)

	writeStdoutLine("The coordinator agent is now running. It should:")

	writeStdoutLine("  1. Delegate Go research to a sub-agent")

	writeStdoutLine("  2. Delegate Rust research to a sub-agent")

	writeStdoutLine("  3. Synthesize the comparison")

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
