// examples/types/main.go
//
// Type-only usage: import agentos/control and agentos/core for the public
// AgentOS contracts.
//
//	go run examples/types/main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

func main() {
	spec := agentos.RunSpec{
		RunID:        "run-example",
		ThreadID:     "thread-example",
		AccountID:    "acct-example",
		ProjectID:    "project-example",
		AgentID:      "agent-example",
		ModelRef:     "gpt-4.1-mini",
		SystemPrompt: "You are a concise assistant.",
		UserMessage:  "Summarize AgentOS in one sentence.",
		RequestedAt:  time.Now().UTC(),
		Backend:      agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
		Metadata: map[string]string{
			"source": "type-example",
		},
	}

	writeJSON("RunSpec", spec)

	tool := agentoscore.ToolDef{
		Type: "function",
		Function: agentoscore.ToolFuncDef{
			Name:        "lookup_document",
			Description: "Look up a document by id",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`),
		},
	}

	writeJSON("ToolDef", tool)
}

func writeJSON(label string, value any) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		log.Fatalf("marshal %s: %v", label, err)
	}

	if _, err := fmt.Fprintf(os.Stdout, "%s:\n%s\n\n", label, data); err != nil {
		log.Fatalf("write %s: %v", label, err)
	}
}
