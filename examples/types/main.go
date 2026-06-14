// examples/types/main.go
//
// Type-only usage: import agentos for the public AgentOS contract.
//
//	go run examples/types/main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
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
		Metadata: map[string]string{
			"source": "type-example",
		},
	}

	writeJSON("RunSpec", spec)

	tool := agentos.ToolDef{
		Type: "function",
		Function: agentos.ToolFuncDef{
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
