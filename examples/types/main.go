// examples/types/main.go
//
// Mode 3 — Type-Only: Import entity/ for shared domain types
//
// Demonstrates importing only the entity package to share domain type
// definitions across microservices without pulling in any infrastructure.
//
//	go run examples/types/main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/TekkenSteve/GoAgent/entity"
)

const (
	typeModel       = "gpt-4o"
	typeTemperature = 0.3
)

func main() {
	msg := entity.Message{
		Role:    entity.RoleUser,
		Content: "Hello from a microservice that only depends on entity/",
	}

	writef(os.Stdout, "=== Type-Only Usage ===\n\n")

	b, err := json.Marshal(msg)
	if err != nil {
		log.Fatalf("json.Marshal: %v", err)
	}

	writef(os.Stdout, "Serialized message:\n  %s\n\n", string(b))

	tool := entity.ToolDef{
		Type: "function",
		Function: entity.ToolFuncDef{
			Name:        "my_tool",
			Description: "A tool defined using GoAgent entity types",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"input":{"type":"string"}}}`),
		},
	}

	b, err = json.MarshalIndent(tool, "", "  ")
	if err != nil {
		log.Fatalf("json.MarshalIndent: %v", err)
	}

	writef(os.Stdout, "Tool definition:\n%s\n\n", string(b))

	req := entity.LLMRequest{
		Messages: []entity.Message{msg},
		Tools:    []entity.ToolDef{tool},
		Config:   entity.LLMConfig{Model: typeModel, Temperature: typeTemperature},
	}

	b, err = json.MarshalIndent(req, "", "  ")
	if err != nil {
		log.Fatalf("json.MarshalIndent: %v", err)
	}

	writef(os.Stdout, "LLM request (ready for your own LLM adapter):\n%s\n", string(b))
}

// writef is a thin wrapper around fmt.Fprintf for stdout writes.
func writef(f *os.File, format string, args ...any) {
	if _, err := fmt.Fprintf(f, format, args...); err != nil {
		log.Fatalf("writef: %v", err)
	}
}
