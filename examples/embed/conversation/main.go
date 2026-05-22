// examples/embed/conversation/main.go
//
// Mode 2 — Library Embedding: Multi-Turn Conversation
//
// Demonstrates maintaining conversation history across multiple agent steps.
// Each call appends the result to the history for the next call.
//
//	go run examples/embed/conversation/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/usecase/agent"
)

const (
	convTemperature = 0.5
	convPromptToks  = 50
	convCompToks    = 30
	convTotalToks   = 80
)

func main() {
	llm := &conversationLLM{turn: 0}
	agentUC := agent.New(llm, nil, nil, nil, nil, nil)

	runID := fmt.Sprintf("embed-conv-%d", time.Now().UnixMilli())
	ctx := context.Background()

	fmt.Fprintf(os.Stdout, "=== Multi-Turn Conversation (Embedded) ===\nRun ID: %s\n\n", runID)

	// Turn 1: initial question
	fmt.Fprintln(os.Stdout, "--- Turn 1 ---")

	result, err := agentUC.ExecuteStep(ctx, &agent.StepRequest{
		RunID:   runID,
		Message: "What is clean architecture?",
		History: nil,
		Config:  entity.LLMConfig{Model: "demo-model", Temperature: convTemperature},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	lastMsg := result.CompleteState[len(result.CompleteState)-1]
	fmt.Fprintf(os.Stdout, "Assistant: %s\n\n", lastMsg.Content)

	// Turn 2: follow-up with accumulated history
	fmt.Fprintln(os.Stdout, "--- Turn 2 (with history) ---")

	result, err = agentUC.ExecuteStep(ctx, &agent.StepRequest{
		RunID:   runID,
		Message: "How does GoAgent implement it?",
		History: result.CompleteState, // reuse full history from previous turn
		Config:  entity.LLMConfig{Model: "demo-model", Temperature: convTemperature},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	lastMsg = result.CompleteState[len(result.CompleteState)-1]
	fmt.Fprintf(os.Stdout, "Assistant: %s\n\n", lastMsg.Content)

	fmt.Fprintf(os.Stdout, "Total messages in history: %d\n", len(result.CompleteState))
}

// conversationLLM simulates context-aware responses for demonstration.
type conversationLLM struct{ turn int }

func (m *conversationLLM) Chat(_ context.Context, req *entity.LLMRequest) (entity.LLMResponse, error) {
	m.turn++

	var content string
	if m.turn == 1 {
		content = "Clean Architecture is a software design philosophy proposed by Robert C. Martin. It separates software into layers with strict dependency rules — outer layers depend on inner layers, never the reverse."
	} else {
		numHistory := len(req.Messages)
		content = fmt.Sprintf("GoAgent implements Clean Architecture using the go-clean-template pattern. It separates input ports (usecase/contracts.go), output ports (repo/contracts.go), and infrastructure adapters. (I processed %d previous messages in the history.)", numHistory)
	}

	return entity.LLMResponse{
		Content:      content,
		FinishReason: "stop",
		Usage:        entity.Usage{PromptTokens: convPromptToks, CompletionTokens: convCompToks, TotalTokens: convTotalToks},
	}, nil
}
