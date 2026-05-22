// examples/embed/react/main.go
//
// Mode 2 — Library Embedding: ReAct (Reasoning + Acting) Pattern
//
// Demonstrates importing GoAgent packages directly to run an agent
// without a running server. No Docker, no HTTP — just Go.
//
//	go run examples/embed/react/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/repo"
	"github.com/TekkenSteve/GoAgent/usecase/agent"
)

const (
	charsPerTokenEstimate = 3
	temperature           = 0.7
	completionTokens      = 20
)

func main() {
	llm := &echoLLM{}

	agentUC := agent.New(llm, nil, nil, nil, nil, nil)

	runID := fmt.Sprintf("embed-react-%d", time.Now().UnixMilli())
	ctx := context.Background()

	fmt.Fprintf(os.Stdout, "=== ReAct (Embedded) ===\nRun ID: %s\n\n", runID)

	result, err := agentUC.ExecuteStep(ctx, &agent.StepRequest{
		RunID:   runID,
		Message: "Calculate 25 * 4 + 10 and explain your reasoning step by step.",
		Config: entity.LLMConfig{
			Model:       "demo-model",
			Temperature: temperature,
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Messages generated: %d\n", len(result.Messages))
	fmt.Fprintf(os.Stdout, "Final message:\n  %s\n", result.Messages[len(result.Messages)-1].Content)

	if result.Usage.TotalTokens > 0 {
		fmt.Fprintf(os.Stdout, "Usage: prompt=%d completion=%d total=%d\n",
			result.Usage.PromptTokens, result.Usage.CompletionTokens, result.Usage.TotalTokens)
	}
}

type echoLLM struct{}

func (m *echoLLM) Chat(_ context.Context, req *entity.LLMRequest) (entity.LLMResponse, error) {
	lastMsg := req.Messages[len(req.Messages)-1].Content

	return entity.LLMResponse{
		Content:      fmt.Sprintf("Echo: %s\n\n(Tokens: prompt=%d)", lastMsg, len(lastMsg)/charsPerTokenEstimate),
		FinishReason: "stop",
		Usage: entity.Usage{
			PromptTokens:     len(lastMsg) / charsPerTokenEstimate,
			CompletionTokens: completionTokens,
			TotalTokens:      len(lastMsg)/charsPerTokenEstimate + completionTokens,
		},
	}, nil
}

func (m *echoLLM) ChatStream(_ context.Context, req *entity.LLMRequest) (<-chan entity.LLMStreamChunk, error) {
	ch := make(chan entity.LLMStreamChunk)

	go func() {
		defer close(ch)

		for word := range strings.FieldsSeq(req.Messages[len(req.Messages)-1].Content) {
			ch <- entity.LLMStreamChunk{Content: word + " "}
		}
	}()

	return ch, nil
}

var (
	_ repo.LLMProvider       = (*echoLLM)(nil)
	_ repo.LLMStreamProvider = (*echoLLM)(nil)
)
