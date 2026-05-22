// examples/embed/tools/main.go
//
// Mode 2 — Library Embedding: Agent with Tool Calling
//
// Demonstrates an agent that calls tools through the ReAct loop.
// The LLM returns a tool_call, the executor runs it, and the agent
// continues with the tool result — all in-process.
//
//	go run examples/embed/tools/main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/repo"
	"github.com/TekkenSteve/GoAgent/usecase/agent"
)

const (
	toolTemperature = 0.3
	toolPromptToks  = 30
	toolCompToks    = 15
	toolTotalToks   = 45
	finalPromptToks = 50
	finalCompToks   = 25
	finalTotalToks  = 75
)

func main() {
	llm := &weatherLLM{}
	tools := &weatherTool{}

	agentUC := agent.New(llm, tools, nil, nil, nil, nil)

	runID := fmt.Sprintf("embed-tools-%d", time.Now().UnixMilli())
	ctx := context.Background()

	fmt.Fprintf(os.Stdout, "=== Agent with Tools (Embedded) ===\nRun ID: %s\n\n", runID)

	result, err := agentUC.ExecuteStep(ctx, &agent.StepRequest{
		RunID:   runID,
		Message: "What is the weather in London today?",
		Tools:   weatherToolDefs(),
		Config: entity.LLMConfig{
			Model:       "demo-model",
			Temperature: toolTemperature,
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Messages: %d | Tool results: %d\n",
		len(result.Messages), len(result.ToolResults))

	for i, m := range result.Messages {
		prefix := fmt.Sprintf("  [%d] role=%s", i, m.Role)
		if len(m.ToolCalls) > 0 {
			fmt.Fprintf(os.Stdout, "%s\n", prefix)

			for _, tc := range m.ToolCalls {
				fmt.Fprintf(os.Stdout, "       -> tool_call: %s(%s)\n", tc.Function.Name, tc.Function.Arguments)
			}
		} else {
			fmt.Fprintf(os.Stdout, "%s content=%.80s\n", prefix, m.Content)
		}
	}

	for _, tr := range result.ToolResults {
		payload, err := json.Marshal(tr.Output)
		if err != nil {
			payload = fmt.Appendf(nil, "%v", tr.Output)
		}

		fmt.Fprintf(os.Stdout, "  tool_result: %s -> %s\n", tr.ToolName, string(payload))
	}
}

type weatherLLM struct{ called bool }

func (m *weatherLLM) Chat(_ context.Context, _ *entity.LLMRequest) (entity.LLMResponse, error) {
	if !m.called {
		m.called = true

		return entity.LLMResponse{
			ToolCalls: []entity.ToolCall{{
				ID:   "call_001",
				Type: "function",
				Function: entity.ToolCallFunction{
					Name:      "get_weather",
					Arguments: `{"city": "London"}`,
				},
			}},
			Usage: entity.Usage{PromptTokens: toolPromptToks, CompletionTokens: toolCompToks, TotalTokens: toolTotalToks},
		}, nil
	}

	return entity.LLMResponse{
		Content:      "The weather in London today is partly cloudy, 18C.",
		FinishReason: "stop",
		Usage:        entity.Usage{PromptTokens: finalPromptToks, CompletionTokens: finalCompToks, TotalTokens: finalTotalToks},
	}, nil
}

type weatherTool struct{}

func (w *weatherTool) Execute(_ context.Context, req *entity.ToolRequest) (entity.ToolResult, error) {
	return entity.ToolResult{
		RunID:      req.RunID,
		ToolCallID: req.ToolCallID,
		ToolName:   req.ToolName,
		Output:     map[string]any{"temperature": "18C", "condition": "partly cloudy", "city": req.Args["city"]},
	}, nil
}

func weatherToolDefs() []entity.ToolDef {
	return []entity.ToolDef{{
		Type: "function",
		Function: entity.ToolFuncDef{
			Name:        "get_weather",
			Description: "Get the current weather for a city",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`),
		},
	}}
}

var (
	_ repo.LLMProvider  = (*weatherLLM)(nil)
	_ repo.ToolExecutor = (*weatherTool)(nil)
)
