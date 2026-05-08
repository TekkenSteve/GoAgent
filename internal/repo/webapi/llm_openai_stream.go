package webapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/pkg/sse"
)

// ChatStream sends a streaming chat completion request and returns a channel of chunks.
func (p *Provider) ChatStream(ctx context.Context, req entity.LLMRequest) (<-chan entity.LLMStreamChunk, error) {
	body := openAIRequest{
		Model:       req.Config.Model,
		Messages:    convertMessages(req.Messages),
		MaxTokens:   req.Config.MaxTokens,
		Temperature: req.Config.Temperature,
		Tools:       convertTools(req.Tools),
		Stream:      true,
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("llm openai stream - marshal: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/chat/completions", strings.NewReader(string(payload)))
	if err != nil {
		return nil, fmt.Errorf("llm openai stream - new request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("llm openai stream - do: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("llm openai stream - api error: status=%d", resp.StatusCode)
	}

	ch := make(chan entity.LLMStreamChunk, 64)

	go func() {
		defer close(ch)
		defer resp.Body.Close()

		// Accumulate tool call deltas by index across chunks
		type toolCallAcc struct {
			ID        string
			Name      string
			Arguments string
		}
		accum := make(map[int]*toolCallAcc)

		emitToolCalls := func() []entity.ToolCall {
			if len(accum) == 0 {
				return nil
			}
			tcs := make([]entity.ToolCall, 0, len(accum))
			for i := 0; i < len(accum); i++ {
				a := accum[i]
				if a == nil {
					continue
				}
				tcs = append(tcs, entity.ToolCall{
					ID:   a.ID,
					Type: "function",
					Function: entity.ToolCallFunction{
						Name:      a.Name,
						Arguments: a.Arguments,
					},
				})
			}
			return tcs
		}

		for evt, err := range sse.Read(resp.Body, nil) {
			if err != nil {
				select {
				case ch <- entity.LLMStreamChunk{
					Content: fmt.Sprintf("stream read error: %v", err),
				}:
				case <-ctx.Done():
				}
				return
			}

			if evt.Data == "[DONE]" {
				return
			}

			var sChunk openAIStreamChunk
			if err := json.Unmarshal([]byte(evt.Data), &sChunk); err != nil {
				continue
			}

			if len(sChunk.Choices) == 0 {
				continue
			}

			choice := sChunk.Choices[0]
			delta := choice.Delta

			// Emit content delta
			if delta.Content != "" {
				select {
				case ch <- entity.LLMStreamChunk{Content: delta.Content}:
				case <-ctx.Done():
					return
				}
			}

			// Accumulate tool call deltas
			for _, tc := range delta.ToolCalls {
				if _, ok := accum[tc.Index]; !ok {
					accum[tc.Index] = &toolCallAcc{}
				}
				if tc.ID != "" {
					accum[tc.Index].ID = tc.ID
				}
				if tc.Function != nil {
					if tc.Function.Name != "" {
						accum[tc.Index].Name = tc.Function.Name
					}
					if tc.Function.Arguments != "" {
						accum[tc.Index].Arguments += tc.Function.Arguments
					}
				}
			}

			// On finish_reason, emit accumulated tool calls and usage
			if choice.FinishReason != nil {
				tcs := emitToolCalls()

				chunk := entity.LLMStreamChunk{
					FinishReason: entity.FinishReason(*choice.FinishReason),
					ToolCalls:    tcs,
				}
				if sChunk.Usage != nil {
					chunk.Usage = entity.Usage{
						PromptTokens:     sChunk.Usage.PromptTokens,
						CompletionTokens: sChunk.Usage.CompletionTokens,
						TotalTokens:      sChunk.Usage.TotalTokens,
					}
				}

				select {
				case ch <- chunk:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return ch, nil
}

// -- OpenAI streaming JSON types --

type openAIStreamChunk struct {
	Choices []openAIStreamChoice `json:"choices"`
	Usage   *openAIUsage         `json:"usage,omitempty"`
}

type openAIStreamChoice struct {
	Delta        openAIStreamDelta `json:"delta"`
	FinishReason *string           `json:"finish_reason"`
}

type openAIStreamDelta struct {
	Content   string                      `json:"content,omitempty"`
	ToolCalls []openAIStreamToolCallDelta `json:"tool_calls,omitempty"`
}

type openAIStreamToolCallDelta struct {
	Index    int                         `json:"index"`
	ID       string                      `json:"id,omitempty"`
	Function *openAIStreamFunctionDelta  `json:"function,omitempty"`
}

type openAIStreamFunctionDelta struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}
