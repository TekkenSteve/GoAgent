package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// Config is the configuration for the OpenAI provider.
type Config struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

// Provider implements repo.LLMProvider for OpenAI-compatible APIs.
type Provider struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

// New creates a new OpenAI provider.
func New(cfg Config) *Provider {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 120 * time.Second}
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.openai.com/v1"
	}
	return &Provider{
		baseURL: cfg.BaseURL,
		apiKey:  cfg.APIKey,
		client:  cfg.HTTPClient,
	}
}

// Chat sends a chat completion request to the OpenAI API.
func (p *Provider) Chat(ctx context.Context, req entity.LLMRequest) (entity.LLMResponse, error) {
	body := openAIRequest{
		Model:       req.Config.Model,
		Messages:    convertMessages(req.Messages),
		MaxTokens:   req.Config.MaxTokens,
		Temperature: req.Config.Temperature,
		Tools:       convertTools(req.Tools),
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return entity.LLMResponse{}, fmt.Errorf("llm openai - marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return entity.LLMResponse{}, fmt.Errorf("llm openai - new request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return entity.LLMResponse{}, fmt.Errorf("llm openai - do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return entity.LLMResponse{}, fmt.Errorf("llm openai - read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return entity.LLMResponse{}, fmt.Errorf("llm openai - api error: status=%d body=%s", resp.StatusCode, string(respBody))
	}

	var openAIResp openAIResponse
	if err := json.Unmarshal(respBody, &openAIResp); err != nil {
		return entity.LLMResponse{}, fmt.Errorf("llm openai - unmarshal response: %w", err)
	}

	if len(openAIResp.Choices) == 0 {
		return entity.LLMResponse{}, fmt.Errorf("llm openai - empty choices")
	}

	choice := openAIResp.Choices[0]
	result := entity.LLMResponse{
		Content:      choice.Message.Content,
		FinishReason: entity.FinishReason(choice.FinishReason),
		Usage: entity.Usage{
			PromptTokens:     openAIResp.Usage.PromptTokens,
			CompletionTokens: openAIResp.Usage.CompletionTokens,
			TotalTokens:      openAIResp.Usage.TotalTokens,
		},
	}

	for _, tc := range choice.Message.ToolCalls {
		result.ToolCalls = append(result.ToolCalls, entity.ToolCall{
			ID:   tc.ID,
			Type: tc.Type,
			Function: entity.ToolCallFunction{
				Name:      tc.Function.Name,
				Arguments: tc.Function.Arguments,
			},
		})
	}

	return result, nil
}

// -- OpenAI API JSON types --

type openAIMessage struct {
	Role       string           `json:"role"`
	Content    string           `json:"content,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	ToolCalls  []openAIToolCall `json:"tool_calls,omitempty"`
}

type openAIToolCall struct {
	ID       string            `json:"id"`
	Type     string            `json:"type"`
	Function openAIToolFunction `json:"function"`
}

type openAIToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type openAITool struct {
	Type     string             `json:"type"`
	Function openAIToolFuncDef  `json:"function"`
}

type openAIToolFuncDef struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  any    `json:"parameters"`
}

type openAIRequest struct {
	Model       string           `json:"model"`
	Messages    []openAIMessage  `json:"messages"`
	MaxTokens   int              `json:"max_tokens,omitempty"`
	Temperature float64          `json:"temperature,omitempty"`
	Tools       []openAITool     `json:"tools,omitempty"`
}

type openAIResponse struct {
	Choices []openAIChoice `json:"choices"`
	Usage   openAIUsage    `json:"usage"`
}

type openAIChoice struct {
	Message      openAIMessage `json:"message"`
	FinishReason string        `json:"finish_reason"`
}

type openAIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// -- Converters --

func convertMessages(msgs []entity.Message) []openAIMessage {
	out := make([]openAIMessage, 0, len(msgs))
	for _, m := range msgs {
		om := openAIMessage{
			Role:    string(m.Role),
			Content: m.Content,
		}
		if m.ToolCallID != "" {
			om.ToolCallID = m.ToolCallID
		}
		if len(m.ToolCalls) > 0 {
			om.ToolCalls = make([]openAIToolCall, 0, len(m.ToolCalls))
			for _, tc := range m.ToolCalls {
				om.ToolCalls = append(om.ToolCalls, openAIToolCall{
					ID:   tc.ID,
					Type: tc.Type,
					Function: openAIToolFunction{
						Name:      tc.Function.Name,
						Arguments: tc.Function.Arguments,
					},
				})
			}
		}
		out = append(out, om)
	}
	return out
}

func convertTools(tools []entity.ToolDef) []openAITool {
	if len(tools) == 0 {
		return nil
	}
	out := make([]openAITool, 0, len(tools))
	for _, t := range tools {
		out = append(out, openAITool{
			Type: t.Type,
			Function: openAIToolFuncDef{
				Name:        t.Function.Name,
				Description: t.Function.Description,
				Parameters:  t.Function.Parameters,
			},
		})
	}
	return out
}
