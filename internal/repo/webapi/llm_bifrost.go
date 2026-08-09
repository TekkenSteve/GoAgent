package webapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
)

var (
	// ErrBifrostChat is returned when the Bifrost SDK reports an error for a chat request.
	ErrBifrostChat = errors.New("bifrost chat error")
	// ErrNoCandidates is returned when no candidates are available for a scenario.
	ErrNoCandidates = errors.New("no candidates")
	// ErrNoProvider is returned when no configured provider matches a scenario.
	ErrNoProvider = errors.New("no configured provider")
)

// ProviderEntry defines a single LLM provider configuration.
type ProviderEntry struct {
	Provider schemas.ModelProvider
	APIKey   string
	BaseURL  string
}

// BifrostProvider implements repo.LLMProvider and repo.LLMStreamProvider
// using the embedded Bifrost Go SDK for unified multi-provider LLM access,
// built-in cost tracking, and automatic key management.
type BifrostProvider struct {
	mu                  sync.RWMutex
	client              *bifrost.Bifrost
	defaultProvider     schemas.ModelProvider
	scenarios           map[string]ScenarioConfig
	defaultScenario     string
	configuredProviders map[schemas.ModelProvider]bool
	account             *bifrostAccount
}

// BifrostConfig configures the Bifrost LLM provider.
type BifrostConfig struct {
	// Providers configures one or more LLM providers (API keys, base URLs).
	Providers []ProviderEntry
	// Scenarios maps scenario names to ordered candidate lists.
	Scenarios map[string]ScenarioConfig
	// DefaultScenario is used when a request has no explicit scenario.
	DefaultScenario string

	// Legacy single-provider fields — used only when Providers is empty.
	Provider schemas.ModelProvider
	APIKey   string
	BaseURL  string
}

// NewBifrost initializes the Bifrost SDK and returns a provider that delegates
// all LLM calls through Bifrost's unified ChatCompletion API.
// The caller must call Close() to release Bifrost resources (workers, connections).
func NewBifrost(cfg *BifrostConfig) (*BifrostProvider, error) {
	// Merge legacy fields into Providers when Providers is empty.
	entries := cfg.Providers
	if len(entries) == 0 {
		entries = []ProviderEntry{
			{
				Provider: cfg.Provider,
				APIKey:   cfg.APIKey,
				BaseURL:  cfg.BaseURL,
			},
		}
	}

	var defaultProvider schemas.ModelProvider

	providerMap := make(map[schemas.ModelProvider]*providerEntry, len(entries))

	configured := make(map[schemas.ModelProvider]bool, len(entries))
	for _, e := range entries {
		providerMap[e.Provider] = &providerEntry{
			apiKey:  e.APIKey,
			baseURL: e.BaseURL,
		}
		if e.APIKey != "" {
			configured[e.Provider] = true
		}

		if defaultProvider == "" {
			defaultProvider = e.Provider
		}
	}

	account := &bifrostAccount{
		providers: providerMap,
	}

	client, err := bifrost.Init(context.Background(), schemas.BifrostConfig{
		Account: account,
		Logger:  bifrost.NewDefaultLogger(schemas.LogLevelError),
	})
	if err != nil {
		return nil, fmt.Errorf("bifrost init: %w", err)
	}

	return &BifrostProvider{
		client:              client,
		defaultProvider:     defaultProvider,
		scenarios:           cfg.Scenarios,
		defaultScenario:     cfg.DefaultScenario,
		configuredProviders: configured,
		account:             account,
	}, nil
}

// Close shuts down the Bifrost client and releases all worker goroutines.
func (p *BifrostProvider) Close() {
	p.client.Shutdown()
}

// ReloadConfig hot-reloads the provider and scenario configuration from a parsed
// config file. Called by LLMConfigWatcher on file changes.
func (p *BifrostProvider) ReloadConfig(cfg *LLMConfigFile) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entries := cfg.ToProviderEntries()
	p.account.setProviders(entries)

	configured := make(map[schemas.ModelProvider]bool, len(entries))
	for _, e := range entries {
		if e.APIKey != "" {
			configured[e.Provider] = true
		}
	}

	p.configuredProviders = configured

	if len(cfg.Scenarios) > 0 {
		p.scenarios = cfg.Scenarios
		p.defaultScenario = cfg.DefaultScenarioName()
	}
}

// resolveProvider picks the provider for a request: use req.Config.Provider when
// non-empty, otherwise fall back to the default provider.
func (p *BifrostProvider) resolveProvider(req *entity.LLMRequest) schemas.ModelProvider {
	if req.Config.Provider != "" {
		return schemas.ModelProvider(req.Config.Provider)
	}

	return p.defaultProvider
}

// Chat sends a synchronous chat completion request via the Bifrost SDK.
func (p *BifrostProvider) Chat(ctx context.Context, req *entity.LLMRequest) (entity.LLMResponse, error) {
	provider, model, params, fallbacks := p.resolveRequest(req)

	bifrostReq := &schemas.BifrostChatRequest{
		Provider:  provider,
		Model:     model,
		Input:     messagesToBifrost(req.Messages),
		Params:    configToBifrostParams(params, req.Tools),
		Fallbacks: fallbacks,
	}

	bifrostCtx := schemas.NewBifrostContext(ctx, schemas.NoDeadline)

	resp, bifrostErr := p.client.ChatCompletionRequest(bifrostCtx, bifrostReq)
	if bifrostErr != nil {
		return entity.LLMResponse{}, fmt.Errorf("%w: %s", ErrBifrostChat, bifrostErr.GetErrorString())
	}

	return chatResponseToEntity(resp), nil
}

// ChatStream sends a streaming chat completion request and returns a channel of chunks.
func (p *BifrostProvider) ChatStream(ctx context.Context, req *entity.LLMRequest) (<-chan entity.LLMStreamChunk, error) {
	provider, model, params, fallbacks := p.resolveRequest(req)

	bifrostReq := &schemas.BifrostChatRequest{
		Provider:  provider,
		Model:     model,
		Input:     messagesToBifrost(req.Messages),
		Params:    configToBifrostParams(params, req.Tools),
		Fallbacks: fallbacks,
	}

	bifrostCtx := schemas.NewBifrostContext(ctx, schemas.NoDeadline)

	streamCh, bifrostErr := p.client.ChatCompletionStreamRequest(bifrostCtx, bifrostReq)
	if bifrostErr != nil {
		return nil, fmt.Errorf("%w: %s", ErrBifrostChat, bifrostErr.GetErrorString())
	}

	out := make(chan entity.LLMStreamChunk)

	go func() {
		defer close(out)

		for chunk := range streamCh {
			if chunk.BifrostError != nil {
				return
			}

			if chunk.BifrostChatResponse != nil {
				ec := streamChunkToEntity(chunk.BifrostChatResponse)
				select {
				case out <- ec:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}

// resolveRequest determines the provider, model, params, and fallbacks for a request.
// It uses the ScenarioRouter when a scenario is specified, falling back to direct
// provider selection for backward compatibility.
func (p *BifrostProvider) resolveRequest(req *entity.LLMRequest) (schemas.ModelProvider, string, entity.LLMConfig, []schemas.Fallback) {
	p.mu.RLock()
	scenarios := p.scenarios
	defaultScenario := p.defaultScenario
	configuredProviders := p.configuredProviders
	p.mu.RUnlock()

	// Use scenario routing when scenarios are configured and request has a scenario.
	if len(scenarios) > 0 && req.Scenario != "" {
		result, err := resolveScenario(scenarios, defaultScenario, req.Scenario, req.Config, configuredProviders)
		if err == nil {
			return result.Provider, result.Model, result.Params, result.Fallbacks
		}
		// Router error fallback — try direct provider selection.
	}

	// Direct provider selection (backward-compatible path).
	provider := p.resolveProvider(req)
	model := req.Config.Model
	params := req.Config

	if model == "" {
		model = "gpt-4.1-mini"
	}

	return provider, model, params, nil
}

// ——— Scenario resolution ———

// ResolveResult contains the fully resolved provider, model, params, and fallbacks.
type ResolveResult struct {
	Provider  schemas.ModelProvider
	Model     string
	Params    entity.LLMConfig
	Fallbacks []schemas.Fallback
}

// resolveScenario picks the best provider/model from the scenario candidates.
func resolveScenario(scenarios map[string]ScenarioConfig, defaultScenario, scenario string, reqOverrides entity.LLMConfig, configuredProviders map[schemas.ModelProvider]bool) (ResolveResult, error) {
	if reqOverrides.Provider != "" {
		return ResolveResult{
			Provider: schemas.ModelProvider(reqOverrides.Provider),
			Model:    reqOverrides.Model,
			Params:   reqOverrides,
		}, nil
	}

	sc, ok := findFallbackScenario(scenarios, defaultScenario, scenario)
	if !ok {
		return ResolveResult{}, fmt.Errorf("%w for scenario %q and no fallback scenario", ErrNoCandidates, scenario)
	}

	primary, fallbackCandidates, found := pickPrimaryAndFallback(sc.Candidates, configuredProviders)
	if !found {
		return ResolveResult{}, fmt.Errorf("%w for scenario %q", ErrNoProvider, scenario)
	}

	cfg := mergeCandidateConfig(entity.LLMConfig{
		Model:       primary.Model,
		MaxTokens:   primary.MaxTokens,
		Temperature: primary.Temperature,
	}, reqOverrides)

	bifrostFallbacks := make([]schemas.Fallback, 0, len(fallbackCandidates))
	for _, f := range fallbackCandidates {
		bifrostFallbacks = append(bifrostFallbacks, schemas.Fallback{
			Provider: schemas.ModelProvider(f.Provider),
			Model:    f.Model,
		})
	}

	return ResolveResult{
		Provider:  schemas.ModelProvider(primary.Provider),
		Model:     cfg.Model,
		Params:    cfg,
		Fallbacks: bifrostFallbacks,
	}, nil
}

// findFallbackScenario searches for a scenario config, falling back to default
// and then to any available scenario if the specified one is not found.
func findFallbackScenario(scenarios map[string]ScenarioConfig, defaultScenario, scenario string) (ScenarioConfig, bool) {
	sc, ok := scenarios[scenario]
	if !ok || len(sc.Candidates) == 0 {
		if scenario != defaultScenario {
			sc, ok = scenarios[defaultScenario]
		}

		if !ok || len(sc.Candidates) == 0 {
			for _, s := range scenarios {
				sc = s

				break
			}
		}
	}

	return sc, len(sc.Candidates) > 0
}

// pickPrimaryAndFallback iterates candidates and selects the first configured
// provider as primary and the rest as fallbacks.
func pickPrimaryAndFallback(candidates []CandidateConfig, configuredProviders map[schemas.ModelProvider]bool) (primary CandidateConfig, fallbacks []CandidateConfig, found bool) {
	for _, c := range candidates {
		if configuredProviders[schemas.ModelProvider(c.Provider)] {
			if !found {
				primary = c
				found = true
			} else {
				fallbacks = append(fallbacks, c)
			}
		}
	}

	return primary, fallbacks, found
}

// mergeCandidateConfig merges request overrides on top of scenario defaults.
func mergeCandidateConfig(base, override entity.LLMConfig) entity.LLMConfig {
	if override.Model != "" {
		base.Model = override.Model
	}

	if override.MaxTokens != 0 {
		base.MaxTokens = override.MaxTokens
	}

	if override.Temperature != 0 {
		base.Temperature = override.Temperature
	}

	return base
}

// ——— Account implementation (provides API keys to Bifrost) ———

type providerEntry struct {
	apiKey  string
	baseURL string
}

type bifrostAccount struct {
	mu        sync.RWMutex
	providers map[schemas.ModelProvider]*providerEntry
}

func (a *bifrostAccount) setProviders(entries []ProviderEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.providers = make(map[schemas.ModelProvider]*providerEntry, len(entries))
	for _, e := range entries {
		if e.APIKey != "" {
			a.providers[e.Provider] = &providerEntry{apiKey: e.APIKey, baseURL: e.BaseURL}
		}
	}
}

func (a *bifrostAccount) GetConfiguredProviders() ([]schemas.ModelProvider, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	keys := make([]schemas.ModelProvider, 0, len(a.providers))
	for k := range a.providers {
		keys = append(keys, k)
	}

	return keys, nil
}

func (a *bifrostAccount) GetKeysForProvider(_ context.Context, providerKey schemas.ModelProvider) ([]schemas.Key, error) {
	a.mu.RLock()
	entry, ok := a.providers[providerKey]
	a.mu.RUnlock()

	if !ok || entry.apiKey == "" {
		return nil, nil
	}

	return []schemas.Key{
		{
			ID:     "default",
			Name:   "default",
			Value:  schemas.EnvVar{Val: entry.apiKey, FromEnv: false, EnvVar: ""},
			Models: schemas.WhiteList{"*"},
			Weight: 1.0,
		},
	}, nil
}

func (a *bifrostAccount) GetConfigForProvider(providerKey schemas.ModelProvider) (*schemas.ProviderConfig, error) {
	a.mu.RLock()
	entry, ok := a.providers[providerKey]
	a.mu.RUnlock()

	if !ok {
		nc := schemas.DefaultNetworkConfig

		return &schemas.ProviderConfig{
			NetworkConfig:            nc,
			ConcurrencyAndBufferSize: schemas.DefaultConcurrencyAndBufferSize,
		}, nil
	}

	nc := schemas.DefaultNetworkConfig
	if entry.baseURL != "" {
		nc.BaseURL = entry.baseURL
	}

	return &schemas.ProviderConfig{
		NetworkConfig:            nc,
		ConcurrencyAndBufferSize: schemas.DefaultConcurrencyAndBufferSize,
	}, nil
}

// ——— Entity → Bifrost conversion ———

func messagesToBifrost(msgs []entity.Message) []schemas.ChatMessage {
	out := make([]schemas.ChatMessage, 0, len(msgs))
	for _, m := range msgs {
		cm := schemas.ChatMessage{
			Role:    roleToBifrost(m.Role),
			Content: stringContent(m.Content),
		}
		if m.ToolCallID != "" {
			cm.ChatToolMessage = &schemas.ChatToolMessage{
				ToolCallID: new(m.ToolCallID),
			}
		}

		if len(m.ToolCalls) > 0 {
			tcs := make([]schemas.ChatAssistantMessageToolCall, 0, len(m.ToolCalls))
			for _, tc := range m.ToolCalls {
				tcs = append(tcs, toolCallToBifrost(tc))
			}

			cm.ChatAssistantMessage = &schemas.ChatAssistantMessage{ToolCalls: tcs}
		}

		out = append(out, cm)
	}

	return out
}

func roleToBifrost(r entity.MessageRole) schemas.ChatMessageRole {
	switch r {
	case entity.RoleUser:
		return schemas.ChatMessageRoleUser
	case entity.RoleAssistant:
		return schemas.ChatMessageRoleAssistant
	case entity.RoleSystem:
		return schemas.ChatMessageRoleSystem
	case entity.RoleTool:
		return schemas.ChatMessageRoleTool
	default:
		return schemas.ChatMessageRoleUser
	}
}

func stringContent(s string) *schemas.ChatMessageContent {
	if s == "" {
		return nil
	}

	return &schemas.ChatMessageContent{ContentStr: new(s)}
}

func toolCallToBifrost(tc entity.ToolCall) schemas.ChatAssistantMessageToolCall {
	args := tc.Function.Arguments
	if args == "" {
		args = "{}"
	}

	return schemas.ChatAssistantMessageToolCall{
		ID:   new(tc.ID),
		Type: new(tc.Type),
		Function: schemas.ChatAssistantMessageToolCallFunction{
			Name:      new(tc.Function.Name),
			Arguments: args,
		},
	}
}

func configToBifrostParams(cfg entity.LLMConfig, tools []entity.ToolDef) *schemas.ChatParameters {
	params := &schemas.ChatParameters{
		MaxCompletionTokens: new(cfg.MaxTokens),
		Temperature:         new(cfg.Temperature),
	}

	if len(tools) > 0 {
		bt := make([]schemas.ChatTool, 0, len(tools))
		for _, t := range tools {
			bt = append(bt, toolDefToBifrost(t))
		}

		params.Tools = bt
	}

	return params
}

func toolDefToBifrost(t entity.ToolDef) schemas.ChatTool {
	return schemas.ChatTool{
		Type: schemas.ChatToolTypeFunction,
		Function: &schemas.ChatToolFunction{
			Name:        t.Function.Name,
			Description: new(t.Function.Description),
			Parameters:  convertParameters(t.Function.Parameters),
		},
	}
}

// convertParameters converts the entity-level `any` parameters
// (typically a JSON-schema map) into the Bifrost typed parameters.
func convertParameters(params any) *schemas.ToolFunctionParameters {
	if params == nil {
		return nil
	}

	data, err := json.Marshal(params)
	if err != nil {
		return nil
	}

	var tfp schemas.ToolFunctionParameters
	if err := json.Unmarshal(data, &tfp); err != nil {
		return nil
	}

	return &tfp
}

// ——— Bifrost → Entity conversion ———

// chatResponseToEntity converts a Bifrost chat response back to our entity type.
func chatResponseToEntity(resp *schemas.BifrostChatResponse) entity.LLMResponse {
	out := entity.LLMResponse{}

	if len(resp.Choices) == 0 {
		return out
	}

	choice := resp.Choices[0]

	// Extract text content from the non-stream choice message.
	if choice.ChatNonStreamResponseChoice != nil && choice.Message != nil {
		msg := choice.Message
		if msg.Content != nil && msg.Content.ContentStr != nil {
			out.Content = *msg.Content.ContentStr
		}
		// Extract tool calls from the assistant message.
		if msg.ChatAssistantMessage != nil {
			out.ToolCalls = toolCallsFromBifrost(msg.ToolCalls)
		}
	}

	if choice.FinishReason != nil {
		out.FinishReason = entity.FinishReason(*choice.FinishReason)
	}

	out.Usage = usageFromBifrost(resp.Usage)

	return out
}

// toolCallsFromBifrost converts Bifrost tool calls to entity tool calls.
func toolCallsFromBifrost(tcs []schemas.ChatAssistantMessageToolCall) []entity.ToolCall {
	if len(tcs) == 0 {
		return nil
	}

	out := make([]entity.ToolCall, 0, len(tcs))
	for _, tc := range tcs {
		name := ""
		if tc.Function.Name != nil {
			name = *tc.Function.Name
		}

		out = append(out, entity.ToolCall{
			ID:   deref(tc.ID),
			Type: deref(tc.Type),
			Function: entity.ToolCallFunction{
				Name:      name,
				Arguments: tc.Function.Arguments,
			},
		})
	}

	return out
}

// usageFromBifrost extracts entity usage from Bifrost usage (with built-in cost).
func usageFromBifrost(u *schemas.BifrostLLMUsage) entity.Usage {
	if u == nil {
		return entity.Usage{}
	}

	usage := entity.Usage{
		PromptTokens:     u.PromptTokens,
		CompletionTokens: u.CompletionTokens,
		TotalTokens:      u.TotalTokens,
	}
	if u.Cost != nil {
		usage.Cost = entity.Money(u.Cost.TotalCost)
	}

	return usage
}

// streamChunkToEntity converts a single Bifrost stream chunk to our entity type.
func streamChunkToEntity(chunk *schemas.BifrostChatResponse) entity.LLMStreamChunk {
	var ec entity.LLMStreamChunk

	if len(chunk.Choices) == 0 {
		if chunk.Usage != nil {
			ec.Usage = usageFromBifrost(chunk.Usage)
		}

		return ec
	}

	choice := chunk.Choices[0]

	if choice.ChatStreamResponseChoice != nil && choice.Delta != nil {
		ec = handleStreamingDelta(choice.Delta)
	}

	// Non-streaming fallback (some chunks may arrive as complete choices)
	if choice.ChatNonStreamResponseChoice != nil && choice.Message != nil {
		contentStr, toolCalls := handleNonStreamingChoice(choice)
		applyNonStreamingContent(contentStr, toolCalls, &ec)
	}

	if choice.FinishReason != nil {
		ec.FinishReason = entity.FinishReason(*choice.FinishReason)
	}

	if chunk.Usage != nil {
		ec.Usage = usageFromBifrost(chunk.Usage)
	}

	return ec
}

// applyNonStreamingContent sets content and tool call fields on the stream chunk
// from the results of a non-streaming response choice.
func applyNonStreamingContent(contentStr string, toolCalls []entity.ToolCall, ec *entity.LLMStreamChunk) {
	if contentStr != "" {
		ec.Content = contentStr
	}

	if len(toolCalls) > 0 {
		ec.ToolCalls = toolCalls
	}
}

// handleStreamingDelta processes a streaming response delta and returns
// the partial fields that need to be set on the stream chunk.
func handleStreamingDelta(delta *schemas.ChatStreamResponseChoiceDelta) entity.LLMStreamChunk {
	var ec entity.LLMStreamChunk

	if delta.Content != nil {
		ec.Content = *delta.Content
	}

	if delta.Reasoning != nil {
		ec.Reasoning = *delta.Reasoning
	}

	// Convert streaming tool call deltas
	if len(delta.ToolCalls) > 0 {
		ec.ToolCallDeltas = make([]entity.ToolCallDelta, 0, len(delta.ToolCalls))
		for _, tc := range delta.ToolCalls {
			d := entity.ToolCallDelta{Index: int(tc.Index)}
			if tc.ID != nil {
				d.ToolCallID = *tc.ID
			}

			if tc.Function.Name != nil {
				d.Name = *tc.Function.Name
			}

			if tc.Function.Arguments != "" {
				d.ArgsDelta = tc.Function.Arguments
			}

			ec.ToolCallDeltas = append(ec.ToolCallDeltas, d)
		}
	}

	return ec
}

// handleNonStreamingChoice extracts content and tool calls from a
// non-streaming response choice.
func handleNonStreamingChoice(choice schemas.BifrostResponseChoice) (string, []entity.ToolCall) {
	if choice.ChatNonStreamResponseChoice == nil || choice.Message == nil {
		return "", nil
	}

	msg := choice.Message

	var contentStr string
	if msg.Content != nil && msg.Content.ContentStr != nil {
		contentStr = *msg.Content.ContentStr
	}

	var toolCalls []entity.ToolCall
	if msg.ChatAssistantMessage != nil {
		toolCalls = toolCallsFromBifrost(msg.ToolCalls)
	}

	return contentStr, toolCalls
}

func deref[T any](p *T) T {
	if p == nil {
		var zero T

		return zero
	}

	return *p
}
