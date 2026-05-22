package toolkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	agenttool "github.com/TekkenSteve/GoAgent/agentfw/tool"
	"github.com/TekkenSteve/GoAgent/entity"
)

var (
	ErrToolMetaNameEmpty     = errors.New("tool meta name must not be empty")
	ErrToolAlreadyRegistered = errors.New("tool already registered")
	ErrToolNotFound          = errors.New("tool not found in registry")
)

// ToolRegistry maps tool names to implementations and serves dual purposes:
//  1. Implements agentfw/tool.Executor for Pipeline integration
//  2. Provides LLM function calling definitions for agent Prep
type ToolRegistry struct {
	mu    sync.RWMutex
	tools map[string]Tool
}

// New creates an empty registry.
func NewRegistry() *ToolRegistry {
	return &ToolRegistry{
		tools: make(map[string]Tool),
	}
}

// Register adds a tool to the registry. The tool's Meta().Name must be unique.
func (r *ToolRegistry) Register(t Tool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	meta := t.Meta()
	if meta.Name == "" {
		return ErrToolMetaNameEmpty
	}

	if _, exists := r.tools[meta.Name]; exists {
		return fmt.Errorf("%w: tool %q already registered", ErrToolAlreadyRegistered, meta.Name)
	}

	r.tools[meta.Name] = t

	return nil
}

// Get returns a registered tool by name.
func (r *ToolRegistry) Get(name string) (Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, ok := r.tools[name]

	return t, ok
}

// Names returns all registered tool names.
func (r *ToolRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.tools))
	for n := range r.tools {
		names = append(names, n)
	}

	return names
}

// Execute implements agentfw/tool.Executor by routing to the registered tool.
func (r *ToolRegistry) Execute(ctx context.Context, req *agenttool.Request) (agenttool.RawResult, error) {
	r.mu.RLock()
	tool, ok := r.tools[req.ToolName]
	r.mu.RUnlock()

	if !ok {
		return agenttool.RawResult{}, fmt.Errorf("%w: tool %q not found in registry", ErrToolNotFound, req.ToolName)
	}

	result, err := tool.Execute(ctx, req.Args)
	if err != nil {
		return agenttool.RawResult{}, fmt.Errorf("tool %q execution: %w", req.ToolName, err)
	}

	payload, err := toMap(result)
	if err != nil {
		return agenttool.RawResult{}, fmt.Errorf("tool %q result: %w", req.ToolName, err)
	}

	return agenttool.RawResult{Payload: payload}, nil
}

// Definitions returns LLM function calling definitions for all registered tools.
// Each definition uses the "function" type expected by OpenAI-compatible APIs.
func (r *ToolRegistry) Definitions() []entity.ToolDef {
	r.mu.RLock()
	defer r.mu.RUnlock()

	defs := make([]entity.ToolDef, 0, len(r.tools))
	for _, t := range r.tools {
		meta := t.Meta()
		defs = append(defs, entity.ToolDef{
			Type: "function",
			Function: entity.ToolFuncDef{
				Name:        meta.Name,
				Description: meta.Description,
				Parameters:  meta.Parameters,
			},
		})
	}

	return defs
}

// toMap converts any JSON-serializable value to map[string]any.
func toMap(v any) (map[string]any, error) {
	if v == nil {
		return map[string]any{}, nil
	}

	if m, ok := v.(map[string]any); ok {
		return m, nil
	}
	// Round-trip through JSON for structs, slices, etc.
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	if len(data) > 0 && data[0] == '{' {
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("unmarshal to map: %w", err)
		}

		return m, nil
	}
	// Scalar or array — wrap in a result envelope
	return map[string]any{"result": v}, nil
}
