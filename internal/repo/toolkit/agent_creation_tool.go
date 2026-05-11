package toolkit

import (
	"context"
	"fmt"
)

// AgentCreationTool allows an agent to dynamically create sub-agents at runtime.
// It registers the new agent and dispatches a child workflow via Temporal.
// This tool is wired with an AgentCreator func by the app layer.
type AgentCreationTool struct {
	meta    ToolMeta
	creator AgentCreatorFn
}

// AgentCreatorFn creates an agent record and returns the agent ID.
// The actual Temporal child workflow dispatch is handled by the caller.
type AgentCreatorFn func(ctx context.Context, agentID, name, systemPrompt, modelRef string, tools []string) error

// NewAgentCreationTool creates a tool that agents can use to create sub-agents.
func NewAgentCreationTool(creator AgentCreatorFn) *AgentCreationTool {
	return &AgentCreationTool{
		meta: ToolMeta{
			Name:        "create_agent",
			Description: "Create a new specialized sub-agent for a specific task. Use this when you need a dedicated agent with custom instructions and tools.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"agent_id": map[string]any{
						"type":        "string",
						"description": "Unique identifier for the new agent",
					},
					"name": map[string]any{
						"type":        "string",
						"description": "Human-readable name for the new agent",
					},
					"system_prompt": map[string]any{
						"type":        "string",
						"description": "System prompt defining the agent's role and behavior",
					},
					"model_ref": map[string]any{
						"type":        "string",
						"description": "Model reference (e.g., gpt-4, claude-3)",
					},
					"tools": map[string]any{
						"type":        "array",
						"items":       map[string]any{"type": "string"},
						"description": "List of tool names the agent can use",
					},
				},
				"required": []string{"agent_id", "name", "system_prompt", "model_ref"},
			},
		},
		creator: creator,
	}
}

// Meta returns the tool metadata for LLM function calling.
func (t *AgentCreationTool) Meta() ToolMeta {
	return t.meta
}

// Execute creates a new agent with the given configuration.
func (t *AgentCreationTool) Execute(ctx context.Context, args map[string]any) (any, error) {
	agentID, _ := args["agent_id"].(string)
	name, _ := args["name"].(string)
	systemPrompt, _ := args["system_prompt"].(string)
	modelRef, _ := args["model_ref"].(string)

	if agentID == "" || name == "" {
		return nil, fmt.Errorf("agent_creation_tool: agent_id and name are required")
	}
	if systemPrompt == "" {
		return nil, fmt.Errorf("agent_creation_tool: system_prompt is required")
	}
	if modelRef == "" {
		modelRef = "gpt-4" // default
	}

	toolsRaw, _ := args["tools"].([]any)
	tools := make([]string, 0, len(toolsRaw))
	for _, t := range toolsRaw {
		if s, ok := t.(string); ok {
			tools = append(tools, s)
		}
	}

	if err := t.creator(ctx, agentID, name, systemPrompt, modelRef, tools); err != nil {
		return nil, fmt.Errorf("agent_creation_tool - create: %w", err)
	}

	return map[string]any{
		"agent_id": agentID,
		"name":     name,
		"status":   "created",
	}, nil
}
