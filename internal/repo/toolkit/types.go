package toolkit

import "context"

// Tool is the interface each tool must implement.
// Tools are stateless by design: all configuration is injected at construction,
// and execution context is passed via Execute args.
type Tool interface {
	// Meta returns the tool's name, description, and JSON Schema parameters
	// used for LLM function calling definition generation.
	Meta() ToolMeta

	// Execute runs the tool with parsed arguments and returns the result.
	// The return value should be JSON-serializable (map, struct, slice, etc.).
	Execute(ctx context.Context, args map[string]any) (any, error)
}

// ToolMeta describes a tool for LLM function calling.
type ToolMeta struct {
	Name        string
	Description string
	Parameters  map[string]any // JSON Schema object
}
