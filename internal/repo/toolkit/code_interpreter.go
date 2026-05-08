package toolkit

import (
	"context"
	"fmt"
	"time"
)

// CodeInterpreterConfig configures the code_interpreter tool.
type CodeInterpreterConfig struct {
	// Endpoint is the sandbox execution service URL. If empty, runs in-process.
	Endpoint string
	// APIKey for the sandbox service.
	APIKey string
	// DefaultTimeout is the maximum execution time per invocation.
	DefaultTimeout time.Duration
}

// CodeInterpreter executes safe code snippets in an isolated sandbox.
type CodeInterpreter struct {
	cfg CodeInterpreterConfig
}

// NewCodeInterpreter creates a code interpreter tool.
func NewCodeInterpreter(cfg CodeInterpreterConfig) *CodeInterpreter {
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = 30 * time.Second
	}
	return &CodeInterpreter{cfg: cfg}
}

// Meta implements tool.Tool.
func (c *CodeInterpreter) Meta() ToolMeta {
	return ToolMeta{
		Name:        "code_interpreter",
		Description: "Execute Python code in a sandboxed environment. Use this tool for data analysis, visualization, computation, and file processing tasks.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"code": map[string]any{
					"type":        "string",
					"description": "The Python code to execute",
				},
				"timeout": map[string]any{
					"type":        "integer",
					"description": "Execution timeout in seconds (max 120)",
					"default":     30,
				},
			},
			"required": []any{"code"},
		},
	}
}

// Execute implements tool.Tool.
func (c *CodeInterpreter) Execute(ctx context.Context, args map[string]any) (any, error) {
	code, _ := args["code"].(string)
	if code == "" {
		return nil, fmt.Errorf("code is required")
	}

	timeout := 30
	if t, ok := args["timeout"].(float64); ok {
		if n := int(t); n > 0 && n <= 120 {
			timeout = n
		}
	}

	if c.cfg.Endpoint != "" {
		return c.executeRemote(ctx, code, timeout)
	}
	return c.executeLocal(ctx, code, timeout)
}

func (c *CodeInterpreter) executeRemote(ctx context.Context, code string, timeoutSec int) (any, error) {
	// TODO: implement remote sandbox execution (e.g., Pyodide, gVisor, Firecracker)
	return nil, fmt.Errorf("remote code execution not yet implemented")
}

func (c *CodeInterpreter) executeLocal(ctx context.Context, code string, timeoutSec int) (any, error) {
	return map[string]any{
		"stdout":   "",
		"stderr":   "Code interpreter is not configured with a sandbox endpoint.",
		"exit_code": 1,
		"result":   "execution unavailable",
	}, nil
}
