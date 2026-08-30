package toolkit

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// defaultCodeInterpreterTimeout is the default timeout for code execution.
const defaultCodeInterpreterTimeout = 30 * time.Second

// defaultCodeInterpreterTimeoutSec is the default timeout in seconds for the Meta parameter.
const defaultCodeInterpreterTimeoutSec = 30

// Package-level sentinel errors for the code interpreter.
var (
	ErrCodeRequired             = errors.New("code is required")
	ErrRemoteCodeNotImplemented = errors.New("remote code execution not yet implemented")
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
		cfg.DefaultTimeout = defaultCodeInterpreterTimeout
	}

	return &CodeInterpreter{cfg: cfg}
}

// Meta implements tool.Tool.
func (c *CodeInterpreter) Meta() ToolMeta {
	return ToolMeta{
		Name:        "code_interpreter",
		Description: "Execute Python code in a sandboxed environment. Use this tool for data analysis, visualization, computation, and file processing tasks.",
		Parameters: map[string]any{
			_schemaKeyType: "object",
			"properties": map[string]any{
				"code": map[string]any{
					_schemaKeyType:        _schemaTypeString,
					_schemaKeyDescription: "The Python code to execute",
				},
				"timeout": map[string]any{
					_schemaKeyType:        "integer",
					_schemaKeyDescription: "Execution timeout in seconds (max 120)",
					"default":             defaultCodeInterpreterTimeoutSec,
				},
			},
			"required": []any{"code"},
		},
	}
}

// Execute implements tool.Tool.
func (c *CodeInterpreter) Execute(ctx context.Context, args map[string]any) (any, error) {
	code, ok := args["code"].(string)
	if !ok {
		code = ""
	}

	if code == "" {
		return nil, fmt.Errorf("%w", ErrCodeRequired)
	}

	timeout := defaultCodeInterpreterTimeoutSec

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

func (c *CodeInterpreter) executeRemote(_ context.Context, _ string, _ int) (any, error) {
	// Remote sandbox execution (e.g., Pyodide, gVisor, Firecracker) is not yet implemented.
	return nil, fmt.Errorf("%w", ErrRemoteCodeNotImplemented)
}

func (c *CodeInterpreter) executeLocal(_ context.Context, _ string, _ int) (any, error) {
	return map[string]any{
		"stdout":    "",
		"stderr":    "Code interpreter is not configured with a sandbox endpoint.",
		"exit_code": 1,
		"result":    "execution unavailable",
	}, nil
}
