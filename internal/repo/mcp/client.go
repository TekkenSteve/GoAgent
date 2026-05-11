package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	mcpclient "github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// Client connects to a single MCP server via the mcp-go library and provides
// tool discovery + execution.
type Client struct {
	name   string
	cfg    ServerConfig
	client *mcpclient.Client
}

// NewClient creates a new MCP client from a server configuration.
func NewClient(cfg ServerConfig) *Client {
	return &Client{
		name: cfg.Name,
		cfg:  cfg,
	}
}

// Start connects to the MCP server and performs the initialize handshake.
// For stdio transport the subprocess is launched by NewStdioMCPClient;
// for SSE/Streamable HTTP we call Start() explicitly.
func (c *Client) Start(ctx context.Context) error {
	cl, err := c.createClient(ctx)
	if err != nil {
		return fmt.Errorf("mcp client %q: %w", c.name, err)
	}
	c.client = cl

	// Initialize handshake — required by MCP protocol
	initReq := mcp.InitializeRequest{
		Params: mcp.InitializeParams{
			ProtocolVersion: mcp.LATEST_PROTOCOL_VERSION,
			Capabilities:    mcp.ClientCapabilities{},
			ClientInfo: mcp.Implementation{
				Name:    "goagent-mcp",
				Version: "1.0.0",
			},
		},
	}
	if _, err := cl.Initialize(ctx, initReq); err != nil {
		cl.Close()
		return fmt.Errorf("initialize: %w", err)
	}

	return nil
}

func (c *Client) createClient(ctx context.Context) (*mcpclient.Client, error) {
	switch c.cfg.Transport {
	case "stdio":
		cl, err := mcpclient.NewStdioMCPClient(
			c.cfg.Command,
			c.cfg.BuildEnv(),
			c.cfg.Args...,
		)
		if err != nil {
			return nil, fmt.Errorf("stdio: %w", err)
		}
		return cl, nil

	case "sse":
		cl, err := mcpclient.NewSSEMCPClient(c.cfg.URL)
		if err != nil {
			return nil, fmt.Errorf("sse create: %w", err)
		}
		if err := cl.Start(ctx); err != nil {
			cl.Close()
			return nil, fmt.Errorf("sse start: %w", err)
		}
		return cl, nil

	case "streamable-http":
		cl, err := mcpclient.NewStreamableHttpClient(c.cfg.URL)
		if err != nil {
			return nil, fmt.Errorf("streamable-http create: %w", err)
		}
		if err := cl.Start(ctx); err != nil {
			cl.Close()
			return nil, fmt.Errorf("streamable-http start: %w", err)
		}
		return cl, nil

	default:
		return nil, fmt.Errorf("unsupported transport %q", c.cfg.Transport)
	}
}

// ListTools discovers all tools available from this MCP server.
func (c *Client) ListTools(ctx context.Context) ([]entity.ToolDef, error) {
	if c.client == nil {
		return nil, fmt.Errorf("mcp client %q not started", c.name)
	}

	result, err := c.client.ListTools(ctx, mcp.ListToolsRequest{})
	if err != nil {
		return nil, fmt.Errorf("list tools: %w", err)
	}

	defs := make([]entity.ToolDef, 0, len(result.Tools))
	for _, t := range result.Tools {
		defs = append(defs, entity.ToolDef{
			Type: "function",
			Function: entity.ToolFuncDef{
				Name:        t.Name,
				Description: t.Description,
				Parameters:  toolInputSchemaToMap(t.InputSchema),
			},
		})
	}
	return defs, nil
}

// CallTool executes a tool on the MCP server.
func (c *Client) CallTool(ctx context.Context, name string, args map[string]any) (string, error) {
	if c.client == nil {
		return "", fmt.Errorf("mcp client %q not started", c.name)
	}

	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Name:      name,
			Arguments: args,
		},
	}

	result, err := c.client.CallTool(ctx, req)
	if err != nil {
		return "", fmt.Errorf("call tool %q: %w", name, err)
	}

	// Concatenate text content items
	var text string
	for _, content := range result.Content {
		if tc, ok := mcp.AsTextContent(content); ok {
			if text != "" {
				text += "\n"
			}
			text += tc.Text
		}
	}

	if result.IsError {
		return text, fmt.Errorf("mcp tool %q returned error: %s", name, text)
	}

	return text, nil
}

// Close shuts down the MCP server connection.
func (c *Client) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// toolInputSchemaToMap converts mcp.ToolInputSchema to the generic map format
// expected by entity.ToolFuncDef.Parameters, using JSON round-trip to preserve
// all schema fields ($defs, additionalProperties, etc.).
func toolInputSchemaToMap(schema mcp.ToolInputSchema) map[string]any {
	data, err := json.Marshal(schema)
	if err != nil {
		return map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		}
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		}
	}
	return m
}
