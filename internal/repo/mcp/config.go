package mcp

import (
	"fmt"
	"os"
)

// ServerConfig defines how to connect to an MCP server.
type ServerConfig struct {
	// Name is a unique identifier for this MCP server.
	Name string `json:"name" yaml:"name"`

	// Transport is the transport type: "stdio" or "sse".
	Transport string `json:"transport" yaml:"transport"`

	// Command is the executable path for stdio transport.
	Command string `json:"command,omitempty" yaml:"command,omitempty"`

	// Args are command-line arguments for stdio transport.
	Args []string `json:"args,omitempty" yaml:"args,omitempty"`

	// Env is additional environment variables for stdio transport.
	Env map[string]string `json:"env,omitempty" yaml:"env,omitempty"`

	// URL is the server URL for SSE transport.
	URL string `json:"url,omitempty" yaml:"url,omitempty"`
}

// Validate checks that the server configuration is valid.
func (c ServerConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("mcp server name is required")
	}
	switch c.Transport {
	case "stdio":
		if c.Command == "" {
			return fmt.Errorf("mcp server %q: command is required for stdio transport", c.Name)
		}
	case "sse":
		if c.URL == "" {
			return fmt.Errorf("mcp server %q: url is required for sse transport", c.Name)
		}
	case "streamable-http":
		if c.URL == "" {
			return fmt.Errorf("mcp server %q: url is required for streamable-http transport", c.Name)
		}
	case "":
		return fmt.Errorf("mcp server %q: transport type is required (stdio, sse, streamable-http)", c.Name)
	default:
		return fmt.Errorf("mcp server %q: unsupported transport %q", c.Name, c.Transport)
	}
	return nil
}

// BuildEnv merges the server's Env map with the current process environment.
// Server-specific env vars override process-level ones.
func (c ServerConfig) BuildEnv() []string {
	merged := os.Environ()
	for k, v := range c.Env {
		merged = append(merged, fmt.Sprintf("%s=%s", k, v))
	}
	return merged
}
