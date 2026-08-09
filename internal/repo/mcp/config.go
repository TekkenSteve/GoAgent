package mcp

import (
	"errors"
	"fmt"
	"os"
)

const (
	// TransportStdio selects the stdio MCP transport, which launches the server as a subprocess.
	TransportStdio = "stdio"
	// TransportSSE selects the SSE MCP transport.
	TransportSSE = "sse"
	// TransportStreamableHTTP selects the streamable HTTP MCP transport.
	TransportStreamableHTTP = "streamable-http"
)

var (
	// ErrMCPServerNameRequired is returned when the MCP server name is empty.
	ErrMCPServerNameRequired = errors.New("mcp server name is required")
	// ErrMCPCommandRequired is returned when the command is missing for the stdio transport.
	ErrMCPCommandRequired = errors.New("command is required for stdio transport")
	// ErrMCPURLRequired is returned when the URL is missing for the SSE transport.
	ErrMCPURLRequired = errors.New("url is required for sse transport")
	// ErrMCPSHAURLRequired is returned when the URL is missing for the streamable-http transport.
	ErrMCPSHAURLRequired = errors.New("url is required for streamable-http transport")
	// ErrMCPTransportRequired is returned when no transport type is configured.
	ErrMCPTransportRequired = errors.New("transport type is required")
	// ErrMCPServerUnsupportedTransport is returned when the configured transport type is not supported.
	ErrMCPServerUnsupportedTransport = errors.New("unsupported transport")
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
func (c *ServerConfig) Validate() error {
	if c.Name == "" {
		return ErrMCPServerNameRequired
	}

	switch c.Transport {
	case TransportStdio:
		if c.Command == "" {
			return fmt.Errorf("%w: %q", ErrMCPCommandRequired, c.Name)
		}
	case TransportSSE:
		if c.URL == "" {
			return fmt.Errorf("%w: %q", ErrMCPURLRequired, c.Name)
		}
	case TransportStreamableHTTP:
		if c.URL == "" {
			return fmt.Errorf("%w: %q", ErrMCPSHAURLRequired, c.Name)
		}
	case "":
		return fmt.Errorf("%w: %q", ErrMCPTransportRequired, c.Name)
	default:
		return fmt.Errorf("%w: %q: %q", ErrMCPServerUnsupportedTransport, c.Name, c.Transport)
	}

	return nil
}

// BuildEnv merges the server's Env map with the current process environment.
// Server-specific env vars override process-level ones.
func (c *ServerConfig) BuildEnv() []string {
	merged := os.Environ()
	for k, v := range c.Env {
		merged = append(merged, fmt.Sprintf("%s=%s", k, v))
	}

	return merged
}
