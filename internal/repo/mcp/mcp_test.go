package mcp_test

import (
	"testing"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo/mcp"
)

func TestServerConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     mcp.ServerConfig
		wantErr bool
	}{
		{name: "empty name", cfg: mcp.ServerConfig{Transport: "stdio", Command: "/bin/echo"}, wantErr: true},
		{name: "valid stdio", cfg: mcp.ServerConfig{Name: "test", Transport: "stdio", Command: "/bin/echo"}, wantErr: false},
		{name: "stdio missing command", cfg: mcp.ServerConfig{Name: "test", Transport: "stdio"}, wantErr: true},
		{name: "valid sse", cfg: mcp.ServerConfig{Name: "test", Transport: "sse", URL: "http://localhost:8080/mcp"}, wantErr: false},
		{name: "sse missing url", cfg: mcp.ServerConfig{Name: "test", Transport: "sse"}, wantErr: true},
		{name: "valid streamable-http", cfg: mcp.ServerConfig{Name: "test", Transport: "streamable-http", URL: "http://localhost:8080/mcp"}, wantErr: false},
		{name: "streamable-http missing url", cfg: mcp.ServerConfig{Name: "test", Transport: "streamable-http"}, wantErr: true},
		{name: "invalid transport", cfg: mcp.ServerConfig{Name: "test", Transport: "foo"}, wantErr: true},
		{name: "empty transport", cfg: mcp.ServerConfig{Name: "test"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestManager_Definitions_Empty(t *testing.T) {
	t.Parallel()

	mgr := mcp.NewManager()

	defs := mgr.Definitions()
	if len(defs) != 0 {
		t.Errorf("expected 0 definitions, got %d", len(defs))
	}
}

func TestManager_RegisteredTools_Empty(t *testing.T) {
	t.Parallel()

	mgr := mcp.NewManager()

	tools := mgr.RegisteredTools()
	if len(tools) != 0 {
		t.Errorf("expected 0 registered tools, got %d", len(tools))
	}
}

func TestMCPServerConfigEntity(t *testing.T) {
	t.Parallel()

	cfg := entity.MCPServerConfig{
		Name:      "test",
		Transport: "stdio",
		Command:   "/bin/echo",
		Args:      []string{"hello"},
	}
	if cfg.Name != "test" {
		t.Errorf("expected Name 'test', got %q", cfg.Name)
	}

	if cfg.Transport != "stdio" {
		t.Errorf("expected Transport 'stdio', got %q", cfg.Transport)
	}
}
