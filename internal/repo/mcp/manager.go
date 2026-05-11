package mcp

import (
	"context"
	"fmt"
	"sync"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo/toolkit"
)

// mcpToolEntry tracks a tool discovered from an MCP server.
type mcpToolEntry struct {
	serverName string
	toolName   string
	def        entity.ToolDef
}

// Manager manages MCP server configurations and provides JIT connection,
// tool discovery, and execution routing.
//
// Servers are registered via RegisterServer (no connection), then connected
// on demand via Connect/ConnectDynamic during Prep. Discovered tools are
// cached for the lifetime of the connection. Execution routes through mcpTool
// wrappers registered with the toolkit registry.
type Manager struct {
	mu       sync.RWMutex
	cfgs     map[string]ServerConfig // registered server configs (not connected)
	servers  map[string]*Client      // active connections (JIT established)
	toolMap  map[string]mcpToolEntry
	registry *toolkit.ToolRegistry // optional: auto-register tools on connect → server binding
}

// NewManager creates an empty MCP manager with no connections.
func NewManager() *Manager {
	return &Manager{
		cfgs:    make(map[string]ServerConfig),
		servers: make(map[string]*Client),
		toolMap: make(map[string]mcpToolEntry),
	}
}

// SetRegistry sets the toolkit registry for automatic tool registration on JIT connect.
// When set, each Connect/ConnectDynamic call auto-registers discovered tools.
func (m *Manager) SetRegistry(registry *toolkit.ToolRegistry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Auto-register any already-discovered tools
	for _, entry := range m.toolMap {
		t := &mcpTool{
			meta: toolkit.ToolMeta{
				Name:        entry.def.Function.Name,
				Description: entry.def.Function.Description,
				Parameters:  toParamsMap(entry.def.Function.Parameters),
			},
			mgr: m,
		}
		_ = registry.Register(t) // skip duplicates
	}
}

// RegisterServer stores a server configuration for later JIT connection.
// No connection is made until Connect() or ConnectDynamic() is called.
func (m *Manager) RegisterServer(cfg ServerConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfgs[cfg.Name] = cfg
	return nil
}

// Connect JIT-connects a previously registered server by name, discovers
// its tools, and returns the tool definitions. If already connected, returns
// cached definitions without reconnecting.
func (m *Manager) Connect(ctx context.Context, name string) ([]entity.ToolDef, error) {
	m.mu.RLock()
	_, hasClient := m.servers[name]
	cfg, hasCfg := m.cfgs[name]
	m.mu.RUnlock()

	if hasClient {
		// Already connected — return cached tool defs
		return m.serverToolDefs(name), nil
	}
	if !hasCfg {
		return nil, fmt.Errorf("mcp server %q not registered", name)
	}

	return m.connectLocked(ctx, cfg)
}

// ConnectDynamic JIT-connects an ad-hoc server (from agent/template config),
// discovers its tools, and returns the tool definitions. The server config
// is also cached for future connections by name.
func (m *Manager) ConnectDynamic(ctx context.Context, cfg ServerConfig) ([]entity.ToolDef, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	_, hasClient := m.servers[cfg.Name]
	m.mu.RUnlock()

	if hasClient {
		return m.serverToolDefs(cfg.Name), nil
	}

	// Also register for future lookups
	m.mu.Lock()
	m.cfgs[cfg.Name] = cfg
	m.mu.Unlock()

	return m.connectLocked(ctx, cfg)
}

// connectLocked performs the actual JIT connection (caller must NOT hold write lock).
func (m *Manager) connectLocked(ctx context.Context, cfg ServerConfig) ([]entity.ToolDef, error) {
	client := NewClient(cfg)
	if err := client.Start(ctx); err != nil {
		return nil, fmt.Errorf("connect mcp server %q: %w", cfg.Name, err)
	}

	tools, err := client.ListTools(ctx)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("discover tools from %q: %w", cfg.Name, err)
	}

	m.mu.Lock()
	m.servers[cfg.Name] = client
	for _, def := range tools {
		name := def.Function.Name
		if _, exists := m.toolMap[name]; exists {
			continue
		}
		m.toolMap[name] = mcpToolEntry{
			serverName: cfg.Name,
			toolName:   name,
			def:        def,
		}
		// Auto-register with toolkit registry if set
		if m.registry != nil {
			_ = m.registry.Register(&mcpTool{
				meta: toolkit.ToolMeta{
					Name:        def.Function.Name,
					Description: def.Function.Description,
					Parameters:  toParamsMap(def.Function.Parameters),
				},
				mgr: m,
			})
		}
	}
	m.mu.Unlock()

	return tools, nil
}

// serverToolDefs returns tool definitions for a specific server (caller holds read lock).
func (m *Manager) serverToolDefs(name string) []entity.ToolDef {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var defs []entity.ToolDef
	for _, entry := range m.toolMap {
		if entry.serverName == name {
			defs = append(defs, entry.def)
		}
	}
	return defs
}

// EnsureConnected checks all given server configs, JIT-connects any that
// aren't yet connected, and returns the combined tool definitions.
// This is the main entry point for PrepMCP.
func (m *Manager) EnsureConnected(ctx context.Context, configs []entity.MCPServerConfig) ([]entity.ToolDef, []string) {
	var allTools []entity.ToolDef
	var errors []string

	for _, ecfg := range configs {
		cfg := ServerConfig{
			Name:      ecfg.Name,
			Transport: ecfg.Transport,
			Command:   ecfg.Command,
			Args:      ecfg.Args,
			Env:       ecfg.Env,
			URL:       ecfg.URL,
		}

		// Check if this server is already registered
		m.mu.RLock()
		_, registered := m.cfgs[cfg.Name]
		m.mu.RUnlock()

		var tools []entity.ToolDef
		var err error

		if registered {
			tools, err = m.Connect(ctx, cfg.Name)
		} else {
			tools, err = m.ConnectDynamic(ctx, cfg)
		}

		if err != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", cfg.Name, err))
			continue
		}
		allTools = append(allTools, tools...)
	}

	return allTools, errors
}

// Definitions returns all discovered MCP tool definitions from all connected servers.
func (m *Manager) Definitions() []entity.ToolDef {
	m.mu.RLock()
	defer m.mu.RUnlock()

	defs := make([]entity.ToolDef, 0, len(m.toolMap))
	for _, entry := range m.toolMap {
		defs = append(defs, entry.def)
	}
	return defs
}

// RemoveServer disconnects and removes an MCP server.
func (m *Manager) RemoveServer(ctx context.Context, name string) error {
	m.mu.Lock()
	client, ok := m.servers[name]
	if ok {
		delete(m.servers, name)
	}
	delete(m.cfgs, name)

	// Remove all tools from this server
	for toolName, entry := range m.toolMap {
		if entry.serverName == name {
			delete(m.toolMap, toolName)
		}
	}
	m.mu.Unlock()

	if ok {
		return client.Close()
	}
	return nil
}

// Close shuts down all MCP server connections.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for name, client := range m.servers {
		if err := client.Close(); err != nil {
			lastErr = fmt.Errorf("close mcp server %q: %w", name, err)
		}
	}
	m.servers = make(map[string]*Client)
	m.cfgs = make(map[string]ServerConfig)
	m.toolMap = make(map[string]mcpToolEntry)

	return lastErr
}

// ListServerTools returns tool definitions grouped by server name.
func (m *Manager) ListServerTools() map[string][]entity.ToolDef {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string][]entity.ToolDef)
	for _, entry := range m.toolMap {
		result[entry.serverName] = append(result[entry.serverName], entry.def)
	}
	return result
}

// RegisteredTools returns a map of tool name to server name for lookup.
func (m *Manager) RegisteredTools() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]string, len(m.toolMap))
	for name, entry := range m.toolMap {
		result[name] = entry.serverName
	}
	return result
}

// --- toolkit.Tool implementation for execution routing ---

// mcpTool wraps an MCP tool reference to implement the toolkit.Tool interface.
type mcpTool struct {
	meta toolkit.ToolMeta
	mgr  *Manager
}

// Meta returns the tool's metadata for LLM function calling.
func (t *mcpTool) Meta() toolkit.ToolMeta {
	return t.meta
}

// Execute routes the tool call to the appropriate MCP server.
// If the server is not connected, returns an error — Prep should have
// established the connection via EnsureConnected.
func (t *mcpTool) Execute(ctx context.Context, args map[string]any) (any, error) {
	t.mgr.mu.RLock()
	entry, ok := t.mgr.toolMap[t.meta.Name]
	t.mgr.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("mcp tool %q not found", t.meta.Name)
	}

	t.mgr.mu.RLock()
	client, ok := t.mgr.servers[entry.serverName]
	t.mgr.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("mcp server %q for tool %q not connected — Prep should have established connection", entry.serverName, t.meta.Name)
	}

	output, err := client.CallTool(ctx, entry.toolName, args)
	if err != nil {
		return nil, fmt.Errorf("mcp tool %q: %w", t.meta.Name, err)
	}

	return map[string]interface{}{"result": output}, nil
}

// toParamsMap converts interface{} to map[string]interface{} for ToolMeta.Parameters.
func toParamsMap(v interface{}) map[string]interface{} {
	if m, ok := v.(map[string]interface{}); ok {
		return m
	}
	return map[string]interface{}{
		"type":       "object",
		"properties": map[string]interface{}{},
	}
}

// RegisterToolsWithRegistry creates toolkit.Tool wrappers for all discovered
// MCP tools and registers them with the given toolkit registry.
// Called after Connect/EnsureConnected to make MCP tools available for execution.
func (m *Manager) RegisterToolsWithRegistry(registry *toolkit.ToolRegistry) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, entry := range m.toolMap {
		t := &mcpTool{
			meta: toolkit.ToolMeta{
				Name:        entry.def.Function.Name,
				Description: entry.def.Function.Description,
				Parameters:  toParamsMap(entry.def.Function.Parameters),
			},
			mgr: m,
		}
		if err := registry.Register(t); err != nil {
			// Skip duplicate registration errors (tool already registered)
			if err.Error() != fmt.Sprintf("tool %q already registered", entry.def.Function.Name) {
				return fmt.Errorf("register mcp tool %q: %w", entry.def.Function.Name, err)
			}
		}
	}
	return nil
}
