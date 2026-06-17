package agentosplan

import (
	"context"
	"fmt"
	"sync"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// StaticCapabilityCatalog is an in-memory capability catalog for tests,
// embedded demos, and WorkerKit assembly.
type StaticCapabilityCatalog struct {
	mu           sync.RWMutex
	capabilities map[capabilityKey]agentos.Capability
}

type capabilityKey struct {
	backend agentos.BackendRef
	name    string
}

// NewStaticCapabilityCatalog creates a catalog from capabilities.
func NewStaticCapabilityCatalog(capabilities []agentos.Capability) (*StaticCapabilityCatalog, error) {
	catalog := &StaticCapabilityCatalog{capabilities: make(map[capabilityKey]agentos.Capability)}
	for _, capability := range capabilities {
		if err := catalog.Register(capability); err != nil {
			return nil, err
		}
	}

	return catalog, nil
}

// Register adds or replaces one capability.
func (c *StaticCapabilityCatalog) Register(capability agentos.Capability) error {
	if capability.Backend.Kind == "" || capability.Backend.Name == "" {
		return fmt.Errorf("%w: capability backend is required", agentos.ErrInvalidBackendRef)
	}
	if capability.Name == "" {
		return fmt.Errorf("%w: capability name is required", agentos.ErrCapabilityNotFound)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.capabilities[capabilityKey{backend: capability.Backend, name: capability.Name}] = capability

	return nil
}

// GetCapability returns a registered capability.
func (c *StaticCapabilityCatalog) GetCapability(_ context.Context, backend agentos.BackendRef, name string) (agentos.Capability, bool, error) {
	if c == nil {
		return agentos.Capability{}, false, nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	capability, ok := c.capabilities[capabilityKey{backend: backend, name: name}]

	return capability, ok, nil
}
