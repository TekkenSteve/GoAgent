package agentosplan

import (
	"context"
	"fmt"
	"sync"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// StaticCapabilityCatalog is an in-memory capability catalog for tests, CLI
// validation, and embedded demos.
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
		if _, _, err := catalog.RegisterCapability(context.Background(), capability, ""); err != nil {
			return nil, err
		}
	}

	return catalog, nil
}

// RegisterCapability adds or replaces one capability.
func (c *StaticCapabilityCatalog) RegisterCapability(_ context.Context, capability agentos.Capability, _ string) (agentos.Capability, bool, error) {
	if c == nil {
		return agentos.Capability{}, false, fmt.Errorf("%w: capability catalog is nil", agentos.ErrCapabilityNotFound)
	}
	if err := ValidateCapability(capability); err != nil {
		return agentos.Capability{}, false, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.capabilities[capabilityKey{backend: capability.Backend, name: capability.Name}] = capability

	return capability, true, nil
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

var (
	_ CapabilityCatalog  = (*StaticCapabilityCatalog)(nil)
	_ CapabilityRegistry = (*StaticCapabilityCatalog)(nil)
)
