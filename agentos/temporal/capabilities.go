package temporal

import "github.com/TekkenSteve/GoAgent/agentos"

// DefaultCapabilities returns capability declarations owned by this default
// Temporal-backed AgentOS implementation.
func DefaultCapabilities() []agentos.Capability {
	return []agentos.Capability{
		NativeRunCapability(),
	}
}

// CapabilitiesWithDefaults prepends this implementation's built-in
// capabilities to host-configured backend declarations.
func CapabilitiesWithDefaults(configured []agentos.Capability) []agentos.Capability {
	defaults := DefaultCapabilities()
	capabilities := make([]agentos.Capability, 0, len(defaults)+len(configured))
	capabilities = append(capabilities, defaults...)
	capabilities = append(capabilities, configured...)

	return capabilities
}

// NativeRunCapability declares the built-in GoAgent native child-run backend.
func NativeRunCapability() agentos.Capability {
	return agentos.Capability{
		Backend: agentos.BackendRef{
			Kind: agentos.BackendKindNative,
			Name: agentos.BackendNameGoAgentNative,
		},
		Name:        agentos.CapabilityRun,
		Description: "Start, signal, and control a GoAgent native agent run.",
		Signals: []agentos.SignalType{
			agentos.SignalUserMessage,
		},
		Controls: []agentos.ControlOperation{
			agentos.ControlPause,
			agentos.ControlResume,
			agentos.ControlCancel,
		},
	}
}
