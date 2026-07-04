package temporal

import (
	"slices"
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestDefaultCapabilitiesDeclareNativeRun(t *testing.T) {
	t.Parallel()

	capabilities := DefaultCapabilities()
	if len(capabilities) != 1 {
		t.Fatalf("capability count = %d", len(capabilities))
	}

	got := capabilities[0]
	if got.Backend.Kind != agentos.BackendKindNative ||
		got.Backend.Name != agentos.BackendNameGoAgentNative ||
		got.Name != agentos.CapabilityRun {
		t.Fatalf("unexpected native run capability: %#v", got)
	}

	if !containsSignal(got.Signals, agentoscore.SignalUserMessage) {
		t.Fatalf("native run signals = %#v, want %q", got.Signals, agentoscore.SignalUserMessage)
	}

	for _, control := range []agentoscore.ControlOperation{
		agentoscore.ControlPause,
		agentoscore.ControlResume,
		agentoscore.ControlCancel,
	} {
		if !containsControl(got.Controls, control) {
			t.Fatalf("native run controls = %#v, want %q", got.Controls, control)
		}
	}
}

func TestCapabilitiesWithDefaultsPreservesConfiguredCapabilities(t *testing.T) {
	t.Parallel()

	configured := []agentos.Capability{
		{
			Backend: agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research-http"},
			Name:    "summarize",
		},
	}

	capabilities := CapabilitiesWithDefaults(configured)
	if len(capabilities) != 2 {
		t.Fatalf("capability count = %d", len(capabilities))
	}

	if capabilities[0].Backend.Kind != agentos.BackendKindNative ||
		capabilities[0].Name != agentos.CapabilityRun ||
		capabilities[1].Backend.Name != "research-http" ||
		capabilities[1].Name != "summarize" {
		t.Fatalf("unexpected capability order: %#v", capabilities)
	}
}

func containsSignal(signals []agentoscore.SignalType, want agentoscore.SignalType) bool {
	return slices.Contains(signals, want)
}

func containsControl(controls []agentoscore.ControlOperation, want agentoscore.ControlOperation) bool {
	return slices.Contains(controls, want)
}
