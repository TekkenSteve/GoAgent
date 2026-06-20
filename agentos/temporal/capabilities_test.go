package temporal

import (
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestDefaultCapabilitiesDeclareNativeRun(t *testing.T) {
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
	if !containsSignal(got.Signals, agentos.SignalUserMessage) {
		t.Fatalf("native run signals = %#v, want %q", got.Signals, agentos.SignalUserMessage)
	}
	for _, control := range []agentos.ControlOperation{
		agentos.ControlPause,
		agentos.ControlResume,
		agentos.ControlCancel,
	} {
		if !containsControl(got.Controls, control) {
			t.Fatalf("native run controls = %#v, want %q", got.Controls, control)
		}
	}
}

func TestCapabilitiesWithDefaultsPreservesConfiguredCapabilities(t *testing.T) {
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

func containsSignal(signals []agentos.SignalType, want agentos.SignalType) bool {
	for _, signal := range signals {
		if signal == want {
			return true
		}
	}

	return false
}

func containsControl(controls []agentos.ControlOperation, want agentos.ControlOperation) bool {
	for _, control := range controls {
		if control == want {
			return true
		}
	}

	return false
}
