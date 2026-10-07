package temporal

import (
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentfwconfig "github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"github.com/stretchr/testify/require"
)

// testAgentFWTaskQueues renders the framework's production queue split as the
// runtime's own type, so the mapping under test sees a valid configuration.
func testAgentFWTaskQueues() TaskQueues {
	queues := agentfwconfig.DefaultTaskQueues()

	return TaskQueues{
		PlanControl:     queues.PlanControl,
		PlanActivity:    queues.PlanActivity,
		ProcessControl:  queues.ProcessControl,
		ProcessActivity: queues.ProcessActivity,
		NativeControl:   queues.NativeControl,
		NativeLLM:       queues.NativeLLM,
		NativeTool:      queues.NativeTool,
		Stream:          queues.Stream,
		Nexus:           queues.Nexus,
	}
}

// TestNexusEndpointForResolvesPeers locks the addressing rule the whole peer
// feature rests on: a node naming a town goes to that town's endpoint, a node
// naming none stays local, and a peer without a mapping is refused rather than
// silently run in the wrong town.
func TestNexusEndpointForResolvesPeers(t *testing.T) {
	t.Parallel()

	peers := map[string]string{"townb": "agentos-townb", "townc": "agentos-townc"}

	tests := []struct {
		name     string
		peer     string
		expected string
		wantErr  bool
	}{
		{name: "local node uses the deployment endpoint", peer: "", expected: "agentos"},
		{name: "peer node uses its town's endpoint", peer: "townb", expected: "agentos-townb"},
		{name: "another peer resolves independently", peer: "townc", expected: "agentos-townc"},
		{name: "an unmapped peer is refused", peer: "townd", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			endpoint, err := nexusEndpointFor(peers, "agentos", tt.peer)
			if tt.wantErr {
				require.Error(t, err)
				require.Empty(t, endpoint, "a refused peer must not fall back to a local run")

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.expected, endpoint)
		})
	}
}

// TestValidatePlanNodePeersRejectsUnknownPeer locks the fail-fast half: a plan
// that names a town this deployment cannot reach is refused before any node
// starts, instead of halfway through the plan's work.
func TestValidatePlanNodePeersRejectsUnknownPeer(t *testing.T) {
	t.Parallel()

	peers := map[string]string{"townb": "agentos-townb"}

	spec := &agentos.RunPlanSpec{
		PlanID: "plan-peers",
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "local"},
			{NodeID: "remote", Peer: "townb"},
		},
	}

	require.NoError(t, validatePlanNodePeers(spec, peers))

	spec.Nodes = append(spec.Nodes, agentos.PlanNodeSpec{NodeID: "unknown", Peer: "townz"})

	err := validatePlanNodePeers(spec, peers)
	require.ErrorIs(t, err, agentoscore.ErrInvalidRunPlan)
	require.Contains(t, err.Error(), `node "unknown"`)
	require.Contains(t, err.Error(), "townz")
}

// TestTemporalConfigCarriesNexusPeers locks the mapping's path into the
// framework config, which is where the plan runtime reads it from.
func TestTemporalConfigCarriesNexusPeers(t *testing.T) {
	t.Parallel()

	peers := map[string]string{"townb": "agentos-townb"}

	config := temporalConfig(&RuntimeConfig{
		TemporalAddress:    "temporal:7233",
		TemporalTaskQueues: testAgentFWTaskQueues(),
		NexusEndpoint:      "agentos-local",
		NexusPeers:         peers,
	})

	require.Equal(t, "agentos-local", config.NexusEndpoint)
	require.Equal(t, peers, config.NexusPeers)

	// A deployment with no peers keeps the framework's nil map, so nothing
	// downstream has to distinguish "empty" from "absent".
	empty := temporalConfig(&RuntimeConfig{TemporalAddress: "temporal:7233"})
	require.Empty(t, empty.NexusPeers)
}
