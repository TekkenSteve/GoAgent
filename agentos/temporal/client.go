package temporal

import (
	agentfwconfig "github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"go.temporal.io/sdk/client"
)

func dialTemporalClient(cfg *agentfwconfig.Temporal) (client.Client, error) {
	return client.Dial(client.Options{
		HostPort:  cfg.Address,
		Namespace: cfg.Namespace,
	})
}
