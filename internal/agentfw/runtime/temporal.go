package runtime

import (
	"fmt"

	agentfwconfig "github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// TemporalRuntime wires Temporal client and worker for the agent framework.
type TemporalRuntime struct {
	Client client.Client
	Worker worker.Worker
}

// NewTemporalRuntime creates a Temporal client and worker using framework config.
func NewTemporalRuntime(cfg agentfwconfig.Temporal) (*TemporalRuntime, error) {
	c, err := client.Dial(client.Options{
		HostPort:  cfg.Address,
		Namespace: cfg.Namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("agentfw runtime - temporal client dial: %w", err)
	}

	w := worker.New(c, cfg.TaskQueue, worker.Options{
		MaxConcurrentWorkflowTaskPollers:   cfg.MaxConcurrentWorkflowTaskPollers,
		MaxConcurrentActivityTaskPollers:   cfg.MaxConcurrentActivityTaskPollers,
		MaxConcurrentActivityExecutionSize: cfg.MaxConcurrentActivityExecution,
	})

	return &TemporalRuntime{
		Client: c,
		Worker: w,
	}, nil
}

// Close closes the Temporal client. The worker should be stopped by caller.
func (r *TemporalRuntime) Close() {
	if r == nil || r.Client == nil {
		return
	}
	r.Client.Close()
}
