package runtime

import (
	"errors"
	"fmt"

	agentfwconfig "github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

var errTemporalConfigRequired = errors.New("agentfw runtime - temporal config is required")

// TemporalRuntime wires Temporal client and worker for the agent framework.
type TemporalRuntime struct {
	Client     client.Client
	Workers    map[string]worker.Worker
	TaskQueues agentfwconfig.TaskQueues
}

// NewTemporalRuntime creates a Temporal client and worker using framework config.
func NewTemporalRuntime(cfg *agentfwconfig.Temporal) (*TemporalRuntime, error) {
	if cfg == nil {
		return nil, errTemporalConfigRequired
	}

	c, err := client.Dial(client.Options{
		HostPort:  cfg.Address,
		Namespace: cfg.Namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("agentfw runtime - temporal client dial: %w", err)
	}

	queues := cfg.TaskQueues
	if err := queues.Validate(); err != nil {
		c.Close()

		return nil, err
	}

	workerOptions := worker.Options{
		MaxConcurrentWorkflowTaskPollers:   cfg.MaxConcurrentWorkflowTaskPollers,
		MaxConcurrentActivityTaskPollers:   cfg.MaxConcurrentActivityTaskPollers,
		MaxConcurrentActivityExecutionSize: cfg.MaxConcurrentActivityExecution,
	}

	workers := make(map[string]worker.Worker)
	for _, queue := range queues.QueueNames() {
		workers[queue] = worker.New(c, queue, workerOptions)
	}

	return &TemporalRuntime{
		Client:     c,
		Workers:    workers,
		TaskQueues: queues,
	}, nil
}

// WorkerFor returns the worker polling one task queue.
func (r *TemporalRuntime) WorkerFor(taskQueue string) (worker.Worker, bool) {
	if r == nil || taskQueue == "" {
		return nil, false
	}

	w, ok := r.Workers[taskQueue]

	return w, ok
}

// Close closes the Temporal client. The worker should be stopped by caller.
func (r *TemporalRuntime) Close() {
	if r == nil || r.Client == nil {
		return
	}

	r.Client.Close()
}
