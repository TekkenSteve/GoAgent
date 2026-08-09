package temporal

import (
	"errors"
	"fmt"

	agentfwconfig "github.com/TekkenSteve/GoAgent/internal/agentfw/config"
)

// TaskQueues names the Temporal queues used by the default AgentOS Temporal implementation.
type TaskQueues struct {
	PlanControl     string
	PlanActivity    string
	ProcessControl  string
	ProcessActivity string
	NativeControl   string
	NativeLLM       string
	NativeTool      string
	Stream          string
}

// ErrTemporalTaskQueuesInvalid reports missing or invalid Temporal task queues.
var ErrTemporalTaskQueuesInvalid = errors.New("agentos temporal task queues: invalid")

// DefaultTaskQueues returns the production-oriented AgentOS queue split.
func DefaultTaskQueues() TaskQueues {
	defaults := agentfwconfig.DefaultTaskQueues()

	return taskQueuesFromAgentFW(&defaults)
}

// Validate ensures every workload class has an explicit, distinct queue.
func (q *TaskQueues) Validate() error {
	if q == nil {
		return fmt.Errorf("%w: task queues are required", ErrTemporalTaskQueuesInvalid)
	}

	agentFWQueues := q.agentFWTaskQueues()
	if err := agentFWQueues.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrTemporalTaskQueuesInvalid, err)
	}

	return nil
}

func (q *TaskQueues) agentFWTaskQueues() agentfwconfig.TaskQueues {
	if q == nil {
		return agentfwconfig.TaskQueues{}
	}

	return agentfwconfig.TaskQueues{
		PlanControl:     q.PlanControl,
		PlanActivity:    q.PlanActivity,
		ProcessControl:  q.ProcessControl,
		ProcessActivity: q.ProcessActivity,
		NativeControl:   q.NativeControl,
		NativeLLM:       q.NativeLLM,
		NativeTool:      q.NativeTool,
		Stream:          q.Stream,
	}
}

func taskQueuesFromAgentFW(q *agentfwconfig.TaskQueues) TaskQueues {
	if q == nil {
		return TaskQueues{}
	}

	return TaskQueues{
		PlanControl:     q.PlanControl,
		PlanActivity:    q.PlanActivity,
		ProcessControl:  q.ProcessControl,
		ProcessActivity: q.ProcessActivity,
		NativeControl:   q.NativeControl,
		NativeLLM:       q.NativeLLM,
		NativeTool:      q.NativeTool,
		Stream:          q.Stream,
	}
}
