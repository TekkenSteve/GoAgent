package config

import (
	"errors"
	"fmt"
	"time"
)

const (
	defaultMaxConcurrentWorkflowTaskPollers = 2
	defaultMaxConcurrentActivityTaskPollers = 2
	defaultMaxConcurrentActivityExecution   = 100
	defaultMaxSteps                         = 100
	defaultContinueAsNewStepThreshold       = 80
	defaultContinueAsNewHistoryThreshold    = 10000
	defaultContinueAsNewStateSizeThreshold  = 512 * 1024 // 512 KiB
	defaultContinueAsNewWallClockThreshold  = 50 * time.Minute
	defaultContinueAsNewMaxContinuations    = 1000
)

// Config contains runtime settings for the agent framework.
type Config struct {
	Enabled bool

	Temporal Temporal
	Runtime  Runtime
}

// Temporal config controls SDK client/worker bootstrap settings.
type Temporal struct {
	Address    string
	Namespace  string
	TaskQueues TaskQueues

	MaxConcurrentWorkflowTaskPollers int
	MaxConcurrentActivityTaskPollers int
	MaxConcurrentActivityExecution   int
}

// TaskQueues names the Temporal queues used by each workload class.
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

var ErrTemporalTaskQueuesInvalid = errors.New("agentfw temporal task queues: invalid")

// DefaultTaskQueues returns the production-oriented AgentOS queue split.
func DefaultTaskQueues() TaskQueues {
	return TaskQueues{
		PlanControl:     "agentos-plan-control",
		PlanActivity:    "agentos-plan-activity",
		ProcessControl:  "agentos-process-control",
		ProcessActivity: "agentos-process-activity",
		NativeControl:   "agentfw-native-control",
		NativeLLM:       "agentfw-native-llm",
		NativeTool:      "agentfw-native-tool",
		Stream:          "agentfw-stream",
	}
}

// QueueNames returns unique queue names in a stable worker startup order.
func (q *TaskQueues) QueueNames() []string {
	if q == nil {
		return nil
	}

	ordered := []string{
		q.PlanControl,
		q.PlanActivity,
		q.ProcessControl,
		q.ProcessActivity,
		q.NativeControl,
		q.NativeLLM,
		q.NativeTool,
		q.Stream,
	}

	seen := make(map[string]struct{}, len(ordered))

	names := make([]string, 0, len(ordered))
	for _, name := range ordered {
		if name == "" {
			continue
		}

		if _, ok := seen[name]; ok {
			continue
		}

		seen[name] = struct{}{}
		names = append(names, name)
	}

	return names
}

// Validate ensures every workload class has an explicit, distinct queue.
func (q *TaskQueues) Validate() error {
	if q == nil {
		return fmt.Errorf("%w: task queues are required", ErrTemporalTaskQueuesInvalid)
	}

	fields := []struct {
		label string
		value string
	}{
		{label: "plan control", value: q.PlanControl},
		{label: "plan activity", value: q.PlanActivity},
		{label: "process control", value: q.ProcessControl},
		{label: "process activity", value: q.ProcessActivity},
		{label: "native control", value: q.NativeControl},
		{label: "native llm", value: q.NativeLLM},
		{label: "native tool", value: q.NativeTool},
		{label: "stream", value: q.Stream},
	}

	seen := make(map[string]string, len(fields))
	for _, field := range fields {
		if field.value == "" {
			return fmt.Errorf("%w: %s task queue is required", ErrTemporalTaskQueuesInvalid, field.label)
		}

		if existing, ok := seen[field.value]; ok {
			return fmt.Errorf("%w: %s and %s use the same task queue %q", ErrTemporalTaskQueuesInvalid, existing, field.label, field.value)
		}

		seen[field.value] = field.label
	}

	return nil
}

// Runtime config controls orchestration behavior outside Temporal server settings.
type Runtime struct {
	DefaultModelRef string

	WorkflowVersion int
	EventSchemaVer  string
	ToolSchemaVer   string
	AgentConfigVer  string

	MaxSteps             int32
	MaxWallClockDuration time.Duration

	ContinueAsNewStepThreshold          int32
	ContinueAsNewHistoryThreshold       int
	ContinueAsNewStateSizeThresholdByte int
	ContinueAsNewWallClockThreshold     time.Duration
	ContinueAsNewMaxContinuations       int32
}

// Default returns a conservative baseline configuration.
func Default() Config {
	return Config{
		Enabled: false,
		Temporal: Temporal{
			Address:    "127.0.0.1:7233",
			Namespace:  "default",
			TaskQueues: DefaultTaskQueues(),

			MaxConcurrentWorkflowTaskPollers: defaultMaxConcurrentWorkflowTaskPollers,
			MaxConcurrentActivityTaskPollers: defaultMaxConcurrentActivityTaskPollers,
			MaxConcurrentActivityExecution:   defaultMaxConcurrentActivityExecution,
		},
		Runtime: Runtime{
			DefaultModelRef: "gpt-4.1-mini",

			WorkflowVersion: 1,
			EventSchemaVer:  "v1",
			ToolSchemaVer:   "v1",
			AgentConfigVer:  "v1",

			MaxSteps:                            defaultMaxSteps,
			MaxWallClockDuration:                time.Hour,
			ContinueAsNewStepThreshold:          defaultContinueAsNewStepThreshold,
			ContinueAsNewHistoryThreshold:       defaultContinueAsNewHistoryThreshold,
			ContinueAsNewStateSizeThresholdByte: defaultContinueAsNewStateSizeThreshold,
			ContinueAsNewWallClockThreshold:     defaultContinueAsNewWallClockThreshold,
			ContinueAsNewMaxContinuations:       defaultContinueAsNewMaxContinuations,
		},
	}
}
