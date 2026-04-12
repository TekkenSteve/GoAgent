package config

import "time"

// Config contains runtime settings for the agent framework.
type Config struct {
	Enabled bool

	Temporal Temporal
	Rollout  Rollout
	Runtime  Runtime
}

// Temporal config controls SDK client/worker bootstrap settings.
type Temporal struct {
	Address   string
	Namespace string
	TaskQueue string

	MaxConcurrentWorkflowTaskPollers int
	MaxConcurrentActivityTaskPollers int
	MaxConcurrentActivityExecution   int
}

// Rollout controls gradual enablement and rollback between legacy and Temporal paths.
type Rollout struct {
	Mode                string
	Percent             int
	AllowlistAccounts   []string
	RollbackForceLegacy bool
	HashSalt            string
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
			Address:   "127.0.0.1:7233",
			Namespace: "default",
			TaskQueue: "agent-framework",

			MaxConcurrentWorkflowTaskPollers: 2,
			MaxConcurrentActivityTaskPollers: 2,
			MaxConcurrentActivityExecution:   100,
		},
		Rollout: Rollout{
			Mode:                "disabled",
			Percent:             0,
			AllowlistAccounts:   nil,
			RollbackForceLegacy: false,
			HashSalt:            "agentfw-v1",
		},
		Runtime: Runtime{
			DefaultModelRef: "gpt-4.1-mini",

			WorkflowVersion: 1,
			EventSchemaVer:  "v1",
			ToolSchemaVer:   "v1",
			AgentConfigVer:  "v1",

			MaxSteps:                            100,
			MaxWallClockDuration:                time.Hour,
			ContinueAsNewStepThreshold:          80,
			ContinueAsNewHistoryThreshold:       10000,
			ContinueAsNewStateSizeThresholdByte: 512 * 1024,
			ContinueAsNewWallClockThreshold:     50 * time.Minute,
			ContinueAsNewMaxContinuations:       1000,
		},
	}
}
