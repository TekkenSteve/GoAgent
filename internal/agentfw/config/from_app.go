package config

import (
	"time"

	appconfig "github.com/TekkenSteve/GoAgent/config"
)

// FromAppConfig maps application config into the framework-specific config model.
func FromAppConfig(cfg *appconfig.Config) Config {
	base := Default()

	base.Enabled = cfg.AgentFW.Enabled

	base.Temporal.Address = cfg.AgentFW.TemporalAddress
	base.Temporal.Namespace = cfg.AgentFW.TemporalNamespace
	base.Temporal.TaskQueue = cfg.AgentFW.TemporalTaskQueue
	base.Temporal.MaxConcurrentWorkflowTaskPollers = cfg.AgentFW.MaxConcurrentWorkflowTaskPollers
	base.Temporal.MaxConcurrentActivityTaskPollers = cfg.AgentFW.MaxConcurrentActivityTaskPollers
	base.Temporal.MaxConcurrentActivityExecution = cfg.AgentFW.MaxConcurrentActivityExecution
	base.Runtime.ContinueAsNewStepThreshold = cfg.AgentFW.ContinueAsNewStepThreshold
	base.Runtime.ContinueAsNewHistoryThreshold = cfg.AgentFW.ContinueAsNewHistoryThreshold
	base.Runtime.ContinueAsNewStateSizeThresholdByte = cfg.AgentFW.ContinueAsNewStateSizeThreshold
	base.Runtime.ContinueAsNewWallClockThreshold = time.Duration(cfg.AgentFW.ContinueAsNewWallClockSeconds) * time.Second
	base.Runtime.ContinueAsNewMaxContinuations = cfg.AgentFW.ContinueAsNewMaxContinuations

	return base
}
