package config

import appconfig "github.com/evrone/go-clean-template/config"

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

	return base
}
