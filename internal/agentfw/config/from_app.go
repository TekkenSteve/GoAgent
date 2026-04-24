package config

import (
	"strings"
	"time"

	appconfig "github.com/TekkenSteve/GoAgent/config"
)

// FromAppConfig maps application config into the framework-specific config model.
func FromAppConfig(cfg *appconfig.Config) Config {
	base := Default()

	base.Enabled = cfg.AgentFW.Enabled
	base.Rollout.Mode = strings.ToLower(strings.TrimSpace(cfg.AgentFW.RolloutMode))
	base.Rollout.Percent = cfg.AgentFW.RolloutPercent
	base.Rollout.RollbackForceLegacy = cfg.AgentFW.RollbackForceLegacy
	base.Rollout.HashSalt = strings.TrimSpace(cfg.AgentFW.RolloutHashSalt)
	if base.Rollout.HashSalt == "" {
		base.Rollout.HashSalt = "agentfw-v1"
	}
	for _, accountID := range strings.Split(cfg.AgentFW.RolloutAllowlist, ",") {
		accountID = strings.TrimSpace(accountID)
		if accountID == "" {
			continue
		}
		base.Rollout.AllowlistAccounts = append(base.Rollout.AllowlistAccounts, accountID)
	}

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
