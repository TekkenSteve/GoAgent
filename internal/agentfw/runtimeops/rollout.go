package runtimeops

import (
	"fmt"
	"hash/fnv"
	"strings"
)

// RolloutMode defines which execution path is active.
type RolloutMode string

const (
	RolloutDisabled RolloutMode = "disabled"
	RolloutShadow   RolloutMode = "shadow"
	RolloutCanary   RolloutMode = "canary"
	RolloutEnabled  RolloutMode = "enabled"
)

// RolloutConfig controls route selection between legacy and Temporal execution paths.
type RolloutConfig struct {
	Mode                RolloutMode
	Percent             int
	AllowlistAccounts   []string
	RollbackForceLegacy bool
	HashSalt            string
}

// RolloutDecision is the stable output for route selection.
type RolloutDecision struct {
	UseTemporal bool
	ShadowOnly  bool
	Reason      string
}

// RolloutSelector applies rollout policy for a run/account identity.
type RolloutSelector struct {
	cfg      RolloutConfig
	allowSet map[string]struct{}
}

// NewRolloutSelector normalizes config and pre-computes allowlist lookup.
func NewRolloutSelector(cfg RolloutConfig) RolloutSelector {
	mode := RolloutMode(strings.ToLower(strings.TrimSpace(string(cfg.Mode))))
	switch mode {
	case RolloutDisabled, RolloutShadow, RolloutCanary, RolloutEnabled:
	default:
		mode = RolloutDisabled
	}

	if cfg.Percent < 0 {
		cfg.Percent = 0
	}
	if cfg.Percent > 100 {
		cfg.Percent = 100
	}
	if strings.TrimSpace(cfg.HashSalt) == "" {
		cfg.HashSalt = "agentfw-v1"
	}

	set := make(map[string]struct{}, len(cfg.AllowlistAccounts))
	for _, accountID := range cfg.AllowlistAccounts {
		accountID = strings.TrimSpace(accountID)
		if accountID == "" {
			continue
		}
		set[accountID] = struct{}{}
	}

	cfg.Mode = mode
	return RolloutSelector{cfg: cfg, allowSet: set}
}

// Decide selects whether the Temporal or legacy path should handle a run.
func (s RolloutSelector) Decide(accountID, runID string) RolloutDecision {
	if s.cfg.RollbackForceLegacy {
		return RolloutDecision{Reason: "rollback_force_legacy"}
	}

	switch s.cfg.Mode {
	case RolloutDisabled:
		return RolloutDecision{Reason: "rollout_disabled"}
	case RolloutShadow:
		return RolloutDecision{UseTemporal: true, ShadowOnly: true, Reason: "shadow_mode"}
	case RolloutEnabled:
		return RolloutDecision{UseTemporal: true, Reason: "rollout_enabled"}
	case RolloutCanary:
		if _, ok := s.allowSet[accountID]; ok {
			return RolloutDecision{UseTemporal: true, Reason: "canary_allowlist"}
		}
		bucket := stableBucket(s.cfg.HashSalt, accountID, runID)
		if bucket < s.cfg.Percent {
			return RolloutDecision{UseTemporal: true, Reason: "canary_percent"}
		}
		return RolloutDecision{Reason: "canary_percent_legacy"}
	default:
		return RolloutDecision{Reason: "rollout_disabled"}
	}
}

func stableBucket(hashSalt, accountID, runID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(fmt.Sprintf("%s:%s:%s", hashSalt, accountID, runID)))
	return int(h.Sum32() % 100)
}
