package runtimeops

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRolloutSelectorDisabled(t *testing.T) {
	selector := NewRolloutSelector(RolloutConfig{Mode: RolloutDisabled})
	decision := selector.Decide("acc-1", "run-1")
	require.False(t, decision.UseTemporal)
	require.Equal(t, "rollout_disabled", decision.Reason)
}

func TestRolloutSelectorEnabled(t *testing.T) {
	selector := NewRolloutSelector(RolloutConfig{Mode: RolloutEnabled})
	decision := selector.Decide("acc-1", "run-1")
	require.True(t, decision.UseTemporal)
	require.False(t, decision.ShadowOnly)
	require.Equal(t, "rollout_enabled", decision.Reason)
}

func TestRolloutSelectorShadow(t *testing.T) {
	selector := NewRolloutSelector(RolloutConfig{Mode: RolloutShadow})
	decision := selector.Decide("acc-1", "run-1")
	require.True(t, decision.UseTemporal)
	require.True(t, decision.ShadowOnly)
	require.Equal(t, "shadow_mode", decision.Reason)
}

func TestRolloutSelectorCanaryAllowlist(t *testing.T) {
	selector := NewRolloutSelector(RolloutConfig{
		Mode:              RolloutCanary,
		Percent:           0,
		AllowlistAccounts: []string{"acc-allow"},
	})

	decision := selector.Decide("acc-allow", "run-1")
	require.True(t, decision.UseTemporal)
	require.Equal(t, "canary_allowlist", decision.Reason)
}

func TestRolloutSelectorCanaryDeterministic(t *testing.T) {
	selector := NewRolloutSelector(RolloutConfig{
		Mode:     RolloutCanary,
		Percent:  35,
		HashSalt: "stable-salt",
	})

	first := selector.Decide("acc-2", "run-2")
	second := selector.Decide("acc-2", "run-2")
	require.Equal(t, first, second)
}

func TestRolloutSelectorRollbackForceLegacy(t *testing.T) {
	selector := NewRolloutSelector(RolloutConfig{
		Mode:                RolloutEnabled,
		RollbackForceLegacy: true,
	})
	decision := selector.Decide("acc-1", "run-1")
	require.False(t, decision.UseTemporal)
	require.Equal(t, "rollback_force_legacy", decision.Reason)
}

func TestRolloutSelectorNormalizesInvalidInput(t *testing.T) {
	selector := NewRolloutSelector(RolloutConfig{
		Mode:    "bad-mode",
		Percent: 200,
	})
	decision := selector.Decide("acc-1", "run-1")
	require.False(t, decision.UseTemporal)
	require.Equal(t, "rollout_disabled", decision.Reason)

	selector = NewRolloutSelector(RolloutConfig{
		Mode:     RolloutCanary,
		Percent:  -10,
		HashSalt: "",
	})
	decision = selector.Decide("acc-1", "run-1")
	require.False(t, decision.UseTemporal)
	require.Equal(t, "canary_percent_legacy", decision.Reason)
}
