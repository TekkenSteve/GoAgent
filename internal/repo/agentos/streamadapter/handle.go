// Package streamadapter maps the native runtime's streaming events onto the
// data-plane contract: entity.StreamEvent → AG-UI stream.Event, published
// through the engine-neutral stream.Publisher. It is the native backend's
// thin adapter — step 1 (map) and step 2 (publish) of the integration
// contract, with usage reported on RUN_FINISHED.
//
// The adapter never learns about the control plane's auth, persistence, or
// frontend connections; it only needs the Handle handed at run start and a
// Publisher behind it (Centrifugo in production, memstream in tests/dev).
package streamadapter

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
)

const (
	// defaultTenant is the channel namespace used when a run or plan carries
	// no account: single-tenant deployments still get a deterministic channel.
	defaultTenant = "default"
)

// ChannelForRun derives the data-plane channel for a run. The channel encodes
// the exact frontend session: `agentos:run:{tenant}:{run_id}`. A missing
// tenant (account) falls back to defaultTenant so single-tenant runs still get
// a deterministic channel.
func ChannelForRun(tenant, runID string) string {
	if tenant == "" {
		tenant = defaultTenant
	}

	return fmt.Sprintf("agentos:run:%s:%s", tenant, runID)
}

// HandleForRun builds the data-plane Handle a backend needs to publish a
// run's timeline.
func HandleForRun(tenant, runID string) *stream.Handle {
	return stream.NewHandle(ChannelForRun(tenant, runID))
}

// ChannelForPlan derives the data-plane channel for a plan's live event tail:
// `agentos:plan:{tenant}:{plan_id}`. A missing tenant (account) falls back to
// defaultTenant, mirroring ChannelForRun.
func ChannelForPlan(tenant, planID string) string {
	if tenant == "" {
		tenant = defaultTenant
	}

	return fmt.Sprintf("agentos:plan:%s:%s", tenant, planID)
}

// HandleForPlan builds the data-plane Handle for a plan's live event tail.
func HandleForPlan(tenant, planID string) *stream.Handle {
	return stream.NewHandle(ChannelForPlan(tenant, planID))
}
