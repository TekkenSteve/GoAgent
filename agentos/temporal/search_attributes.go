package temporal

import (
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// lifecycleSearchAttributeSync keeps a workflow's visible lifecycle in sync with
// its durable state, deduplicating consecutive transitions so per-iteration
// no-op upserts do not bloat history. Both the plan and process workflows reuse
// it behind their own workflow.GetVersion marker.
type lifecycleSearchAttributeSync struct {
	enabled    workflow.Version
	runID      string
	lastSynced string
}

func newLifecycleSearchAttributeSync(enabled workflow.Version, runID string) *lifecycleSearchAttributeSync {
	return &lifecycleSearchAttributeSync{enabled: enabled, runID: runID}
}

// sync upserts the current lifecycle when it differs from the last synced one.
// Change-detection avoids no-op upserts on signal-only iterations bloating
// history; the upsert itself is best-effort and never fails the run.
func (s *lifecycleSearchAttributeSync) sync(ctx workflow.Context, lifecycle string) {
	if lifecycle == s.lastSynced {
		return
	}

	syncLifecycleSearchAttributes(ctx, s.enabled, s.runID, lifecycle)
	s.lastSynced = lifecycle
}

// syncLifecycleSearchAttributes upserts the visibility attributes that keep a
// durable plan/process observable by lifecycle state. Plans and processes reuse
// the same registered keys as agent runs (goagent.run_id, goagent.lifecycle_state)
// so one visibility query spans both execution families:
//
//	temporal workflow list -q "goagent.run_id='plan-<id>' AND goagent.lifecycle_state='blocked'"
//
// UpsertSearchAttributes is merge semantics — the lifecycle_state key reflects
// the current lifecycle while other keys are left untouched. Guarded by a
// workflow.GetVersion marker because it emits history events; executions started
// before the marker replay without the calls. Visibility is best-effort: a
// failed upsert (e.g. an unregistered key) must not fail the run.
func syncLifecycleSearchAttributes(ctx workflow.Context, enabled workflow.Version, runID, lifecycleState string) {
	if enabled < 1 {
		return
	}

	if err := workflow.UpsertTypedSearchAttributes(ctx,
		temporal.NewSearchAttributeKeyKeyword(orchestration.SearchAttrRunID).ValueSet(runID),
		temporal.NewSearchAttributeKeyKeyword(orchestration.SearchAttrLifecycleState).ValueSet(lifecycleState),
	); err != nil {
		workflow.GetLogger(ctx).Warn("failed to upsert lifecycle search attributes", "error", err)
	}
}
