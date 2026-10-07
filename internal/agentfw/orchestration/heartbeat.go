package orchestration

import (
	"context"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/activity"
)

// heartbeatInterval is how often long-running activities report liveness to
// the Temporal server. It must stay well below any configured
// HeartbeatTimeout so a live worker is never marked dead and retried
// (retrying a live LLM call would duplicate the call and its cost).
const heartbeatInterval = 15 * time.Second

// heartbeatProgress is the heartbeat detail payload carried across retry
// attempts (via activity.GetHeartbeatDetails) and visible in server logs.
// Phase identifies the running stage; WrittenEvents tracks stream deltas
// already delivered for progress observability; ToolName identifies the
// tool being executed.
type heartbeatProgress struct {
	Phase         string `json:"phase"`
	ToolName      string `json:"tool_name,omitempty"`
	WrittenEvents int64  `json:"written_events,omitempty"`
}

// startHeartbeatLoop reports activity liveness on a fixed interval until
// stop() is called or the activity context is canceled. detailProvider is
// invoked on each tick and its result is attached to the heartbeat.
func startHeartbeatLoop(ctx context.Context, detailProvider func() any) (stop func()) {
	var (
		done   = make(chan struct{})
		closed atomic.Bool
	)

	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				var detail any
				if detailProvider != nil {
					detail = detailProvider()
				}

				activity.RecordHeartbeat(ctx, detail)
			}
		}
	}()

	return func() {
		if closed.CompareAndSwap(false, true) {
			close(done)
		}
	}
}
