package orchestration

import (
	"context"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	// StreamWorkflowName is the workflow type name for streaming agent execution.
	StreamWorkflowName = "agentfw.stream-workflow.v1"
	// StreamStepActivityName is the activity name for the streaming agent step.
	StreamStepActivityName = "agentfw.stream-step-activity.v1"
)

// StreamWorkflowInput is the serializable input for the streaming workflow.
type StreamWorkflowInput struct {
	SessionID    string
	SystemPrompt string
	Message      string
	History      []entity.Message
	Tools        []entity.ToolDef
	Config       entity.LLMConfig
}

// StreamWorkflow is a single-activity workflow that executes the agent with
// streaming output. Events are written to the EventStore by the activity
// and consumed by the TemporalStreamExecutor on the controller side.
//
// The workflow accepts "agent-command" signals for external control:
//   - "cancel": completes the workflow, cancelling the running activity
//   - "pause":  enters a wait loop until "resume" or "cancel" is received
//   - "resume": exits the pause wait loop
func StreamWorkflow(ctx workflow.Context, input StreamWorkflowInput) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})

	// Signal channel for external control (cancel/pause/resume).
	// The controller sends signals via client.SignalWorkflow().
	signalCh := workflow.GetSignalChannel(ctx, "agent-command")
	var signal string

	future := workflow.ExecuteActivity(ctx, StreamStepActivityName, input)

	// Activity-completion select loop with interleaved signal handling.
	// Signals are checked between activity heartbeats and on completion.
	for {
		selector := workflow.NewSelector(ctx)

		var gotSignal bool
		selector.AddReceive(signalCh, func(c workflow.ReceiveChannel, _ bool) {
			_ = c.Receive(ctx, &signal)
			gotSignal = true
		})

		var futureDone bool
		selector.AddFuture(future, func(f workflow.Future) {
			futureDone = true
		})

		selector.Select(ctx)

		if gotSignal {
			_ = gotSignal
			switch signal {
			case "cancel":
				// Workflow returns immediately → Temporal cancels the
				// running activity → activity ctx.Err() fires → cleanup.
				return nil
			case "pause":
				// Wait for resume or cancel signal. The activity keeps
				// running during pause (event streaming continues).
			pauseLoop:
				for {
					var s string
					signalCh.Receive(ctx, &s)
					switch s {
					case "resume":
						break pauseLoop
					case "cancel":
						return nil
					}
				}
				// Resume: fall through and continue the select loop.
				continue
			}
		}

		if futureDone {
			return future.Get(ctx, nil)
		}
	}
}

// temporalEventWriter implements usecase.StreamEventWriter by appending events
// to the EventStore and recording Temporal heartbeats for liveness tracking.
// On activity retry, it replays the heartbeat to skip already-written events.
type temporalEventWriter struct {
	eventStore      stream.EventStore
	sessionID       string
	runID           string
	resumeSequence  int64
	currentSequence int64
}

func (w *temporalEventWriter) WriteEvent(ctx context.Context, event entity.StreamEvent) error {
	// On retry, skip events that were already written in the previous attempt.
	if w.currentSequence < w.resumeSequence {
		w.currentSequence++
		return nil
	}

	seq, err := w.eventStore.Append(ctx, w.sessionID, w.runID, event)
	if err != nil {
		return fmt.Errorf("temporal event writer append: %w", err)
	}
	w.currentSequence = seq

	// Heartbeat notifies Temporal Server that the activity is still alive.
	// The heartbeat payload carries the last written sequence for retry recovery.
	activity.RecordHeartbeat(ctx, seq)
	return nil
}

// ExecuteStreamingStep runs the agent with streaming output inside a Temporal activity.
// Events are written to the EventStore via temporalEventWriter, which also records
// heartbeats. On retry, the heartbeat payload allows skipping already-written events.
func (a *AgentActivities) ExecuteStreamingStep(ctx context.Context, input StreamWorkflowInput) error {
	actInfo := activity.GetInfo(ctx)

	// Recover resume sequence from heartbeat on retry.
	var resumeSequence int64
	if err := activity.GetHeartbeatDetails(ctx, &resumeSequence); err != nil {
		resumeSequence = 0
	}

	writer := &temporalEventWriter{
		eventStore:      a.eventStore,
		sessionID:       input.SessionID,
		runID:           actInfo.ActivityID,
		resumeSequence:  resumeSequence,
		currentSequence: 0,
	}

	req := entity.StreamRequest{
		RunID:        input.SessionID,
		SystemPrompt: input.SystemPrompt,
		Message:      input.Message,
		History:      input.History,
		Tools:        input.Tools,
		Config:       input.Config,
	}

	return a.agentUC.ExecuteStreamSync(ctx, req, writer)
}

// compile-time interface check
var _ usecase.StreamEventWriter = (*temporalEventWriter)(nil)
