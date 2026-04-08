package orchestration

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

const (
	// AgentWorkflowName is the public workflow type name for agent execution.
	AgentWorkflowName = "agentfw.agent-workflow.v1"
)

// WorkflowInput is the deterministic workflow input payload.
type WorkflowInput struct {
	Request ExecuteRequest
	// TargetSteps is used for deterministic progression in baseline orchestration.
	TargetSteps int32
}

// WorkflowResult is the deterministic workflow output payload.
type WorkflowResult struct {
	RunID          string
	LifecycleState string
	Step           int32
	CompletedAt    time.Time
}

// AgentWorkflow is the minimal orchestration placeholder.
//
// Business steps will be implemented incrementally from OpenSpec tasks.
func AgentWorkflow(ctx workflow.Context, input WorkflowInput) (WorkflowResult, error) {
	targetSteps := input.TargetSteps
	if targetSteps <= 0 {
		targetSteps = 1
	}

	status := RunStatus{
		RunID:          input.Request.RunID,
		LifecycleState: "created",
		Step:           0,
		UpdatedAt:      workflow.Now(ctx),
	}

	if err := workflow.SetQueryHandler(ctx, QueryRunStatus, func() (RunStatus, error) {
		return status, nil
	}); err != nil {
		return WorkflowResult{}, err
	}

	status.LifecycleState = "running"
	status.UpdatedAt = workflow.Now(ctx)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	for status.Step < targetSteps {
		cancelled, err := applyPendingControlSignals(ctx, &status)
		if err != nil {
			return WorkflowResult{}, err
		}
		if cancelled {
			return WorkflowResult{
				RunID:          status.RunID,
				LifecycleState: "cancelled",
				Step:           status.Step,
				CompletedAt:    workflow.Now(ctx),
			}, nil
		}

		if status.LifecycleState == "paused" {
			cancelled, err = waitForResumeOrCancel(ctx, &status)
			if err != nil {
				return WorkflowResult{}, err
			}
			if cancelled {
				return WorkflowResult{
					RunID:          status.RunID,
					LifecycleState: "cancelled",
					Step:           status.Step,
					CompletedAt:    workflow.Now(ctx),
				}, nil
			}
		}

		// Ensure "resumed" state transitions to "running" before activity execution
		if status.LifecycleState == "resumed" {
			status.LifecycleState = "running"
			status.UpdatedAt = workflow.Now(ctx)
		}

		var actRes EchoActivityResult

		var actRes EchoActivityResult
		if err := workflow.ExecuteActivity(ctx, EchoActivityName, EchoActivityInput{
			RunID: status.RunID,
		}).Get(ctx, &actRes); err != nil {
			status.LifecycleState = "failed"
			status.Reason = err.Error()
			status.UpdatedAt = workflow.Now(ctx)
			return WorkflowResult{}, err
		}

		status.Step++
		status.UpdatedAt = workflow.Now(ctx)
	}

	status.LifecycleState = "completed"
	status.UpdatedAt = workflow.Now(ctx)

	return WorkflowResult{
		RunID:          status.RunID,
		LifecycleState: status.LifecycleState,
		Step:           status.Step,
		CompletedAt:    status.UpdatedAt,
	}, nil
}

func applyPendingControlSignals(ctx workflow.Context, status *RunStatus) (bool, error) {
	pauseCh := workflow.GetSignalChannel(ctx, SignalPause)
	resumeCh := workflow.GetSignalChannel(ctx, SignalResume)
	cancelCh := workflow.GetSignalChannel(ctx, SignalCancel)

	var ignored string
	for {
		handled := false

		if cancelCh.ReceiveAsync(&ignored) {
			if err := ValidateControlOperation(status.LifecycleState, ControlCancel); err != nil {
				status.Reason = err.Error()
				status.UpdatedAt = workflow.Now(ctx)
				handled = true
			} else {
				status.LifecycleState = "cancelled"
				status.UpdatedAt = workflow.Now(ctx)
				return true, nil
			}
		}

		if pauseCh.ReceiveAsync(&ignored) {
			if err := ValidateControlOperation(status.LifecycleState, ControlPause); err != nil {
				status.Reason = err.Error()
				status.UpdatedAt = workflow.Now(ctx)
			} else {
				status.LifecycleState = "paused"
				status.Reason = ""
				status.UpdatedAt = workflow.Now(ctx)
			}
			handled = true
		}

		if resumeCh.ReceiveAsync(&ignored) {
			if err := ValidateControlOperation(status.LifecycleState, ControlResume); err != nil {
				status.Reason = err.Error()
				status.UpdatedAt = workflow.Now(ctx)
			} else {
				status.LifecycleState = "resumed"
				status.Reason = ""
				status.UpdatedAt = workflow.Now(ctx)
			}
			handled = true
		}

		if !handled {
			return false, nil
		}
	}
}

func waitForResumeOrCancel(ctx workflow.Context, status *RunStatus) (bool, error) {
	pauseCh := workflow.GetSignalChannel(ctx, SignalPause)
	resumeCh := workflow.GetSignalChannel(ctx, SignalResume)
	cancelCh := workflow.GetSignalChannel(ctx, SignalCancel)

	for {
		var signalPayload string
		selector := workflow.NewSelector(ctx)

		selector.AddReceive(cancelCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &signalPayload)
			status.LifecycleState = "cancelled"
			status.UpdatedAt = workflow.Now(ctx)
		})

		selector.AddReceive(resumeCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &signalPayload)
			if err := ValidateControlOperation(status.LifecycleState, ControlResume); err != nil {
				status.Reason = err.Error()
			} else {
				status.LifecycleState = "resumed"
				status.Reason = ""
			}
			status.UpdatedAt = workflow.Now(ctx)
		})

		selector.AddReceive(pauseCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &signalPayload)
			if err := ValidateControlOperation(status.LifecycleState, ControlPause); err != nil {
				status.Reason = err.Error()
				status.UpdatedAt = workflow.Now(ctx)
			}
		})

		selector.Select(ctx)

		if status.LifecycleState == "cancelled" {
			return true, nil
		}

		if status.LifecycleState == "resumed" {
			status.LifecycleState = "running"
			status.UpdatedAt = workflow.Now(ctx)
			return false, nil
		}
	}
}
