package temporal

import (
	"context"
	"fmt"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosprocess"
)

// ProcessActivities bridge ProcessWorkflow decisions to the durable process store.
type ProcessActivities struct {
	Runtime *agentosprocess.Runtime
}

// NewProcessActivities creates activities for generic durable processes.
func NewProcessActivities(index agentosprocess.ProcessIndex, events agentosprocess.ProcessEventStore) (*ProcessActivities, error) {
	runtime, err := agentosprocess.NewRuntime(index, events)
	if err != nil {
		return nil, err
	}

	return &ProcessActivities{Runtime: runtime}, nil
}

type startProcessActivityInput struct {
	Spec agentosproc.Spec
}

// StartProcessActivity starts a durable process from the given spec.
func (a *ProcessActivities) StartProcessActivity(ctx context.Context, input *startProcessActivityInput) (agentosproc.Status, error) {
	if a == nil || a.Runtime == nil {
		return agentosproc.Status{}, fmt.Errorf("%w: process activity runtime is required", agentoscore.ErrInvalidProcess)
	}

	return a.Runtime.StartProcess(ctx, &input.Spec)
}

type signalProcessActivityInput struct {
	Ref    agentosproc.Ref
	Signal agentoscore.Signal
}

// SignalProcessActivity delivers a signal to a process and returns its refreshed status.
func (a *ProcessActivities) SignalProcessActivity(ctx context.Context, input *signalProcessActivityInput) (agentosproc.Status, error) {
	if a == nil || a.Runtime == nil {
		return agentosproc.Status{}, fmt.Errorf("%w: process activity runtime is required", agentoscore.ErrInvalidProcess)
	}

	if err := a.Runtime.SignalProcess(ctx, input.Ref, &input.Signal); err != nil {
		return agentosproc.Status{}, err
	}

	return a.Runtime.StatusProcess(ctx, input.Ref)
}

type controlProcessActivityInput struct {
	Ref     agentosproc.Ref
	Control agentoscore.ControlRequest
}

// ControlProcessActivity delivers a lifecycle control to a process and returns its refreshed status.
func (a *ProcessActivities) ControlProcessActivity(ctx context.Context, input *controlProcessActivityInput) (agentosproc.Status, error) {
	if a == nil || a.Runtime == nil {
		return agentosproc.Status{}, fmt.Errorf("%w: process activity runtime is required", agentoscore.ErrInvalidProcess)
	}

	if err := a.Runtime.ControlProcess(ctx, input.Ref, &input.Control); err != nil {
		return agentosproc.Status{}, err
	}

	return a.Runtime.StatusProcess(ctx, input.Ref)
}

type fireProcessTimerActivityInput struct {
	Ref   agentosproc.Ref
	Timer agentosproc.TimerSpec
	At    string
}

// FireProcessTimerActivity fires a scheduled timer as a signal to a process and returns its refreshed status.
func (a *ProcessActivities) FireProcessTimerActivity(ctx context.Context, input *fireProcessTimerActivityInput) (agentosproc.Status, error) {
	if a == nil || a.Runtime == nil {
		return agentosproc.Status{}, fmt.Errorf("%w: process activity runtime is required", agentoscore.ErrInvalidProcess)
	}

	signal := agentoscore.Signal{
		Type:           input.Timer.Signal,
		IdempotencyKey: processTimerIdempotencyKey(input.Ref.ProcessID, input.Timer.TimerID, input.At),
		Payload:        cloneAnyMap(input.Timer.Payload),
	}
	if signal.Type == "" {
		signal.Type = agentoscore.SignalType("process.timer." + input.Timer.TimerID)
	}

	if err := a.Runtime.SignalProcess(ctx, input.Ref, &signal); err != nil {
		return agentosproc.Status{}, err
	}

	return a.Runtime.StatusProcess(ctx, input.Ref)
}

func processTimerIdempotencyKey(processID, timerID, at string) string {
	return processID + ":timer:" + timerID + ":" + at
}
