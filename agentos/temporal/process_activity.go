package temporal

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
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
	Spec agentos.ProcessSpec
}

func (a *ProcessActivities) StartProcessActivity(ctx context.Context, input *startProcessActivityInput) (agentos.ProcessStatus, error) {
	if a == nil || a.Runtime == nil {
		return agentos.ProcessStatus{}, fmt.Errorf("%w: process activity runtime is required", agentos.ErrInvalidProcess)
	}

	return a.Runtime.StartProcess(ctx, &input.Spec)
}

type signalProcessActivityInput struct {
	Ref    agentos.ProcessRef
	Signal agentos.Signal
}

func (a *ProcessActivities) SignalProcessActivity(ctx context.Context, input *signalProcessActivityInput) (agentos.ProcessStatus, error) {
	if a == nil || a.Runtime == nil {
		return agentos.ProcessStatus{}, fmt.Errorf("%w: process activity runtime is required", agentos.ErrInvalidProcess)
	}

	if err := a.Runtime.SignalProcess(ctx, input.Ref, &input.Signal); err != nil {
		return agentos.ProcessStatus{}, err
	}

	return a.Runtime.StatusProcess(ctx, input.Ref)
}

type controlProcessActivityInput struct {
	Ref     agentos.ProcessRef
	Control agentos.ControlRequest
}

func (a *ProcessActivities) ControlProcessActivity(ctx context.Context, input *controlProcessActivityInput) (agentos.ProcessStatus, error) {
	if a == nil || a.Runtime == nil {
		return agentos.ProcessStatus{}, fmt.Errorf("%w: process activity runtime is required", agentos.ErrInvalidProcess)
	}

	if err := a.Runtime.ControlProcess(ctx, input.Ref, &input.Control); err != nil {
		return agentos.ProcessStatus{}, err
	}

	return a.Runtime.StatusProcess(ctx, input.Ref)
}

type fireProcessTimerActivityInput struct {
	Ref   agentos.ProcessRef
	Timer agentos.ProcessTimerSpec
	At    string
}

func (a *ProcessActivities) FireProcessTimerActivity(ctx context.Context, input *fireProcessTimerActivityInput) (agentos.ProcessStatus, error) {
	if a == nil || a.Runtime == nil {
		return agentos.ProcessStatus{}, fmt.Errorf("%w: process activity runtime is required", agentos.ErrInvalidProcess)
	}

	signal := agentos.Signal{
		Type:           input.Timer.Signal,
		IdempotencyKey: processTimerIdempotencyKey(input.Ref.ProcessID, input.Timer.TimerID, input.At),
		Payload:        cloneAnyMap(input.Timer.Payload),
	}
	if signal.Type == "" {
		signal.Type = agentos.SignalType("process.timer." + input.Timer.TimerID)
	}

	if err := a.Runtime.SignalProcess(ctx, input.Ref, &signal); err != nil {
		return agentos.ProcessStatus{}, err
	}

	return a.Runtime.StatusProcess(ctx, input.Ref)
}

func processTimerIdempotencyKey(processID, timerID, at string) string {
	return processID + ":timer:" + timerID + ":" + at
}
