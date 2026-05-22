package runtime

import (
	"errors"
	"fmt"
)

// ErrNilRuntime is returned when StartWorker receives a nil runtime or worker.
var ErrNilRuntime = errors.New("agentfw runtime - start worker: nil runtime")

// Registrar installs workflows and activities into worker before start.
type Registrar interface {
	RegisterWorkflows(rt *TemporalRuntime)
	RegisterActivities(rt *TemporalRuntime)
}

// StartWorker registers framework components and starts polling.
func StartWorker(rt *TemporalRuntime, registrar Registrar) error {
	if rt == nil || rt.Worker == nil {
		return ErrNilRuntime
	}

	if registrar != nil {
		registrar.RegisterWorkflows(rt)
		registrar.RegisterActivities(rt)
	}

	if err := rt.Worker.Start(); err != nil {
		return fmt.Errorf("agentfw runtime - start worker: %w", err)
	}

	return nil
}

// StopWorker stops polling and closes temporal client.
func StopWorker(rt *TemporalRuntime) {
	if rt == nil {
		return
	}

	if rt.Worker != nil {
		rt.Worker.Stop()
	}

	rt.Close()
}
