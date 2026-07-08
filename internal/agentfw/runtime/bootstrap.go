package runtime

import (
	"errors"
	"fmt"
	"slices"
)

// ErrNilRuntime is returned when StartWorker receives a nil runtime or worker.
var ErrNilRuntime = errors.New("agentfw runtime - start worker: nil runtime")

// Registrar installs workflows and activities into worker before start.
type Registrar interface {
	RegisterWorkflows(rt *TemporalRuntime) error
	RegisterActivities(rt *TemporalRuntime) error
}

// StartWorker registers framework components and starts polling.
func StartWorker(rt *TemporalRuntime, registrar Registrar) error {
	if rt == nil || len(rt.Workers) == 0 {
		return ErrNilRuntime
	}

	if registrar != nil {
		if err := registrar.RegisterWorkflows(rt); err != nil {
			return err
		}

		if err := registrar.RegisterActivities(rt); err != nil {
			return err
		}
	}

	started := make([]string, 0, len(rt.Workers))
	for _, taskQueue := range sortedWorkerTaskQueues(rt) {
		w := rt.Workers[taskQueue]
		if w == nil {
			continue
		}

		if err := w.Start(); err != nil {
			for _, startedQueue := range started {
				rt.Workers[startedQueue].Stop()
			}

			return fmt.Errorf("agentfw runtime - start worker %q: %w", taskQueue, err)
		}

		started = append(started, taskQueue)
	}

	return nil
}

// StopWorker stops polling and closes temporal client.
func StopWorker(rt *TemporalRuntime) {
	if rt == nil {
		return
	}

	for _, taskQueue := range sortedWorkerTaskQueues(rt) {
		if rt.Workers[taskQueue] != nil {
			rt.Workers[taskQueue].Stop()
		}
	}

	rt.Close()
}

func sortedWorkerTaskQueues(rt *TemporalRuntime) []string {
	if rt == nil {
		return nil
	}

	taskQueues := make([]string, 0, len(rt.Workers))
	for taskQueue := range rt.Workers {
		taskQueues = append(taskQueues, taskQueue)
	}

	slices.Sort(taskQueues)

	return taskQueues
}
