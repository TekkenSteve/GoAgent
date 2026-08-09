package runtime

import (
	"errors"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

var (
	errRegistrarRuntimeRequired       = errors.New("agentfw runtime registrar: temporal runtime is required")
	errRegistrarTaskQueueUnconfigured = errors.New("agentfw runtime registrar: task queue is not configured")
)

// DefaultRegistrar registers the framework's baseline workflow and activities.
type DefaultRegistrar struct {
	activities *orchestration.AgentActivities
}

// NewDefaultRegistrar creates a baseline registrar instance.
func NewDefaultRegistrar(activities *orchestration.AgentActivities) DefaultRegistrar {
	return DefaultRegistrar{activities: activities}
}

// RegisterWorkflows registers workflow definitions.
func (r DefaultRegistrar) RegisterWorkflows(rt *TemporalRuntime) error {
	nativeControlWorker, err := r.requiredWorker(rt, rt.TaskQueues.NativeControl, "native control")
	if err != nil {
		return err
	}

	streamWorker, err := r.requiredWorker(rt, rt.TaskQueues.Stream, "stream")
	if err != nil {
		return err
	}

	nativeControlWorker.RegisterWorkflowWithOptions(orchestration.AgentWorkflow, workflow.RegisterOptions{
		Name: orchestration.AgentWorkflowName,
	})
	streamWorker.RegisterWorkflowWithOptions(orchestration.StreamAgentWorkflow, workflow.RegisterOptions{
		Name: orchestration.StreamWorkflowName,
	})
	nativeControlWorker.RegisterWorkflowWithOptions(orchestration.Workflow, workflow.RegisterOptions{
		Name: orchestration.OrchestrationWorkflowName,
	})

	return nil
}

// RegisterActivities registers activity definitions.
func (r DefaultRegistrar) RegisterActivities(rt *TemporalRuntime) error {
	if r.activities == nil {
		return nil
	}

	nativeControlWorker, err := r.requiredWorker(rt, rt.TaskQueues.NativeControl, "native control")
	if err != nil {
		return err
	}

	nativeLLMActivityWorker, err := r.requiredWorker(rt, rt.TaskQueues.NativeLLM, "native llm activity")
	if err != nil {
		return err
	}

	nativeToolActivityWorker, err := r.requiredWorker(rt, rt.TaskQueues.NativeTool, "native tool activity")
	if err != nil {
		return err
	}

	streamWorker, err := r.requiredWorker(rt, rt.TaskQueues.Stream, "stream")
	if err != nil {
		return err
	}

	// Register each activity individually with the name the workflow uses
	// (RegisterActivity without options would use the reflection-based function name,
	// which won't match the workflow's activity type name).
	nativeControlWorker.RegisterActivityWithOptions(r.activities.PrepareActivity, activity.RegisterOptions{
		Name: orchestration.PrepareActivityName,
	})
	nativeLLMActivityWorker.RegisterActivityWithOptions(r.activities.LLMStepActivity, activity.RegisterOptions{
		Name: orchestration.LLMStepActivityName,
	})
	nativeToolActivityWorker.RegisterActivityWithOptions(r.activities.ToolExecActivity, activity.RegisterOptions{
		Name: orchestration.ToolExecActivityName,
	})
	nativeControlWorker.RegisterActivityWithOptions(r.activities.SnapshotHistoryActivity, activity.RegisterOptions{
		Name: orchestration.SnapshotHistoryActivityName,
	})
	nativeControlWorker.RegisterActivityWithOptions(r.activities.LoadHistoryActivity, activity.RegisterOptions{
		Name: orchestration.LoadHistoryActivityName,
	})
	streamWorker.RegisterActivityWithOptions(r.activities.InitStreamActivity, activity.RegisterOptions{
		Name: orchestration.InitStreamActivityName,
	})
	streamWorker.RegisterActivityWithOptions(r.activities.LLMStreamActivity, activity.RegisterOptions{
		Name: orchestration.LLMStreamActivityName,
	})
	streamWorker.RegisterActivityWithOptions(r.activities.ToolExecStreamActivity, activity.RegisterOptions{
		Name: orchestration.ToolExecStreamActivityName,
	})
	streamWorker.RegisterActivityWithOptions(r.activities.FinishStreamActivity, activity.RegisterOptions{
		Name: orchestration.FinishStreamActivityName,
	})

	return nil
}

func (r DefaultRegistrar) requiredWorker(rt *TemporalRuntime, taskQueue, label string) (worker.Worker, error) {
	if rt == nil {
		return nil, errRegistrarRuntimeRequired
	}

	w, ok := rt.WorkerFor(taskQueue)
	if !ok || w == nil {
		return nil, fmt.Errorf("%w: %s task queue %q", errRegistrarTaskQueueUnconfigured, label, taskQueue)
	}

	return w, nil
}
