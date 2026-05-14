package runtime

import (
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
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
func (r DefaultRegistrar) RegisterWorkflows(rt *TemporalRuntime) {
	rt.Worker.RegisterWorkflowWithOptions(orchestration.AgentWorkflow, workflow.RegisterOptions{
		Name: orchestration.AgentWorkflowName,
	})
	rt.Worker.RegisterWorkflowWithOptions(orchestration.StreamAgentWorkflow, workflow.RegisterOptions{
		Name: orchestration.StreamWorkflowName,
	})
	rt.Worker.RegisterWorkflowWithOptions(orchestration.OrchestrationWorkflow, workflow.RegisterOptions{
		Name: orchestration.OrchestrationWorkflowName,
	})
	rt.Worker.RegisterWorkflowWithOptions(orchestration.TriggerFireWorkflow, workflow.RegisterOptions{
		Name: orchestration.TriggerFireWorkflowName,
	})
}

// RegisterActivities registers activity definitions.
func (r DefaultRegistrar) RegisterActivities(rt *TemporalRuntime) {
	if r.activities == nil {
		return
	}
	// Register each activity individually with the name the workflow uses
	// (RegisterActivity without options would use the reflection-based function name,
	// which won't match the workflow's activity type name).
	rt.Worker.RegisterActivityWithOptions(r.activities.PrepareActivity, activity.RegisterOptions{
		Name: orchestration.PrepareActivityName,
	})
	rt.Worker.RegisterActivityWithOptions(r.activities.LLMStepActivity, activity.RegisterOptions{
		Name: orchestration.LLMStepActivityName,
	})
	rt.Worker.RegisterActivityWithOptions(r.activities.ToolExecActivity, activity.RegisterOptions{
		Name: orchestration.ToolExecActivityName,
	})
	rt.Worker.RegisterActivityWithOptions(r.activities.InitStreamActivity, activity.RegisterOptions{
		Name: orchestration.InitStreamActivityName,
	})
	rt.Worker.RegisterActivityWithOptions(r.activities.LLMStreamActivity, activity.RegisterOptions{
		Name: orchestration.LLMStreamActivityName,
	})
	rt.Worker.RegisterActivityWithOptions(r.activities.ToolExecStreamActivity, activity.RegisterOptions{
		Name: orchestration.ToolExecStreamActivityName,
	})
	rt.Worker.RegisterActivityWithOptions(r.activities.FinishStreamActivity, activity.RegisterOptions{
		Name: orchestration.FinishStreamActivityName,
	})
	rt.Worker.RegisterActivityWithOptions(r.activities.FireTriggerActivity, activity.RegisterOptions{
		Name: orchestration.FireTriggerActivityName,
	})
}
