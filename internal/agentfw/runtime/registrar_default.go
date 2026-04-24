package runtime

import (
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// DefaultRegistrar registers the framework's baseline workflow and activities.
type DefaultRegistrar struct{}

// NewDefaultRegistrar creates a baseline registrar instance.
func NewDefaultRegistrar() DefaultRegistrar {
	return DefaultRegistrar{}
}

// RegisterWorkflows registers workflow definitions.
func (DefaultRegistrar) RegisterWorkflows(rt *TemporalRuntime) {
	rt.Worker.RegisterWorkflowWithOptions(orchestration.AgentWorkflow, workflow.RegisterOptions{
		Name: orchestration.AgentWorkflowName,
	})
}

// RegisterActivities registers activity definitions.
func (DefaultRegistrar) RegisterActivities(rt *TemporalRuntime) {
	rt.Worker.RegisterActivityWithOptions(orchestration.EchoActivity, activity.RegisterOptions{
		Name: orchestration.EchoActivityName,
	})
}
