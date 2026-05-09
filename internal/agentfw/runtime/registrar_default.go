package runtime

import (
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
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
	rt.Worker.RegisterWorkflowWithOptions(orchestration.StreamWorkflow, workflow.RegisterOptions{
		Name: orchestration.StreamWorkflowName,
	})
}

// RegisterActivities registers activity definitions.
func (r DefaultRegistrar) RegisterActivities(rt *TemporalRuntime) {
	if r.activities != nil {
		rt.Worker.RegisterActivity(r.activities)
	}
}
