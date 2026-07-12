package temporal

import (
	"errors"
	"fmt"
)

const (
	currentPlanWorkflowVersion             = 1
	currentProcessWorkflowVersion          = 1
	currentScheduleDispatchWorkflowVersion = 1
)

var errTemporalWorkflowVersionInvalid = errors.New("agentos temporal workflow version: invalid")

type workflowVersionPin struct {
	Name    string
	Version int
}

func agentOSWorkflowVersionPins() []workflowVersionPin {
	return []workflowVersionPin{
		{Name: PlanWorkflowName, Version: currentPlanWorkflowVersion},
		{Name: ProcessWorkflowName, Version: currentProcessWorkflowVersion},
		{Name: ScheduleDispatchWorkflowName, Version: currentScheduleDispatchWorkflowVersion},
	}
}

func validatePlanWorkflowVersion(got int) error {
	return validateWorkflowVersion(PlanWorkflowName, got, currentPlanWorkflowVersion)
}

func validateProcessWorkflowVersion(got int) error {
	return validateWorkflowVersion(ProcessWorkflowName, got, currentProcessWorkflowVersion)
}

func validateScheduleDispatchWorkflowVersion(got int) error {
	return validateWorkflowVersion(ScheduleDispatchWorkflowName, got, currentScheduleDispatchWorkflowVersion)
}

func validateWorkflowVersion(workflowName string, got, want int) error {
	if got == want {
		return nil
	}

	return fmt.Errorf("%w: %s got %d want %d", errTemporalWorkflowVersionInvalid, workflowName, got, want)
}
