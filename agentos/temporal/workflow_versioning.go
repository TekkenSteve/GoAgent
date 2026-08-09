package temporal

import (
	"errors"
	"fmt"
)

const (
	currentPlanWorkflowVersion            = 1
	currentProcessWorkflowVersion         = 1
	currentTriggerDispatchWorkflowVersion = 2

	// workflowVersionCompatibilityWindow is how many previous workflow
	// versions remain replayable after a version bump. In-flight workflows
	// started on the previous version must keep replaying when the new worker
	// code is deployed; structural differences between versions are bridged
	// with workflow.GetVersion guards in the workflow code (never by failing
	// validation on replay).
	workflowVersionCompatibilityWindow = 1
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
		{Name: TriggerDispatchWorkflowName, Version: currentTriggerDispatchWorkflowVersion},
	}
}

func validatePlanWorkflowVersion(got int) error {
	return validateWorkflowVersion(PlanWorkflowName, got, currentPlanWorkflowVersion)
}

func validateProcessWorkflowVersion(got int) error {
	return validateWorkflowVersion(ProcessWorkflowName, got, currentProcessWorkflowVersion)
}

func validateTriggerDispatchWorkflowVersion(got int) error {
	return validateWorkflowVersion(TriggerDispatchWorkflowName, got, currentTriggerDispatchWorkflowVersion)
}

// validateWorkflowVersion rejects only workflows outside the replay
// compatibility window. A run pinned to the current version or up to
// workflowVersionCompatibilityWindow versions older is accepted, so a rolling
// deploy that bumps the version constant does not kill in-flight runs on
// replay. Version 0 (unpinned) and versions newer than the worker are rejected.
func validateWorkflowVersion(workflowName string, got, want int) error {
	minAccepted := max(want-workflowVersionCompatibilityWindow, 1)

	if got >= minAccepted && got <= want {
		return nil
	}

	return fmt.Errorf("%w: %s got %d outside replay compatibility window for worker version %d", errTemporalWorkflowVersionInvalid, workflowName, got, want)
}
