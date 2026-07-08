package orchestration

import (
	"errors"
	"fmt"

	"go.temporal.io/sdk/temporal"
)

var ErrWorkflowTaskQueuesInvalid = errors.New("agentfw workflow task queues: invalid")

func (q *WorkflowTaskQueues) ValidateNativeAgent() error {
	if q == nil {
		return fmt.Errorf("%w: task queues are required", ErrWorkflowTaskQueuesInvalid)
	}

	return validateWorkflowTaskQueues([]workflowTaskQueueField{
		{label: "native control", value: q.NativeControl},
		{label: "native llm activity", value: q.NativeLLM},
		{label: "native tool activity", value: q.NativeTool},
	})
}

func (q *WorkflowTaskQueues) ValidateStreamAgent() error {
	if q == nil {
		return fmt.Errorf("%w: task queues are required", ErrWorkflowTaskQueuesInvalid)
	}

	return validateWorkflowTaskQueues([]workflowTaskQueueField{
		{label: "stream activity", value: q.Stream},
		{label: "native control", value: q.NativeControl},
		{label: "native llm activity", value: q.NativeLLM},
		{label: "native tool activity", value: q.NativeTool},
	})
}

func (q *WorkflowTaskQueues) ValidateTriggerFire() error {
	if q == nil {
		return fmt.Errorf("%w: task queues are required", ErrWorkflowTaskQueuesInvalid)
	}

	return validateWorkflowTaskQueues([]workflowTaskQueueField{
		{label: "trigger activity", value: q.Trigger},
		{label: "native control", value: q.NativeControl},
		{label: "native llm activity", value: q.NativeLLM},
		{label: "native tool activity", value: q.NativeTool},
	})
}

type workflowTaskQueueField struct {
	label string
	value string
}

func validateWorkflowTaskQueues(fields []workflowTaskQueueField) error {
	seen := make(map[string]string, len(fields))
	for _, field := range fields {
		if field.value == "" {
			return fmt.Errorf("%w: %s task queue is required", ErrWorkflowTaskQueuesInvalid, field.label)
		}

		if existing, ok := seen[field.value]; ok {
			return fmt.Errorf("%w: %s and %s use the same task queue %q", ErrWorkflowTaskQueuesInvalid, existing, field.label, field.value)
		}

		seen[field.value] = field.label
	}

	return nil
}

func nonRetryableWorkflowValidationError(err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), "validation", err)
}
