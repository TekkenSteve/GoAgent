package orchestration

import (
	"errors"
	"fmt"

	"go.temporal.io/sdk/temporal"
)

// ErrWorkflowTaskQueuesInvalid reports invalid or missing workflow task
// queue configuration; it is returned by the WorkflowTaskQueues validation
// methods.
var ErrWorkflowTaskQueuesInvalid = errors.New("agentfw workflow task queues: invalid")

// ValidateNativeAgent ensures the task queues required by the native agent
// workflow (control, LLM activity, tool activity) are set and mutually
// distinct.
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

// ValidateStreamAgent ensures the task queues required by the stream agent
// workflow (control, stream activity, LLM activity, tool activity) are set
// and mutually distinct.
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
