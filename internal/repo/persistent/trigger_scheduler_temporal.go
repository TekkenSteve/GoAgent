package persistent

import (
	"context"
	"fmt"
	"strings"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/client"
)

// TemporalTriggerScheduler implements repo.TriggerScheduler using
// Temporal cron workflows. Each scheduled trigger creates a cron-scheduled
// TriggerFireWorkflow that dispatches agent execution.
type TemporalTriggerScheduler struct {
	client    client.Client
	taskQueue string
}

// NewTemporalTriggerScheduler creates a new Temporal-based trigger scheduler.
func NewTemporalTriggerScheduler(c client.Client, taskQueue string) *TemporalTriggerScheduler {
	return &TemporalTriggerScheduler{
		client:    c,
		taskQueue: taskQueue,
	}
}

// Schedule creates a Temporal cron workflow for the given trigger.
func (s *TemporalTriggerScheduler) Schedule(ctx context.Context, trigger *entity.TriggerSpec) error {
	workflowID := triggerWorkflowID(trigger.ID)

	opts := client.StartWorkflowOptions{
		ID:           workflowID,
		TaskQueue:    s.taskQueue,
		CronSchedule: trigger.CronExpression,
	}

	_, err := s.client.ExecuteWorkflow(ctx, opts, orchestration.TriggerFireWorkflowName, trigger.ID)
	if err != nil {
		return fmt.Errorf("TemporalTriggerScheduler - Schedule - execute: %w", err)
	}

	return nil
}

// Unschedule terminates the cron workflow for the given trigger.
func (s *TemporalTriggerScheduler) Unschedule(ctx context.Context, triggerID string) error {
	workflowID := triggerWorkflowID(triggerID)

	err := s.client.TerminateWorkflow(ctx, workflowID, "", "trigger unscheduled")
	if err != nil {
		// Ignore not-found — the workflow may not exist or was already terminated
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "NotFound") {
			return nil
		}

		return fmt.Errorf("TemporalTriggerScheduler - Unschedule - terminate: %w", err)
	}

	return nil
}

func triggerWorkflowID(triggerID string) string {
	return "trigger-cron-" + triggerID
}
