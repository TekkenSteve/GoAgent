package trigger

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo"
)

// UseCase manages trigger lifecycle and coordinates scheduling.
type UseCase struct {
	triggerRepo  repo.TriggerRepo
	scheduler    repo.TriggerScheduler
	templateRepo repo.WorkflowTemplateRepo
}

// New creates a trigger usecase.
func New(triggerRepo repo.TriggerRepo, scheduler repo.TriggerScheduler, templateRepo repo.WorkflowTemplateRepo) *UseCase {
	return &UseCase{
		triggerRepo:  triggerRepo,
		scheduler:    scheduler,
		templateRepo: templateRepo,
	}
}

// Create creates a new trigger and schedules it if it's a scheduled trigger.
func (uc *UseCase) Create(ctx context.Context, req entity.CreateTriggerRequest) (entity.TriggerSpec, error) {
	if req.Name == "" {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Create - name is required")
	}
	if req.TemplateID == "" {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Create - template_id is required")
	}
	if req.TriggerType == entity.TriggerSchedule && req.CronExpression == "" {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Create - cron_expression is required for schedule triggers")
	}

	// Persist the trigger first
	trigger, err := uc.triggerRepo.Create(ctx, req)
	if err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Create - repo: %w", err)
	}

	// Schedule with Temporal if it's an active scheduled trigger
	if trigger.IsActive && trigger.TriggerType == entity.TriggerSchedule {
		if err := uc.scheduler.Schedule(ctx, trigger); err != nil {
			// Log but don't fail — the trigger is persisted; scheduling can be retried
			return trigger, fmt.Errorf("TriggerUseCase - Create - persisted but schedule failed: %w", err)
		}
	}

	return trigger, nil
}

// Get retrieves a trigger by ID.
func (uc *UseCase) Get(ctx context.Context, triggerID string) (entity.TriggerSpec, error) {
	record, exists, err := uc.triggerRepo.Get(ctx, triggerID)
	if err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Get - repo: %w", err)
	}
	if !exists {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Get - not found: %s", triggerID)
	}
	return record, nil
}

// Update updates a trigger and reschedules if the cron expression changed.
func (uc *UseCase) Update(ctx context.Context, triggerID string, req entity.UpdateTriggerRequest) (entity.TriggerSpec, error) {
	// Read current trigger to detect schedule changes
	current, err := uc.Get(ctx, triggerID)
	if err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Update - get current: %w", err)
	}

	trigger, err := uc.triggerRepo.Update(ctx, triggerID, req)
	if err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Update - repo: %w", err)
	}

	// Reschedule if this is a scheduled trigger and the schedule may have changed
	if current.TriggerType == entity.TriggerSchedule {
		wasActive := current.IsActive
		nowActive := trigger.IsActive
		cronChanged := req.CronExpression != nil

		if wasActive && (!nowActive || cronChanged) {
			// Unschedule the old cron workflow
			_ = uc.scheduler.Unschedule(ctx, triggerID)
		}
		if nowActive && (!wasActive || cronChanged) {
			// Schedule the (possibly updated) cron workflow
			_ = uc.scheduler.Schedule(ctx, trigger)
		}
	}

	return trigger, nil
}

// Delete removes a trigger and unschedules it if it was scheduled.
func (uc *UseCase) Delete(ctx context.Context, triggerID string) error {
	// Read current to know if we need to unschedule
	current, err := uc.Get(ctx, triggerID)
	if err != nil {
		return fmt.Errorf("TriggerUseCase - Delete - get current: %w", err)
	}

	if err := uc.triggerRepo.Delete(ctx, triggerID); err != nil {
		return fmt.Errorf("TriggerUseCase - Delete - repo: %w", err)
	}

	// Unschedule if it was an active scheduled trigger
	if current.TriggerType == entity.TriggerSchedule && current.IsActive {
		if err := uc.scheduler.Unschedule(ctx, triggerID); err != nil {
			return fmt.Errorf("TriggerUseCase - Delete - unschedule: %w", err)
		}
	}

	return nil
}

// ListByTemplate retrieves all triggers for a template.
func (uc *UseCase) ListByTemplate(ctx context.Context, templateID string) ([]entity.TriggerSpec, error) {
	return uc.triggerRepo.ListByTemplate(ctx, templateID)
}

// Fire validates a trigger exists and records the fire time.
// Called by FireTriggerActivity when a cron schedule fires.
// Returns the trigger spec so the caller can use it for further processing.
func (uc *UseCase) Fire(ctx context.Context, triggerID string) (entity.TriggerSpec, error) {
	trigger, err := uc.Get(ctx, triggerID)
	if err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Fire - get trigger: %w", err)
	}

	if err := uc.triggerRepo.RecordFired(ctx, triggerID); err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Fire - record fired: %w", err)
	}

	return trigger, nil
}

// Toggle enables or disables a trigger by ID.
func (uc *UseCase) Toggle(ctx context.Context, triggerID string, isActive bool) (entity.TriggerSpec, error) {
	return uc.Update(ctx, triggerID, entity.UpdateTriggerRequest{
		IsActive: &isActive,
	})
}

// LogTriggerExecution records a rich audit trail for a trigger execution.
// Called by FireTriggerActivity after prompt resolution to log the actual
// prompt used, execution variables, and any error.  Logging is best-effort.
func (uc *UseCase) LogTriggerExecution(ctx context.Context, triggerID string, opts entity.TriggerEventLog) {
	_ = uc.triggerRepo.InsertTriggerEvent(ctx, opts)
}
