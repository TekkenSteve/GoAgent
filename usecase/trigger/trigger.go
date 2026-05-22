package trigger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/repo"
	"github.com/google/uuid"
)

var (
	ErrTriggerNameRequired     = errors.New("trigger name is required")
	ErrTriggerTemplateRequired = errors.New("trigger template_id is required")
	ErrTriggerCronRequired     = errors.New("cron_expression is required for schedule triggers")
	ErrTriggerNotFound         = errors.New("trigger not found")
	ErrNoActiveTriggers        = errors.New("no active triggers for event")
	ErrAllTriggersFailed       = errors.New("all matching triggers failed")
	ErrTemplateNotFound        = errors.New("template not found for trigger")
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
func (uc *UseCase) Create(ctx context.Context, req *entity.CreateTriggerRequest) (entity.TriggerSpec, error) {
	if req.Name == "" {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Create - %w", ErrTriggerNameRequired)
	}

	if req.TemplateID == "" {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Create - %w", ErrTriggerTemplateRequired)
	}

	if req.TriggerType == entity.TriggerSchedule && req.CronExpression == "" {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Create - %w", ErrTriggerCronRequired)
	}

	// Persist the trigger first
	trigger, err := uc.triggerRepo.Create(ctx, req)
	if err != nil {
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Create - repo: %w", err)
	}

	// Schedule with Temporal if it's an active scheduled trigger
	if trigger.IsActive && trigger.TriggerType == entity.TriggerSchedule {
		if err := uc.scheduler.Schedule(ctx, &trigger); err != nil {
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
		return entity.TriggerSpec{}, fmt.Errorf("TriggerUseCase - Get - %w: %s", ErrTriggerNotFound, triggerID)
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
	if err := uc.rescheduleTrigger(ctx, &current, req, &trigger); err != nil {
		return trigger, fmt.Errorf("TriggerUseCase - Update - reschedule: %w", err)
	}

	return trigger, nil
}

// rescheduleTrigger handles unscheduling and rescheduling of cron triggers
// when the schedule or active state changes.
func (uc *UseCase) rescheduleTrigger(ctx context.Context, current *entity.TriggerSpec, req entity.UpdateTriggerRequest, updated *entity.TriggerSpec) error {
	if current.TriggerType != entity.TriggerSchedule {
		return nil
	}

	wasActive := current.IsActive
	nowActive := updated.IsActive
	cronChanged := req.CronExpression != nil

	if wasActive && (!nowActive || cronChanged) {
		// Unschedule the old cron workflow
		if err := uc.scheduler.Unschedule(ctx, current.ID); err != nil {
			return fmt.Errorf("unschedule: %w", err)
		}
	}

	if nowActive && (!wasActive || cronChanged) {
		// Schedule the (possibly updated) cron workflow
		if err := uc.scheduler.Schedule(ctx, updated); err != nil {
			return fmt.Errorf("schedule: %w", err)
		}
	}

	return nil
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
func (uc *UseCase) LogTriggerExecution(ctx context.Context, _ string, opts *entity.TriggerEventLog) {
	if err := uc.triggerRepo.InsertTriggerEvent(ctx, opts); err != nil {
		_ = err // Logging is best-effort; discard InsertTriggerEvent errors.
	}
}

// HandleEvent processes an incoming webhook event for event-type triggers.
// It finds all active event triggers matching the event_slug, resolves prompt
// variables from the payload, records the fire, and returns resolved data
// for workflow execution. Returns an error if no matching triggers are found.
func (uc *UseCase) HandleEvent(ctx context.Context, eventSlug string, payload map[string]string) ([]entity.TriggerFireResult, error) {
	allActive, err := uc.triggerRepo.ListActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("TriggerUseCase - HandleEvent - list active: %w", err)
	}

	var matches []entity.TriggerSpec

	for i := range allActive {
		t := allActive[i]
		if t.TriggerType == entity.TriggerEvent && t.EventSlug == eventSlug {
			matches = append(matches, t)
		}
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("TriggerUseCase - HandleEvent - %w: %s", ErrNoActiveTriggers, eventSlug)
	}

	results := make([]entity.TriggerFireResult, 0, len(matches))
	for i := range matches {
		result, err := uc.dispatchEvent(ctx, &matches[i], payload)
		if err != nil {
			continue
		}

		if result != nil {
			results = append(results, *result)
		}
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("TriggerUseCase - HandleEvent - %w: %s", ErrAllTriggersFailed, eventSlug)
	}

	return results, nil
}

// dispatchEvent processes a single trigger match: records the fire, resolves
// prompt variables from the payload, loads the template, and returns a
// TriggerFireResult for workflow execution.
func (uc *UseCase) dispatchEvent(ctx context.Context, t *entity.TriggerSpec, payload map[string]string) (*entity.TriggerFireResult, error) {
	// Record the fire timestamp
	if err := uc.triggerRepo.RecordFired(ctx, t.ID); err != nil {
		uc.LogTriggerExecution(ctx, t.ID, &entity.TriggerEventLog{
			TriggerID:   t.ID,
			TemplateID:  t.TemplateID,
			TriggerType: t.TriggerType,
			Success:     false,
			Message:     fmt.Sprintf("RecordFired failed: %v", err),
			FiredAt:     time.Now().UTC(),
		})

		return nil, err
	}

	// Resolve prompt variables from the webhook payload
	specCopy := *t
	specCopy.TemplateVarsVals = payload
	resolvedPrompt := specCopy.ResolvePrompt()

	// Load template for system prompt and default model
	tmpl, exists, err := uc.templateRepo.Get(ctx, t.TemplateID)
	if err != nil || !exists {
		return nil, fmt.Errorf("%w %s", ErrTemplateNotFound, t.ID)
	}

	systemPrompt := tmpl.SystemPrompt
	if resolvedPrompt != "" {
		if systemPrompt != "" {
			systemPrompt += "\n" + resolvedPrompt
		} else {
			systemPrompt = resolvedPrompt
		}
	}

	model := tmpl.DefaultModel
	if model == "" {
		model = "gpt-4"
	}

	runID := uuid.New().String()
	now := time.Now().UTC()

	// Rich audit log
	uc.LogTriggerExecution(ctx, t.ID, &entity.TriggerEventLog{
		TriggerID:     t.ID,
		TemplateID:    t.TemplateID,
		TriggerType:   t.TriggerType,
		Success:       true,
		AgentPrompt:   resolvedPrompt,
		ExecVariables: payload,
		FiredAt:       now,
	})

	return &entity.TriggerFireResult{
		TriggerID:    t.ID,
		RunID:        runID,
		Name:         t.Name,
		SystemPrompt: systemPrompt,
		Message:      resolvedPrompt,
		ModelRef:     model,
		FiredAt:      now,
	}, nil
}
