package trigger_test

import (
	"context"
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase/trigger"
)

const testTriggerID = "t-1"

var (
	errDB                   = errors.New("db error")
	errSchedulerUnavailable = errors.New("scheduler unavailable")
)

// -- mockTriggerRepo --

type mockTriggerRepo struct {
	createFunc             func(context.Context, *entity.CreateTriggerRequest) (entity.TriggerSpec, error)
	getFunc                func(context.Context, string) (entity.TriggerSpec, bool, error)
	updateFunc             func(context.Context, string, entity.UpdateTriggerRequest) (entity.TriggerSpec, error)
	deleteFunc             func(context.Context, string) error
	listByTemplateFunc     func(context.Context, string) ([]entity.TriggerSpec, error)
	listByTypeFunc         func(context.Context, entity.TriggerType) ([]entity.TriggerSpec, error)
	listActiveFunc         func(context.Context) ([]entity.TriggerSpec, error)
	recordFiredFunc        func(context.Context, string) error
	insertTriggerEventFunc func(context.Context, *entity.TriggerEventLog) error
}

func (m *mockTriggerRepo) Create(ctx context.Context, req *entity.CreateTriggerRequest) (entity.TriggerSpec, error) {
	if m.createFunc != nil {
		return m.createFunc(ctx, req)
	}

	panic("unexpected call to Create")
}

func (m *mockTriggerRepo) Get(ctx context.Context, triggerID string) (entity.TriggerSpec, bool, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, triggerID)
	}

	panic("unexpected call to Get")
}

func (m *mockTriggerRepo) Update(ctx context.Context, triggerID string, req entity.UpdateTriggerRequest) (entity.TriggerSpec, error) {
	if m.updateFunc != nil {
		return m.updateFunc(ctx, triggerID, req)
	}

	panic("unexpected call to Update")
}

func (m *mockTriggerRepo) Delete(ctx context.Context, triggerID string) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, triggerID)
	}

	panic("unexpected call to Delete")
}

func (m *mockTriggerRepo) ListByTemplate(ctx context.Context, templateID string) ([]entity.TriggerSpec, error) {
	if m.listByTemplateFunc != nil {
		return m.listByTemplateFunc(ctx, templateID)
	}

	panic("unexpected call to ListByTemplate")
}

func (m *mockTriggerRepo) ListByType(ctx context.Context, triggerType entity.TriggerType) ([]entity.TriggerSpec, error) {
	if m.listByTypeFunc != nil {
		return m.listByTypeFunc(ctx, triggerType)
	}

	panic("unexpected call to ListByType")
}

func (m *mockTriggerRepo) ListActive(ctx context.Context) ([]entity.TriggerSpec, error) {
	if m.listActiveFunc != nil {
		return m.listActiveFunc(ctx)
	}

	panic("unexpected call to ListActive")
}

func (m *mockTriggerRepo) RecordFired(ctx context.Context, triggerID string) error {
	if m.recordFiredFunc != nil {
		return m.recordFiredFunc(ctx, triggerID)
	}

	panic("unexpected call to RecordFired")
}

func (m *mockTriggerRepo) InsertTriggerEvent(ctx context.Context, event *entity.TriggerEventLog) error {
	if m.insertTriggerEventFunc != nil {
		return m.insertTriggerEventFunc(ctx, event)
	}

	panic("unexpected call to InsertTriggerEvent")
}

// -- mockTriggerScheduler --

type mockTriggerScheduler struct {
	scheduleFunc   func(context.Context, *entity.TriggerSpec) error
	unscheduleFunc func(context.Context, string) error
}

func (m *mockTriggerScheduler) Schedule(ctx context.Context, spec *entity.TriggerSpec) error {
	if m.scheduleFunc != nil {
		return m.scheduleFunc(ctx, spec)
	}

	panic("unexpected call to Schedule")
}

func (m *mockTriggerScheduler) Unschedule(ctx context.Context, triggerID string) error {
	if m.unscheduleFunc != nil {
		return m.unscheduleFunc(ctx, triggerID)
	}

	panic("unexpected call to Unschedule")
}

// -- helper --

func newTriggerUC(t *testing.T, r *mockTriggerRepo, s *mockTriggerScheduler) *trigger.UseCase {
	t.Helper()

	return trigger.New(r, s, nil)
}

// expectNoScheduleScheduler creates a scheduler that fails the test if Schedule is called.
func expectNoScheduleScheduler(t *testing.T) *mockTriggerScheduler {
	t.Helper()

	return &mockTriggerScheduler{
		scheduleFunc: func(_ context.Context, _ *entity.TriggerSpec) error {
			t.Error("unexpected Schedule call")

			return nil
		},
	}
}

// expectNoUnscheduleScheduler creates a scheduler that fails the test if Unschedule is called.
func expectNoUnscheduleScheduler(t *testing.T) *mockTriggerScheduler {
	t.Helper()

	return &mockTriggerScheduler{
		unscheduleFunc: func(_ context.Context, _ string) error {
			t.Error("unexpected Unschedule call")

			return nil
		},
	}
}

// createTriggerWithNoSchedule creates a trigger via the use case using a scheduler
// that should not receive Schedule calls.
func createTriggerWithNoSchedule(t *testing.T, createFunc func(context.Context, *entity.CreateTriggerRequest) (entity.TriggerSpec, error), req *entity.CreateTriggerRequest) {
	t.Helper()

	repo := &mockTriggerRepo{
		createFunc: createFunc,
	}
	uc := newTriggerUC(t, repo, expectNoScheduleScheduler(t))

	_, err := uc.Create(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

// deleteTriggerWithNoUnschedule deletes a trigger via the use case using a scheduler
// that should not receive Unschedule calls.
func deleteTriggerWithNoUnschedule(t *testing.T, spec *entity.TriggerSpec) {
	t.Helper()

	repo := &mockTriggerRepo{
		getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
			return *spec, true, nil
		},
		deleteFunc: func(_ context.Context, _ string) error { return nil },
	}

	uc := newTriggerUC(t, repo, expectNoUnscheduleScheduler(t))
	if err := uc.Delete(context.Background(), testTriggerID); err != nil {
		t.Fatal(err)
	}
}

// -- Create --

func TestTriggerCreate_Validation(t *testing.T) {
	t.Parallel()

	t.Run("empty name", func(t *testing.T) {
		t.Parallel()

		uc := newTriggerUC(t, &mockTriggerRepo{}, &mockTriggerScheduler{})

		_, err := uc.Create(context.Background(), &entity.CreateTriggerRequest{
			TemplateID: testTriggerID,
		})
		if err == nil {
			t.Fatal("expected error for empty name")
		}
	})

	t.Run("empty template id", func(t *testing.T) {
		t.Parallel()
		uc := newTriggerUC(t, &mockTriggerRepo{}, &mockTriggerScheduler{})

		_, err := uc.Create(context.Background(), &entity.CreateTriggerRequest{
			Name: "test",
		})
		if err == nil {
			t.Fatal("expected error for empty template_id")
		}
	})

	t.Run("schedule without cron", func(t *testing.T) {
		t.Parallel()
		uc := newTriggerUC(t, &mockTriggerRepo{}, &mockTriggerScheduler{})

		_, err := uc.Create(context.Background(), &entity.CreateTriggerRequest{
			Name:        "test",
			TemplateID:  testTriggerID,
			TriggerType: entity.TriggerSchedule,
		})
		if err == nil {
			t.Fatal("expected error for schedule without cron_expression")
		}
	})
}

func TestTriggerCreate_Success(t *testing.T) {
	t.Parallel()

	t.Run("active schedule schedules via cron", testTriggerCreateActiveSchedule)
	t.Run("inactive schedule does not schedule", testTriggerCreateInactiveSchedule)
	t.Run("event trigger does not schedule", testTriggerCreateEventTrigger)
}

func testTriggerCreateActiveSchedule(t *testing.T) {
	t.Parallel()

	var scheduled bool

	repo := &mockTriggerRepo{
		createFunc: func(_ context.Context, req *entity.CreateTriggerRequest) (entity.TriggerSpec, error) {
			return entity.TriggerSpec{
				ID:             testTriggerID,
				Name:           req.Name,
				TemplateID:     req.TemplateID,
				TriggerType:    req.TriggerType,
				CronExpression: req.CronExpression,
				IsActive:       req.IsActive,
			}, nil
		},
	}
	uc := newTriggerUC(t, repo, &mockTriggerScheduler{
		scheduleFunc: func(_ context.Context, _ *entity.TriggerSpec) error {
			scheduled = true

			return nil
		},
	})

	_, err := uc.Create(context.Background(), &entity.CreateTriggerRequest{
		Name:           "Daily",
		TemplateID:     testTriggerID,
		TriggerType:    entity.TriggerSchedule,
		CronExpression: "0 9 * * *",
		IsActive:       true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !scheduled {
		t.Error("expected Schedule call for active scheduled trigger")
	}
}

func testTriggerCreateInactiveSchedule(t *testing.T) {
	t.Parallel()
	createTriggerWithNoSchedule(
		t,
		func(_ context.Context, req *entity.CreateTriggerRequest) (entity.TriggerSpec, error) {
			return entity.TriggerSpec{
				ID:             testTriggerID,
				CronExpression: req.CronExpression,
				TriggerType:    req.TriggerType,
				IsActive:       false,
			}, nil
		},
		&entity.CreateTriggerRequest{
			Name:           "Inactive",
			TemplateID:     testTriggerID,
			TriggerType:    entity.TriggerSchedule,
			CronExpression: "0 9 * * *",
			IsActive:       false,
		},
	)
}

func testTriggerCreateEventTrigger(t *testing.T) {
	t.Parallel()
	createTriggerWithNoSchedule(
		t,
		func(_ context.Context, req *entity.CreateTriggerRequest) (entity.TriggerSpec, error) {
			return entity.TriggerSpec{
				ID:          testTriggerID,
				TriggerType: req.TriggerType,
				EventSlug:   req.EventSlug,
				IsActive:    true,
			}, nil
		},
		&entity.CreateTriggerRequest{
			Name:        "Webhook",
			TemplateID:  testTriggerID,
			TriggerType: entity.TriggerEvent,
			EventSlug:   "order.created",
			IsActive:    true,
		},
	)
}

func TestTriggerCreate_Errors(t *testing.T) {
	t.Parallel()

	t.Run("repo error propagated", func(t *testing.T) {
		t.Parallel()

		repo := &mockTriggerRepo{
			createFunc: func(_ context.Context, _ *entity.CreateTriggerRequest) (entity.TriggerSpec, error) {
				return entity.TriggerSpec{}, errDB
			},
		}
		uc := newTriggerUC(t, repo, &mockTriggerScheduler{})

		_, err := uc.Create(context.Background(), &entity.CreateTriggerRequest{
			Name:           "test",
			TemplateID:     testTriggerID,
			TriggerType:    entity.TriggerSchedule,
			CronExpression: "0 9 * * *",
		})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("schedule failure returns trigger with warning", func(t *testing.T) {
		t.Parallel()

		repo := &mockTriggerRepo{
			createFunc: func(_ context.Context, req *entity.CreateTriggerRequest) (entity.TriggerSpec, error) {
				return entity.TriggerSpec{
					ID:             testTriggerID,
					TriggerType:    req.TriggerType,
					CronExpression: req.CronExpression,
					IsActive:       true,
				}, nil
			},
		}
		scheduler := &mockTriggerScheduler{
			scheduleFunc: func(_ context.Context, _ *entity.TriggerSpec) error {
				return errSchedulerUnavailable
			},
		}
		uc := newTriggerUC(t, repo, scheduler)

		result, err := uc.Create(context.Background(), &entity.CreateTriggerRequest{
			Name:           "Daily",
			TemplateID:     testTriggerID,
			TriggerType:    entity.TriggerSchedule,
			CronExpression: "0 9 * * *",
			IsActive:       true,
		})
		if err == nil {
			t.Fatal("expected error when scheduler fails")
		}

		if result.ID != testTriggerID {
			t.Error("expected trigger to be returned despite schedule failure")
		}
	})
}

// -- Get --

func TestTriggerGet(t *testing.T) {
	t.Parallel()

	t.Run("found", func(t *testing.T) {
		t.Parallel()

		repo := &mockTriggerRepo{
			getFunc: func(_ context.Context, triggerID string) (entity.TriggerSpec, bool, error) {
				return entity.TriggerSpec{ID: triggerID, Name: "test", TemplateID: testTriggerID}, true, nil
			},
		}
		uc := newTriggerUC(t, repo, &mockTriggerScheduler{})

		result, err := uc.Get(context.Background(), testTriggerID)
		if err != nil {
			t.Fatal(err)
		}

		if result.ID != testTriggerID {
			t.Errorf("expected t-1, got %s", result.ID)
		}
	})
	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		repo := &mockTriggerRepo{
			getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
				return entity.TriggerSpec{}, false, nil
			},
		}
		uc := newTriggerUC(t, repo, &mockTriggerScheduler{})

		_, err := uc.Get(context.Background(), "missing")
		if err == nil {
			t.Fatal("expected error for not found")
		}
	})
}

// -- Update (rescheduling logic) --

func activeScheduleTrigger() entity.TriggerSpec {
	return entity.TriggerSpec{
		ID:             testTriggerID,
		TemplateID:     "tmpl-1",
		TriggerType:    entity.TriggerSchedule,
		CronExpression: "0 9 * * *",
		IsActive:       true,
	}
}

func TestTriggerUpdate_Rescheduling(t *testing.T) {
	t.Parallel()

	t.Run("disable active unschedules", testTriggerUpdateDisableActive)
	t.Run("enable inactive schedules", testTriggerUpdateEnableInactive)
	t.Run("cron change reschedules", testTriggerUpdateCronChange)
	t.Run("non-config change triggers no scheduler", testTriggerUpdateNonConfig)
	t.Run("event trigger update no scheduling", testTriggerUpdateEventNoSched)
}

func testTriggerUpdateDisableActive(t *testing.T) {
	t.Parallel()

	var unscheduled bool

	repo := &mockTriggerRepo{
		getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
			return activeScheduleTrigger(), true, nil
		},
		updateFunc: func(_ context.Context, _ string, req entity.UpdateTriggerRequest) (entity.TriggerSpec, error) {
			updated := activeScheduleTrigger()
			updated.IsActive = *req.IsActive

			return updated, nil
		},
	}
	scheduler := &mockTriggerScheduler{
		scheduleFunc: func(_ context.Context, _ *entity.TriggerSpec) error {
			t.Error("unexpected Schedule")

			return nil
		},
		unscheduleFunc: func(_ context.Context, _ string) error {
			unscheduled = true

			return nil
		},
	}
	uc := newTriggerUC(t, repo, scheduler)
	inactive := false

	_, err := uc.Update(context.Background(), testTriggerID, entity.UpdateTriggerRequest{IsActive: &inactive})
	if err != nil {
		t.Fatal(err)
	}

	if !unscheduled {
		t.Error("expected Unschedule when disabling active scheduled trigger")
	}
}

func testTriggerUpdateEnableInactive(t *testing.T) {
	t.Parallel()

	var scheduled bool

	inactiveSchedule := activeScheduleTrigger()
	inactiveSchedule.IsActive = false
	repo := &mockTriggerRepo{
		getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
			return inactiveSchedule, true, nil
		},
		updateFunc: func(_ context.Context, _ string, req entity.UpdateTriggerRequest) (entity.TriggerSpec, error) {
			updated := inactiveSchedule
			updated.IsActive = *req.IsActive

			return updated, nil
		},
	}
	scheduler := &mockTriggerScheduler{
		scheduleFunc: func(_ context.Context, _ *entity.TriggerSpec) error {
			scheduled = true

			return nil
		},
		unscheduleFunc: func(_ context.Context, _ string) error {
			t.Error("unexpected Unschedule")

			return nil
		},
	}
	uc := newTriggerUC(t, repo, scheduler)
	active := true

	_, err := uc.Update(context.Background(), testTriggerID, entity.UpdateTriggerRequest{IsActive: &active})
	if err != nil {
		t.Fatal(err)
	}

	if !scheduled {
		t.Error("expected Schedule when enabling inactive scheduled trigger")
	}
}

func testTriggerUpdateCronChange(t *testing.T) {
	t.Parallel()

	var (
		unscheduled bool
		scheduled   bool
	)

	repo := &mockTriggerRepo{
		getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
			return activeScheduleTrigger(), true, nil
		},
		updateFunc: func(_ context.Context, _ string, req entity.UpdateTriggerRequest) (entity.TriggerSpec, error) {
			updated := activeScheduleTrigger()
			updated.CronExpression = *req.CronExpression

			return updated, nil
		},
	}
	scheduler := &mockTriggerScheduler{
		scheduleFunc: func(_ context.Context, _ *entity.TriggerSpec) error {
			scheduled = true

			return nil
		},
		unscheduleFunc: func(_ context.Context, _ string) error {
			unscheduled = true

			return nil
		},
	}
	uc := newTriggerUC(t, repo, scheduler)
	newCron := "0 10 * * *"

	_, err := uc.Update(context.Background(), testTriggerID, entity.UpdateTriggerRequest{CronExpression: &newCron})
	if err != nil {
		t.Fatal(err)
	}

	if !unscheduled {
		t.Error("expected Unschedule on cron change")
	}

	if !scheduled {
		t.Error("expected Schedule on cron change")
	}
}

func testTriggerUpdateNonConfig(t *testing.T) {
	t.Parallel()

	repo := &mockTriggerRepo{
		getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
			return activeScheduleTrigger(), true, nil
		},
		updateFunc: func(_ context.Context, _ string, req entity.UpdateTriggerRequest) (entity.TriggerSpec, error) {
			updated := activeScheduleTrigger()
			updated.Name = *req.Name

			return updated, nil
		},
	}
	scheduler := &mockTriggerScheduler{
		scheduleFunc: func(_ context.Context, _ *entity.TriggerSpec) error {
			t.Error("unexpected Schedule")

			return nil
		},
		unscheduleFunc: func(_ context.Context, _ string) error {
			t.Error("unexpected Unschedule")

			return nil
		},
	}
	uc := newTriggerUC(t, repo, scheduler)
	newName := "Renamed"

	_, err := uc.Update(context.Background(), testTriggerID, entity.UpdateTriggerRequest{Name: &newName})
	if err != nil {
		t.Fatal(err)
	}
}

func testTriggerUpdateEventNoSched(t *testing.T) {
	t.Parallel()

	eventTrig := entity.TriggerSpec{
		ID: testTriggerID, TemplateID: "tmpl-1",
		TriggerType: entity.TriggerEvent, EventSlug: "order.created", IsActive: true,
	}
	repo := &mockTriggerRepo{
		getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
			return eventTrig, true, nil
		},
		updateFunc: func(_ context.Context, _ string, _ entity.UpdateTriggerRequest) (entity.TriggerSpec, error) {
			return eventTrig, nil
		},
	}
	scheduler := &mockTriggerScheduler{
		scheduleFunc: func(_ context.Context, _ *entity.TriggerSpec) error {
			t.Error("unexpected Schedule")

			return nil
		},
		unscheduleFunc: func(_ context.Context, _ string) error {
			t.Error("unexpected Unschedule")

			return nil
		},
	}
	uc := newTriggerUC(t, repo, scheduler)
	newName := "Renamed Event"

	_, err := uc.Update(context.Background(), testTriggerID, entity.UpdateTriggerRequest{Name: &newName})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTriggerUpdate_Errors(t *testing.T) {
	t.Parallel()

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		repo := &mockTriggerRepo{
			getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
				return entity.TriggerSpec{}, false, nil
			},
		}
		uc := newTriggerUC(t, repo, &mockTriggerScheduler{})

		_, err := uc.Update(context.Background(), "missing", entity.UpdateTriggerRequest{})
		if err == nil {
			t.Fatal("expected error for not found")
		}
	})
}

// -- Delete --

func TestTriggerDelete(t *testing.T) {
	t.Parallel()

	t.Run("active schedule unschedules then deletes", testTriggerDeleteActiveSchedule)
	t.Run("inactive schedule no unschedule", testTriggerDeleteInactiveSchedule)
	t.Run("event trigger no unschedule", testTriggerDeleteEventTrigger)
}

func testTriggerDeleteActiveSchedule(t *testing.T) {
	t.Parallel()

	var unscheduled bool

	repo := &mockTriggerRepo{
		getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
			return entity.TriggerSpec{ID: testTriggerID, TriggerType: entity.TriggerSchedule, IsActive: true}, true, nil
		},
		deleteFunc: func(_ context.Context, _ string) error { return nil },
	}

	uc := newTriggerUC(t, repo, &mockTriggerScheduler{
		unscheduleFunc: func(_ context.Context, _ string) error {
			unscheduled = true

			return nil
		},
	})
	if err := uc.Delete(context.Background(), testTriggerID); err != nil {
		t.Fatal(err)
	}

	if !unscheduled {
		t.Error("expected Unschedule for active schedule trigger deletion")
	}
}

func testTriggerDeleteInactiveSchedule(t *testing.T) {
	t.Parallel()
	deleteTriggerWithNoUnschedule(t, &entity.TriggerSpec{
		ID: testTriggerID, TriggerType: entity.TriggerSchedule, IsActive: false,
	})
}

func testTriggerDeleteEventTrigger(t *testing.T) {
	t.Parallel()
	deleteTriggerWithNoUnschedule(t, &entity.TriggerSpec{
		ID: testTriggerID, TriggerType: entity.TriggerEvent, IsActive: true,
	})
}

// -- Fire --
func TestTriggerFire(t *testing.T) {
	t.Parallel()
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		var recorded bool

		repo := &mockTriggerRepo{
			getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
				return entity.TriggerSpec{ID: testTriggerID, TemplateID: "tmpl-1"}, true, nil
			},
			recordFiredFunc: func(_ context.Context, _ string) error {
				recorded = true

				return nil
			},
		}
		uc := newTriggerUC(t, repo, &mockTriggerScheduler{})

		result, err := uc.Fire(context.Background(), testTriggerID)
		if err != nil {
			t.Fatal(err)
		}

		if result.ID != testTriggerID {
			t.Errorf("expected t-1, got %s", result.ID)
		}

		if !recorded {
			t.Error("expected RecordFired to be called")
		}
	})
	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		repo := &mockTriggerRepo{
			getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
				return entity.TriggerSpec{}, false, nil
			},
		}

		uc := newTriggerUC(t, repo, &mockTriggerScheduler{})
		if _, err := uc.Fire(context.Background(), "missing"); err == nil {
			t.Fatal("expected error for not found")
		}
	})
	t.Run("record fired error", func(t *testing.T) {
		t.Parallel()

		repo := &mockTriggerRepo{
			getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
				return entity.TriggerSpec{ID: testTriggerID}, true, nil
			},
			recordFiredFunc: func(_ context.Context, _ string) error { return errDB },
		}
		uc := newTriggerUC(t, repo, &mockTriggerScheduler{})

		_, err := uc.Fire(context.Background(), testTriggerID)
		if err == nil {
			t.Fatal("expected error when RecordFired fails")
		}
	})
}

// -- Toggle (delegates to Update) --

func TestTriggerToggle(t *testing.T) {
	t.Parallel()
	t.Run("disable active schedule trigger", func(t *testing.T) {
		t.Parallel()

		var unscheduled bool

		repo := &mockTriggerRepo{
			getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
				return entity.TriggerSpec{ID: testTriggerID, TriggerType: entity.TriggerSchedule, IsActive: true}, true, nil
			},
			updateFunc: func(_ context.Context, _ string, req entity.UpdateTriggerRequest) (entity.TriggerSpec, error) {
				if req.IsActive == nil || *req.IsActive {
					t.Error("expected IsActive=false")
				}

				return entity.TriggerSpec{ID: testTriggerID, IsActive: false}, nil
			},
		}
		scheduler := &mockTriggerScheduler{
			unscheduleFunc: func(_ context.Context, _ string) error {
				unscheduled = true

				return nil
			},
		}
		uc := newTriggerUC(t, repo, scheduler)

		_, err := uc.Toggle(context.Background(), testTriggerID, false)
		if err != nil {
			t.Fatal(err)
		}

		if !unscheduled {
			t.Error("expected Unschedule when disabling active schedule trigger")
		}
	})
	t.Run("enable inactive schedule trigger", func(t *testing.T) {
		t.Parallel()

		repo := &mockTriggerRepo{
			getFunc: func(_ context.Context, _ string) (entity.TriggerSpec, bool, error) {
				return entity.TriggerSpec{ID: testTriggerID, TriggerType: entity.TriggerSchedule, IsActive: false}, true, nil
			},
			updateFunc: func(_ context.Context, _ string, req entity.UpdateTriggerRequest) (entity.TriggerSpec, error) {
				if req.IsActive == nil || !*req.IsActive {
					t.Error("expected IsActive=true")
				}

				return entity.TriggerSpec{ID: testTriggerID, IsActive: true}, nil
			},
		}
		scheduler := &mockTriggerScheduler{
			scheduleFunc: func(_ context.Context, _ *entity.TriggerSpec) error { return nil },
		}
		uc := newTriggerUC(t, repo, scheduler)

		_, err := uc.Toggle(context.Background(), testTriggerID, true)
		if err != nil {
			t.Fatal(err)
		}
	})
}

// -- ListByTemplate --

func TestTriggerListByTemplate(t *testing.T) {
	t.Parallel()

	t.Run("returns triggers", func(t *testing.T) {
		t.Parallel()

		repo := &mockTriggerRepo{
			listByTemplateFunc: func(_ context.Context, templateID string) ([]entity.TriggerSpec, error) {
				return []entity.TriggerSpec{
					{ID: testTriggerID, TemplateID: templateID, Name: "First"},
					{ID: "t-2", TemplateID: templateID, Name: "Second"},
				}, nil
			},
		}
		uc := newTriggerUC(t, repo, &mockTriggerScheduler{})

		results, err := uc.ListByTemplate(context.Background(), "tmpl-1")
		if err != nil {
			t.Fatal(err)
		}

		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		repo := &mockTriggerRepo{
			listByTemplateFunc: func(_ context.Context, _ string) ([]entity.TriggerSpec, error) {
				return nil, nil
			},
		}
		uc := newTriggerUC(t, repo, &mockTriggerScheduler{})

		results, err := uc.ListByTemplate(context.Background(), "tmpl-1")
		if err != nil {
			t.Fatal(err)
		}

		if len(results) != 0 {
			t.Errorf("expected 0 results, got %d", len(results))
		}
	})
}

// -- LogTriggerExecution (fire-and-forget, best-effort) --

func TestTriggerLogExecution(t *testing.T) {
	t.Parallel()

	t.Run("inserts event log", func(t *testing.T) {
		t.Parallel()

		var inserted bool

		repo := &mockTriggerRepo{
			insertTriggerEventFunc: func(_ context.Context, event *entity.TriggerEventLog) error {
				inserted = true

				if event.TriggerID != testTriggerID {
					t.Errorf("expected t-1, got %s", event.TriggerID)
				}

				if event.TemplateID != "tmpl-1" {
					t.Errorf("expected tmpl-1, got %s", event.TemplateID)
				}

				return nil
			},
		}
		uc := newTriggerUC(t, repo, &mockTriggerScheduler{})
		uc.LogTriggerExecution(context.Background(), testTriggerID, &entity.TriggerEventLog{
			TriggerID:  testTriggerID,
			TemplateID: "tmpl-1",
			Success:    true,
		})

		if !inserted {
			t.Error("expected InsertTriggerEvent to be called")
		}
	})
}
