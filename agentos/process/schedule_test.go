package process

import "testing"

func TestValidateRecurringSchedule(t *testing.T) {
	t.Parallel()

	valid := RecurringSchedule{
		ScheduleID:     "schedule-1",
		DispatchID:     "dispatch-1",
		CronExpression: "0 9 * * *",
		TimeZone:       "Asia/Shanghai",
		OverlapPolicy:  ScheduleOverlapSkip,
	}
	if err := ValidateRecurringSchedule(valid); err != nil {
		t.Fatalf("ValidateRecurringSchedule() error = %v", err)
	}

	for _, test := range []struct {
		name     string
		schedule RecurringSchedule
	}{
		{name: "missing schedule ID", schedule: RecurringSchedule{DispatchID: "dispatch-1", CronExpression: "0 9 * * *", TimeZone: "UTC", OverlapPolicy: ScheduleOverlapSkip}},
		{name: "missing dispatch ID", schedule: RecurringSchedule{ScheduleID: "schedule-1", CronExpression: "0 9 * * *", TimeZone: "UTC", OverlapPolicy: ScheduleOverlapSkip}},
		{name: "missing cron", schedule: RecurringSchedule{ScheduleID: "schedule-1", DispatchID: "dispatch-1", TimeZone: "UTC", OverlapPolicy: ScheduleOverlapSkip}},
		{name: "invalid time zone", schedule: RecurringSchedule{ScheduleID: "schedule-1", DispatchID: "dispatch-1", CronExpression: "0 9 * * *", TimeZone: "Mars/Olympus", OverlapPolicy: ScheduleOverlapSkip}},
		{name: "unsupported overlap", schedule: RecurringSchedule{ScheduleID: "schedule-1", DispatchID: "dispatch-1", CronExpression: "0 9 * * *", TimeZone: "UTC", OverlapPolicy: "allow_all"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidateRecurringSchedule(test.schedule); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestValidateScheduleDispatch(t *testing.T) {
	t.Parallel()
	if err := ValidateScheduleDispatch(ScheduleDispatch{DispatchID: "dispatch-1"}); err != nil {
		t.Fatalf("ValidateScheduleDispatch() error = %v", err)
	}
	if err := ValidateScheduleDispatch(ScheduleDispatch{}); err == nil {
		t.Fatal("expected validation error")
	}
}
