//go:build postgres_integration

package persistent

import (
	"errors"
	"testing"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
)

func TestAgentOSProcessPostgresDurablePersistence(t *testing.T) {
	t.Parallel()

	ctx, pg, suffix := newAgentOSPlanPostgresIntegrationDB(t)
	assertPostgresTables(t, pg, "processes", "process_events", "process_status_updates")

	repo := NewAgentOSProcessRepo(pg)
	spec := postgresIntegrationProcessSpec("process-"+suffix, "process-start-"+suffix)
	status := postgresIntegrationProcessStatus(&spec, agentos.ProcessRunning)

	first, created, err := repo.CreateProcess(ctx, &spec, &status)
	if err != nil {
		t.Fatalf("CreateProcess first: %v", err)
	}
	if !created {
		t.Fatal("CreateProcess first created=false, want true")
	}

	replayed, created, err := repo.CreateProcess(ctx, &spec, &status)
	if err != nil {
		t.Fatalf("CreateProcess replay: %v", err)
	}
	if created || replayed.LifecycleState != first.LifecycleState {
		t.Fatalf("CreateProcess replay = %#v created=%v, want created=false and %#v", replayed, created, first)
	}

	changed := spec
	changed.Resource.ResourceID = "changed-resource"
	if _, _, err := repo.CreateProcess(ctx, &changed, &status); !errors.Is(err, agentoscore.ErrInvalidProcess) {
		t.Fatalf("CreateProcess changed error = %v, want ErrInvalidProcess", err)
	}

	waiting := postgresIntegrationProcessStatus(&spec, agentos.ProcessWaiting)
	waiting.Reason = "operator pause"
	updated, err := repo.UpdateProcessStatus(ctx, &waiting, "status-update-"+suffix)
	if err != nil {
		t.Fatalf("UpdateProcessStatus: %v", err)
	}
	if updated.LifecycleState != agentos.ProcessWaiting {
		t.Fatalf("updated lifecycle = %q, want waiting", updated.LifecycleState)
	}

	waiting.LifecycleState = agentos.ProcessFailed
	replayedStatus, err := repo.UpdateProcessStatus(ctx, &waiting, "status-update-"+suffix)
	if err != nil {
		t.Fatalf("UpdateProcessStatus replay: %v", err)
	}
	if replayedStatus.LifecycleState != agentos.ProcessWaiting {
		t.Fatalf("replayed status = %#v, want first idempotent status", replayedStatus)
	}

	processes, err := repo.ListProcesses(ctx, &agentos.Scope{
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		Resource:       spec.Resource,
		LifecycleState: agentos.ProcessWaiting,
	})
	if err != nil {
		t.Fatalf("ListProcesses: %v", err)
	}
	if len(processes) != 1 || processes[0].ProcessID != spec.ProcessID {
		t.Fatalf("processes = %#v, want updated process", processes)
	}

	event := postgresIntegrationProcessEvent(&spec, agentoscore.EventProcessWaiting)
	firstEvent, err := repo.AppendProcessEvent(ctx, &event, "event-"+suffix)
	if err != nil {
		t.Fatalf("AppendProcessEvent: %v", err)
	}
	if firstEvent.Sequence != 1 || firstEvent.EventID == "" {
		t.Fatalf("first event identity = (%q,%d), want assigned sequence 1", firstEvent.EventID, firstEvent.Sequence)
	}

	replayedEvent, err := repo.AppendProcessEvent(ctx, &event, "event-"+suffix)
	if err != nil {
		t.Fatalf("AppendProcessEvent replay: %v", err)
	}
	if replayedEvent.EventID != firstEvent.EventID || replayedEvent.Sequence != firstEvent.Sequence {
		t.Fatalf("replayed event = %#v, want %#v", replayedEvent, firstEvent)
	}

	events, err := repo.ListProcessEvents(ctx, &agentos.EventScope{
		ProcessID: spec.ProcessID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
	})
	if err != nil {
		t.Fatalf("ListProcessEvents: %v", err)
	}
	if len(events) != 1 || events[0].EventType != agentoscore.EventProcessWaiting {
		t.Fatalf("events = %#v, want one process.waiting event", events)
	}
}

func postgresIntegrationProcessSpec(processID, idempotencyKey string) agentos.Spec {
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)

	return agentos.Spec{
		ProcessID:      processID,
		Kind:           "resource-lifecycle",
		AccountID:      "acct-process",
		ProjectID:      "proj-process",
		IdempotencyKey: idempotencyKey,
		Resource: agentos.ResourceRef{
			Kind:       "generic-resource",
			ResourceID: "resource-" + processID,
			AccountID:  "acct-process",
			ProjectID:  "proj-process",
		},
		RequestedAt: now,
	}
}

func postgresIntegrationProcessStatus(spec *agentos.Spec, lifecycle string) agentos.Status {
	return agentos.Status{
		ProcessID:      spec.ProcessID,
		Kind:           spec.Kind,
		AccountID:      spec.AccountID,
		ProjectID:      spec.ProjectID,
		Resource:       spec.Resource,
		LifecycleState: lifecycle,
		StartedAt:      spec.RequestedAt,
		UpdatedAt:      spec.RequestedAt,
	}
}

func postgresIntegrationProcessEvent(spec *agentos.Spec, eventType agentoscore.EventType) agentos.Event {
	return agentos.Event{
		Event: agentoscore.Event{
			EventType: eventType,
			ProcessID: spec.ProcessID,
			Timestamp: spec.RequestedAt,
			Payload:   map[string]any{"source": "postgres-integration"},
		},
		ProcessID: spec.ProcessID,
		AccountID: spec.AccountID,
		ProjectID: spec.ProjectID,
		Resource:  spec.Resource,
	}
}
