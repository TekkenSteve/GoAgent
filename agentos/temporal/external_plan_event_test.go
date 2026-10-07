package temporal

import (
	"errors"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

func TestPlanRuntimeIngestExternalPlanEventPersistsAndPublishes(t *testing.T) {
	t.Parallel()

	store, ref := externalEventPlanStore(t)
	publisher := &fakePlanEventPublisher{}
	runtime := &planRuntime{planIndex: store, planEvents: store, planPublisher: publisher}

	input := externalPlanEventInput(ref, "node-1", "run-1", "backend-event-1")

	stored, err := runtime.IngestExternalPlanEvent(t.Context(), &input)
	if err != nil {
		t.Fatalf("IngestExternalPlanEvent: %v", err)
	}

	if stored.ExternalEventID != input.Event.EventID || stored.EventID == input.Event.EventID {
		t.Fatalf("stored event identities = %#v", stored)
	}

	if stored.Sequence != 1 || !publisher.called || publisher.event.EventID != stored.EventID {
		t.Fatalf("stored=%#v publisher=%#v", stored, publisher)
	}

	replay, err := runtime.IngestExternalPlanEvent(t.Context(), &input)
	if err != nil {
		t.Fatalf("IngestExternalPlanEvent replay: %v", err)
	}

	if replay.EventID != stored.EventID || replay.Sequence != stored.Sequence {
		t.Fatalf("replay=%#v stored=%#v", replay, stored)
	}
}

func TestPlanRuntimeIngestExternalPlanEventRejectsWrongNodeRunAndScope(t *testing.T) {
	t.Parallel()

	store, ref := externalEventPlanStore(t)
	runtime := &planRuntime{planIndex: store, planEvents: store, planPublisher: &fakePlanEventPublisher{}}

	wrongRun := externalPlanEventInput(ref, "node-1", "wrong-run", "backend-event-1")
	if _, err := runtime.IngestExternalPlanEvent(t.Context(), &wrongRun); !errors.Is(err, agentoscore.ErrInvalidPlanEvent) {
		t.Fatalf("wrong run error = %v, want ErrInvalidPlanEvent", err)
	}

	wrongNode := externalPlanEventInput(ref, "wrong-node", "run-1", "backend-event-2")
	if _, err := runtime.IngestExternalPlanEvent(t.Context(), &wrongNode); !errors.Is(err, agentoscore.ErrInvalidPlanEvent) {
		t.Fatalf("wrong node error = %v, want ErrInvalidPlanEvent", err)
	}

	wrongScope := externalPlanEventInput(ref, "node-1", "run-1", "backend-event-3")

	wrongScope.Plan.AccountID = "other-account"
	if _, err := runtime.IngestExternalPlanEvent(t.Context(), &wrongScope); !errors.Is(err, agentoscore.ErrPlanRouteNotFound) {
		t.Fatalf("wrong scope error = %v, want ErrPlanRouteNotFound", err)
	}
}

func externalEventPlanStore(t *testing.T) (*agentosplan.MemoryPlanStore, agentos.PlanRef) {
	t.Helper()

	store := agentosplan.NewMemoryPlanStore()
	spec := agentos.RunPlanSpec{
		PlanID:         "plan-external-events",
		AccountID:      "account-1",
		ProjectID:      "project-1",
		IdempotencyKey: "plan-external-events-start",
		Nodes: []agentos.PlanNodeSpec{{
			NodeID: "node-1",
			Run: agentos.RunSpec{
				RunID:   "run-1",
				Backend: agentos.BackendRef{Kind: agentos.BackendKindTemporalExternal, Name: "external"},
			},
		}},
	}

	status := agentosplan.NewState(&spec, time.Now().UTC()).Status
	if _, _, err := store.CreatePlan(t.Context(), &spec, &status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}

	return store, agentos.PlanRef{PlanID: spec.PlanID, AccountID: spec.AccountID, ProjectID: spec.ProjectID}
}

func externalPlanEventInput(ref agentos.PlanRef, nodeID, runID, eventID string) agentos.ExternalPlanEvent {
	return agentos.ExternalPlanEvent{
		Plan:   ref,
		NodeID: nodeID,
		Event: agentoscore.Event{
			EventID:   eventID,
			EventType: agentoscore.EventAgentStepCompleted,
			RunID:     runID,
			Sequence:  1,
			Timestamp: time.Date(2026, 7, 15, 8, 0, 0, 0, time.UTC),
			Source:    "external-agent",
			Payload:   map[string]any{"output": "ok"},
		},
	}
}
