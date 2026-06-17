package agentos

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRunPlanSpecJSONSchema(t *testing.T) {
	data, err := RunPlanSpecJSONSchema()
	if err != nil {
		t.Fatalf("RunPlanSpecJSONSchema: %v", err)
	}

	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("schema json: %v", err)
	}
	if len(schema) == 0 {
		t.Fatal("schema is empty")
	}
}

func TestRunPlanPublicTypesDoNotExposeInternalEntity(t *testing.T) {
	publicTypes := []reflect.Type{
		reflect.TypeOf(RunPlanSpec{}),
		reflect.TypeOf(PlanNodeSpec{}),
		reflect.TypeOf(PlanEdgeSpec{}),
		reflect.TypeOf(RunPlanStatus{}),
		reflect.TypeOf(PlanNodeStatus{}),
	}
	for _, typ := range publicTypes {
		assertNoInternalEntity(t, typ, map[reflect.Type]bool{})
	}
}

func TestPlanEventCodecRoundTrip(t *testing.T) {
	event := PlanEvent{
		Event: Event{
			EventID:   "evt-1",
			EventType: EventPlanStarted,
			Sequence:  7,
			Timestamp: time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC),
			Payload: map[string]any{
				"reason": "started",
			},
		},
		PlanID: "plan-1",
	}

	data, err := MarshalPlanEvent(event)
	if err != nil {
		t.Fatalf("MarshalPlanEvent: %v", err)
	}
	got, err := UnmarshalPlanEvent(data)
	if err != nil {
		t.Fatalf("UnmarshalPlanEvent: %v", err)
	}
	if got.PlanID != event.PlanID || got.EventType != event.EventType || got.Sequence != event.Sequence {
		t.Fatalf("event = %#v", got)
	}
}

func TestPlanEventCodecRejectsMissingPlanID(t *testing.T) {
	_, err := MarshalPlanEvent(PlanEvent{Event: Event{EventType: EventPlanStarted}})
	if !errors.Is(err, ErrInvalidPlanEvent) {
		t.Fatalf("error = %v, want ErrInvalidPlanEvent", err)
	}
}

func assertNoInternalEntity(t *testing.T, typ reflect.Type, seen map[reflect.Type]bool) {
	t.Helper()
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Map {
		assertNoInternalEntity(t, typ.Elem(), seen)

		return
	}
	if seen[typ] {
		return
	}
	seen[typ] = true

	if strings.Contains(typ.PkgPath(), "/internal/entity") {
		t.Fatalf("public AgentOS type exposes internal entity type: %s", typ)
	}
	if typ.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < typ.NumField(); i++ {
		assertNoInternalEntity(t, typ.Field(i).Type, seen)
	}
}
