package agentos

import (
	"encoding/json"
	"errors"
	"os"
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

func TestPlanDeltaSpecJSONSchema(t *testing.T) {
	data, err := PlanDeltaSpecJSONSchema()
	if err != nil {
		t.Fatalf("PlanDeltaSpecJSONSchema: %v", err)
	}

	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("schema json: %v", err)
	}
	if len(schema) == 0 {
		t.Fatal("schema is empty")
	}
}

func TestCapabilityCatalogSpecJSONSchema(t *testing.T) {
	data, err := CapabilityCatalogSpecJSONSchema()
	if err != nil {
		t.Fatalf("CapabilityCatalogSpecJSONSchema: %v", err)
	}

	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("schema json: %v", err)
	}
	if len(schema) == 0 {
		t.Fatal("schema is empty")
	}
}

func TestArtifactSchemaCatalogSpecJSONSchema(t *testing.T) {
	data, err := ArtifactSchemaCatalogSpecJSONSchema()
	if err != nil {
		t.Fatalf("ArtifactSchemaCatalogSpecJSONSchema: %v", err)
	}

	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("schema json: %v", err)
	}
	if len(schema) == 0 {
		t.Fatal("schema is empty")
	}
}

func TestRunPlanSpecJSONSchemaFileIsCurrent(t *testing.T) {
	generated, err := RunPlanSpecJSONSchema()
	if err != nil {
		t.Fatalf("RunPlanSpecJSONSchema: %v", err)
	}
	stored, err := os.ReadFile("../docs/schemas/run_plan.schema.json")
	if err != nil {
		t.Fatalf("ReadFile schema: %v", err)
	}
	if string(stored) != string(generated) {
		t.Fatal("docs/schemas/run_plan.schema.json is stale; run make agentos-plan-schema")
	}
}

func TestPlanDeltaSpecJSONSchemaFileIsCurrent(t *testing.T) {
	generated, err := PlanDeltaSpecJSONSchema()
	if err != nil {
		t.Fatalf("PlanDeltaSpecJSONSchema: %v", err)
	}
	stored, err := os.ReadFile("../docs/schemas/plan_delta.schema.json")
	if err != nil {
		t.Fatalf("ReadFile schema: %v", err)
	}
	if string(stored) != string(generated) {
		t.Fatal("docs/schemas/plan_delta.schema.json is stale; run make agentos-plan-schema")
	}
}

func TestCapabilityCatalogSpecJSONSchemaFileIsCurrent(t *testing.T) {
	generated, err := CapabilityCatalogSpecJSONSchema()
	if err != nil {
		t.Fatalf("CapabilityCatalogSpecJSONSchema: %v", err)
	}
	stored, err := os.ReadFile("../docs/schemas/capability_catalog.schema.json")
	if err != nil {
		t.Fatalf("ReadFile schema: %v", err)
	}
	if string(stored) != string(generated) {
		t.Fatal("docs/schemas/capability_catalog.schema.json is stale; run make agentos-plan-schema")
	}
}

func TestArtifactSchemaCatalogSpecJSONSchemaFileIsCurrent(t *testing.T) {
	generated, err := ArtifactSchemaCatalogSpecJSONSchema()
	if err != nil {
		t.Fatalf("ArtifactSchemaCatalogSpecJSONSchema: %v", err)
	}
	stored, err := os.ReadFile("../docs/schemas/artifact_schema_catalog.schema.json")
	if err != nil {
		t.Fatalf("ReadFile schema: %v", err)
	}
	if string(stored) != string(generated) {
		t.Fatal("docs/schemas/artifact_schema_catalog.schema.json is stale; run make agentos-plan-schema")
	}
}

func TestRunPlanPublicTypesDoNotExposeInternalEntity(t *testing.T) {
	publicTypes := []reflect.Type{
		reflect.TypeOf(RunPlanSpec{}),
		reflect.TypeOf(PlanNodeSpec{}),
		reflect.TypeOf(PlanEdgeSpec{}),
		reflect.TypeOf(CapabilityCatalogSpec{}),
		reflect.TypeOf(ArtifactSchemaCatalogSpec{}),
		reflect.TypeOf(RunPlanStatus{}),
		reflect.TypeOf(PlanNodeStatus{}),
		reflect.TypeOf(RunStatus{}),
	}
	for _, typ := range publicTypes {
		assertNoInternalEntity(t, typ, map[reflect.Type]bool{})
	}
}

func TestRunStatusDoesNotExposeNativeStep(t *testing.T) {
	if _, ok := reflect.TypeOf(RunStatus{}).FieldByName("Step"); ok {
		t.Fatal("agentos.RunStatus must expose generic Progress, not native Step")
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

func TestPlanEventCodecRequiresStoredEventIdentity(t *testing.T) {
	valid := PlanEvent{
		Event: Event{
			EventID:   "evt-1",
			EventType: EventPlanStarted,
			Sequence:  1,
			Timestamp: time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
		},
		PlanID: "plan-1",
	}
	cases := map[string]PlanEvent{
		"event id": func() PlanEvent {
			event := valid
			event.EventID = ""
			return event
		}(),
		"sequence": func() PlanEvent {
			event := valid
			event.Sequence = 0
			return event
		}(),
		"timestamp": func() PlanEvent {
			event := valid
			event.Timestamp = time.Time{}
			return event
		}(),
	}
	for name, event := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := MarshalPlanEvent(event); !errors.Is(err, ErrInvalidPlanEvent) {
				t.Fatalf("MarshalPlanEvent error = %v, want ErrInvalidPlanEvent", err)
			}
		})
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
