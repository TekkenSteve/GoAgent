package agentos

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRunPlanSpecJSONSchema(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

func TestPlanJSONSchemaDispatchesAllPublicKinds(t *testing.T) {
	t.Parallel()

	expectedKinds := []PlanSchemaKind{
		PlanSchemaKindRunPlan,
		PlanSchemaKindPlanDelta,
		PlanSchemaKindCapabilityCatalog,
		PlanSchemaKindArtifactSchemaCatalog,
	}

	kinds := PlanSchemaKinds()
	if !reflect.DeepEqual(kinds, expectedKinds) {
		t.Fatalf("schema kinds = %#v, want %#v", kinds, expectedKinds)
	}

	for _, kind := range kinds {
		data, err := PlanJSONSchema(kind)
		if err != nil {
			t.Fatalf("PlanJSONSchema(%s): %v", kind, err)
		}

		var schema map[string]any
		if err := json.Unmarshal(data, &schema); err != nil {
			t.Fatalf("schema %s json: %v", kind, err)
		}

		if len(schema) == 0 {
			t.Fatalf("schema %s is empty", kind)
		}
	}
}

func TestPlanJSONSchemaRejectsUnknownKind(t *testing.T) {
	t.Parallel()

	_, err := PlanJSONSchema("unknown")
	if !errors.Is(err, ErrInvalidRunPlan) {
		t.Fatalf("PlanJSONSchema unknown error = %v, want ErrInvalidRunPlan", err)
	}
}

func TestPlanJSONSchemasDoNotRequireOptionalWireFields(t *testing.T) {
	t.Parallel()

	runPlanSchema := decodePlanSchema(t, mustPlanSchema(t, RunPlanSpecJSONSchema))
	assertSchemaDoesNotRequire(t, runPlanSchema, []string{"requested_at", "policy"})
	assertSchemaDoesNotRequire(t, schemaProperty(t, runPlanSchema, "properties", "nodes", "items"), []string{"policy"})
	assertSchemaDoesNotRequire(t, schemaProperty(t, runPlanSchema, "properties", "nodes", "items", "properties", "run"), []string{"requested_at"})

	planDeltaSchema := decodePlanSchema(t, mustPlanSchema(t, PlanDeltaSpecJSONSchema))
	assertSchemaDoesNotRequire(t, schemaProperty(t, planDeltaSchema, "properties", "nodes", "items"), []string{"policy"})
	assertSchemaDoesNotRequire(t, schemaProperty(t, planDeltaSchema, "properties", "nodes", "items", "properties", "run"), []string{"requested_at"})
}

func TestRunPlanSpecJSONSchemaFileIsCurrent(t *testing.T) {
	t.Parallel()

	generated, err := RunPlanSpecJSONSchema()
	if err != nil {
		t.Fatalf("RunPlanSpecJSONSchema: %v", err)
	}

	stored, err := os.ReadFile("../docs/schemas/run_plan.schema.json")
	if err != nil {
		t.Fatalf("ReadFile schema: %v", err)
	}

	if !bytes.Equal(stored, generated) {
		t.Fatal("docs/schemas/run_plan.schema.json is stale; run make agentos-plan-schema")
	}
}

func mustPlanSchema(t *testing.T, fn func() ([]byte, error)) []byte {
	t.Helper()

	data, err := fn()
	if err != nil {
		t.Fatalf("schema: %v", err)
	}

	return data
}

func decodePlanSchema(t *testing.T, data []byte) map[string]any {
	t.Helper()

	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("schema json: %v", err)
	}

	return schema
}

func schemaProperty(t *testing.T, schema map[string]any, path ...string) map[string]any {
	t.Helper()

	current := any(schema)
	for _, segment := range path {
		object, ok := current.(map[string]any)
		if !ok {
			t.Fatalf("schema path %v reached non-object %#v", path, current)
		}

		current = object[segment]
	}

	object, ok := current.(map[string]any)
	if !ok {
		t.Fatalf("schema path %v = %#v, want object", path, current)
	}

	return object
}

func assertSchemaDoesNotRequire(t *testing.T, schema map[string]any, fields []string) {
	t.Helper()

	requiredValues, ok := schema["required"].([]any)
	if !ok {
		requiredValues = nil
	}

	required := make(map[string]bool, len(requiredValues))
	for _, value := range requiredValues {
		name, ok := value.(string)
		if !ok {
			t.Fatalf("required contains non-string %#v", value)
		}

		required[name] = true
	}

	for _, field := range fields {
		if required[field] {
			t.Fatalf("schema unexpectedly requires optional field %q", field)
		}
	}
}

func TestPlanDeltaSpecJSONSchemaFileIsCurrent(t *testing.T) {
	t.Parallel()

	generated, err := PlanDeltaSpecJSONSchema()
	if err != nil {
		t.Fatalf("PlanDeltaSpecJSONSchema: %v", err)
	}

	stored, err := os.ReadFile("../docs/schemas/plan_delta.schema.json")
	if err != nil {
		t.Fatalf("ReadFile schema: %v", err)
	}

	if !bytes.Equal(stored, generated) {
		t.Fatal("docs/schemas/plan_delta.schema.json is stale; run make agentos-plan-schema")
	}
}

func TestCapabilityCatalogSpecJSONSchemaFileIsCurrent(t *testing.T) {
	t.Parallel()

	generated, err := CapabilityCatalogSpecJSONSchema()
	if err != nil {
		t.Fatalf("CapabilityCatalogSpecJSONSchema: %v", err)
	}

	stored, err := os.ReadFile("../docs/schemas/capability_catalog.schema.json")
	if err != nil {
		t.Fatalf("ReadFile schema: %v", err)
	}

	if !bytes.Equal(stored, generated) {
		t.Fatal("docs/schemas/capability_catalog.schema.json is stale; run make agentos-plan-schema")
	}
}

func TestArtifactSchemaCatalogSpecJSONSchemaFileIsCurrent(t *testing.T) {
	t.Parallel()

	generated, err := ArtifactSchemaCatalogSpecJSONSchema()
	if err != nil {
		t.Fatalf("ArtifactSchemaCatalogSpecJSONSchema: %v", err)
	}

	stored, err := os.ReadFile("../docs/schemas/artifact_schema_catalog.schema.json")
	if err != nil {
		t.Fatalf("ReadFile schema: %v", err)
	}

	if !bytes.Equal(stored, generated) {
		t.Fatal("docs/schemas/artifact_schema_catalog.schema.json is stale; run make agentos-plan-schema")
	}
}

func TestRunPlanPublicTypesDoNotExposeInternalEntity(t *testing.T) {
	t.Parallel()

	publicTypes := []reflect.Type{
		reflect.TypeFor[RunPlanSpec](),
		reflect.TypeFor[PlanNodeSpec](),
		reflect.TypeFor[PlanEdgeSpec](),
		reflect.TypeFor[CapabilityCatalogSpec](),
		reflect.TypeFor[ArtifactSchemaCatalogSpec](),
		reflect.TypeFor[RunPlanStatus](),
		reflect.TypeFor[PlanNodeStatus](),
		reflect.TypeFor[RunStatus](),
	}
	for _, typ := range publicTypes {
		assertNoInternalEntity(t, typ, map[reflect.Type]bool{})
	}
}

func TestRunStatusDoesNotExposeNativeStep(t *testing.T) {
	t.Parallel()

	if _, ok := reflect.TypeFor[RunStatus]().FieldByName("Step"); ok {
		t.Fatal("agentos.RunStatus must expose generic Progress, not native Step")
	}
}

func TestPlanEventCodecRoundTrip(t *testing.T) {
	t.Parallel()

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
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
	}

	data, err := MarshalPlanEvent(&event)
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
	t.Parallel()

	event := PlanEvent{Event: Event{EventType: EventPlanStarted}}
	_, err := MarshalPlanEvent(&event)

	if !errors.Is(err, ErrInvalidPlanEvent) {
		t.Fatalf("error = %v, want ErrInvalidPlanEvent", err)
	}
}

func TestPlanEventCodecRejectsNilEvent(t *testing.T) {
	t.Parallel()

	_, err := MarshalPlanEvent(nil)
	if !errors.Is(err, ErrInvalidPlanEvent) {
		t.Fatalf("error = %v, want ErrInvalidPlanEvent", err)
	}
}

func TestPlanEventCodecRejectsMissingTenantScope(t *testing.T) {
	t.Parallel()

	valid := PlanEvent{
		Event: Event{
			EventID:   "evt-1",
			EventType: EventPlanStarted,
			Sequence:  1,
			Timestamp: time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC),
		},
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
	}

	cases := map[string]PlanEvent{
		"account id": func() PlanEvent {
			event := valid
			event.AccountID = ""

			return event
		}(),
		"project id": func() PlanEvent {
			event := valid
			event.ProjectID = ""

			return event
		}(),
	}
	for name, event := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, err := MarshalPlanEvent(&event); !errors.Is(err, ErrInvalidPlanEvent) {
				t.Fatalf("MarshalPlanEvent error = %v, want ErrInvalidPlanEvent", err)
			}
		})
	}
}

func TestPlanEventCodecRequiresStoredEventIdentity(t *testing.T) {
	t.Parallel()

	valid := PlanEvent{
		Event: Event{
			EventID:   "evt-1",
			EventType: EventPlanStarted,
			Sequence:  1,
			Timestamp: time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
		},
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
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
			t.Parallel()

			if _, err := MarshalPlanEvent(&event); !errors.Is(err, ErrInvalidPlanEvent) {
				t.Fatalf("MarshalPlanEvent error = %v, want ErrInvalidPlanEvent", err)
			}
		})
	}
}

func TestPlanEventToEventCarriesPlanScopeInPayload(t *testing.T) {
	t.Parallel()

	event := PlanEvent{
		Event: Event{
			EventID:   "evt-1",
			EventType: EventPlanNodeStarted,
			RunID:     "run-1",
			Sequence:  3,
			Timestamp: time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC),
			Payload:   map[string]any{"status": "running"},
		},
		PlanID:    "plan-1",
		AccountID: "acct-1",
		ProjectID: "proj-1",
		NodeID:    "node-1",
	}

	generic := event.ToEvent()
	if generic.EventID != event.EventID || generic.EventType != event.EventType || generic.Sequence != event.Sequence {
		t.Fatalf("generic event identity = %#v, want plan event identity %#v", generic, event.Event)
	}

	wantPayload := map[string]any{
		"status":     "running",
		"plan_id":    "plan-1",
		"account_id": "acct-1",
		"project_id": "proj-1",
		"node_id":    "node-1",
	}
	for key, want := range wantPayload {
		if got := generic.Payload[key]; got != want {
			t.Fatalf("payload[%q] = %#v, want %#v in %#v", key, got, want, generic.Payload)
		}
	}
}

func TestSignalJSONUsesPublicWireFieldNames(t *testing.T) {
	t.Parallel()

	sentAt := time.Date(2026, 6, 20, 12, 30, 0, 0, time.UTC)

	data, err := json.Marshal(Signal{
		Type:           SignalPlanApprove,
		IdempotencyKey: "approve-1",
		ActorID:        "operator-1",
		Payload:        map[string]any{"reason": "looks good"},
		SentAt:         sentAt,
	})
	if err != nil {
		t.Fatalf("Marshal signal: %v", err)
	}

	raw := string(data)
	for _, want := range []string{`"idempotency_key"`, `"actor_id"`, `"sent_at"`} {
		if !strings.Contains(raw, want) {
			t.Fatalf("signal json = %s, missing %s", raw, want)
		}
	}

	for _, forbidden := range []string{`"IdempotencyKey"`, `"ActorID"`, `"SentAt"`} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("signal json = %s, contains Go field name %s", raw, forbidden)
		}
	}

	var got Signal
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal signal: %v", err)
	}

	assertSignalRoundTrip(t, &got, sentAt)
}

func assertSignalRoundTrip(t *testing.T, got *Signal, sentAt time.Time) {
	t.Helper()

	if got.Type != SignalPlanApprove ||
		got.IdempotencyKey != "approve-1" ||
		got.ActorID != "operator-1" ||
		!got.SentAt.Equal(sentAt) ||
		got.Payload["reason"] != "looks good" {
		t.Fatalf("signal = %#v", got)
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

	for field := range typ.Fields() {
		assertNoInternalEntity(t, field.Type, seen)
	}
}
