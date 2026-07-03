package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

const (
	Plan1    = "plan-1"
	Node1    = "node-1"
	Expanded = "expanded"
)

var (
	errTestRedisUnavailable    = errors.New("redis unavailable")
	errTestPostgresUnavailable = errors.New("postgres unavailable")
)

func TestPlanActivitiesStartStatusControl(t *testing.T) {
	t.Parallel()

	runtime := &fakePlanRuntime{}
	activities := newTestPlanActivities(t, runtime)
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}

	started, err := activities.StartPlanNodeActivity(context.Background(), &startPlanNodeInput{
		PlanID:    Plan1,
		AccountID: "acct-1",
		ProjectID: "proj-1",
		Node: agentos.PlanNodeSpec{
			NodeID: Node1,
			Run: agentos.RunSpec{
				RunID:   "run-1",
				Backend: ref,
			},
		},
	})
	if err != nil {
		t.Fatalf("StartPlanNodeActivity: %v", err)
	}

	if started.Status.RunID != Run1 || runtime.started.Backend != ref {
		t.Fatalf("unexpected start: %#v %#v", started, runtime.started)
	}

	status, err := activities.StatusPlanNodeActivity(context.Background(), statusPlanNodeInput{RunID: "run-1"})
	if err != nil {
		t.Fatalf("StatusPlanNodeActivity: %v", err)
	}

	if status.Status.LifecycleState != "completed" {
		t.Fatalf("status = %#v", status)
	}

	if err := activities.ControlPlanNodeActivity(context.Background(), &controlPlanNodeInput{RunID: "run-1", Control: agentos.ControlRequest{Operation: agentos.ControlCancel}}); err != nil {
		t.Fatalf("ControlPlanNodeActivity: %v", err)
	}

	if runtime.control != agentos.ControlCancel {
		t.Fatalf("control = %q", runtime.control)
	}
}

func TestPlanActivitiesStartPlanNodeRejectsBackendRunIDDrift(t *testing.T) {
	t.Parallel()

	runtime := &fakePlanRuntime{startStatus: agentos.RunStatus{RunID: "backend-run", LifecycleState: "running"}}
	activities := newTestPlanActivities(t, runtime)
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}

	_, err := activities.StartPlanNodeActivity(context.Background(), &startPlanNodeInput{
		PlanID:    Plan1,
		AccountID: "acct-1",
		ProjectID: "proj-1",
		Node: agentos.PlanNodeSpec{
			NodeID: Node1,
			Run: agentos.RunSpec{
				RunID:   "run-1",
				Backend: ref,
			},
		},
	})
	if !errors.Is(err, agentos.ErrInvalidRunSpec) {
		t.Fatalf("StartPlanNodeActivity error = %v, want ErrInvalidRunSpec", err)
	}
}

func TestPlanActivitiesConstructorRequiresTransitionStore(t *testing.T) {
	t.Parallel()

	store := agentosplan.NewMemoryPlanStore()

	_, err := NewPlanActivitiesWithCatalogAndSchemas(
		&fakePlanRuntime{},
		nil,
		nil,
		nil,
		nil,
		agentosplan.NewMemoryArtifactStore(),
	)
	if !errors.Is(err, agentos.ErrInvalidRunPlan) {
		t.Fatalf("missing transition store error = %v, want ErrInvalidRunPlan", err)
	}

	_, err = NewPlanActivitiesWithCatalogAndSchemas(
		&fakePlanRuntime{},
		nil,
		nil,
		store,
		nil,
		nil,
	)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("missing artifact store error = %v, want ErrInvalidArtifact", err)
	}
}

func TestPlanActivitiesResolvePlanNodeInputMapsInput(t *testing.T) {
	t.Parallel()

	runtime := &fakePlanRuntime{}
	activities := newTestPlanActivities(t, runtime)
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}

	resolved, err := activities.ResolvePlanNodeInputActivity(context.Background(), &resolvePlanNodeInputInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    Plan1,
			AccountID: "acct-1",
			ProjectID: "proj-1",
			Inputs: map[string]any{
				"task": map[string]any{"topic": "artifact routing"},
			},
		},
		Node: agentos.PlanNodeSpec{
			NodeID: Node1,
			Run: agentos.RunSpec{
				RunID:   "run-1",
				Backend: ref,
				Input:   map[string]any{"existing": true},
			},
			Inputs: []agentos.InputMapping{
				{Target: "topic", SourcePath: "task.topic", Required: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("ResolvePlanNodeInputActivity: %v", err)
	}

	if resolved.Input["existing"] != true {
		t.Fatalf("existing input = %#v", resolved.Input)
	}

	if resolved.Input["topic"] != "artifact routing" {
		t.Fatalf("topic input = %#v", resolved.Input)
	}

	if resolved.Trace.MappingCount != 1 || resolved.Trace.InputDigest == "" {
		t.Fatalf("trace = %#v", resolved.Trace)
	}
}

func TestPlanActivitiesResolvePlanNodeInputDereferencesArtifactPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	activities := newTestPlanActivities(t, &fakePlanRuntime{})

	ref, err := putArtifact(ctx, activities.ArtifactStore, &agentos.ArtifactRef{
		ArtifactID: "artifact-summary",
		PlanID:     Plan1,
		NodeID:     "research",
		RunID:      "run-research",
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{
		"body": map[string]any{"title": "artifact mapping"},
	}, "plan-1:artifact-summary")
	if err != nil {
		t.Fatalf("Put artifact: %v", err)
	}

	resolved, err := activities.ResolvePlanNodeInputActivity(ctx, &resolvePlanNodeInputInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    Plan1,
			AccountID: "acct-1",
			ProjectID: "proj-1",
		},
		Status: agentos.RunPlanStatus{
			PlanID:    Plan1,
			Artifacts: []agentos.ArtifactRef{ref},
		},
		Node: agentos.PlanNodeSpec{
			NodeID: "verify",
			Run:    agentos.RunSpec{RunID: "run-verify"},
			Inputs: []agentos.InputMapping{
				{
					Target:         "summary_title",
					SourceNodeID:   "research",
					SourceArtifact: "summary",
					SourcePath:     "body.title",
					Required:       true,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ResolvePlanNodeInputActivity: %v", err)
	}

	if resolved.Input["summary_title"] != "artifact mapping" {
		t.Fatalf("summary_title = %#v", resolved.Input["summary_title"])
	}
}

func TestPlanActivitiesPublishArtifactsIsIdempotent(t *testing.T) {
	t.Parallel()
	activities := newTestPlanActivities(t, &fakePlanRuntime{})
	input := publishPlanArtifactsInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    Plan1,
			AccountID: "acct-1",
			ProjectID: "proj-1",
		},
		Node: agentos.PlanNodeSpec{
			NodeID: Node1,
		},
		Status: agentos.RunStatus{
			RunID:          "run-1",
			LifecycleState: "completed",
			Artifacts: []agentos.ArtifactRef{
				{ArtifactID: "artifact-1", Name: "summary", Kind: agentos.ArtifactKindObject},
			},
		},
	}

	first, err := activities.PublishPlanArtifactsActivity(context.Background(), &input)
	if err != nil {
		t.Fatalf("first PublishPlanArtifactsActivity: %v", err)
	}

	second, err := activities.PublishPlanArtifactsActivity(context.Background(), &input)
	if err != nil {
		t.Fatalf("second PublishPlanArtifactsActivity: %v", err)
	}

	if len(first.Artifacts) != 1 || len(second.Artifacts) != 1 {
		t.Fatalf("published artifacts = %#v %#v", first.Artifacts, second.Artifacts)
	}

	if second.Artifacts[0].ArtifactID != first.Artifacts[0].ArtifactID {
		t.Fatalf("idempotent artifact id = %q, want %q", second.Artifacts[0].ArtifactID, first.Artifacts[0].ArtifactID)
	}

	if first.Artifacts[0].PlanID != Plan1 || first.Artifacts[0].NodeID != Node1 || first.Artifacts[0].RunID != Run1 {
		t.Fatalf("artifact scope = %#v", first.Artifacts[0])
	}
}

func TestPublishPlanArtifactsActivityHistoryInputContainsArtifactRefsOnly(t *testing.T) {
	t.Parallel()

	input := publishPlanArtifactsInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    Plan1,
			AccountID: "acct-1",
			ProjectID: "proj-1",
		},
		Node: agentos.PlanNodeSpec{NodeID: Node1},
		Status: agentos.RunStatus{
			RunID:          "run-1",
			LifecycleState: "completed",
			Artifacts: []agentos.ArtifactRef{
				{
					ArtifactID: "artifact-1",
					PlanID:     Plan1,
					NodeID:     Node1,
					RunID:      "run-1",
					Name:       "summary",
					Kind:       agentos.ArtifactKindObject,
					URI:        "s3://artifact-bucket/plan-1/artifact-1",
					SizeBytes:  128,
					Digest:     "sha256:digest",
				},
			},
		},
	}

	data, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("Marshal publish input: %v", err)
	}

	var encoded map[string]any
	if err := json.Unmarshal(data, &encoded); err != nil {
		t.Fatalf("Unmarshal publish input: %v", err)
	}

	artifact := encodedArtifactRefFromHistoryInput(t, encoded)

	if _, exists := artifact["payload"]; exists {
		t.Fatalf("Temporal activity input embedded artifact payload: %#v", artifact)
	}

	if artifact["artifact_id"] != "artifact-1" || artifact["uri"] == "" || artifact["digest"] == "" {
		t.Fatalf("encoded artifact ref = %#v", artifact)
	}
}

func TestPlanActivitiesPublishArtifactsRetainsStoredPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	activities := newTestPlanActivities(t, &fakePlanRuntime{})
	node := agentos.PlanNodeSpec{NodeID: "research"}
	status := agentos.RunStatus{RunID: "run-research"}

	key, err := agentosplan.ArtifactPublishIdempotencyKey(Plan1, node.NodeID, status.RunID, "summary")
	if err != nil {
		t.Fatalf("ArtifactPublishIdempotencyKey: %v", err)
	}

	ref, err := putArtifact(ctx, activities.ArtifactStore, &agentos.ArtifactRef{
		ArtifactID: "artifact-summary",
		PlanID:     Plan1,
		NodeID:     node.NodeID,
		RunID:      status.RunID,
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{"value": "from backend"}, key)
	if err != nil {
		t.Fatalf("Put artifact payload: %v", err)
	}

	status.Artifacts = []agentos.ArtifactRef{ref}

	output, err := activities.PublishPlanArtifactsActivity(ctx, &publishPlanArtifactsInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    Plan1,
			AccountID: "acct-1",
			ProjectID: "proj-1",
		},
		Node:   node,
		Status: status,
	})
	if err != nil {
		t.Fatalf("PublishPlanArtifactsActivity: %v", err)
	}

	if len(output.Artifacts) != 1 || output.Artifacts[0].Digest != ref.Digest {
		t.Fatalf("published artifacts = %#v, want %#v", output.Artifacts, ref)
	}

	scope := agentos.PlanArtifactScope{
		PlanID:     Plan1,
		AccountID:  "acct-1",
		ProjectID:  "proj-1",
		ArtifactID: ref.ArtifactID,
	}

	_, payload, err := activities.ArtifactStore.Get(ctx, &scope)
	if err != nil {
		t.Fatalf("Get artifact: %v", err)
	}

	resultMap, ok := payload.(map[string]any)
	if !ok {
		t.Fatalf("payload is not a map: %#v", payload)
	}

	value := resultMap["value"]
	if value != "from backend" {
		t.Fatalf("payload = %#v, want original payload", payload)
	}
}

func TestPlanActivitiesPublishArtifactsValidatesSchemaRefPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	activities := newTestPlanActivities(t, &fakePlanRuntime{})

	schemas, err := agentosplan.NewStaticArtifactSchemaCatalog([]agentos.ArtifactSchema{
		{Ref: "schema:summary", Schema: json.RawMessage(`{
			"type": "object",
			"properties": {"score": {"type": "number"}},
			"required": ["score"]
		}`)},
	})
	if err != nil {
		t.Fatalf("NewStaticArtifactSchemaCatalog: %v", err)
	}

	activities.ArtifactSchemas = schemas
	node := agentos.PlanNodeSpec{
		NodeID: "research",
		Outputs: []agentos.ArtifactSpec{
			{Name: "summary", Kind: agentos.ArtifactKindObject, SchemaRef: "schema:summary"},
		},
	}
	status := agentos.RunStatus{RunID: "run-research", LifecycleState: "completed"}

	ref, err := putArtifact(ctx, activities.ArtifactStore, &agentos.ArtifactRef{
		ArtifactID: "artifact-summary",
		PlanID:     Plan1,
		NodeID:     node.NodeID,
		RunID:      status.RunID,
		Name:       "summary",
		Kind:       agentos.ArtifactKindObject,
	}, map[string]any{"score": "high"}, "plan-1:artifact-summary")
	if err != nil {
		t.Fatalf("Put artifact payload: %v", err)
	}

	status.Artifacts = []agentos.ArtifactRef{ref}

	_, err = activities.PublishPlanArtifactsActivity(ctx, &publishPlanArtifactsInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    Plan1,
			AccountID: "acct-1",
			ProjectID: "proj-1",
		},
		Node:   node,
		Status: status,
	})
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("schema mismatch error = %v, want ErrInvalidArtifact", err)
	}
}

func TestPlanActivitiesPublishArtifactsValidatesSuccessfulOutputContract(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}
	activities := newTestPlanActivities(t, &fakePlanRuntime{}, agentos.Capability{
		Backend: ref,
		Name:    "run",
		OutputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"artifacts": {
						"type": "array",
						"items": {
							"type": "object",
							"properties": {
								"metadata": {
									"type": "object",
									"properties": {
										"quality": {"enum": ["approved"]}
									},
									"required": ["quality"]
								}
							},
							"required": ["metadata"]
						}
					}
				},
				"required": ["artifacts"]
			}`),
	})

	_, err := activities.PublishPlanArtifactsActivity(context.Background(), &publishPlanArtifactsInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    Plan1,
			AccountID: "acct-1",
			ProjectID: "proj-1",
		},
		Node: agentos.PlanNodeSpec{
			NodeID:     "research",
			Capability: "run",
			Run:        agentos.RunSpec{RunID: "run-research", Backend: ref},
			Outputs: []agentos.ArtifactSpec{
				{Name: "summary", Kind: agentos.ArtifactKindObject, MediaType: "application/json", Required: true},
			},
		},
		Status: agentos.RunStatus{
			RunID:          "run-research",
			LifecycleState: "completed",
			Artifacts: []agentos.ArtifactRef{
				{
					ArtifactID: "artifact-1",
					Name:       "summary",
					Kind:       agentos.ArtifactKindObject,
					MediaType:  "application/json",
					Metadata:   map[string]string{"quality": "draft"},
				},
			},
		},
	})
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}

func TestPlanActivitiesPublishArtifactsDoesNotRequireOutputsForFailedRun(t *testing.T) {
	t.Parallel()
	activities := newTestPlanActivities(t, &fakePlanRuntime{})

	_, err := activities.PublishPlanArtifactsActivity(context.Background(), &publishPlanArtifactsInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    Plan1,
			AccountID: "acct-1",
			ProjectID: "proj-1",
		},
		Node: agentos.PlanNodeSpec{
			NodeID: Node1,
			Outputs: []agentos.ArtifactSpec{
				{Name: "summary", Kind: agentos.ArtifactKindObject, Required: true},
			},
		},
		Status: agentos.RunStatus{
			RunID:          "run-1",
			LifecycleState: "failed",
		},
	})
	if err != nil {
		t.Fatalf("PublishPlanArtifactsActivity failed run: %v", err)
	}
}

func TestPlanActivitiesPersistPlanStatePublishesStoredEvent(t *testing.T) {
	t.Parallel()
	activities := newTestPlanActivities(t, &fakePlanRuntime{})
	publisher := &fakePlanEventPublisher{}
	activities.PlanEventPublisher = publisher
	spec := agentos.RunPlanSpec{
		PlanID:         Plan1,
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "plan-start-key",
	}
	status := agentos.RunPlanStatus{
		PlanID:         Plan1,
		LifecycleState: agentos.PlanLifecycleRunning,
	}
	createPlanForActivityTest(t, activities, &spec, &status)

	output, err := activities.PersistPlanStateActivity(context.Background(), &persistPlanStateInput{
		Spec:   spec,
		Status: status,
		Event: agentos.PlanEvent{
			Event: agentos.Event{
				EventType: agentos.EventPlanStarted,
			},
			PlanID: Plan1,
		},
		IdempotencyKey: "event-key",
	})
	if err != nil {
		t.Fatalf("PersistPlanStateActivity: %v", err)
	}

	if output.Event.Sequence == 0 {
		t.Fatalf("persisted event sequence = %d", output.Event.Sequence)
	}

	if publisher.event.Sequence != output.Event.Sequence || publisher.event.EventID != output.Event.EventID {
		t.Fatalf("published event = %#v, want %#v", publisher.event, output.Event)
	}
}

func TestPlanActivitiesPersistPlanStateDoesNotFailOnLivePublishError(t *testing.T) {
	t.Parallel()
	activities := newTestPlanActivities(t, &fakePlanRuntime{})
	publisher := &fakePlanEventPublisher{err: errTestRedisUnavailable}
	activities.PlanEventPublisher = publisher
	spec := agentos.RunPlanSpec{
		PlanID:         Plan1,
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		IdempotencyKey: "plan-start-key",
	}
	status := agentos.RunPlanStatus{
		PlanID:         Plan1,
		LifecycleState: agentos.PlanLifecycleRunning,
	}
	createPlanForActivityTest(t, activities, &spec, &status)

	output, err := activities.PersistPlanStateActivity(context.Background(), &persistPlanStateInput{
		Spec:   spec,
		Status: status,
		Event: agentos.PlanEvent{
			Event: agentos.Event{
				EventType: agentos.EventPlanStarted,
			},
			PlanID: Plan1,
		},
		IdempotencyKey: "event-key",
	})
	if err != nil {
		t.Fatalf("PersistPlanStateActivity live publish failure: %v", err)
	}

	if output.Event.Sequence == 0 {
		t.Fatalf("persisted event sequence = %d", output.Event.Sequence)
	}

	if !publisher.called {
		t.Fatal("live publisher was not called")
	}
}

func TestPlanActivitiesPersistPlanStateDoesNotPublishWhenDurableTransitionFails(t *testing.T) {
	t.Parallel()
	activities := newTestPlanActivities(t, &fakePlanRuntime{})
	publisher := &fakePlanEventPublisher{}
	activities.PlanEventPublisher = publisher
	activities.PlanTransitionStore = failingPlanTransitionStore{err: errTestPostgresUnavailable}

	_, err := activities.PersistPlanStateActivity(context.Background(), &persistPlanStateInput{
		Spec: agentos.RunPlanSpec{
			PlanID:         Plan1,
			AccountID:      "acct-1",
			ProjectID:      "proj-1",
			IdempotencyKey: "plan-start-key",
		},
		Status: agentos.RunPlanStatus{
			PlanID:         Plan1,
			LifecycleState: agentos.PlanLifecycleRunning,
		},
		Event: agentos.PlanEvent{
			Event: agentos.Event{
				EventType: agentos.EventPlanStarted,
			},
			PlanID: Plan1,
		},
		IdempotencyKey: "event-key",
	})
	if err == nil {
		t.Fatal("PersistPlanStateActivity succeeded despite durable transition failure")
	}

	if publisher.called {
		t.Fatalf("live publisher was called with event %#v despite durable append failure", publisher.event)
	}
}

func TestPlanActivitiesEvaluatePlanExpansionValidatesDelta(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	artifactStore := agentosplan.NewMemoryArtifactStore()

	deltaRef, err := putArtifact(context.Background(), artifactStore, &agentos.ArtifactRef{ArtifactID: "delta-1", PlanID: "plan-expand", NodeID: "seed", RunID: "run-seed", Name: "expand", Kind: agentos.ArtifactKindPlanDelta}, agentosplan.PlanDelta{
		Nodes: []agentos.PlanNodeSpec{{NodeID: Expanded, Capability: "expand", Run: agentos.RunSpec{RunID: "run-expanded", Backend: ref}}},
		Edges: []agentos.PlanEdgeSpec{{EdgeID: "seed-expanded", From: "seed", To: Expanded, On: agentos.EdgeOnSuccess}},
	}, "delta-key")
	if err != nil {
		t.Fatalf("Put delta artifact: %v", err)
	}

	activities, err := NewPlanActivitiesWithStores(&fakePlanRuntime{}, []agentos.Capability{{Backend: ref, Name: "expand", Controls: []agentos.ControlOperation{agentos.ControlPause, agentos.ControlResume}}}, agentosplan.NewMemoryPlanStore(), nil, artifactStore)
	if err != nil {
		t.Fatalf("NewPlanActivitiesWithStores: %v", err)
	}

	output, err := activities.EvaluatePlanExpansionActivity(context.Background(), &evaluatePlanExpansionInput{
		Spec:      agentos.RunPlanSpec{PlanID: "plan-expand", AccountID: "acct-expand", ProjectID: "proj-expand", Policy: agentos.PlanPolicy{MaxNodes: 2}, Nodes: []agentos.PlanNodeSpec{{NodeID: "seed", Run: agentos.RunSpec{RunID: "run-seed", Backend: ref}}}},
		Status:    agentos.RunPlanStatus{PlanID: "plan-expand"},
		Node:      agentos.PlanNodeSpec{NodeID: "seed", Run: agentos.RunSpec{RunID: "run-seed", Backend: ref}},
		RunStatus: agentos.RunStatus{RunID: "run-seed", LifecycleState: "completed"},
		Artifacts: []agentos.ArtifactRef{deltaRef},
	})
	if err != nil {
		t.Fatalf("EvaluatePlanExpansionActivity: %v", err)
	}

	assertPlanExpansionApplied(t, &output, ref)
}

func TestPlanActivitiesValidatePlanUsesCapabilityCatalog(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}
	activities := newTestPlanActivities(t, &fakePlanRuntime{}, agentos.Capability{
		Backend: ref,
		Name:    "known",
	})

	_, err := activities.ValidatePlanActivity(context.Background(), &validatePlanInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    Plan1,
			AccountID: "acct-1",
			ProjectID: "proj-1",
			Nodes: []agentos.PlanNodeSpec{
				{
					NodeID:     "research",
					Capability: "missing",
					Run: agentos.RunSpec{
						RunID:   "run-research",
						Backend: ref,
					},
				},
			},
		},
	})
	if !errors.Is(err, agentos.ErrCapabilityNotFound) {
		t.Fatalf("error = %v, want ErrCapabilityNotFound", err)
	}

	output, err := activities.ValidatePlanActivity(context.Background(), &validatePlanInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    "plan-2",
			AccountID: "acct-2",
			ProjectID: "proj-2",
			Nodes: []agentos.PlanNodeSpec{
				{
					NodeID:     "research",
					Capability: "known",
					Run: agentos.RunSpec{
						RunID:   "run-research",
						Backend: ref,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ValidatePlanActivity known capability: %v", err)
	}

	if len(output.ControlsByNode["research"]) != 0 {
		t.Fatalf("controls = %#v", output.ControlsByNode)
	}
}

func TestPlanActivitiesValidatePlanReturnsCapabilityControls(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}
	activities := newTestPlanActivities(t, &fakePlanRuntime{}, agentos.Capability{
		Backend:  ref,
		Name:     "known",
		Controls: []agentos.ControlOperation{agentos.ControlPause, agentos.ControlResume},
	})

	output, err := activities.ValidatePlanActivity(context.Background(), &validatePlanInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    Plan1,
			AccountID: "acct-1",
			ProjectID: "proj-1",
			Nodes: []agentos.PlanNodeSpec{
				{
					NodeID:     "research",
					Capability: "known",
					Run: agentos.RunSpec{
						RunID:   "run-research",
						Backend: ref,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ValidatePlanActivity: %v", err)
	}

	if got := output.ControlsByNode["research"]; len(got) != 2 || got[0] != agentos.ControlPause || got[1] != agentos.ControlResume {
		t.Fatalf("controls = %#v", output.ControlsByNode)
	}
}

func TestPlanActivitiesValidatePlanUsesInjectedCapabilityCatalog(t *testing.T) {
	t.Parallel()

	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}

	catalog, err := agentosplan.NewStaticCapabilityCatalog([]agentos.Capability{
		{Backend: ref, Name: "run", Controls: []agentos.ControlOperation{agentos.ControlCancel}},
	})
	if err != nil {
		t.Fatalf("NewStaticCapabilityCatalog: %v", err)
	}

	store := agentosplan.NewMemoryPlanStore()

	activities, err := NewPlanActivitiesWithCatalog(
		&fakePlanRuntime{},
		catalog,
		store,
		nil,
		agentosplan.NewMemoryArtifactStore(),
	)
	if err != nil {
		t.Fatalf("NewPlanActivitiesWithCatalog: %v", err)
	}

	output, err := activities.ValidatePlanActivity(context.Background(), &validatePlanInput{
		Spec: agentos.RunPlanSpec{
			PlanID:    Plan1,
			AccountID: "acct-1",
			ProjectID: "proj-1",
			Nodes: []agentos.PlanNodeSpec{
				{
					NodeID:     "research",
					Capability: "run",
					Run: agentos.RunSpec{
						RunID:   "run-research",
						Backend: ref,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ValidatePlanActivity: %v", err)
	}

	if got := output.ControlsByNode["research"]; len(got) != 1 || got[0] != agentos.ControlCancel {
		t.Fatalf("controls = %#v", output.ControlsByNode)
	}
}

func newTestPlanActivities(t *testing.T, runtime agentos.Runtime, capabilities ...agentos.Capability) *PlanActivities {
	t.Helper()

	store := agentosplan.NewMemoryPlanStore()

	activities, err := NewPlanActivitiesWithStores(
		runtime,
		capabilities,
		store,
		nil,
		agentosplan.NewMemoryArtifactStore(),
	)
	if err != nil {
		t.Fatalf("NewPlanActivitiesWithStores: %v", err)
	}

	return activities
}

func createPlanForActivityTest(t *testing.T, activities *PlanActivities, spec *agentos.RunPlanSpec, status *agentos.RunPlanStatus) {
	t.Helper()

	planIndex, ok := activities.PlanTransitionStore.(agentosplan.PlanIndex)
	if !ok {
		t.Fatalf("activity test store %T does not implement PlanIndex", activities.PlanTransitionStore)
	}

	if _, _, err := planIndex.CreatePlan(context.Background(), spec, status); err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}
}

func encodedArtifactRefFromHistoryInput(t *testing.T, encoded map[string]any) map[string]any {
	t.Helper()

	status, ok := encoded["Status"].(map[string]any)
	if !ok {
		t.Fatalf("encoded status = %#v", encoded["Status"])
	}

	artifacts, ok := status["artifacts"].([]any)
	if !ok || len(artifacts) != 1 {
		t.Fatalf("encoded artifacts = %#v", status["artifacts"])
	}

	artifact, ok := artifacts[0].(map[string]any)
	if !ok {
		t.Fatalf("encoded artifact = %#v", artifacts[0])
	}

	return artifact
}

func assertPlanExpansionApplied(t *testing.T, output *EvaluatePlanExpansionOutput, ref agentos.BackendRef) {
	t.Helper()

	if !output.Expanded {
		t.Fatal("expansion was not applied")
	}

	if len(output.Spec.Nodes) != 2 || output.Spec.Nodes[1].NodeID != Expanded {
		t.Fatalf("expanded spec nodes = %#v", output.Spec.Nodes)
	}

	if output.Plan.NodeByID[Expanded].Run.RunID != "run-expanded" {
		t.Fatalf("expanded executable plan = %#v", output.Plan.NodeByID)
	}

	assertExpandedControls(t, output.ControlsByNode)
	assertExpandedCapability(t, output.CapabilitiesByNode, ref)
}

func assertExpandedControls(t *testing.T, controlsByNode map[string][]agentos.ControlOperation) {
	t.Helper()

	if got := controlsByNode[Expanded]; len(got) != 2 || got[0] != agentos.ControlPause || got[1] != agentos.ControlResume {
		t.Fatalf("expanded controls = %#v", controlsByNode)
	}
}

func assertExpandedCapability(t *testing.T, capabilitiesByNode map[string]agentosplan.CapabilitySelectionTrace, ref agentos.BackendRef) {
	t.Helper()

	trace, ok := capabilitiesByNode[Expanded]
	if !ok {
		t.Fatalf("expanded capability trace missing: %#v", capabilitiesByNode)
	}

	if trace.Backend != ref || trace.Capability != "expand" {
		t.Fatalf("expanded capability trace = %#v", trace)
	}
}

type fakePlanRuntime struct {
	started     agentos.RunSpec
	startStatus agentos.RunStatus
	control     agentos.ControlOperation
}

type fakePlanEventPublisher struct {
	event  agentos.PlanEvent
	err    error
	called bool
}

func (p *fakePlanEventPublisher) PublishPlanEvent(_ context.Context, event *agentos.PlanEvent) error {
	p.called = true
	p.event = *event

	return p.err
}

type failingPlanTransitionStore struct {
	err error
}

func (s failingPlanTransitionStore) PersistPlanTransition(context.Context, *agentosplan.PlanStateSnapshot, *agentos.PlanEvent, string) (agentos.PlanEvent, error) {
	return agentos.PlanEvent{}, s.err
}

func (r *fakePlanRuntime) Start(_ context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	r.started = *spec
	if r.startStatus.RunID != "" || r.startStatus.LifecycleState != "" {
		return r.startStatus, nil
	}

	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running", UpdatedAt: time.Now()}, nil
}

func (r *fakePlanRuntime) StartPlanNode(ctx context.Context, _, _ string, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	return r.Start(ctx, spec)
}

func (r *fakePlanRuntime) Signal(context.Context, string, *agentos.Signal) error {
	return nil
}

func (r *fakePlanRuntime) Status(context.Context, string) (agentos.RunStatus, error) {
	return agentos.RunStatus{RunID: "run-1", LifecycleState: "completed", UpdatedAt: time.Now()}, nil
}

func (r *fakePlanRuntime) Control(_ context.Context, _ string, control *agentos.ControlRequest) error {
	r.control = control.Operation

	return nil
}

func (r *fakePlanRuntime) Subscribe(context.Context, agentos.StreamScope) (agentos.Subscription, error) {
	return nil, nil
}

func (r *fakePlanRuntime) Close() error {
	return nil
}
