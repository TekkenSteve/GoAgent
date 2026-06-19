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

func TestPlanActivitiesStartStatusControl(t *testing.T) {
	runtime := &fakePlanRuntime{}
	activities := NewPlanActivities(runtime)
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}

	started, err := activities.StartPlanNodeActivity(context.Background(), startPlanNodeInput{
		PlanID: "plan-1",
		Node: agentos.PlanNodeSpec{
			NodeID: "node-1",
			Run: agentos.RunSpec{
				RunID:   "run-1",
				Backend: ref,
			},
		},
	})
	if err != nil {
		t.Fatalf("StartPlanNodeActivity: %v", err)
	}
	if started.Status.RunID != "run-1" || runtime.started.Backend != ref {
		t.Fatalf("unexpected start: %#v %#v", started, runtime.started)
	}

	status, err := activities.StatusPlanNodeActivity(context.Background(), statusPlanNodeInput{RunID: "run-1"})
	if err != nil {
		t.Fatalf("StatusPlanNodeActivity: %v", err)
	}
	if status.Status.LifecycleState != "completed" {
		t.Fatalf("status = %#v", status)
	}

	if err := activities.ControlPlanNodeActivity(context.Background(), controlPlanNodeInput{RunID: "run-1", Control: agentos.ControlRequest{Operation: agentos.ControlCancel}}); err != nil {
		t.Fatalf("ControlPlanNodeActivity: %v", err)
	}
	if runtime.control != agentos.ControlCancel {
		t.Fatalf("control = %q", runtime.control)
	}
}

func TestPlanActivitiesStartResolvesMappedInput(t *testing.T) {
	runtime := &fakePlanRuntime{}
	activities := NewPlanActivities(runtime)
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}

	started, err := activities.StartPlanNodeActivity(context.Background(), startPlanNodeInput{
		PlanID: "plan-1",
		PlanInputs: map[string]any{
			"task": map[string]any{"topic": "artifact routing"},
		},
		Node: agentos.PlanNodeSpec{
			NodeID: "node-1",
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
		t.Fatalf("StartPlanNodeActivity: %v", err)
	}
	if started.Status.RunID != "run-1" {
		t.Fatalf("started = %#v", started)
	}
	if runtime.started.Input["existing"] != true {
		t.Fatalf("existing input = %#v", runtime.started.Input)
	}
	if runtime.started.Input["topic"] != "artifact routing" {
		t.Fatalf("topic input = %#v", runtime.started.Input)
	}
}

func TestPlanActivitiesPublishArtifactsIsIdempotent(t *testing.T) {
	activities := NewPlanActivities(&fakePlanRuntime{})
	input := publishPlanArtifactsInput{
		PlanID: "plan-1",
		Node: agentos.PlanNodeSpec{
			NodeID: "node-1",
		},
		Status: agentos.RunStatus{
			RunID:          "run-1",
			LifecycleState: "completed",
			Artifacts: []agentos.ArtifactRef{
				{ArtifactID: "artifact-1", Name: "summary", Kind: agentos.ArtifactKindObject},
			},
		},
	}

	first, err := activities.PublishPlanArtifactsActivity(context.Background(), input)
	if err != nil {
		t.Fatalf("first PublishPlanArtifactsActivity: %v", err)
	}
	second, err := activities.PublishPlanArtifactsActivity(context.Background(), input)
	if err != nil {
		t.Fatalf("second PublishPlanArtifactsActivity: %v", err)
	}
	if len(first.Artifacts) != 1 || len(second.Artifacts) != 1 {
		t.Fatalf("published artifacts = %#v %#v", first.Artifacts, second.Artifacts)
	}
	if second.Artifacts[0].ArtifactID != first.Artifacts[0].ArtifactID {
		t.Fatalf("idempotent artifact id = %q, want %q", second.Artifacts[0].ArtifactID, first.Artifacts[0].ArtifactID)
	}
	if first.Artifacts[0].PlanID != "plan-1" || first.Artifacts[0].NodeID != "node-1" || first.Artifacts[0].RunID != "run-1" {
		t.Fatalf("artifact scope = %#v", first.Artifacts[0])
	}
}

func TestPlanActivitiesPublishArtifactsValidatesSuccessfulOutputContract(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}
	activities, err := NewPlanActivitiesWithCapabilities(&fakePlanRuntime{}, []agentos.Capability{
		{
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
		},
	})
	if err != nil {
		t.Fatalf("NewPlanActivitiesWithCapabilities: %v", err)
	}

	_, err = activities.PublishPlanArtifactsActivity(context.Background(), publishPlanArtifactsInput{
		PlanID: "plan-1",
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
	activities := NewPlanActivities(&fakePlanRuntime{})
	_, err := activities.PublishPlanArtifactsActivity(context.Background(), publishPlanArtifactsInput{
		PlanID: "plan-1",
		Node: agentos.PlanNodeSpec{
			NodeID: "node-1",
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
	activities := NewPlanActivities(&fakePlanRuntime{})
	publisher := &fakePlanEventPublisher{}
	activities.PlanEventPublisher = publisher

	output, err := activities.PersistPlanStateActivity(context.Background(), persistPlanStateInput{
		Spec: agentos.RunPlanSpec{
			PlanID:         "plan-1",
			IdempotencyKey: "plan-start-key",
		},
		Status: agentos.RunPlanStatus{
			PlanID:         "plan-1",
			LifecycleState: agentos.PlanLifecycleRunning,
		},
		Event: agentos.PlanEvent{
			Event: agentos.Event{
				EventType: agentos.EventPlanStarted,
			},
			PlanID: "plan-1",
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

func TestPlanActivitiesEvaluatePlanExpansionValidatesDelta(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative}
	artifactStore := agentosplan.NewMemoryArtifactStore()
	deltaRef, err := artifactStore.Put(context.Background(), agentos.ArtifactRef{
		ArtifactID: "delta-1",
		PlanID:     "plan-expand",
		NodeID:     "seed",
		RunID:      "run-seed",
		Name:       "expand",
		Kind:       agentos.ArtifactKindPlanDelta,
	}, agentosplan.PlanDelta{
		Nodes: []agentos.PlanNodeSpec{
			{NodeID: "expanded", Run: agentos.RunSpec{RunID: "run-expanded", Backend: ref}},
		},
		Edges: []agentos.PlanEdgeSpec{
			{EdgeID: "seed-expanded", From: "seed", To: "expanded", On: agentos.EdgeOnSuccess},
		},
	}, "delta-key")
	if err != nil {
		t.Fatalf("Put delta artifact: %v", err)
	}
	activities, err := NewPlanActivitiesWithStores(
		&fakePlanRuntime{},
		nil,
		agentosplan.NewMemoryPlanStore(),
		agentosplan.NewMemoryPlanStore(),
		nil,
		nil,
		artifactStore,
	)
	if err != nil {
		t.Fatalf("NewPlanActivitiesWithStores: %v", err)
	}

	output, err := activities.EvaluatePlanExpansionActivity(context.Background(), evaluatePlanExpansionInput{
		Spec: agentos.RunPlanSpec{
			PlanID: "plan-expand",
			Policy: agentos.PlanPolicy{
				MaxNodes: 2,
			},
			Nodes: []agentos.PlanNodeSpec{
				{NodeID: "seed", Run: agentos.RunSpec{RunID: "run-seed", Backend: ref}},
			},
		},
		Status: agentos.RunPlanStatus{PlanID: "plan-expand"},
		Node:   agentos.PlanNodeSpec{NodeID: "seed", Run: agentos.RunSpec{RunID: "run-seed", Backend: ref}},
		RunStatus: agentos.RunStatus{
			RunID:          "run-seed",
			LifecycleState: "completed",
		},
		Artifacts: []agentos.ArtifactRef{deltaRef},
	})
	if err != nil {
		t.Fatalf("EvaluatePlanExpansionActivity: %v", err)
	}
	if !output.Expanded {
		t.Fatal("expansion was not applied")
	}
	if len(output.Spec.Nodes) != 2 || output.Spec.Nodes[1].NodeID != "expanded" {
		t.Fatalf("expanded spec nodes = %#v", output.Spec.Nodes)
	}
	if output.Plan.NodeByID["expanded"].Run.RunID != "run-expanded" {
		t.Fatalf("expanded executable plan = %#v", output.Plan.NodeByID)
	}
}

func TestPlanActivitiesValidatePlanUsesCapabilityCatalog(t *testing.T) {
	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}
	activities, err := NewPlanActivitiesWithCapabilities(&fakePlanRuntime{}, []agentos.Capability{
		{Backend: ref, Name: "known"},
	})
	if err != nil {
		t.Fatalf("NewPlanActivitiesWithCapabilities: %v", err)
	}

	_, err = activities.ValidatePlanActivity(context.Background(), validatePlanInput{
		Spec: agentos.RunPlanSpec{
			PlanID: "plan-1",
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

	output, err := activities.ValidatePlanActivity(context.Background(), validatePlanInput{
		Spec: agentos.RunPlanSpec{
			PlanID: "plan-2",
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
	ref := agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "research"}
	activities, err := NewPlanActivitiesWithCapabilities(&fakePlanRuntime{}, []agentos.Capability{
		{Backend: ref, Name: "known", Controls: []agentos.ControlOperation{agentos.ControlPause, agentos.ControlResume}},
	})
	if err != nil {
		t.Fatalf("NewPlanActivitiesWithCapabilities: %v", err)
	}

	output, err := activities.ValidatePlanActivity(context.Background(), validatePlanInput{
		Spec: agentos.RunPlanSpec{
			PlanID: "plan-1",
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
		store,
		nil,
		nil,
		agentosplan.NewMemoryArtifactStore(),
	)
	if err != nil {
		t.Fatalf("NewPlanActivitiesWithCatalog: %v", err)
	}

	output, err := activities.ValidatePlanActivity(context.Background(), validatePlanInput{
		Spec: agentos.RunPlanSpec{
			PlanID: "plan-1",
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

type fakePlanRuntime struct {
	started agentos.RunSpec
	control agentos.ControlOperation
}

type fakePlanEventPublisher struct {
	event agentos.PlanEvent
}

func (p *fakePlanEventPublisher) PublishPlanEvent(_ context.Context, event agentos.PlanEvent) error {
	p.event = event

	return nil
}

func (r *fakePlanRuntime) Start(_ context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	r.started = spec

	return agentos.RunStatus{RunID: spec.RunID, LifecycleState: "running", UpdatedAt: time.Now()}, nil
}

func (r *fakePlanRuntime) Signal(context.Context, string, agentos.Signal) error {
	return nil
}

func (r *fakePlanRuntime) Status(context.Context, string) (agentos.RunStatus, error) {
	return agentos.RunStatus{RunID: "run-1", LifecycleState: "completed", UpdatedAt: time.Now()}, nil
}

func (r *fakePlanRuntime) Control(_ context.Context, _ string, control agentos.ControlRequest) error {
	r.control = control.Operation

	return nil
}

func (r *fakePlanRuntime) Subscribe(context.Context, agentos.StreamScope) (agentos.Subscription, error) {
	return nil, nil
}

func (r *fakePlanRuntime) Close() error {
	return nil
}
