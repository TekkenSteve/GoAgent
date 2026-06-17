package temporal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
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

	if err := activities.ControlPlanNodeActivity(context.Background(), controlPlanNodeInput{RunID: "run-1", Operation: agentos.ControlCancel}); err != nil {
		t.Fatalf("ControlPlanNodeActivity: %v", err)
	}
	if runtime.control != agentos.ControlCancel {
		t.Fatalf("control = %q", runtime.control)
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

type fakePlanRuntime struct {
	started agentos.RunSpec
	control agentos.ControlOperation
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

func (r *fakePlanRuntime) Control(_ context.Context, _ string, op agentos.ControlOperation) error {
	r.control = op

	return nil
}

func (r *fakePlanRuntime) Subscribe(context.Context, agentos.StreamScope) (agentos.Subscription, error) {
	return nil, nil
}

func (r *fakePlanRuntime) Close() error {
	return nil
}
