package temporal

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosprocess"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

func TestNewWorkerKitRequiresPostgresURL(t *testing.T) {
	t.Parallel()

	cfg := WorkerConfig{}

	_, err := NewWorkerKit(context.Background(), &cfg)
	if !errors.Is(err, ErrWorkerPostgresURLRequired) {
		t.Fatalf("NewWorkerKit error = %v, want %v", err, ErrWorkerPostgresURLRequired)
	}
}

func TestNewWorkerKitRequiresRedisURL(t *testing.T) {
	t.Parallel()

	cfg := WorkerConfig{
		PostgresURL: "postgres://localhost:5432/testdb",
	}

	_, err := NewWorkerKit(context.Background(), &cfg)
	if !errors.Is(err, ErrWorkerRedisURLRequired) {
		t.Fatalf("NewWorkerKit error = %v, want %v", err, ErrWorkerRedisURLRequired)
	}
}

func TestNewWorkerKitRequiresArtifactStoreBackend(t *testing.T) {
	t.Parallel()

	cfg := WorkerConfig{
		PostgresURL:        "postgres://localhost:5432/testdb",
		RedisURL:           "redis://localhost:6379",
		TemporalTaskQueues: DefaultTaskQueues(),
	}

	_, err := NewWorkerKit(context.Background(), &cfg)
	if !errors.Is(err, ErrWorkerArtifactStoreBackendRequired) {
		t.Fatalf("NewWorkerKit error = %v, want %v", err, ErrWorkerArtifactStoreBackendRequired)
	}
}

func TestNewWorkerKitRequiresLocalArtifactStoreRoot(t *testing.T) {
	t.Parallel()

	cfg := WorkerConfig{
		PostgresURL:        "postgres://localhost:5432/testdb",
		RedisURL:           "redis://localhost:6379",
		TemporalTaskQueues: DefaultTaskQueues(),
		ArtifactStore: ArtifactStoreConfig{
			Backend: ArtifactStoreBackendLocal,
		},
	}

	_, err := NewWorkerKit(context.Background(), &cfg)
	if !errors.Is(err, ErrWorkerArtifactStoreLocalRootRequired) {
		t.Fatalf("NewWorkerKit error = %v, want %v", err, ErrWorkerArtifactStoreLocalRootRequired)
	}
}

func TestNewWorkerKitRequiresS3ArtifactStoreBucket(t *testing.T) {
	t.Parallel()

	cfg := WorkerConfig{
		PostgresURL:        "postgres://localhost:5432/testdb",
		RedisURL:           "redis://localhost:6379",
		TemporalTaskQueues: DefaultTaskQueues(),
		ArtifactStore: ArtifactStoreConfig{
			Backend: ArtifactStoreBackendS3,
		},
	}

	_, err := NewWorkerKit(context.Background(), &cfg)
	if !errors.Is(err, ErrWorkerArtifactStoreS3BucketRequired) {
		t.Fatalf("NewWorkerKit error = %v, want %v", err, ErrWorkerArtifactStoreS3BucketRequired)
	}
}

func TestWorkerKitRegistersPlanWorkflowAndActivities(t *testing.T) {
	t.Parallel()
	kit := &WorkerKit{
		planActivities:    newTestPlanActivities(t, &fakePlanRuntime{}),
		processActivities: newTestProcessActivities(t),
	}
	workers := fakeWorkerSet()

	if err := kit.Register(workers.workerSet()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	for _, name := range []string{
		PlanWorkflowName,
	} {
		if !workers.planControl.workflowRegistered(name) {
			t.Fatalf("workflow %q was not registered on plan control; got %#v", name, workers.planControl.workflows)
		}
	}

	for _, name := range []string{
		ValidatePlanActivityName,
		ResolvePlanNodeInputActivityName,
		StartPlanNodeActivityName,
		StatusPlanNodeActivityName,
		ControlPlanNodeActivityName,
		PublishPlanArtifactsActivityName,
		EvaluatePlanExpansionActivityName,
		PersistPlanStateActivityName,
	} {
		if !workers.planActivity.activityRegistered(name) {
			t.Fatalf("activity %q was not registered on plan activity; got %#v", name, workers.planActivity.activities)
		}
	}

	if len(workers.nativeControl.workflows) != 0 || len(workers.nativeLLM.activities) != 0 || len(workers.nativeTool.activities) != 0 {
		t.Fatalf("native workers should not receive registrations for a plan-only kit: %#v %#v %#v", workers.nativeControl, workers.nativeLLM, workers.nativeTool)
	}
}

func TestWorkerKitRegistersProcessWorkflowAndActivities(t *testing.T) {
	t.Parallel()

	kit := &WorkerKit{
		planActivities:    newTestPlanActivities(t, &fakePlanRuntime{}),
		processActivities: newTestProcessActivities(t),
	}
	workers := fakeWorkerSet()

	if err := kit.Register(workers.workerSet()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if !workers.processControl.workflowRegistered(ProcessWorkflowName) {
		t.Fatalf("process workflow was not registered on process control; got %#v", workers.processControl.workflows)
	}

	for _, name := range []string{
		StartProcessActivityName,
		SignalProcessActivityName,
		ControlProcessActivityName,
		FireProcessTimerActivityName,
	} {
		if !workers.processActivity.activityRegistered(name) {
			t.Fatalf("process activity %q was not registered on process activity; got %#v", name, workers.processActivity.activities)
		}
	}
}

func TestWorkerKitRegistersNativeWorkloadsOnDedicatedWorkers(t *testing.T) {
	t.Parallel()

	kit := &WorkerKit{
		activities:        &orchestration.AgentActivities{},
		planActivities:    newTestPlanActivities(t, &fakePlanRuntime{}),
		processActivities: newTestProcessActivities(t),
	}
	workers := fakeWorkerSet()

	if err := kit.Register(workers.workerSet()); err != nil {
		t.Fatalf("Register: %v", err)
	}

	assertNativeControlRegistrations(t, workers.nativeControl)
	assertNativeActivityRegistrations(t, workers.nativeLLM, workers.nativeTool)
	assertStreamRegistrations(t, workers.stream)
	assertTriggerRegistrations(t, workers.trigger)
}

func assertNativeControlRegistrations(t *testing.T, worker *fakeWorker) {
	t.Helper()

	for _, name := range []string{
		orchestration.AgentWorkflowName,
		orchestration.OrchestrationWorkflowName,
	} {
		if !worker.workflowRegistered(name) {
			t.Fatalf("workflow %q was not registered on native control; got %#v", name, worker.workflows)
		}
	}

	if !worker.activityRegistered(orchestration.PrepareActivityName) {
		t.Fatalf("prepare activity was not registered on native control; got %#v", worker.activities)
	}
}

func assertNativeActivityRegistrations(t *testing.T, llmWorker, toolWorker *fakeWorker) {
	t.Helper()

	if !llmWorker.activityRegistered(orchestration.LLMStepActivityName) {
		t.Fatalf("llm activity was not registered on native llm worker; got %#v", llmWorker.activities)
	}

	if !toolWorker.activityRegistered(orchestration.ToolExecActivityName) {
		t.Fatalf("tool activity was not registered on native tool worker; got %#v", toolWorker.activities)
	}
}

func assertStreamRegistrations(t *testing.T, worker *fakeWorker) {
	t.Helper()

	if !worker.workflowRegistered(orchestration.StreamWorkflowName) {
		t.Fatalf("stream workflow was not isolated on stream worker; got %#v", worker.workflows)
	}

	for _, name := range []string{
		orchestration.InitStreamActivityName,
		orchestration.LLMStreamActivityName,
		orchestration.ToolExecStreamActivityName,
		orchestration.FinishStreamActivityName,
	} {
		if !worker.activityRegistered(name) {
			t.Fatalf("stream activity %q was not isolated on stream worker; got %#v", name, worker.activities)
		}
	}
}

func assertTriggerRegistrations(t *testing.T, worker *fakeWorker) {
	t.Helper()

	if !worker.workflowRegistered(orchestration.TriggerFireWorkflowName) {
		t.Fatalf("trigger workflow was not isolated on trigger worker; got %#v", worker.workflows)
	}

	if !worker.activityRegistered(orchestration.FireTriggerActivityName) {
		t.Fatalf("trigger activity was not isolated on trigger worker; got %#v", worker.activities)
	}
}

func newTestProcessActivities(t *testing.T) *ProcessActivities {
	t.Helper()

	store := agentosprocess.NewMemoryStore()

	activities, err := NewProcessActivities(store, store)
	if err != nil {
		t.Fatalf("NewProcessActivities: %v", err)
	}

	return activities
}

type fakeWorker struct {
	workflows  []string
	activities []string
}

func (w *fakeWorker) RegisterWorkflow(any) {}

func (w *fakeWorker) RegisterWorkflowWithOptions(_ any, options workflow.RegisterOptions) {
	w.workflows = append(w.workflows, options.Name)
}

func (w *fakeWorker) RegisterActivity(any) {}

func (w *fakeWorker) RegisterActivityWithOptions(_ any, options activity.RegisterOptions) {
	w.activities = append(w.activities, options.Name)
}

func (w *fakeWorker) RegisterNexusService(*nexus.Service) {}

func (w *fakeWorker) Start() error { return nil }

func (w *fakeWorker) Run(<-chan any) error { return nil }

func (w *fakeWorker) Stop() {}

func (w *fakeWorker) workflowRegistered(name string) bool {
	return slices.Contains(w.workflows, name)
}

func (w *fakeWorker) activityRegistered(name string) bool {
	return slices.Contains(w.activities, name)
}

type fakeWorkers struct {
	planControl     *fakeWorker
	planActivity    *fakeWorker
	processControl  *fakeWorker
	processActivity *fakeWorker
	nativeControl   *fakeWorker
	nativeLLM       *fakeWorker
	nativeTool      *fakeWorker
	stream          *fakeWorker
	trigger         *fakeWorker
}

func fakeWorkerSet() fakeWorkers {
	return fakeWorkers{
		planControl:     &fakeWorker{},
		planActivity:    &fakeWorker{},
		processControl:  &fakeWorker{},
		processActivity: &fakeWorker{},
		nativeControl:   &fakeWorker{},
		nativeLLM:       &fakeWorker{},
		nativeTool:      &fakeWorker{},
		stream:          &fakeWorker{},
		trigger:         &fakeWorker{},
	}
}

func (w fakeWorkers) workerSet() *WorkerSet {
	return &WorkerSet{
		PlanControl:     w.planControl,
		PlanActivity:    w.planActivity,
		ProcessControl:  w.processControl,
		ProcessActivity: w.processActivity,
		NativeControl:   w.nativeControl,
		NativeLLM:       w.nativeLLM,
		NativeTool:      w.nativeTool,
		Stream:          w.stream,
		Trigger:         w.trigger,
	}
}
