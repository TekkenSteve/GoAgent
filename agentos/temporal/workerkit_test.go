package temporal

import (
	"context"
	"errors"
	"slices"
	"testing"

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
		PostgresURL: "postgres://localhost:5432/testdb",
		RedisURL:    "redis://localhost:6379",
	}

	_, err := NewWorkerKit(context.Background(), &cfg)
	if !errors.Is(err, ErrWorkerArtifactStoreBackendRequired) {
		t.Fatalf("NewWorkerKit error = %v, want %v", err, ErrWorkerArtifactStoreBackendRequired)
	}
}

func TestNewWorkerKitRequiresLocalArtifactStoreRoot(t *testing.T) {
	t.Parallel()

	cfg := WorkerConfig{
		PostgresURL: "postgres://localhost:5432/testdb",
		RedisURL:    "redis://localhost:6379",
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
		PostgresURL: "postgres://localhost:5432/testdb",
		RedisURL:    "redis://localhost:6379",
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
	kit := &WorkerKit{planActivities: newTestPlanActivities(t, &fakePlanRuntime{})}
	worker := &fakeWorker{}

	if err := kit.Register(worker); err != nil {
		t.Fatalf("Register: %v", err)
	}

	for _, name := range []string{
		PlanWorkflowName,
	} {
		if !worker.workflowRegistered(name) {
			t.Fatalf("workflow %q was not registered; got %#v", name, worker.workflows)
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
		if !worker.activityRegistered(name) {
			t.Fatalf("activity %q was not registered; got %#v", name, worker.activities)
		}
	}
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
