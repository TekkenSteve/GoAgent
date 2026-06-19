package temporal

import (
	"context"
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

func TestNewWorkerKitRequiresPostgresURL(t *testing.T) {
	_, err := NewWorkerKit(context.Background(), WorkerConfig{})
	if !errors.Is(err, ErrWorkerPostgresURLRequired) {
		t.Fatalf("NewWorkerKit error = %v, want %v", err, ErrWorkerPostgresURLRequired)
	}
}

func TestNewWorkerKitRequiresRedisURL(t *testing.T) {
	_, err := NewWorkerKit(context.Background(), WorkerConfig{
		PostgresURL: "postgres://user:pass@localhost:5432/db",
	})
	if !errors.Is(err, ErrWorkerRedisURLRequired) {
		t.Fatalf("NewWorkerKit error = %v, want %v", err, ErrWorkerRedisURLRequired)
	}
}

func TestNewWorkerKitRequiresArtifactStoreBackend(t *testing.T) {
	_, err := NewWorkerKit(context.Background(), WorkerConfig{
		PostgresURL: "postgres://user:pass@localhost:5432/db",
		RedisURL:    "redis://localhost:6379",
	})
	if !errors.Is(err, ErrWorkerArtifactStoreBackendRequired) {
		t.Fatalf("NewWorkerKit error = %v, want %v", err, ErrWorkerArtifactStoreBackendRequired)
	}
}

func TestNewWorkerKitRequiresLocalArtifactStoreRoot(t *testing.T) {
	_, err := NewWorkerKit(context.Background(), WorkerConfig{
		PostgresURL: "postgres://user:pass@localhost:5432/db",
		RedisURL:    "redis://localhost:6379",
		ArtifactStore: ArtifactStoreConfig{
			Backend: ArtifactStoreBackendLocal,
		},
	})
	if !errors.Is(err, ErrWorkerArtifactStoreLocalRootRequired) {
		t.Fatalf("NewWorkerKit error = %v, want %v", err, ErrWorkerArtifactStoreLocalRootRequired)
	}
}

func TestNewWorkerKitRequiresS3ArtifactStoreBucket(t *testing.T) {
	_, err := NewWorkerKit(context.Background(), WorkerConfig{
		PostgresURL: "postgres://user:pass@localhost:5432/db",
		RedisURL:    "redis://localhost:6379",
		ArtifactStore: ArtifactStoreConfig{
			Backend: ArtifactStoreBackendS3,
		},
	})
	if !errors.Is(err, ErrWorkerArtifactStoreS3BucketRequired) {
		t.Fatalf("NewWorkerKit error = %v, want %v", err, ErrWorkerArtifactStoreS3BucketRequired)
	}
}

func TestWorkerKitRegistersPlanWorkflowAndActivities(t *testing.T) {
	kit := &WorkerKit{planActivities: NewPlanActivities(&fakePlanRuntime{})}
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

func (w *fakeWorker) RegisterWorkflow(interface{}) {}

func (w *fakeWorker) RegisterWorkflowWithOptions(_ interface{}, options workflow.RegisterOptions) {
	w.workflows = append(w.workflows, options.Name)
}

func (w *fakeWorker) RegisterActivity(interface{}) {}

func (w *fakeWorker) RegisterActivityWithOptions(_ interface{}, options activity.RegisterOptions) {
	w.activities = append(w.activities, options.Name)
}

func (w *fakeWorker) RegisterNexusService(*nexus.Service) {}

func (w *fakeWorker) Start() error { return nil }

func (w *fakeWorker) Run(<-chan interface{}) error { return nil }

func (w *fakeWorker) Stop() {}

func (w *fakeWorker) workflowRegistered(name string) bool {
	for _, registered := range w.workflows {
		if registered == name {
			return true
		}
	}

	return false
}

func (w *fakeWorker) activityRegistered(name string) bool {
	for _, registered := range w.activities {
		if registered == name {
			return true
		}
	}

	return false
}
