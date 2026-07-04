package agentosprojection

import (
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosaction"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosbatch"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosledger"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosprocess"
)

func TestRuntimeImplementsProjectionRuntime(t *testing.T) {
	t.Parallel()

	var _ agentos.ProjectionRuntime = (*Runtime)(nil)
}

func TestRuntimeGetResourceProjectionAggregatesDurableStores(t *testing.T) {
	t.Parallel()

	fixture := newProjectionFixture(t)
	resource := fixture.resource

	projection, err := fixture.runtime.GetResourceProjection(t.Context(), &agentos.ResourceProjectionScope{
		Resource: resource,
	})
	if err != nil {
		t.Fatalf("GetResourceProjection: %v", err)
	}

	if projection.Resource != resource ||
		len(projection.Processes) != 1 ||
		len(projection.Ledger) != 1 ||
		len(projection.Actions) != 1 ||
		len(projection.Worksets) != 1 {
		t.Fatalf("projection = %#v, want one entry from each store", projection)
	}
}

func TestRuntimeListResourceProjectionsSummarizesProcessReadModel(t *testing.T) {
	t.Parallel()

	fixture := newProjectionFixture(t)

	summaries, err := fixture.runtime.ListResourceProjections(t.Context(), &agentos.ResourceProjectionListScope{
		AccountID:    fixture.resource.AccountID,
		ProjectID:    fixture.resource.ProjectID,
		ResourceKind: fixture.resource.Kind,
	})
	if err != nil {
		t.Fatalf("ListResourceProjections: %v", err)
	}

	if len(summaries) != 1 ||
		summaries[0].Resource != fixture.resource ||
		summaries[0].ProcessCount != 1 ||
		summaries[0].LatestProcessID != "process-1" {
		t.Fatalf("summaries = %#v, want resource process summary", summaries)
	}
}

type projectionFixture struct {
	runtime  *Runtime
	resource agentos.ResourceRef
}

func newProjectionFixture(t *testing.T) projectionFixture {
	t.Helper()

	processStore := agentosprocess.NewMemoryStore()
	ledgerStore := agentosledger.NewMemoryStore()
	actionStore := agentosaction.NewMemoryStore()
	worksetStore := agentosbatch.NewMemoryStore()

	runtime, err := NewRuntime(Config{
		Processes: processStore,
		Ledger:    ledgerStore,
		Actions:   actionStore,
		Worksets:  worksetStore,
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	resource := agentos.ResourceRef{
		Kind:       "resource-kind",
		ResourceID: "resource-1",
		AccountID:  "acct-1",
		ProjectID:  "proj-1",
	}

	startProcess(t, processStore, resource, now)
	appendLedger(t, ledgerStore, resource, now.Add(time.Minute))
	requestAction(t, actionStore, resource, now.Add(2*time.Minute))
	startWorkset(t, worksetStore, resource, now.Add(3*time.Minute))

	return projectionFixture{runtime: runtime, resource: resource}
}

func startProcess(t *testing.T, store *agentosprocess.MemoryStore, resource agentos.ResourceRef, requestedAt time.Time) {
	t.Helper()

	spec := agentos.Spec{
		ProcessID:      "process-1",
		Kind:           "resource-lifecycle",
		AccountID:      resource.AccountID,
		ProjectID:      resource.ProjectID,
		IdempotencyKey: "process-key-1",
		Resource:       resource,
		RequestedAt:    requestedAt,
	}
	status := agentos.Status{LifecycleState: agentos.ProcessRunning}

	if _, _, err := store.CreateProcess(t.Context(), &spec, &status); err != nil {
		t.Fatalf("CreateProcess: %v", err)
	}
}

func appendLedger(t *testing.T, store *agentosledger.MemoryStore, resource agentos.ResourceRef, occurredAt time.Time) {
	t.Helper()

	_, err := store.AppendLedgerEntry(t.Context(), &agentos.LedgerEntrySpec{
		EntryID:        "ledger-1",
		IdempotencyKey: "ledger-key-1",
		AccountID:      resource.AccountID,
		ProjectID:      resource.ProjectID,
		ProcessID:      "process-1",
		Resource:       resource,
		Kind:           agentos.LedgerEntryEvidence,
		OccurredAt:     occurredAt,
		Summary:        "evidence recorded",
	})
	if err != nil {
		t.Fatalf("AppendLedgerEntry: %v", err)
	}
}

func requestAction(t *testing.T, store *agentosaction.MemoryStore, resource agentos.ResourceRef, requestedAt time.Time) {
	t.Helper()

	runtime, err := agentosaction.NewRuntime(store)
	if err != nil {
		t.Fatalf("NewActionRuntime: %v", err)
	}

	_, err = runtime.RequestAction(t.Context(), &agentos.GovernedActionSpec{
		ActionID:       "action-1",
		IdempotencyKey: "action-key-1",
		AccountID:      resource.AccountID,
		ProjectID:      resource.ProjectID,
		ProcessID:      "process-1",
		Resource:       resource,
		Kind:           "investigation-action",
		RequestedAt:    requestedAt,
	})
	if err != nil {
		t.Fatalf("RequestAction: %v", err)
	}
}

func startWorkset(t *testing.T, store *agentosbatch.MemoryStore, resource agentos.ResourceRef, requestedAt time.Time) {
	t.Helper()

	runtime, err := agentosbatch.NewRuntime(store)
	if err != nil {
		t.Fatalf("NewBatchRuntime: %v", err)
	}

	_, err = runtime.StartWorkset(t.Context(), &agentos.WorksetSpec{
		WorksetID:      "workset-1",
		IdempotencyKey: "workset-key-1",
		AccountID:      resource.AccountID,
		ProjectID:      resource.ProjectID,
		ProcessID:      "process-1",
		Resource:       resource,
		Kind:           "batch-hunt",
		RequestedAt:    requestedAt,
		ItemsRef: agentos.WorksetItemsRef{
			Kind: "object-store",
			URI:  "s3://bucket/items.jsonl",
		},
	})
	if err != nil {
		t.Fatalf("StartWorkset: %v", err)
	}
}
