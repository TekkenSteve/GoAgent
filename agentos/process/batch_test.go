package process

import (
	"errors"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestValidateWorksetSpecAcceptsReferencedBatch(t *testing.T) {
	t.Parallel()

	spec := validWorksetSpec()

	if err := ValidateWorksetSpec(&spec); err != nil {
		t.Fatalf("ValidateWorksetSpec: %v", err)
	}
}

func TestValidateWorksetSpecRequiresIdentity(t *testing.T) {
	t.Parallel()

	missingID := validWorksetSpec()
	missingID.WorksetID = ""

	idErr := ValidateWorksetSpec(&missingID)
	if !errors.Is(idErr, core.ErrInvalidWorkset) {
		t.Fatalf("missing workset id error = %v, want core.ErrInvalidWorkset", idErr)
	}

	missingKey := validWorksetSpec()
	missingKey.IdempotencyKey = ""

	keyErr := ValidateWorksetSpec(&missingKey)
	if !errors.Is(keyErr, core.ErrInvalidWorkset) {
		t.Fatalf("missing idempotency key error = %v, want core.ErrInvalidWorkset", keyErr)
	}

	missingKind := validWorksetSpec()
	missingKind.Kind = ""

	kindErr := ValidateWorksetSpec(&missingKind)
	if !errors.Is(kindErr, core.ErrInvalidWorkset) {
		t.Fatalf("missing kind error = %v, want core.ErrInvalidWorkset", kindErr)
	}
}

func TestValidateWorksetSpecRequiresTenantAndProcessOrResourceScope(t *testing.T) {
	t.Parallel()

	account := validWorksetSpec()
	account.AccountID = ""

	accountErr := ValidateWorksetSpec(&account)
	if !errors.Is(accountErr, core.ErrInvalidWorkset) {
		t.Fatalf("account scope error = %v, want core.ErrInvalidWorkset", accountErr)
	}

	scope := validWorksetSpec()
	scope.ProcessID = ""
	scope.Resource = ResourceRef{}

	scopeErr := ValidateWorksetSpec(&scope)
	if !errors.Is(scopeErr, core.ErrInvalidWorkset) {
		t.Fatalf("process/resource scope error = %v, want core.ErrInvalidWorkset", scopeErr)
	}
}

func TestValidateWorksetSpecRejectsInvalidItemsRef(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*WorksetItemsRef)
	}{
		{name: "kind", edit: func(ref *WorksetItemsRef) { ref.Kind = "" }},
		{name: "target", edit: func(ref *WorksetItemsRef) {
			ref.URI = ""
			ref.ArtifactID = ""
		}},
		{name: "count", edit: func(ref *WorksetItemsRef) { ref.Count = -1 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec := validWorksetSpec()
			tt.edit(&spec.ItemsRef)

			err := ValidateWorksetSpec(&spec)
			if !errors.Is(err, core.ErrInvalidWorkset) {
				t.Fatalf("ValidateWorksetSpec error = %v, want core.ErrInvalidWorkset", err)
			}
		})
	}
}

func TestValidateWorksetSpecEnforcesChunkPolicy(t *testing.T) {
	t.Parallel()

	spec := validWorksetSpec()
	spec.Policy = WorksetPolicy{MaxChunks: 1, MaxChunkSize: 5, MaxConcurrency: 2}
	spec.Chunks = []WorksetChunkSpec{
		validWorksetChunk("chunk-1", 4),
		validWorksetChunk("chunk-2", 4),
	}

	err := ValidateWorksetSpec(&spec)
	if !errors.Is(err, core.ErrInvalidWorkset) {
		t.Fatalf("chunk count error = %v, want core.ErrInvalidWorkset", err)
	}

	spec.Chunks = []WorksetChunkSpec{validWorksetChunk("chunk-1", 6)}

	err = ValidateWorksetSpec(&spec)
	if !errors.Is(err, core.ErrInvalidWorkset) {
		t.Fatalf("chunk size error = %v, want core.ErrInvalidWorkset", err)
	}

	spec.Chunks = []WorksetChunkSpec{validWorksetChunk("chunk-1", 4)}
	spec.Chunks[0].Concurrency = 3

	err = ValidateWorksetSpec(&spec)
	if !errors.Is(err, core.ErrInvalidWorkset) {
		t.Fatalf("chunk concurrency error = %v, want core.ErrInvalidWorkset", err)
	}
}

func TestValidateWorksetScopeRequiresTenantScope(t *testing.T) {
	t.Parallel()

	err := ValidateWorksetScope(&WorksetScope{AccountID: "acct-1", Limit: -1})
	if !errors.Is(err, core.ErrInvalidWorksetScope) {
		t.Fatalf("ValidateWorksetScope error = %v, want core.ErrInvalidWorksetScope", err)
	}
}

func validWorksetSpec() WorksetSpec {
	return WorksetSpec{
		WorksetID:      "workset-1",
		IdempotencyKey: "workset-key-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		ProcessID:      "process-1",
		Resource: ResourceRef{
			Kind:       "resource-kind",
			ResourceID: "resource-1",
			AccountID:  "acct-1",
			ProjectID:  "proj-1",
		},
		Kind:        "batch-operation",
		RequestedAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
		ItemsRef: WorksetItemsRef{
			Kind:      "dataset",
			URI:       "s3://bucket/items.jsonl",
			MediaType: "application/jsonl",
			Count:     10,
		},
		Chunks: []WorksetChunkSpec{
			validWorksetChunk("chunk-1", 5),
			validWorksetChunk("chunk-2", 5),
		},
		Policy: WorksetPolicy{MaxItems: 10, MaxChunkSize: 5, MaxChunks: 2, MaxConcurrency: 2},
	}
}

func validWorksetChunk(chunkID string, count int64) WorksetChunkSpec {
	return WorksetChunkSpec{
		ChunkID:   chunkID,
		ItemCount: count,
		ItemsRef: WorksetItemsRef{
			Kind:  "chunk",
			URI:   "s3://bucket/" + chunkID + ".jsonl",
			Count: count,
		},
		Concurrency: 1,
	}
}
