package artifact

import (
	"context"
	"errors"
	"testing"
)

func TestNewBlobStoreRequiresBackend(t *testing.T) {
	t.Parallel()

	_, err := NewBlobStore(context.Background(), &Config{})
	if !errors.Is(err, ErrBackendRequired) {
		t.Fatalf("NewBlobStore error = %v, want %v", err, ErrBackendRequired)
	}
}

func TestNewBlobStoreRejectsUnknownBackend(t *testing.T) {
	t.Parallel()

	_, err := NewBlobStore(context.Background(), &Config{Backend: Backend("memory")})
	if !errors.Is(err, ErrBackendUnknown) {
		t.Fatalf("NewBlobStore error = %v, want %v", err, ErrBackendUnknown)
	}
}

func TestNewBlobStoreBuildsLocalBackend(t *testing.T) {
	t.Parallel()

	store, err := NewBlobStore(context.Background(), &Config{
		Backend: BackendLocal,
		Local: LocalConfig{
			Root: t.TempDir(),
		},
	})
	if err != nil {
		t.Fatalf("NewBlobStore: %v", err)
	}

	if _, ok := store.(*LocalBlobStore); !ok {
		t.Fatalf("store type = %T, want *LocalBlobStore", store)
	}
}
