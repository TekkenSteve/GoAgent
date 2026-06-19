package artifact

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestLocalBlobStorePutGet(t *testing.T) {
	store, err := NewLocalBlobStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalBlobStore: %v", err)
	}

	payload := []byte(`{"ok":true}`)
	object, err := store.Put(context.Background(), "plan-1/artifact-1", payload)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if object.URI != "local://artifact/plan-1/artifact-1" {
		t.Fatalf("URI = %q", object.URI)
	}
	if object.SizeBytes != int64(len(payload)) {
		t.Fatalf("SizeBytes = %d", object.SizeBytes)
	}
	if !strings.HasPrefix(object.Digest, "sha256:") {
		t.Fatalf("Digest = %q", object.Digest)
	}

	got, err := store.Get(context.Background(), object.URI)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
}

func TestLocalBlobStoreRejectsNonLocalKeys(t *testing.T) {
	store, err := NewLocalBlobStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalBlobStore: %v", err)
	}

	if _, err := store.Put(context.Background(), "../escape", []byte("nope")); err == nil {
		t.Fatal("Put accepted non-local key")
	}
}
