package persistent

import (
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
)

func TestDecodeStoredArtifactPayloadRejectsSizeMismatch(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"ok":true}`)
	ref := agentos.ArtifactRef{
		ArtifactID: "artifact-1",
		MediaType:  "application/json",
		SizeBytes:  int64(len(payload) + 1),
		Digest:     agentosplan.DigestArtifactPayload(payload),
	}

	_, err := decodeStoredArtifactPayload(&ref, payload)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}

func TestDecodeStoredArtifactPayloadRejectsDigestMismatch(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"ok":true}`)
	ref := agentos.ArtifactRef{
		ArtifactID: "artifact-1",
		MediaType:  "application/json",
		SizeBytes:  int64(len(payload)),
		Digest:     agentosplan.DigestArtifactPayload([]byte(`{"ok":false}`)),
	}

	_, err := decodeStoredArtifactPayload(&ref, payload)
	if !errors.Is(err, agentos.ErrInvalidArtifact) {
		t.Fatalf("error = %v, want ErrInvalidArtifact", err)
	}
}

func TestDecodeStoredArtifactPayloadDecodesVerifiedPayload(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"ok":true}`)
	ref := agentos.ArtifactRef{
		ArtifactID: "artifact-1",
		MediaType:  "application/json",
		SizeBytes:  int64(len(payload)),
		Digest:     agentosplan.DigestArtifactPayload(payload),
	}

	decoded, err := decodeStoredArtifactPayload(&ref, payload)
	if err != nil {
		t.Fatalf("decodeStoredArtifactPayload: %v", err)
	}

	object, ok := decoded.(map[string]any)
	if !ok || object["ok"] != true {
		t.Fatalf("decoded = %#v, want JSON object", decoded)
	}
}
