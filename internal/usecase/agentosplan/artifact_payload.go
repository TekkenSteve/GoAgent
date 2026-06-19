package agentosplan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// EncodeArtifactPayload converts an artifact payload into stable bytes for
// storage and idempotency metadata.
func EncodeArtifactPayload(payload any, mediaType string) ([]byte, string, error) {
	if mediaType == "" {
		mediaType = "application/json"
	}
	switch value := payload.(type) {
	case []byte:
		if mediaType == "application/json" {
			mediaType = "application/octet-stream"
		}

		return value, mediaType, nil
	case string:
		if mediaType == "application/json" {
			mediaType = "text/plain"
		}

		return []byte(value), mediaType, nil
	default:
		data, err := json.Marshal(value)
		if err != nil {
			return nil, "", fmt.Errorf("%w: encode artifact payload: %s", agentos.ErrInvalidArtifact, err)
		}

		return data, mediaType, nil
	}
}

// DecodeArtifactPayload converts stored artifact bytes back into their public
// payload representation.
func DecodeArtifactPayload(data []byte, mediaType string) (any, error) {
	switch mediaType {
	case "application/json", "":
		var value any
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, fmt.Errorf("%w: decode artifact payload: %s", agentos.ErrInvalidArtifact, err)
		}

		return value, nil
	case "text/plain":
		return string(data), nil
	default:
		return data, nil
	}
}

// DigestArtifactPayload returns the stable digest recorded on ArtifactRef.
func DigestArtifactPayload(payload []byte) string {
	sum := sha256.Sum256(payload)

	return "sha256:" + hex.EncodeToString(sum[:])
}
