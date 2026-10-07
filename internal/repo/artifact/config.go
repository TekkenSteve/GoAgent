// Package artifact persists artifact payloads to local or S3 blob stores.
package artifact

import (
	"context"
	"errors"
	"fmt"
)

// Backend identifies the storage backend for artifact payloads.
type Backend string

const (
	// BackendLocal is the local filesystem artifact storage backend.
	BackendLocal Backend = "local"
	// BackendS3 is the S3-compatible object store artifact storage backend.
	BackendS3 Backend = "s3"
)

var (
	// ErrBackendRequired is returned when no artifact storage backend is configured.
	ErrBackendRequired = errors.New("artifact blob store: backend is required")
	// ErrBackendUnknown is returned when the configured artifact storage backend is not recognized.
	ErrBackendUnknown = errors.New("artifact blob store: backend is unknown")
)

// Config selects the artifact storage backend and its per-backend settings.
type Config struct {
	Backend Backend
	Local   LocalConfig
	S3      S3Config
}

// LocalConfig configures the local filesystem artifact storage backend.
type LocalConfig struct {
	Root string
}

// NewBlobStore creates a BlobStore for the backend selected by cfg.
func NewBlobStore(ctx context.Context, cfg *Config) (BlobStore, error) {
	switch cfg.Backend {
	case BackendLocal:
		return NewLocalBlobStore(cfg.Local.Root)
	case BackendS3:
		return NewS3BlobStore(ctx, &cfg.S3)
	case "":
		return nil, ErrBackendRequired
	default:
		return nil, fmt.Errorf("%w: %s", ErrBackendUnknown, cfg.Backend)
	}
}
