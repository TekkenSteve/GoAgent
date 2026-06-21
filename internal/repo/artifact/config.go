package artifact

import (
	"context"
	"errors"
	"fmt"
)

type Backend string

const (
	BackendLocal Backend = "local"
	BackendS3    Backend = "s3"
)

var (
	ErrBackendRequired = errors.New("artifact blob store: backend is required")
	ErrBackendUnknown  = errors.New("artifact blob store: backend is unknown")
)

type Config struct {
	Backend Backend
	Local   LocalConfig
	S3      S3Config
}

type LocalConfig struct {
	Root string
}

func NewBlobStore(ctx context.Context, cfg Config) (BlobStore, error) {
	switch cfg.Backend {
	case BackendLocal:
		return NewLocalBlobStore(cfg.Local.Root)
	case BackendS3:
		return NewS3BlobStore(ctx, cfg.S3)
	case "":
		return nil, ErrBackendRequired
	default:
		return nil, fmt.Errorf("%w: %s", ErrBackendUnknown, cfg.Backend)
	}
}
