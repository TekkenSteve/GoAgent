package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
)

const (
	defaultDirPerm  os.FileMode = 0o755
	defaultFilePerm os.FileMode = 0o600
)

var (
	errLocalBlobStoreRootRequired   = errors.New("artifact local blob store: root is required")
	errLocalBlobStoreInvalidKey     = errors.New("artifact local blob store: invalid key")
	errLocalBlobStoreUnsupportedURI = errors.New("artifact local blob store: unsupported uri")
	errLocalBlobStoreInvalidURIKey  = errors.New("artifact local blob store: invalid uri key")
)

const (
	localScheme = "local"
	localHost   = "artifact"
)

// BlobObject describes a payload stored outside Temporal history.
type BlobObject struct {
	URI       string
	SizeBytes int64
	Digest    string
}

// BlobStore stores raw artifact payload bytes.
type BlobStore interface {
	Put(ctx context.Context, key string, payload []byte) (BlobObject, error)
	Get(ctx context.Context, uri string) ([]byte, error)
}

// LocalBlobStore stores artifact payloads on the local filesystem.
type LocalBlobStore struct {
	root string
}

// NewLocalBlobStore creates a local filesystem blob store rooted at root.
func NewLocalBlobStore(root string) (*LocalBlobStore, error) {
	if root == "" {
		return nil, errLocalBlobStoreRootRequired
	}

	clean, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("artifact local blob store: root path: %w", err)
	}

	if err := os.MkdirAll(clean, defaultDirPerm); err != nil {
		return nil, fmt.Errorf("artifact local blob store: mkdir root: %w", err)
	}

	return &LocalBlobStore{root: clean}, nil
}

// Put writes payload to the blob store under key and returns the stored blob's metadata.
func (s *LocalBlobStore) Put(_ context.Context, key string, payload []byte) (BlobObject, error) {
	pathOnDisk, err := s.pathForKey(key)
	if err != nil {
		return BlobObject{}, err
	}

	if err := os.MkdirAll(filepath.Dir(pathOnDisk), defaultDirPerm); err != nil {
		return BlobObject{}, fmt.Errorf("artifact local blob store: mkdir: %w", err)
	}

	tmp := pathOnDisk + ".tmp"
	if err := os.WriteFile(tmp, payload, defaultFilePerm); err != nil {
		return BlobObject{}, fmt.Errorf("artifact local blob store: write: %w", err)
	}

	if err := os.Rename(tmp, pathOnDisk); err != nil {
		return BlobObject{}, fmt.Errorf("artifact local blob store: commit: %w", err)
	}

	sum := sha256.Sum256(payload)

	return BlobObject{
		URI:       localURI(key),
		SizeBytes: int64(len(payload)),
		Digest:    "sha256:" + hex.EncodeToString(sum[:]),
	}, nil
}

// Get reads and returns the artifact payload stored at the given local blob URI.
func (s *LocalBlobStore) Get(_ context.Context, uri string) (data []byte, err error) {
	key, err := keyFromLocalURI(uri)
	if err != nil {
		return nil, err
	}

	clean, err := s.cleanKey(key)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenInRoot(s.root, clean)
	if err != nil {
		return nil, fmt.Errorf("artifact local blob store: open: %w", err)
	}

	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("artifact local blob store: close: %w", closeErr))
		}
	}()

	data, err = io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("artifact local blob store: read: %w", err)
	}

	return data, nil
}

func (s *LocalBlobStore) cleanKey(key string) (string, error) {
	if !filepath.IsLocal(key) {
		return "", fmt.Errorf("%w: %q", errLocalBlobStoreInvalidKey, key)
	}

	clean := filepath.Clean(key)

	if !filepath.IsLocal(clean) {
		return "", fmt.Errorf("%w: clean key %q", errLocalBlobStoreInvalidKey, key)
	}

	return clean, nil
}

func (s *LocalBlobStore) pathForKey(key string) (string, error) {
	clean, err := s.cleanKey(key)
	if err != nil {
		return "", err
	}

	return filepath.Join(s.root, clean), nil
}

func localURI(key string) string {
	return (&url.URL{Scheme: localScheme, Host: localHost, Path: "/" + path.Clean(key)}).String()
}

func keyFromLocalURI(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("artifact local blob store: parse uri: %w", err)
	}

	if parsed.Scheme != localScheme || parsed.Host != localHost {
		return "", fmt.Errorf("%w: %q", errLocalBlobStoreUnsupportedURI, raw)
	}

	key := path.Clean(parsed.Path)

	key = key[1:]
	if key == "" || !filepath.IsLocal(key) {
		return "", fmt.Errorf("%w: %q", errLocalBlobStoreInvalidURIKey, raw)
	}

	return key, nil
}
