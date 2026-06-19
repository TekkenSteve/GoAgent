package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
)

const localScheme = "local"
const localHost = "artifact"

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
		return nil, fmt.Errorf("artifact local blob store: root is required")
	}
	clean, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("artifact local blob store: root path: %w", err)
	}
	if err := os.MkdirAll(clean, 0o755); err != nil {
		return nil, fmt.Errorf("artifact local blob store: mkdir root: %w", err)
	}

	return &LocalBlobStore{root: clean}, nil
}

func (s *LocalBlobStore) Put(_ context.Context, key string, payload []byte) (BlobObject, error) {
	pathOnDisk, err := s.pathForKey(key)
	if err != nil {
		return BlobObject{}, err
	}
	if err := os.MkdirAll(filepath.Dir(pathOnDisk), 0o755); err != nil {
		return BlobObject{}, fmt.Errorf("artifact local blob store: mkdir: %w", err)
	}
	tmp := pathOnDisk + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
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

func (s *LocalBlobStore) Get(_ context.Context, uri string) ([]byte, error) {
	key, err := keyFromLocalURI(uri)
	if err != nil {
		return nil, err
	}
	pathOnDisk, err := s.pathForKey(key)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(pathOnDisk)
	if err != nil {
		return nil, fmt.Errorf("artifact local blob store: read: %w", err)
	}

	return data, nil
}

func (s *LocalBlobStore) pathForKey(key string) (string, error) {
	if !filepath.IsLocal(key) {
		return "", fmt.Errorf("artifact local blob store: invalid key %q", key)
	}
	clean := filepath.Clean(key)
	pathOnDisk := filepath.Join(s.root, clean)
	if !filepath.IsLocal(clean) {
		return "", fmt.Errorf("artifact local blob store: invalid clean key %q", key)
	}

	return pathOnDisk, nil
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
		return "", fmt.Errorf("artifact local blob store: unsupported uri %q", raw)
	}
	key := path.Clean(parsed.Path)
	key = key[1:]
	if key == "" || !filepath.IsLocal(key) {
		return "", fmt.Errorf("artifact local blob store: invalid uri key %q", raw)
	}

	return key, nil
}
