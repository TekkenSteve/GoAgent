package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const s3Scheme = "s3"

var (
	ErrS3BucketRequired          = errors.New("artifact s3 blob store: bucket is required")
	ErrS3RegionRequired          = errors.New("artifact s3 blob store: region is required")
	ErrS3AccessKeyIDRequired     = errors.New("artifact s3 blob store: access key id is required")
	ErrS3SecretAccessKeyRequired = errors.New("artifact s3 blob store: secret access key is required")

	errS3BlobStoreClientRequired = errors.New("artifact s3 blob store: client is required")
	errS3BlobStoreUnsupportedURI = errors.New("artifact s3 blob store: unsupported uri")
	errS3BlobStoreURIBucket      = errors.New("artifact s3 blob store: uri bucket does not match configured bucket")
	errS3BlobStoreURIKeyRequired = errors.New("artifact s3 blob store: uri key is required")
	errS3BlobStoreKeyRequired    = errors.New("artifact s3 blob store: key is required")
	errS3BlobStoreInvalidKey     = errors.New("artifact s3 blob store: invalid key")
)

type S3Config struct {
	Bucket          string
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	ForcePathStyle  bool
}

type s3ObjectClient interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3BlobStore stores artifact payloads in any S3-compatible object store.
type S3BlobStore struct {
	client s3ObjectClient
	bucket string
}

func NewS3BlobStore(_ context.Context, cfg *S3Config) (*S3BlobStore, error) {
	if cfg.Bucket == "" {
		return nil, ErrS3BucketRequired
	}

	if cfg.Region == "" {
		return nil, ErrS3RegionRequired
	}

	if cfg.AccessKeyID == "" {
		return nil, ErrS3AccessKeyIDRequired
	}

	if cfg.SecretAccessKey == "" {
		return nil, ErrS3SecretAccessKeyRequired
	}

	awsCfg := aws.Config{
		Region: cfg.Region,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			cfg.SessionToken,
		)),
	}
	if cfg.Endpoint != "" {
		awsCfg.BaseEndpoint = aws.String(cfg.Endpoint)
	}

	client := s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		options.UsePathStyle = cfg.ForcePathStyle
	})

	return NewS3BlobStoreWithClient(client, cfg.Bucket)
}

func NewS3BlobStoreWithClient(client s3ObjectClient, bucket string) (*S3BlobStore, error) {
	if client == nil {
		return nil, errS3BlobStoreClientRequired
	}

	if bucket == "" {
		return nil, ErrS3BucketRequired
	}

	return &S3BlobStore{client: client, bucket: bucket}, nil
}

func (s *S3BlobStore) Put(ctx context.Context, key string, payload []byte) (BlobObject, error) {
	objectKey, err := cleanS3Key(key)
	if err != nil {
		return BlobObject{}, err
	}

	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(payload),
	}); err != nil {
		return BlobObject{}, fmt.Errorf("artifact s3 blob store: put object: %w", err)
	}

	sum := sha256.Sum256(payload)

	return BlobObject{
		URI:       s3URI(s.bucket, objectKey),
		SizeBytes: int64(len(payload)),
		Digest:    "sha256:" + hex.EncodeToString(sum[:]),
	}, nil
}

func (s *S3BlobStore) Get(ctx context.Context, rawURI string) ([]byte, error) {
	objectKey, err := keyFromS3URI(rawURI, s.bucket)
	if err != nil {
		return nil, err
	}

	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, fmt.Errorf("artifact s3 blob store: get object: %w", err)
	}
	defer output.Body.Close()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("artifact s3 blob store: read object: %w", err)
	}

	return data, nil
}

func s3URI(bucket, key string) string {
	return (&url.URL{Scheme: s3Scheme, Host: bucket, Path: "/" + key}).String()
}

func keyFromS3URI(rawURI, expectedBucket string) (string, error) {
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return "", fmt.Errorf("artifact s3 blob store: parse uri: %w", err)
	}

	if parsed.Scheme != s3Scheme {
		return "", fmt.Errorf("%w: %q", errS3BlobStoreUnsupportedURI, rawURI)
	}

	if parsed.Host != expectedBucket {
		return "", fmt.Errorf("%w: uri bucket %q does not match configured bucket %q", errS3BlobStoreURIBucket, parsed.Host, expectedBucket)
	}

	key := strings.TrimPrefix(parsed.EscapedPath(), "/")
	if key == "" {
		return "", errS3BlobStoreURIKeyRequired
	}

	unescaped, err := url.PathUnescape(key)
	if err != nil {
		return "", fmt.Errorf("artifact s3 blob store: uri key: %w", err)
	}

	return cleanS3Key(unescaped)
}

func cleanS3Key(key string) (string, error) {
	if key == "" {
		return "", errS3BlobStoreKeyRequired
	}

	clean := path.Clean(key)
	if clean == "." || path.IsAbs(clean) || hasParentPathSegment(key) {
		return "", fmt.Errorf("%w: %q", errS3BlobStoreInvalidKey, key)
	}

	return clean, nil
}

func hasParentPathSegment(key string) bool {
	return slices.Contains(strings.Split(key, "/"), "..")
}
