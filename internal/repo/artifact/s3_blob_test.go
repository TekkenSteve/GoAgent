package artifact

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestNewS3BlobStoreRequiresExplicitConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  S3Config
		want error
	}{
		{name: "bucket", cfg: S3Config{}, want: ErrS3BucketRequired},
		{name: "region", cfg: S3Config{Bucket: "artifacts"}, want: ErrS3RegionRequired},
		{name: "access key", cfg: S3Config{Bucket: "artifacts", Region: "us-east-1"}, want: ErrS3AccessKeyIDRequired},
		{name: "secret key", cfg: S3Config{Bucket: "artifacts", Region: "us-east-1", AccessKeyID: "access"}, want: ErrS3SecretAccessKeyRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewS3BlobStore(context.Background(), tt.cfg)
			if !errors.Is(err, tt.want) {
				t.Fatalf("NewS3BlobStore error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestS3BlobStorePutGet(t *testing.T) {
	client := newFakeS3Client()
	store, err := NewS3BlobStoreWithClient(client, "artifacts")
	if err != nil {
		t.Fatalf("NewS3BlobStoreWithClient: %v", err)
	}

	payload := []byte(`{"ok":true}`)
	object, err := store.Put(context.Background(), "plan-1/artifact-1", payload)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if object.URI != "s3://artifacts/plan-1/artifact-1" {
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

func TestS3BlobStoreRejectsInvalidKeys(t *testing.T) {
	store, err := NewS3BlobStoreWithClient(newFakeS3Client(), "artifacts")
	if err != nil {
		t.Fatalf("NewS3BlobStoreWithClient: %v", err)
	}

	for _, key := range []string{"", "../escape", "plan/../escape", "/absolute"} {
		if _, err := store.Put(context.Background(), key, []byte("nope")); err == nil {
			t.Fatalf("Put accepted invalid key %q", key)
		}
	}
}

func TestS3BlobStoreRejectsWrongBucketURI(t *testing.T) {
	store, err := NewS3BlobStoreWithClient(newFakeS3Client(), "artifacts")
	if err != nil {
		t.Fatalf("NewS3BlobStoreWithClient: %v", err)
	}

	if _, err := store.Get(context.Background(), "s3://other/plan-1/artifact-1"); err == nil {
		t.Fatal("Get accepted URI from another bucket")
	}
}

type fakeS3Client struct {
	objects map[string][]byte
}

func newFakeS3Client() *fakeS3Client {
	return &fakeS3Client{objects: map[string][]byte{}}
}

func (c *fakeS3Client) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if input == nil || input.Bucket == nil || input.Key == nil || input.Body == nil {
		return nil, errors.New("invalid put input")
	}
	payload, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	c.objects[aws.ToString(input.Bucket)+"/"+aws.ToString(input.Key)] = payload

	return &s3.PutObjectOutput{}, nil
}

func (c *fakeS3Client) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if input == nil || input.Bucket == nil || input.Key == nil {
		return nil, errors.New("invalid get input")
	}
	payload, ok := c.objects[aws.ToString(input.Bucket)+"/"+aws.ToString(input.Key)]
	if !ok {
		return nil, errors.New("object not found")
	}

	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(payload))}, nil
}
