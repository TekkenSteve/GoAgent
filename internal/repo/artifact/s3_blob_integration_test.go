//go:build s3_integration

package artifact

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

func TestS3BlobStoreMinIOIntegrationPutGet(t *testing.T) {
	ctx := t.Context()
	cfg := s3IntegrationConfig(t)
	createS3IntegrationBucket(t, ctx, cfg)

	store, err := NewS3BlobStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3BlobStore: %v", err)
	}

	payload := []byte(`{"artifact":"s3-compatible","ok":true}`)
	object, err := store.Put(ctx, "plans/plan-1/artifacts/artifact-1.json", payload)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if object.URI != "s3://"+cfg.Bucket+"/plans/plan-1/artifacts/artifact-1.json" {
		t.Fatalf("URI = %q", object.URI)
	}
	if object.SizeBytes != int64(len(payload)) {
		t.Fatalf("SizeBytes = %d, want %d", object.SizeBytes, len(payload))
	}

	got, err := store.Get(ctx, object.URI)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
}

func s3IntegrationConfig(t *testing.T) S3Config {
	t.Helper()

	endpoint := os.Getenv("GOAGENT_S3_TEST_ENDPOINT")
	bucket := os.Getenv("GOAGENT_S3_TEST_BUCKET")
	accessKeyID := os.Getenv("GOAGENT_S3_TEST_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("GOAGENT_S3_TEST_SECRET_ACCESS_KEY")
	if endpoint == "" || bucket == "" || accessKeyID == "" || secretAccessKey == "" {
		t.Fatal("GOAGENT_S3_TEST_ENDPOINT, GOAGENT_S3_TEST_BUCKET, GOAGENT_S3_TEST_ACCESS_KEY_ID, and GOAGENT_S3_TEST_SECRET_ACCESS_KEY are required for s3_integration tests")
	}

	region := os.Getenv("GOAGENT_S3_TEST_REGION")
	if region == "" {
		region = "us-east-1"
	}
	forcePathStyle := true
	if raw := os.Getenv("GOAGENT_S3_TEST_FORCE_PATH_STYLE"); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			t.Fatalf("GOAGENT_S3_TEST_FORCE_PATH_STYLE: %v", err)
		}
		forcePathStyle = parsed
	}

	return S3Config{
		Bucket:          bucket,
		Region:          region,
		Endpoint:        endpoint,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		ForcePathStyle:  forcePathStyle,
	}
}

func createS3IntegrationBucket(t *testing.T, ctx context.Context, cfg S3Config) {
	t.Helper()

	client := s3.NewFromConfig(aws.Config{
		Region: cfg.Region,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			cfg.SessionToken,
		)),
		BaseEndpoint: aws.String(cfg.Endpoint),
	}, func(options *s3.Options) {
		options.UsePathStyle = cfg.ForcePathStyle
	})

	for range 60 {
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)})
		if err == nil {
			return
		}
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "BucketAlreadyOwnedByYou" {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)}); err != nil {
		t.Fatalf("CreateBucket %q: %v", cfg.Bucket, err)
	}
}
