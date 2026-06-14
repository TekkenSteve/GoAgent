package temporal

import (
	"context"
	"errors"
	"testing"
)

func TestNewWorkerKitRequiresPostgresURL(t *testing.T) {
	_, err := NewWorkerKit(context.Background(), WorkerConfig{})
	if !errors.Is(err, ErrWorkerPostgresURLRequired) {
		t.Fatalf("NewWorkerKit error = %v, want %v", err, ErrWorkerPostgresURLRequired)
	}
}

func TestNewWorkerKitRequiresRedisURL(t *testing.T) {
	_, err := NewWorkerKit(context.Background(), WorkerConfig{
		PostgresURL: "postgres://user:pass@localhost:5432/db",
	})
	if !errors.Is(err, ErrWorkerRedisURLRequired) {
		t.Fatalf("NewWorkerKit error = %v, want %v", err, ErrWorkerRedisURLRequired)
	}
}
