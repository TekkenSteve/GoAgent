package temporal

import (
	"context"
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

func TestRuntimeOptionsWithDefaultRunBackendIndexUsesPostgresConfig(t *testing.T) {
	wantIndex := fakeRunBackendIndex{}
	closed := false
	called := false
	rt := &runtime{}

	opts, err := rt.runtimeOptionsWithDefaultRunBackendIndex(
		RuntimeConfig{PostgresURL: "postgres://agentos"},
		runtimeOptions{},
		func(cfg RuntimeConfig) (RunBackendIndex, func() error, error) {
			called = true
			if cfg.PostgresURL != "postgres://agentos" {
				t.Fatalf("PostgresURL = %q", cfg.PostgresURL)
			}

			return wantIndex, func() error {
				closed = true

				return nil
			}, nil
		},
	)
	if err != nil {
		t.Fatalf("runtimeOptionsWithDefaultRunBackendIndex: %v", err)
	}
	if !called {
		t.Fatal("default run backend index factory was not called")
	}
	if opts.runBackendIndex != wantIndex {
		t.Fatalf("runBackendIndex = %#v, want %#v", opts.runBackendIndex, wantIndex)
	}
	if len(rt.closers) != 1 {
		t.Fatalf("closers = %d, want 1", len(rt.closers))
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !closed {
		t.Fatal("default run backend index close function was not called")
	}
}

func TestRuntimeOptionsWithDefaultRunBackendIndexKeepsExplicitIndex(t *testing.T) {
	wantIndex := fakeRunBackendIndex{}
	rt := &runtime{}

	opts, err := rt.runtimeOptionsWithDefaultRunBackendIndex(
		RuntimeConfig{PostgresURL: "postgres://agentos"},
		runtimeOptions{runBackendIndex: wantIndex},
		func(RuntimeConfig) (RunBackendIndex, func() error, error) {
			t.Fatal("factory was called despite explicit run backend index")

			return nil, nil, nil
		},
	)
	if err != nil {
		t.Fatalf("runtimeOptionsWithDefaultRunBackendIndex: %v", err)
	}
	if opts.runBackendIndex != wantIndex {
		t.Fatalf("runBackendIndex = %#v, want %#v", opts.runBackendIndex, wantIndex)
	}
	if len(rt.closers) != 0 {
		t.Fatalf("closers = %d, want 0", len(rt.closers))
	}
}

func TestRuntimeOptionsWithDefaultRunBackendIndexPropagatesFactoryError(t *testing.T) {
	wantErr := errors.New("postgres unavailable")
	rt := &runtime{}

	_, err := rt.runtimeOptionsWithDefaultRunBackendIndex(
		RuntimeConfig{PostgresURL: "postgres://agentos"},
		runtimeOptions{},
		func(RuntimeConfig) (RunBackendIndex, func() error, error) {
			return nil, nil, wantErr
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if len(rt.closers) != 0 {
		t.Fatalf("closers = %d, want 0", len(rt.closers))
	}
}

func TestRuntimeOptionsWithDefaultRunBackendIndexRequiresPostgresURL(t *testing.T) {
	rt := &runtime{}

	_, err := rt.runtimeOptionsWithDefaultRunBackendIndex(
		RuntimeConfig{},
		runtimeOptions{},
		newRuntimeRunBackendIndex,
	)
	if !errors.Is(err, ErrRuntimePostgresURLRequired) {
		t.Fatalf("error = %v, want %v", err, ErrRuntimePostgresURLRequired)
	}
	if len(rt.closers) != 0 {
		t.Fatalf("closers = %d, want 0", len(rt.closers))
	}
}

type fakeRunBackendIndex struct{}

func (fakeRunBackendIndex) Bind(context.Context, agentos.RunSpec, agentos.RunStatus) error {
	return nil
}

func (fakeRunBackendIndex) BindPlanNode(context.Context, string, string, agentos.RunSpec, agentos.RunStatus) error {
	return nil
}

func (fakeRunBackendIndex) GetRunBackend(context.Context, string) (agentos.RunBackendOwnership, bool, error) {
	return agentos.RunBackendOwnership{}, false, nil
}

func (fakeRunBackendIndex) Resolve(context.Context, string) (agentos.BackendRef, error) {
	return agentos.BackendRef{}, nil
}
