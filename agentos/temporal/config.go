package temporal

import (
	"context"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// RuntimeConfig configures the default Temporal/Redis runtime implementation.
type RuntimeConfig struct {
	TemporalAddress          string
	TemporalNamespace        string
	TemporalTaskQueue        string
	RedisURL                 string
	TemporalExternalBackends []ExternalBackendConfig
	HTTPBackends             []HTTPBackendConfig
}

// ExternalBackendConfig configures a temporal_external AgentOS backend.
type ExternalBackendConfig struct {
	Name         string
	TaskQueue    string
	WorkflowType string
	QueryType    string
	Signals      ExternalSignalNames
}

// ExternalSignalNames maps AgentOS signals and controls to external workflow signal names.
type ExternalSignalNames struct {
	Pause    string
	Resume   string
	Cancel   string
	Defaults map[agentos.SignalType]string
}

// HTTPBackendConfig configures an HTTP AgentOS backend.
type HTTPBackendConfig struct {
	Name     string
	Endpoint string
	Headers  map[string]string
}

// RunBackendIndex persists run ownership for Signal/Control/Status routing.
type RunBackendIndex interface {
	Bind(ctx context.Context, spec agentos.RunSpec) error
	Resolve(ctx context.Context, runID string) (agentos.BackendRef, error)
}

type runtimeOptions struct {
	runBackendIndex RunBackendIndex
}

// RuntimeOption customizes runtime construction.
type RuntimeOption func(*runtimeOptions)

// WithRunBackendIndex provides a durable run -> backend route index.
func WithRunBackendIndex(index RunBackendIndex) RuntimeOption {
	return func(opts *runtimeOptions) {
		opts.runBackendIndex = index
	}
}

// WorkerConfig configures registration of GoAgent workflows and activities into
// a Temporal worker.
type WorkerConfig struct {
	TemporalAddress   string
	TemporalNamespace string
	TemporalTaskQueue string
	PostgresURL       string
	PostgresPoolMax   int
	RedisURL          string
	LLMConfigPath     string
	LogLevel          string

	RegisterEnvTools      bool
	EnsureDefaultTemplate bool
}
