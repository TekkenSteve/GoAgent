package temporal

import (
	"context"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/repo/memory"
)

// RuntimeConfig configures the default Temporal/Redis runtime implementation.
type RuntimeConfig struct {
	TemporalAddress          string
	TemporalNamespace        string
	TemporalTaskQueue        string
	PostgresURL              string
	PostgresPoolMax          int
	RedisURL                 string
	ArtifactStore            ArtifactStoreConfig
	TemporalExternalBackends []ExternalBackendConfig
	HTTPBackends             []HTTPBackendConfig
	GRPCBackends             []GRPCBackendConfig
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

// GRPCBackendConfig configures a gRPC AgentOS backend.
type GRPCBackendConfig struct {
	Name      string
	Target    string
	Authority string
	Insecure  bool
	Service   string
	Methods   GRPCMethodNames
}

// GRPCMethodNames maps AgentOS operations to external gRPC unary method names.
type GRPCMethodNames struct {
	Start   string
	Signal  string
	Control string
	Status  string
}

type ArtifactStoreBackend string

const (
	ArtifactStoreBackendLocal ArtifactStoreBackend = "local"
	ArtifactStoreBackendS3    ArtifactStoreBackend = "s3"
)

// ArtifactStoreConfig selects the blob store used for large AgentOS plan
// artifacts. Artifact metadata is still stored in the durable plan database.
type ArtifactStoreConfig struct {
	Backend ArtifactStoreBackend
	Local   LocalArtifactStoreConfig
	S3      S3ArtifactStoreConfig
}

type LocalArtifactStoreConfig struct {
	Root string
}

type S3ArtifactStoreConfig struct {
	Bucket          string
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	ForcePathStyle  bool
}

// RunBackendIndex persists run ownership for Signal/Control/Status routing.
type RunBackendIndex interface {
	Bind(ctx context.Context, spec agentos.RunSpec) error
	BindPlanNode(ctx context.Context, planID, nodeID string, spec agentos.RunSpec, status agentos.RunStatus) error
	Resolve(ctx context.Context, runID string) (agentos.BackendRef, error)
}

type runtimeOptions struct {
	runBackendIndex RunBackendIndex
	backendSelector RunBackendSelector
}

// RunBackendSelector resolves a backend when RunSpec.Backend is intentionally empty.
type RunBackendSelector interface {
	Select(ctx context.Context, spec agentos.RunSpec) (agentos.BackendRef, error)
}

// RuntimeOption customizes runtime construction.
type RuntimeOption func(*runtimeOptions)

// WithRunBackendIndex provides a durable run -> backend route index.
func WithRunBackendIndex(index RunBackendIndex) RuntimeOption {
	return func(opts *runtimeOptions) {
		opts.runBackendIndex = index
	}
}

// WithRunBackendSelector installs an optional backend policy resolver.
func WithRunBackendSelector(selector RunBackendSelector) RuntimeOption {
	return func(opts *runtimeOptions) {
		opts.backendSelector = selector
	}
}

// NewEphemeralRunBackendIndex creates a process-local run route index for
// embedded demos and tests. Production runtimes should inject a durable index.
func NewEphemeralRunBackendIndex() RunBackendIndex {
	return memory.NewAgentOSRunIndex()
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
	ArtifactStore     ArtifactStoreConfig

	TemporalExternalBackends []ExternalBackendConfig
	HTTPBackends             []HTTPBackendConfig
	GRPCBackends             []GRPCBackendConfig
	Capabilities             []agentos.Capability
	ArtifactSchemas          []agentos.ArtifactSchema

	RegisterEnvTools      bool
	EnsureDefaultTemplate bool
}
