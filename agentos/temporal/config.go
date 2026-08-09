package temporal

import (
	"context"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosruntime "github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
)

// RuntimeConfig configures the default Temporal/Redis runtime implementation.
type RuntimeConfig struct {
	TemporalAddress          string
	TemporalNamespace        string
	TemporalTaskQueues       TaskQueues
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
	Defaults map[agentoscore.SignalType]string
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

// ArtifactStoreBackend selects the blob store implementation used for large plan artifacts.
type ArtifactStoreBackend string

const (
	// ArtifactStoreBackendLocal stores artifacts on the local filesystem.
	ArtifactStoreBackendLocal ArtifactStoreBackend = "local"
	// ArtifactStoreBackendS3 stores artifacts in an S3-compatible bucket.
	ArtifactStoreBackendS3 ArtifactStoreBackend = "s3"
)

// ArtifactStoreConfig selects the blob store used for large AgentOS plan
// artifacts. Artifact metadata is still stored in the durable plan database.
type ArtifactStoreConfig struct {
	Backend ArtifactStoreBackend
	Local   LocalArtifactStoreConfig
	S3      S3ArtifactStoreConfig
}

// LocalArtifactStoreConfig configures the local filesystem artifact backend.
type LocalArtifactStoreConfig struct {
	Root string
}

// S3ArtifactStoreConfig configures the S3-compatible artifact backend.
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
type RunBackendIndex = agentosruntime.RunBackendIndex

type runtimeOptions struct {
	runBackendIndex        RunBackendIndex
	runBackendIndexFactory runtimeRunBackendIndexFactory
	backendSelector        RunBackendSelector
}

// RunBackendSelector resolves a backend when RunSpec.Backend is intentionally empty.
type RunBackendSelector interface {
	Select(ctx context.Context, spec *agentos.RunSpec) (agentos.BackendRef, error)
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

// WorkerConfig configures registration of GoAgent workflows and activities into
// a Temporal worker.
type WorkerConfig struct {
	TemporalAddress    string
	TemporalNamespace  string
	TemporalTaskQueues TaskQueues
	PostgresURL        string
	PostgresPoolMax    int
	RedisURL           string
	LLMConfigPath      string
	LogLevel           string
	ArtifactStore      ArtifactStoreConfig

	TemporalExternalBackends []ExternalBackendConfig
	HTTPBackends             []HTTPBackendConfig
	GRPCBackends             []GRPCBackendConfig
	Capabilities             []agentos.Capability
	ArtifactSchemas          []agentos.ArtifactSchema

	RegisterEnvTools      bool
	EnsureDefaultTemplate bool
}
