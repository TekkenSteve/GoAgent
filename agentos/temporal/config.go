package temporal

import (
	"context"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosstream "github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/runprojection"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	agentosruntime "github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

// RuntimeConfig configures the default Temporal runtime implementation.
type RuntimeConfig struct {
	TemporalAddress    string
	TemporalNamespace  string
	TemporalTaskQueues TaskQueues
	PostgresURL        string
	PostgresPoolMax    int
	Subscriber         agentosstream.Subscriber
	// Publisher is the data-plane write side one-shot external backends (HTTP /
	// gRPC / temporal_external) mirror their observable lifecycle onto: Start
	// success → RUN_STARTED, a terminal Status → RUN_FINISHED / RUN_ERROR /
	// RUN_CANCELLED. Nil degrades external backends to no-op lifecycle
	// publishing — the run still works, its milestones just never reach the bus.
	Publisher agentosstream.Publisher
	// ProjectionController optionally attaches the run milestone projector to
	// external-backend runs' channels, so their lifecycle milestones persist to
	// Postgres like the native path's do. Nil leaves external-run milestones on
	// the bus live tail only.
	ProjectionController runprojection.Controller
	// Logger reports data-plane publish failures from the external-backend
	// lifecycle adapters. Nil drops those diagnostics.
	Logger                   logger.Interface
	PlanEventPublisher       agentosplan.PlanEventPublisher
	PlanEventSubscriber      agentosplan.PlanEventSubscriber
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

	// StreamCentrifugo configures the Centrifugo data-plane bus the streaming
	// activities mirror their AG-UI timeline onto — the default transport. Leave
	// empty to degrade the data plane to the in-process memstream bus (fine for
	// a single-machine run, not a production transport). Requires an `agentos`
	// namespace (covering agentos:run:* / agentos:plan:* channels) with
	// history_size/history_ttl on the server so publications carry replayable
	// offsets; the dev default also needs anonymous subscribe + history access.
	StreamCentrifugo StreamCentrifugoConfig
}

// StreamCentrifugoConfig is the data-plane transport for the AG-UI timeline.
// An empty BaseURL degrades the data plane to the in-process memstream bus.
type StreamCentrifugoConfig struct {
	BaseURL string
	APIKey  string
}
