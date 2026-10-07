package temporal

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	agenttool "github.com/TekkenSteve/GoAgent/internal/agentfw/tool"
	"github.com/TekkenSteve/GoAgent/internal/authz"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/planstream"
	artifactrepo "github.com/TekkenSteve/GoAgent/internal/repo/artifact"
	"github.com/TekkenSteve/GoAgent/internal/repo/cached"
	"github.com/TekkenSteve/GoAgent/internal/repo/compressor"
	"github.com/TekkenSteve/GoAgent/internal/repo/framework"
	mcpRepo "github.com/TekkenSteve/GoAgent/internal/repo/mcp"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	"github.com/TekkenSteve/GoAgent/internal/repo/toolkit"
	"github.com/TekkenSteve/GoAgent/internal/repo/webapi"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agent"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	billingpkg "github.com/TekkenSteve/GoAgent/internal/usecase/billing"
	templatepkg "github.com/TekkenSteve/GoAgent/internal/usecase/template"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"go.temporal.io/sdk/client"
)

var (
	// ErrWorkerConfigRequired reports a missing worker config.
	ErrWorkerConfigRequired = errors.New("agentos temporal worker: config is required")
	// ErrWorkerPostgresURLRequired reports a missing Postgres URL in the worker config.
	ErrWorkerPostgresURLRequired = errors.New("agentos temporal worker: postgres url is required")
	// ErrWorkerArtifactStoreBackendRequired reports a missing artifact store backend in the worker config.
	ErrWorkerArtifactStoreBackendRequired = errors.New("agentos temporal worker: artifact store backend is required")
	// ErrWorkerArtifactStoreBackendUnknown reports an unknown artifact store backend in the worker config.
	ErrWorkerArtifactStoreBackendUnknown = errors.New("agentos temporal worker: artifact store backend is unknown")
	// ErrWorkerArtifactStoreLocalRootRequired reports a missing artifact local root in the worker config.
	ErrWorkerArtifactStoreLocalRootRequired = errors.New("agentos temporal worker: artifact local root is required")
	// ErrWorkerArtifactStoreS3BucketRequired reports a missing artifact S3 bucket in the worker config.
	ErrWorkerArtifactStoreS3BucketRequired = errors.New("agentos temporal worker: artifact s3 bucket is required")
	// ErrWorkerArtifactStoreS3RegionRequired reports a missing artifact S3 region in the worker config.
	ErrWorkerArtifactStoreS3RegionRequired = errors.New("agentos temporal worker: artifact s3 region is required")
	// ErrWorkerArtifactStoreS3AccessKeyRequired reports a missing artifact S3 access key ID in the worker config.
	ErrWorkerArtifactStoreS3AccessKeyRequired = errors.New("agentos temporal worker: artifact s3 access key id is required")
	// ErrWorkerArtifactStoreS3SecretKeyRequired reports a missing artifact S3 secret access key in the worker config.
	ErrWorkerArtifactStoreS3SecretKeyRequired = errors.New("agentos temporal worker: artifact s3 secret access key is required")
)

func newWorkerKit(ctx context.Context, cfg *WorkerConfig) (*WorkerKit, error) {
	resources, err := openWorkerResources(cfg)
	if err != nil {
		return nil, err
	}

	dataPlane, err := newWorkerDataPlane(cfg, resources)
	if err != nil {
		resources.close()

		return nil, err
	}

	kit, err := buildWorkerKit(ctx, resources.dependencies(), dataPlane)
	if err != nil {
		dataPlane.Close()
		resources.close()

		return nil, err
	}

	infra, err := initWorkerPlanRuntime(ctx, cfg, resources.postgres, resources.temporalClient, dataPlane, resources.logger)
	if err != nil {
		dataPlane.Close()
		resources.close()

		return nil, err
	}

	configureWorkerPlanRuntime(kit, cfg, resources, infra)

	return kit, nil
}

// NewPlanWorkerKit creates the minimal worker registration kit required by
// the AgentOS RunPlan control plane.
func NewPlanWorkerKit(ctx context.Context, cfg *WorkerConfig) (*PlanWorkerKit, error) {
	resources, err := openWorkerResources(cfg)
	if err != nil {
		return nil, err
	}

	dataPlane, err := newWorkerDataPlane(cfg, resources)
	if err != nil {
		resources.close()

		return nil, err
	}

	infra, err := initWorkerPlanRuntime(ctx, cfg, resources.postgres, resources.temporalClient, dataPlane, resources.logger)
	if err != nil {
		dataPlane.Close()
		resources.close()

		return nil, err
	}

	return &PlanWorkerKit{
		planActivities:        infra.planActivities,
		planCommandReconciler: newPlanCommandReconciler(newPlanTemporalClient(resources.temporalClient), &cfg.TemporalTaskQueues, workerNexusEndpoint(cfg), cfg.NexusPeers, infra.planStore, infra.planStore, infra.planStore),
		closeFns: []func() error{
			infra.planClose,
			dataPlane.Close,
			func() error {
				resources.close()

				return nil
			},
		},
	}, nil
}

type workerResources struct {
	cfg            *WorkerConfig
	logger         *logger.Logger
	postgres       *postgres.Postgres
	temporalClient client.Client
}

func openWorkerResources(cfg *WorkerConfig) (*workerResources, error) {
	if cfg == nil {
		return nil, ErrWorkerConfigRequired
	}

	if cfg.PostgresURL == "" {
		return nil, ErrWorkerPostgresURLRequired
	}

	if err := cfg.TemporalTaskQueues.Validate(); err != nil {
		return nil, err
	}

	if err := validateWorkerArtifactStore(&cfg.ArtifactStore); err != nil {
		return nil, err
	}

	l := logger.New(cfg.LogLevel)

	pg, err := newPostgres(cfg)
	if err != nil {
		return nil, fmt.Errorf("agentos temporal worker - postgres: %w", err)
	}

	fwTemporal := temporalConfig(&RuntimeConfig{
		TemporalAddress:    cfg.TemporalAddress,
		TemporalNamespace:  cfg.TemporalNamespace,
		TemporalTaskQueues: cfg.TemporalTaskQueues,
	})

	temporalClient, err := client.Dial(client.Options{
		HostPort:  fwTemporal.Address,
		Namespace: fwTemporal.Namespace,
	})
	if err != nil {
		pg.Close()

		return nil, fmt.Errorf("agentos temporal worker - temporal client: %w", err)
	}

	return &workerResources{
		cfg:            cfg,
		logger:         l,
		postgres:       pg,
		temporalClient: temporalClient,
	}, nil
}

// newWorkerDataPlane assembles the shared data-plane bus (Centrifugo by
// default, degrading to the in-process memstream bus when no base URL is
// configured) plus the milestone recorder. The single instance feeds the run
// activities' publisher and fact recorder and the plan runtime's
// subscriber/plan bus, so in-process subscribers see the activities'
// publishes.
func newWorkerDataPlane(cfg *WorkerConfig, resources *workerResources) (*StreamingDataPlane, error) {
	// The whole config travels: EventOutbox decides whether the recorder's
	// milestones are queued for the fact log alongside being persisted.
	return NewStreamingDataPlane(cfg.StreamCentrifugo, resources.postgres, resources.logger)
}

func (r *workerResources) dependencies() *workerDependencies {
	return &workerDependencies{
		cfg:            r.cfg,
		logger:         r.logger,
		postgres:       r.postgres,
		temporalClient: r.temporalClient,
	}
}

func (r *workerResources) close() {
	r.temporalClient.Close()

	r.postgres.Close()
}

func configureWorkerPlanRuntime(kit *WorkerKit, cfg *WorkerConfig, resources *workerResources, infra *workerPlanRuntime) {
	kit.planActivities = infra.planActivities
	kit.processActivities = infra.processActivities
	kit.planCommandReconciler = newPlanCommandReconciler(newPlanTemporalClient(resources.temporalClient), &cfg.TemporalTaskQueues, workerNexusEndpoint(cfg), cfg.NexusPeers, infra.planStore, infra.planStore, infra.planStore)
	kit.closeFns = append(
		kit.closeFns,
		func() error {
			resources.temporalClient.Close()

			return nil
		},
		infra.planClose,
		func() error {
			resources.postgres.Close()

			return nil
		},
	)
}

type workerPlanRuntime struct {
	planActivities    *PlanActivities
	processActivities *ProcessActivities
	planClose         func() error
	planStore         *temporalrepo.AgentOSPlanRepo
}

func initWorkerPlanRuntime(ctx context.Context, cfg *WorkerConfig, pg *postgres.Postgres, temporalClient client.Client, dataPlane *StreamingDataPlane, l logger.Interface) (*workerPlanRuntime, error) {
	runBackendIndex := temporalrepo.NewRunBackendIndexRepo(pg)
	planStore := temporalrepo.NewAgentOSPlanRepo(pg)
	blobCfg := artifactBlobConfig(&cfg.ArtifactStore)

	blobStore, err := artifactrepo.NewBlobStore(ctx, &blobCfg)
	if err != nil {
		return nil, fmt.Errorf("agentos temporal worker - artifact blob store: %w", err)
	}

	artifactStore := temporalrepo.NewAgentOSArtifactRepo(pg, blobStore)
	capabilityCatalog := temporalrepo.NewAgentOSCapabilityCatalogRepo(pg)
	artifactSchemaCatalog := temporalrepo.NewAgentOSArtifactSchemaCatalogRepo(pg)

	if err := agentosplan.RegisterCapabilities(ctx, capabilityCatalog, CapabilitiesWithDefaults(cfg.Capabilities)); err != nil {
		return nil, fmt.Errorf("agentos temporal worker - register capabilities: %w", err)
	}

	if err := agentosplan.RegisterArtifactSchemas(ctx, artifactSchemaCatalog, cfg.ArtifactSchemas); err != nil {
		return nil, fmt.Errorf("agentos temporal worker - register artifact schemas: %w", err)
	}

	// The live plan event tail rides the shared data plane (planbus): the
	// publisher carries plan activities' fan-out and the subscriber feeds the
	// runtime's SubscribePlan live tail.
	planEventStream := planstream.New(dataPlane.Publisher, dataPlane.Subscriber)
	processStore := temporalrepo.NewAgentOSProcessRepo(pg)

	planRuntime, err := NewRuntimeWithClient(ctx, &RuntimeConfig{
		TemporalAddress:          cfg.TemporalAddress,
		TemporalNamespace:        cfg.TemporalNamespace,
		TemporalTaskQueues:       cfg.TemporalTaskQueues,
		TemporalExternalBackends: cfg.TemporalExternalBackends,
		HTTPBackends:             cfg.HTTPBackends,
		GRPCBackends:             cfg.GRPCBackends,
		DSHBackends:              cfg.DSHBackends,
		ArtifactStore:            cfg.ArtifactStore,
		Subscriber:               dataPlane.Subscriber,
		Publisher:                dataPlane.Publisher,
		Facts:                    dataPlane.Facts,
		Logger:                   l,
		PlanEventPublisher:       planEventStream,
		PlanEventSubscriber:      planEventStream,
	}, temporalClient, WithRunBackendIndex(runBackendIndex))
	if err != nil {
		return nil, fmt.Errorf("agentos temporal worker - plan runtime: %w", err)
	}

	planActivities, err := NewPlanActivitiesWithCatalogAndSchemas(planRuntime, capabilityCatalog, artifactSchemaCatalog, planStore, planEventStream, artifactStore)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("agentos temporal worker - plan activities: %w", err), planRuntime.Close())
	}

	processActivities, err := NewProcessActivities(processStore, processStore)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("agentos temporal worker - process activities: %w", err), planRuntime.Close())
	}

	return &workerPlanRuntime{
		planActivities:    planActivities,
		processActivities: processActivities,
		planClose:         planRuntime.Close,
		planStore:         planStore,
	}, nil
}

func validateWorkerArtifactStore(cfg *ArtifactStoreConfig) error {
	return validateArtifactStoreConfig(cfg, &artifactStoreValidationErrors{
		backendRequired:   ErrWorkerArtifactStoreBackendRequired,
		backendUnknown:    ErrWorkerArtifactStoreBackendUnknown,
		localRootRequired: ErrWorkerArtifactStoreLocalRootRequired,
		s3BucketRequired:  ErrWorkerArtifactStoreS3BucketRequired,
		s3RegionRequired:  ErrWorkerArtifactStoreS3RegionRequired,
		s3AccessRequired:  ErrWorkerArtifactStoreS3AccessKeyRequired,
		s3SecretRequired:  ErrWorkerArtifactStoreS3SecretKeyRequired,
	})
}

func artifactBlobConfig(cfg *ArtifactStoreConfig) artifactrepo.Config {
	return artifactrepo.Config{
		Backend: artifactrepo.Backend(cfg.Backend),
		Local: artifactrepo.LocalConfig{
			Root: cfg.Local.Root,
		},
		S3: artifactrepo.S3Config{
			Bucket:          cfg.S3.Bucket,
			Region:          cfg.S3.Region,
			Endpoint:        cfg.S3.Endpoint,
			AccessKeyID:     cfg.S3.AccessKeyID,
			SecretAccessKey: cfg.S3.SecretAccessKey,
			SessionToken:    cfg.S3.SessionToken,
			ForcePathStyle:  cfg.S3.ForcePathStyle,
		},
	}
}

func newPostgres(cfg *WorkerConfig) (*postgres.Postgres, error) {
	if cfg.PostgresPoolMax > 0 {
		return postgres.New(cfg.PostgresURL, postgres.MaxPoolSize(cfg.PostgresPoolMax))
	}

	return postgres.New(cfg.PostgresURL)
}

type workerDependencies struct {
	cfg            *WorkerConfig
	logger         *logger.Logger
	postgres       *postgres.Postgres
	temporalClient client.Client
}

func buildWorkerKit(ctx context.Context, deps *workerDependencies, dataPlane *StreamingDataPlane) (*WorkerKit, error) {
	persistentAgentRepo := temporalrepo.NewAgentRepo(deps.postgres)
	agentRepo := cached.NewAgentRepo(persistentAgentRepo)
	templateRepo := temporalrepo.NewWorkflowTemplateRepo(deps.postgres)
	templateUC := templatepkg.New(templateRepo)

	llmResult, err := webapi.LoadLLMProviders(deps.cfg.LLMConfigPath)
	if err != nil {
		return nil, fmt.Errorf("agentos temporal worker - load llm providers: %w", err)
	}

	llmProvider, err := newBifrostProvider(deps.cfg.LLMConfigPath, llmResult, deps.logger)
	if err != nil {
		return nil, err
	}

	billingRepo := temporalrepo.NewBillingRepo(deps.postgres)
	billingUC := billingpkg.New(billingRepo, nil, billingRepo)

	toolRegistry := toolkit.NewRegistry()
	if deps.cfg.RegisterEnvTools {
		registerEnvTools(deps.logger, toolRegistry)
	}

	// Every protective stage is present, or the worker refuses to start.
	toolPipe, err := agenttool.NewPipeline(&agenttool.PipelineConfig{
		Authorizer: &agenttool.TenantAuthorizer{
			Authorizer: authz.TenantAuthorizer{},
			Audit:      agenttool.NewLoggerAuditSink(deps.logger),
		},
		Executor:    toolRegistry,
		Redactor:    agenttool.NewRedactor(nil),
		Policies:    &agenttool.DefaultPolicyProvider{},
		Idempotency: temporalrepo.NewToolIdempotencyStore(deps.postgres),
	})
	if err != nil {
		return nil, fmt.Errorf("agentfw worker - tool pipeline: %w", err)
	}

	toolExecutor := framework.NewToolPipeline(toolPipe)
	agentCompressor := compressor.New(compressor.Config{LLM: llmProvider})
	agentUC := agent.New(llmProvider, toolExecutor, agentCompressor, toolRegistry, agentRepo)
	agentUC.SetLogger(deps.logger)

	// Nil policy here is fail-closed: an embedder that declares no MCP
	// allowlist gets a manager that launches nothing.
	mcpManager := mcpRepo.NewManager(deps.cfg.MCPTransportPolicy)
	mcpManager.SetRegistry(toolRegistry)

	activities := orchestration.NewAgentActivities(agentUC, deps.logger).
		WithMCPManager(mcpManager).
		WithBilling(billingUC)

	kit := &WorkerKit{activities: activities, runBackendResolver: deps.cfg.RunBackendResolver}

	configureStreamingProjection(activities, dataPlane)

	registerToolsOnRegistry(deps.logger, toolRegistry)

	if deps.cfg.EnsureDefaultTemplate {
		if err := templateUC.EnsureDefault(ctx); err != nil && deps.logger != nil {
			deps.logger.Warn("agentos temporal worker - ensure default template: %v", err)
		}
	}

	return kit, nil
}

// configureStreamingProjection wires the shared data plane onto the run
// activities: the publisher mirrors the AG-UI timeline and the milestone
// recorder makes each fact durable before its publish. The data plane is
// assembled once by the kit entry points (Centrifugo by default, degrading to
// the in-process memstream bus when no base URL is configured), so a worker
// always has a live transport.
func configureStreamingProjection(activities *orchestration.AgentActivities, dataPlane *StreamingDataPlane) {
	if dataPlane == nil {
		return
	}

	activities.WithStreamPublisher(dataPlane.Publisher)
	activities.WithMilestoneRecorder(dataPlane.Facts)
}

func registerToolsOnRegistry(l *logger.Logger, toolRegistry *toolkit.ToolRegistry) {
	registerAgentCreationTool(l, toolRegistry)
}

func registerAgentCreationTool(l *logger.Logger, toolRegistry *toolkit.ToolRegistry) {
	agentCreator := func(_ context.Context, _, _, _, _ string, _ []string) error {
		return nil
	}
	if err := toolRegistry.Register(toolkit.NewAgentCreationTool(agentCreator)); err != nil && l != nil {
		l.Warn("agentos temporal worker - register agent_creation_tool: %v", err)
	}
}

func registerEnvTools(l *logger.Logger, toolRegistry *toolkit.ToolRegistry) {
	if apiKey := os.Getenv("TAVILY_API_KEY"); apiKey != "" {
		if err := toolRegistry.Register(toolkit.NewWebSearch(toolkit.WebSearchConfig{
			Provider: "tavily",
			APIKey:   apiKey,
		})); err != nil && l != nil {
			l.Warn("agentos temporal worker - register web_search: %v", err)
		}
	}

	if apiKey := os.Getenv("FIRECRAWL_API_KEY"); apiKey != "" {
		if err := toolRegistry.Register(toolkit.NewScrapeWebpage(toolkit.ScrapeWebpageConfig{
			APIKey: apiKey,
		})); err != nil && l != nil {
			l.Warn("agentos temporal worker - register scrape_webpage: %v", err)
		}
	}

	if endpoint := os.Getenv("CODE_INTERPRETER_ENDPOINT"); endpoint != "" {
		if err := toolRegistry.Register(toolkit.NewCodeInterpreter(toolkit.CodeInterpreterConfig{
			Endpoint: endpoint,
			APIKey:   os.Getenv("CODE_INTERPRETER_API_KEY"),
		})); err != nil && l != nil {
			l.Warn("agentos temporal worker - register code_interpreter: %v", err)
		}
	}
}

func newBifrostProvider(llmConfigPath string, llmResult *webapi.LLMProvidersResult, l *logger.Logger) (*webapi.BifrostProvider, error) {
	llmProvider, err := webapi.NewBifrost(&webapi.BifrostConfig{
		Providers:       llmResult.Providers,
		Scenarios:       llmResult.Scenarios,
		DefaultScenario: llmResult.DefaultScenario,
	})
	if err != nil {
		return nil, fmt.Errorf("agentos temporal worker - new bifrost: %w", err)
	}

	startLLMConfigWatcher(llmConfigPath, llmProvider, l)

	return llmProvider, nil
}

func startLLMConfigWatcher(llmConfigPath string, llmProvider *webapi.BifrostProvider, l *logger.Logger) {
	if llmConfigPath == "" {
		return
	}

	err := webapi.WatchLLMConfig(llmConfigPath, func(updated *webapi.LLMConfigFile) {
		llmProvider.ReloadConfig(updated)

		if l != nil {
			l.Info("agentos temporal worker - LLM config reloaded from %s", llmConfigPath)
		}
	})
	if err != nil {
		if l != nil {
			l.Warn("agentos temporal worker - LLM config watcher: %v", err)
		}

		return
	}

	if l != nil {
		l.Info("agentos temporal worker - LLM config watcher started for %s", llmConfigPath)
	}
}
