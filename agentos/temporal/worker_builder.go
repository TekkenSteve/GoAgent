package temporal

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	agenttool "github.com/TekkenSteve/GoAgent/internal/agentfw/tool"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	goredis "github.com/TekkenSteve/GoAgent/internal/pkg/redis"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/planstream"
	artifactrepo "github.com/TekkenSteve/GoAgent/internal/repo/artifact"
	"github.com/TekkenSteve/GoAgent/internal/repo/cached"
	"github.com/TekkenSteve/GoAgent/internal/repo/compressor"
	"github.com/TekkenSteve/GoAgent/internal/repo/framework"
	mcpRepo "github.com/TekkenSteve/GoAgent/internal/repo/mcp"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	pipelinepkg "github.com/TekkenSteve/GoAgent/internal/repo/pipeline"
	repostream "github.com/TekkenSteve/GoAgent/internal/repo/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/centrifugo"
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
	// ErrWorkerRedisURLRequired reports a missing Redis URL in the worker config.
	ErrWorkerRedisURLRequired = errors.New("agentos temporal worker: redis url is required")
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
	resources, err := openWorkerResources(ctx, cfg)
	if err != nil {
		return nil, err
	}

	kit, batchWriter, err := buildWorkerKit(ctx, resources.dependencies())
	if err != nil {
		resources.close()

		return nil, err
	}

	infra, err := initWorkerPlanRuntime(ctx, cfg, resources.postgres, resources.redis, resources.temporalClient)
	if err != nil {
		resources.close()

		return nil, err
	}

	configureWorkerPlanRuntime(kit, cfg, resources, batchWriter, infra)

	return kit, nil
}

// NewPlanWorkerKit creates the minimal worker registration kit required by
// the AgentOS RunPlan control plane.
func NewPlanWorkerKit(ctx context.Context, cfg *WorkerConfig) (*PlanWorkerKit, error) {
	resources, err := openWorkerResources(ctx, cfg)
	if err != nil {
		return nil, err
	}

	infra, err := initWorkerPlanRuntime(ctx, cfg, resources.postgres, resources.redis, resources.temporalClient)
	if err != nil {
		resources.close()

		return nil, err
	}

	return &PlanWorkerKit{
		planActivities:        infra.planActivities,
		planCommandReconciler: newPlanCommandReconciler(newPlanTemporalClient(resources.temporalClient), &cfg.TemporalTaskQueues, infra.planStore, infra.planStore, infra.planStore),
		closeFns: []func() error{
			infra.planClose,
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
	redis          *goredis.Redis
	temporalClient client.Client
}

func openWorkerResources(ctx context.Context, cfg *WorkerConfig) (*workerResources, error) {
	if cfg == nil {
		return nil, ErrWorkerConfigRequired
	}

	if cfg.PostgresURL == "" {
		return nil, ErrWorkerPostgresURLRequired
	}

	if cfg.RedisURL == "" {
		return nil, ErrWorkerRedisURLRequired
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

	rdb, err := goredis.New(ctx, cfg.RedisURL)
	if err != nil {
		pg.Close()

		return nil, fmt.Errorf("agentos temporal worker - redis: %w", err)
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

		return nil, errors.Join(fmt.Errorf("agentos temporal worker - temporal client: %w", err), rdb.Close())
	}

	return &workerResources{
		cfg:            cfg,
		logger:         l,
		postgres:       pg,
		redis:          rdb,
		temporalClient: temporalClient,
	}, nil
}

func (r *workerResources) dependencies() *workerDependencies {
	return &workerDependencies{
		cfg:            r.cfg,
		logger:         r.logger,
		postgres:       r.postgres,
		redis:          r.redis,
		temporalClient: r.temporalClient,
	}
}

func (r *workerResources) close() {
	r.temporalClient.Close()

	if err := r.redis.Close(); err != nil {
		r.logger.Error("worker - close redis", err)
	}

	r.postgres.Close()
}

func configureWorkerPlanRuntime(kit *WorkerKit, cfg *WorkerConfig, resources *workerResources, batchWriter *pipelinepkg.BatchWriter, infra *workerPlanRuntime) {
	kit.planActivities = infra.planActivities
	kit.processActivities = infra.processActivities
	kit.planCommandReconciler = newPlanCommandReconciler(newPlanTemporalClient(resources.temporalClient), &cfg.TemporalTaskQueues, infra.planStore, infra.planStore, infra.planStore)
	kit.closeFns = append(
		kit.closeFns,
		func() error {
			if batchWriter != nil {
				batchWriter.Stop()
			}

			return nil
		},
		func() error {
			resources.temporalClient.Close()

			return nil
		},
		infra.planClose,
		func() error {
			return resources.redis.Close()
		},
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

func initWorkerPlanRuntime(ctx context.Context, cfg *WorkerConfig, pg *postgres.Postgres, rdb *goredis.Redis, temporalClient client.Client) (*workerPlanRuntime, error) {
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

	planEventStream := planstream.NewRedisPlanEventStream(rdb)
	processStore := temporalrepo.NewAgentOSProcessRepo(pg)

	planRuntime, err := NewRuntimeWithClient(ctx, &RuntimeConfig{
		TemporalAddress:          cfg.TemporalAddress,
		TemporalNamespace:        cfg.TemporalNamespace,
		TemporalTaskQueues:       cfg.TemporalTaskQueues,
		TemporalExternalBackends: cfg.TemporalExternalBackends,
		HTTPBackends:             cfg.HTTPBackends,
		GRPCBackends:             cfg.GRPCBackends,
		ArtifactStore:            cfg.ArtifactStore,
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
	redis          *goredis.Redis
	temporalClient client.Client
}

func buildWorkerKit(ctx context.Context, deps *workerDependencies) (*WorkerKit, *pipelinepkg.BatchWriter, error) {
	messageRepo := temporalrepo.NewMessageRepo(deps.postgres)
	persistentAgentRepo := temporalrepo.NewAgentRepo(deps.postgres)
	agentRepo := cached.NewAgentRepo(persistentAgentRepo)
	templateRepo := temporalrepo.NewWorkflowTemplateRepo(deps.postgres)
	templateUC := templatepkg.New(templateRepo)

	llmResult, err := webapi.LoadLLMProviders(deps.cfg.LLMConfigPath)
	if err != nil {
		return nil, nil, fmt.Errorf("agentos temporal worker - load llm providers: %w", err)
	}

	llmProvider, err := newBifrostProvider(deps.cfg.LLMConfigPath, llmResult, deps.logger)
	if err != nil {
		return nil, nil, err
	}

	billingRepo := temporalrepo.NewBillingRepo(deps.postgres)
	billingUC := billingpkg.New(billingRepo, nil, billingRepo)

	toolRegistry := toolkit.NewRegistry()
	if deps.cfg.RegisterEnvTools {
		registerEnvTools(deps.logger, toolRegistry)
	}

	toolPipe := agenttool.Pipeline{Executor: toolRegistry}
	toolExecutor := framework.NewToolPipeline(&toolPipe)
	agentCompressor := compressor.New(compressor.Config{LLM: llmProvider})
	wal := pipelinepkg.NewWriteAheadLog(deps.redis.GeneralClient)
	dlq := pipelinepkg.NewDeadLetterQueue(deps.redis.GeneralClient)
	batchWriter := pipelinepkg.NewBatchWriter(wal, dlq, messageRepo, deps.logger)
	batchWriter.Start()

	agentUC := agent.New(llmProvider, toolExecutor, wal, agentCompressor, toolRegistry, agentRepo)
	agentUC.SetLogger(deps.logger)

	mcpManager := mcpRepo.NewManager()
	mcpManager.SetRegistry(toolRegistry)

	eventSequencer := repostream.NewRedisSequencer(deps.redis)
	eventStore := repostream.NewRedisEventStore(deps.redis, eventSequencer)
	activities := orchestration.NewAgentActivities(agentUC, eventStore, deps.logger).
		WithMCPManager(mcpManager).
		WithBilling(billingUC)

	if deps.cfg.StreamCentrifugo.BaseURL != "" {
		streamPub, err := centrifugo.NewPublisher(centrifugo.Config{
			BaseURL: deps.cfg.StreamCentrifugo.BaseURL,
			APIKey:  deps.cfg.StreamCentrifugo.APIKey,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("agentos temporal worker - stream publisher: %w", err)
		}

		activities.WithStreamPublisher(streamPub)
	}

	registerToolsOnRegistry(deps.logger, toolRegistry)

	if deps.cfg.EnsureDefaultTemplate {
		if err := templateUC.EnsureDefault(ctx); err != nil && deps.logger != nil {
			deps.logger.Warn("agentos temporal worker - ensure default template: %v", err)
		}
	}

	return &WorkerKit{activities: activities}, batchWriter, nil
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
