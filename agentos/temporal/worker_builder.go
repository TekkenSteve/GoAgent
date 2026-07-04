package temporal

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	agenttool "github.com/TekkenSteve/GoAgent/internal/agentfw/tool"
	"github.com/TekkenSteve/GoAgent/internal/entity"
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
	"github.com/TekkenSteve/GoAgent/internal/repo/toolkit"
	"github.com/TekkenSteve/GoAgent/internal/repo/webapi"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agent"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	billingpkg "github.com/TekkenSteve/GoAgent/internal/usecase/billing"
	templatepkg "github.com/TekkenSteve/GoAgent/internal/usecase/template"
	triggerpkg "github.com/TekkenSteve/GoAgent/internal/usecase/trigger"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"go.temporal.io/sdk/client"
)

var (
	ErrWorkerConfigRequired                   = errors.New("agentos temporal worker: config is required")
	ErrWorkerPostgresURLRequired              = errors.New("agentos temporal worker: postgres url is required")
	ErrWorkerRedisURLRequired                 = errors.New("agentos temporal worker: redis url is required")
	ErrWorkerArtifactStoreBackendRequired     = errors.New("agentos temporal worker: artifact store backend is required")
	ErrWorkerArtifactStoreBackendUnknown      = errors.New("agentos temporal worker: artifact store backend is unknown")
	ErrWorkerArtifactStoreLocalRootRequired   = errors.New("agentos temporal worker: artifact local root is required")
	ErrWorkerArtifactStoreS3BucketRequired    = errors.New("agentos temporal worker: artifact s3 bucket is required")
	ErrWorkerArtifactStoreS3RegionRequired    = errors.New("agentos temporal worker: artifact s3 region is required")
	ErrWorkerArtifactStoreS3AccessKeyRequired = errors.New("agentos temporal worker: artifact s3 access key id is required")
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
		_ = rdb.Close()
		pg.Close()

		return nil, fmt.Errorf("agentos temporal worker - temporal client: %w", err)
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
	_ = r.redis.Close()
	r.postgres.Close()
}

func configureWorkerPlanRuntime(kit *WorkerKit, cfg *WorkerConfig, resources *workerResources, batchWriter *pipelinepkg.BatchWriter, infra *workerPlanRuntime) {
	kit.planActivities = infra.planActivities
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
			resources.redis.Close()

			return nil
		},
		func() error {
			resources.postgres.Close()

			return nil
		},
	)
}

type workerPlanRuntime struct {
	planActivities *PlanActivities
	planClose      func() error
	planStore      *temporalrepo.AgentOSPlanRepo
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
		planRuntime.Close()

		return nil, fmt.Errorf("agentos temporal worker - plan activities: %w", err)
	}

	return &workerPlanRuntime{
		planActivities: planActivities,
		planClose:      planRuntime.Close,
		planStore:      planStore,
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
	triggerRepo := temporalrepo.NewTriggerRepo(deps.postgres)
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
	workflowTaskQueues := orchestration.WorkflowTaskQueues{
		NativeControl: deps.cfg.TemporalTaskQueues.NativeControl,
		NativeLLM:     deps.cfg.TemporalTaskQueues.NativeLLM,
		NativeTool:    deps.cfg.TemporalTaskQueues.NativeTool,
		Stream:        deps.cfg.TemporalTaskQueues.Stream,
		Trigger:       deps.cfg.TemporalTaskQueues.Trigger,
	}
	triggerScheduler := temporalrepo.NewTemporalTriggerScheduler(deps.temporalClient, deps.cfg.TemporalTaskQueues.Trigger, &workflowTaskQueues)
	triggerUC := triggerpkg.New(triggerRepo, triggerScheduler, templateRepo)
	mcpManager := mcpRepo.NewManager()
	mcpManager.SetRegistry(toolRegistry)

	eventSequencer := repostream.NewRedisSequencer(deps.redis)
	eventStore := repostream.NewRedisEventStore(deps.redis, eventSequencer)
	activities := orchestration.NewAgentActivities(agentUC, eventStore, deps.logger).
		WithTemplateRepo(templateRepo).
		WithTriggerUC(triggerUC).
		WithMCPManager(mcpManager).
		WithBilling(billingUC)
	registerToolsOnRegistry(deps.logger, toolRegistry, triggerUC, triggerScheduler)

	if deps.cfg.EnsureDefaultTemplate {
		if err := templateUC.EnsureDefault(ctx); err != nil && deps.logger != nil {
			deps.logger.Warn("agentos temporal worker - ensure default template: %v", err)
		}
	}

	return &WorkerKit{activities: activities}, batchWriter, nil
}

func registerToolsOnRegistry(l *logger.Logger, toolRegistry *toolkit.ToolRegistry, triggerUC *triggerpkg.UseCase, triggerScheduler *temporalrepo.TemporalTriggerScheduler) {
	registerAgentCreationTool(l, toolRegistry)
	registerTriggerCreationTool(l, toolRegistry, triggerUC, triggerScheduler)
	registerTriggerManagementTools(l, toolRegistry, triggerUC)
}

func registerAgentCreationTool(l *logger.Logger, toolRegistry *toolkit.ToolRegistry) {
	agentCreator := func(_ context.Context, _, _, _, _ string, _ []string) error {
		return nil
	}
	if err := toolRegistry.Register(toolkit.NewAgentCreationTool(agentCreator)); err != nil && l != nil {
		l.Warn("agentos temporal worker - register agent_creation_tool: %v", err)
	}
}

func registerTriggerCreationTool(l *logger.Logger, toolRegistry *toolkit.ToolRegistry, triggerUC *triggerpkg.UseCase, triggerScheduler *temporalrepo.TemporalTriggerScheduler) {
	triggerCreator := func(ctx context.Context, templateID, name, cronExpression, agentPrompt string, templateVars []string, templateVarsVals map[string]string) (string, error) {
		t, err := triggerUC.Create(ctx, &entity.CreateTriggerRequest{
			TemplateID:       templateID,
			Name:             name,
			TriggerType:      entity.TriggerSchedule,
			CronExpression:   cronExpression,
			AgentPrompt:      agentPrompt,
			TemplateVars:     templateVars,
			TemplateVarsVals: templateVarsVals,
		})
		if err != nil {
			return "", err
		}

		return t.ID, nil
	}

	triggerScheduleFn := func(ctx context.Context, triggerID, _ string) error {
		trigger, err := triggerUC.Get(ctx, triggerID)
		if err != nil {
			return err
		}

		return triggerScheduler.Schedule(ctx, &trigger)
	}
	if err := toolRegistry.Register(toolkit.NewTriggerTool(triggerCreator, triggerScheduleFn)); err != nil && l != nil {
		l.Warn("agentos temporal worker - register create_trigger: %v", err)
	}
}

func registerTriggerManagementTools(l *logger.Logger, toolRegistry *toolkit.ToolRegistry, triggerUC *triggerpkg.UseCase) {
	triggerLister := func(ctx context.Context, templateID string) ([]entity.TriggerSpec, error) {
		return triggerUC.ListByTemplate(ctx, templateID)
	}
	if err := toolRegistry.Register(toolkit.NewListTriggersTool(triggerLister)); err != nil && l != nil {
		l.Warn("agentos temporal worker - register list_triggers: %v", err)
	}

	triggerToggler := func(ctx context.Context, triggerID string, isActive bool) (entity.TriggerSpec, error) {
		return triggerUC.Toggle(ctx, triggerID, isActive)
	}
	if err := toolRegistry.Register(toolkit.NewToggleTriggerTool(triggerToggler)); err != nil && l != nil {
		l.Warn("agentos temporal worker - register toggle_trigger: %v", err)
	}

	triggerDeleter := func(ctx context.Context, triggerID string) error {
		return triggerUC.Delete(ctx, triggerID)
	}
	if err := toolRegistry.Register(toolkit.NewDeleteTriggerTool(triggerDeleter)); err != nil && l != nil {
		l.Warn("agentos temporal worker - register delete_trigger: %v", err)
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
