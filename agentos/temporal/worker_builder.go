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
	billingpkg "github.com/TekkenSteve/GoAgent/internal/usecase/billing"
	templatepkg "github.com/TekkenSteve/GoAgent/internal/usecase/template"
	triggerpkg "github.com/TekkenSteve/GoAgent/internal/usecase/trigger"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"go.temporal.io/sdk/client"
)

var (
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

func newWorkerKit(ctx context.Context, cfg WorkerConfig) (*WorkerKit, error) {
	if cfg.PostgresURL == "" {
		return nil, ErrWorkerPostgresURLRequired
	}
	if cfg.RedisURL == "" {
		return nil, ErrWorkerRedisURLRequired
	}
	if err := validateWorkerArtifactStore(cfg.ArtifactStore); err != nil {
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

	fwTemporal := temporalConfig(RuntimeConfig{
		TemporalAddress:   cfg.TemporalAddress,
		TemporalNamespace: cfg.TemporalNamespace,
		TemporalTaskQueue: cfg.TemporalTaskQueue,
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

	kit, batchWriter, err := buildWorkerKit(ctx, workerDependencies{
		cfg:            cfg,
		logger:         l,
		postgres:       pg,
		redis:          rdb,
		temporalClient: temporalClient,
	})
	if err != nil {
		temporalClient.Close()
		_ = rdb.Close()
		pg.Close()

		return nil, err
	}

	runBackendIndex := temporalrepo.NewRunBackendIndexRepo(pg)
	planStore := temporalrepo.NewAgentOSPlanRepo(pg)
	blobStore, err := artifactrepo.NewBlobStore(ctx, artifactBlobConfig(cfg.ArtifactStore))
	if err != nil {
		temporalClient.Close()
		_ = rdb.Close()
		pg.Close()

		return nil, fmt.Errorf("agentos temporal worker - artifact blob store: %w", err)
	}
	artifactStore := temporalrepo.NewAgentOSArtifactRepo(pg, blobStore)
	planEventStream := repostream.NewRedisPlanEventStream(rdb)
	planRuntime, err := NewRuntimeWithClient(ctx, RuntimeConfig{
		TemporalAddress:          cfg.TemporalAddress,
		TemporalNamespace:        cfg.TemporalNamespace,
		TemporalTaskQueue:        cfg.TemporalTaskQueue,
		TemporalExternalBackends: cfg.TemporalExternalBackends,
		HTTPBackends:             cfg.HTTPBackends,
		GRPCBackends:             cfg.GRPCBackends,
	}, temporalClient, WithRunBackendIndex(runBackendIndex))
	if err != nil {
		temporalClient.Close()
		_ = rdb.Close()
		pg.Close()

		return nil, fmt.Errorf("agentos temporal worker - plan runtime: %w", err)
	}
	planActivities, err := NewPlanActivitiesWithStores(planRuntime, cfg.Capabilities, planStore, planStore, planEventStream, runBackendIndex, artifactStore)
	if err != nil {
		temporalClient.Close()
		_ = rdb.Close()
		pg.Close()

		return nil, fmt.Errorf("agentos temporal worker - plan activities: %w", err)
	}
	kit.planActivities = planActivities

	kit.closeFns = append(kit.closeFns,
		func() error {
			if batchWriter != nil {
				batchWriter.Stop()
			}

			return nil
		},
		func() error {
			temporalClient.Close()

			return nil
		},
		planRuntime.Close,
		rdb.Close,
		func() error {
			pg.Close()

			return nil
		},
	)

	return kit, nil
}

func validateWorkerArtifactStore(cfg ArtifactStoreConfig) error {
	switch cfg.Backend {
	case ArtifactStoreBackendLocal:
		if cfg.Local.Root == "" {
			return ErrWorkerArtifactStoreLocalRootRequired
		}
	case ArtifactStoreBackendS3:
		if cfg.S3.Bucket == "" {
			return ErrWorkerArtifactStoreS3BucketRequired
		}
		if cfg.S3.Region == "" {
			return ErrWorkerArtifactStoreS3RegionRequired
		}
		if cfg.S3.AccessKeyID == "" {
			return ErrWorkerArtifactStoreS3AccessKeyRequired
		}
		if cfg.S3.SecretAccessKey == "" {
			return ErrWorkerArtifactStoreS3SecretKeyRequired
		}
	case "":
		return ErrWorkerArtifactStoreBackendRequired
	default:
		return fmt.Errorf("%w: %s", ErrWorkerArtifactStoreBackendUnknown, cfg.Backend)
	}

	return nil
}

func artifactBlobConfig(cfg ArtifactStoreConfig) artifactrepo.Config {
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

func newPostgres(cfg WorkerConfig) (*postgres.Postgres, error) {
	if cfg.PostgresPoolMax > 0 {
		return postgres.New(cfg.PostgresURL, postgres.MaxPoolSize(cfg.PostgresPoolMax))
	}

	return postgres.New(cfg.PostgresURL)
}

type workerDependencies struct {
	cfg            WorkerConfig
	logger         *logger.Logger
	postgres       *postgres.Postgres
	redis          *goredis.Redis
	temporalClient client.Client
}

func buildWorkerKit(ctx context.Context, deps workerDependencies) (*WorkerKit, *pipelinepkg.BatchWriter, error) {
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

	fwTemporal := temporalConfig(RuntimeConfig{
		TemporalAddress:   deps.cfg.TemporalAddress,
		TemporalNamespace: deps.cfg.TemporalNamespace,
		TemporalTaskQueue: deps.cfg.TemporalTaskQueue,
	})
	triggerScheduler := temporalrepo.NewTemporalTriggerScheduler(deps.temporalClient, fwTemporal.TaskQueue)
	triggerUC := triggerpkg.New(triggerRepo, triggerScheduler, templateRepo)

	mcpManager := mcpRepo.NewManager()
	mcpManager.SetRegistry(toolRegistry)
	if deps.logger != nil {
		deps.logger.Info("agentos temporal worker - mcp manager created for per-agent JIT tool registration")
	}

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
	agentCreator := func(_ context.Context, _, _, _, _ string, _ []string) error {
		return nil
	}
	if err := toolRegistry.Register(toolkit.NewAgentCreationTool(agentCreator)); err != nil && l != nil {
		l.Warn("agentos temporal worker - register agent_creation_tool: %v", err)
	}

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

	if llmConfigPath != "" {
		if err := webapi.WatchLLMConfig(llmConfigPath, func(updated *webapi.LLMConfigFile) {
			llmProvider.ReloadConfig(updated)
			if l != nil {
				l.Info("agentos temporal worker - LLM config reloaded from %s", llmConfigPath)
			}
		}); err != nil {
			if l != nil {
				l.Warn("agentos temporal worker - LLM config watcher: %v", err)
			}
		} else if l != nil {
			l.Info("agentos temporal worker - LLM config watcher started for %s", llmConfigPath)
		}
	}

	return llmProvider, nil
}
