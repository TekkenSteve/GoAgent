// Package app configures and runs application.
package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/TekkenSteve/GoAgent/agentos"
	agentostemporal "github.com/TekkenSteve/GoAgent/agentos/temporal"
	"github.com/TekkenSteve/GoAgent/config"
	agentfwconfig "github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	agentfwruntime "github.com/TekkenSteve/GoAgent/internal/agentfw/runtime"
	agentfwops "github.com/TekkenSteve/GoAgent/internal/agentfw/runtimeops"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/tool"
	amqp_rpc "github.com/TekkenSteve/GoAgent/internal/controller/amqp_rpc"
	"github.com/TekkenSteve/GoAgent/internal/controller/grpc"
	nats_rpc "github.com/TekkenSteve/GoAgent/internal/controller/nats_rpc"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi"
	restapiv1 "github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo/cached"
	"github.com/TekkenSteve/GoAgent/internal/repo/compressor"
	"github.com/TekkenSteve/GoAgent/internal/repo/framework"
	mcpRepo "github.com/TekkenSteve/GoAgent/internal/repo/mcp"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	pipelinepkg "github.com/TekkenSteve/GoAgent/internal/repo/pipeline"
	repostream "github.com/TekkenSteve/GoAgent/internal/repo/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/toolkit"
	"github.com/TekkenSteve/GoAgent/internal/repo/webapi"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agent"
	billingpkg "github.com/TekkenSteve/GoAgent/internal/usecase/billing"
	agentfwusecase "github.com/TekkenSteve/GoAgent/internal/usecase/executor"
	"github.com/TekkenSteve/GoAgent/internal/usecase/history"
	templatepkg "github.com/TekkenSteve/GoAgent/internal/usecase/template"
	triggerpkg "github.com/TekkenSteve/GoAgent/internal/usecase/trigger"
	"github.com/TekkenSteve/GoAgent/pkg/grpcserver"
	"github.com/TekkenSteve/GoAgent/pkg/httpserver"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	natsRPCServer "github.com/TekkenSteve/GoAgent/pkg/nats/nats_rpc/server"
	"github.com/TekkenSteve/GoAgent/pkg/postgres"
	rmqRPCServer "github.com/TekkenSteve/GoAgent/pkg/rabbitmq/rmq_rpc/server"
	goredis "github.com/TekkenSteve/GoAgent/pkg/redis"
)

// Run creates objects via constructors.
func Run(cfg *config.Config) { //nolint: gocyclo,cyclop,funlen,gocritic,nolintlint,gocognit,nestif
	l := logger.New(cfg.Log.Level)

	var (
		temporalRuntime *agentfwruntime.TemporalRuntime
		agentOSRuntime  agentos.Runtime
		batchWriter     *pipelinepkg.BatchWriter
		agentUC         *agent.UseCase
		wsHub           *repostream.WebSocketHub
		cancelWorkflow  restapiv1.CancelWorkflowFn
		signalWorkflow  restapiv1.SignalWorkflowFn
		templateUC      *templatepkg.UseCase
		triggerUC       *triggerpkg.UseCase
	)

	fwCfg := agentfwconfig.FromAppConfig(cfg)
	selector := agentfwops.NewRolloutSelector(agentfwops.RolloutConfig{
		Mode:                agentfwops.RolloutMode(fwCfg.Rollout.Mode),
		Percent:             fwCfg.Rollout.Percent,
		AllowlistAccounts:   fwCfg.Rollout.AllowlistAccounts,
		RollbackForceLegacy: fwCfg.Rollout.RollbackForceLegacy,
		HashSalt:            fwCfg.Rollout.HashSalt,
	})
	controlDecision := selector.Decide("", "bootstrap")

	// Repository — created early for agent components below
	pg, err := postgres.New(cfg.PG.URL, postgres.MaxPoolSize(cfg.PG.PoolMax))
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - postgres.New: %w", err))
	}
	defer pg.Close()

	messageRepo := temporalrepo.NewMessageRepo(pg)
	persistentAgentRepo := temporalrepo.NewAgentRepo(pg)
	agentRepo := cached.NewAgentRepo(persistentAgentRepo)
	templateRepo := temporalrepo.NewWorkflowTemplateRepo(pg)
	triggerRepo := temporalrepo.NewTriggerRepo(pg)
	templateUC = templatepkg.New(templateRepo)

	ctx := context.Background()

	// Redis — for write-ahead log and warm-state persistence
	rdb, err := goredis.New(ctx, cfg.Redis.URL)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - redis.New: %w", err))
	}
	defer rdb.Close()

	// Event Store infrastructure for streaming
	eventSequencer := repostream.NewRedisSequencer(rdb)
	eventStore := repostream.NewRedisEventStore(rdb, eventSequencer)
	streamSubscriber := repostream.NewRedisSubscriber(rdb.Hub())
	sseGateway := repostream.NewSSEGateway()

	// WebSocket Hub for bidirectional streaming (Phase 4)
	wsHub = repostream.NewWebSocketHub()

	if !controlDecision.UseTemporal {
		l.Info("app - Run - agent framework temporal worker skipped: %s", controlDecision.Reason)
	} else {
		tc := initTemporalComponents(l, cfg, &fwCfg, pg, rdb, messageRepo, agentRepo, templateRepo, triggerRepo, templateUC, eventStore)
		if tc != nil {
			temporalRuntime = tc.runtime
			agentOSRuntime = tc.agentOSRuntime
			batchWriter = tc.batchWriter
			agentUC = tc.agentUC
			triggerUC = tc.triggerUC
			cancelWorkflow = tc.cancelWorkflow
			signalWorkflow = tc.signalWorkflow
		}
	}

	if temporalRuntime != nil {
		defer temporalRuntime.Close()
	}

	if batchWriter != nil {
		defer batchWriter.Stop()
	}

	// Use-Case
	var (
		agentExecutor usecase.AgentExecutor
		orchExecutor  usecase.OrchestrationExecutor
	)

	if temporalRuntime != nil {
		temporalRepo := temporalrepo.NewExecutorTemporal(temporalRuntime.Client, fwCfg.Temporal)
		orchExecutor = agentfwusecase.New(temporalRepo)
		if agentOSRuntime != nil {
			agentExecutor = newAgentOSExecutor(agentOSRuntime)
		} else {
			agentExecutor = agentfwusecase.New(temporalRepo)
		}
	} else {
		l.Warn("app - Run - agent executor is nil, agent endpoints will be unavailable")
	}

	historyUC := history.New(messageRepo)

	var streamExecutor usecase.StreamExecutor

	switch {
	case temporalRuntime != nil:
		streamExecutor = temporalrepo.NewTemporalStreamExecutor(temporalRuntime.Client, fwCfg.Temporal.TaskQueue)

		l.Info("app - Run - stream executor: temporal")
	case agentUC != nil:
		streamExecutor = agentUC

		l.Info("app - Run - stream executor: in-process")
	default:
		l.Warn("app - Run - stream executor unavailable (agent usecase not initialized)")
	}

	// RabbitMQ RPC Server
	rmqRouter := amqp_rpc.NewRouter(agentExecutor, l)

	rmqServer, err := rmqRPCServer.New(cfg.RMQ.URL, cfg.RMQ.ServerExchange, rmqRouter, l)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - rmqServer - server.New: %w", err))
	}

	// NATS RPC Server
	natsRouter := nats_rpc.NewRouter(agentExecutor, l)

	natsServer, err := natsRPCServer.New(cfg.NATS.URL, cfg.NATS.ServerExchange, natsRouter, l)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - natsServer - server.New: %w", err))
	}

	// gRPC Server
	grpcServer := grpcserver.New(l, grpcserver.Port(cfg.GRPC.Port))
	grpc.NewRouter(grpcServer.App, agentExecutor, l)

	// HTTP Server
	httpServer := httpserver.New(l, httpserver.Port(cfg.HTTP.Port), httpserver.Prefork(cfg.HTTP.UsePreforkMode))
	restapi.NewRouter(httpServer.App, cfg, agentExecutor, orchExecutor, historyUC, streamExecutor, l, rdb,
		eventStore, streamSubscriber, sseGateway, wsHub, cancelWorkflow, signalWorkflow, templateUC, triggerUC)

	// Start servers
	rmqServer.Start()
	natsServer.Start()
	grpcServer.Start()
	httpServer.Start()

	// Waiting signal
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	select {
	case s := <-interrupt:
		l.Info("app - Run - signal: %s", s.String())
	case err = <-httpServer.Notify():
		l.Error(fmt.Errorf("app - Run - httpServer.Notify: %w", err))
	case err = <-grpcServer.Notify():
		l.Error(fmt.Errorf("app - Run - grpcServer.Notify: %w", err))
	case err = <-rmqServer.Notify():
		l.Error(fmt.Errorf("app - Run - rmqServer.Notify: %w", err))
	case err = <-natsServer.Notify():
		l.Error(fmt.Errorf("app - Run - natsServer.Notify: %w", err))
	}

	// Shutdown
	err = httpServer.Shutdown()
	if err != nil {
		l.Error(fmt.Errorf("app - Run - httpServer.Shutdown: %w", err))
	}

	err = grpcServer.Shutdown()
	if err != nil {
		l.Error(fmt.Errorf("app - Run - grpcServer.Shutdown: %w", err))
	}

	err = rmqServer.Shutdown()
	if err != nil {
		l.Error(fmt.Errorf("app - Run - rmqServer.Shutdown: %w", err))
	}

	err = natsServer.Shutdown()
	if err != nil {
		l.Error(fmt.Errorf("app - Run - natsServer.Shutdown: %w", err))
	}

	if temporalRuntime != nil {
		agentfwruntime.StopWorker(temporalRuntime)
	}
}

// temporalComponents holds the initialized components returned by initTemporalComponents.
type temporalComponents struct {
	runtime        *agentfwruntime.TemporalRuntime
	agentOSRuntime agentos.Runtime
	batchWriter    *pipelinepkg.BatchWriter
	agentUC        *agent.UseCase
	triggerUC      *triggerpkg.UseCase
	toolRegistry   *toolkit.ToolRegistry
	cancelWorkflow restapiv1.CancelWorkflowFn
	signalWorkflow restapiv1.SignalWorkflowFn
	llmProvider    *webapi.BifrostProvider
}

func initTemporalComponents(
	l *logger.Logger,
	cfg *config.Config,
	fwCfg *agentfwconfig.Config,
	pg *postgres.Postgres,
	rdb *goredis.Redis,
	messageRepo *temporalrepo.MessageRepo,
	agentRepo *cached.AgentRepo,
	templateRepo *temporalrepo.WorkflowTemplateRepo,
	triggerRepo *temporalrepo.TriggerRepo,
	templateUC *templatepkg.UseCase,
	eventStore stream.EventStore,
) *temporalComponents {
	runtime, err := agentfwruntime.NewTemporalRuntime(fwCfg.Temporal)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentfw.NewTemporalRuntime: %w", err))
	}

	llmResult, err := webapi.LoadLLMProviders(cfg.AgentFW.LLMConfigPath)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - LoadLLMProviders: %w", err))
	}

	comp := initAgentComponents(l, cfg, fwCfg, pg, rdb, runtime, llmResult, messageRepo, agentRepo, templateRepo, triggerRepo, eventStore)

	registrar := agentfwruntime.NewDefaultRegistrar(comp.activities)
	if err := agentfwruntime.StartWorker(runtime, registrar); err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentfw.StartWorker: %w", err))
	}

	registerToolsOnRegistry(l, comp.toolRegistry, comp.triggerUC, comp.triggerScheduler)

	if err := templateUC.EnsureDefault(context.Background()); err != nil {
		l.Warn("app - Run - ensure default template: %v", err)
	}

	signalWorkflow := func(ctx context.Context, workflowID, signalName string, arg any) error {
		return runtime.Client.SignalWorkflow(ctx, workflowID, "", signalName, arg)
	}

	cancelWorkflow := func(ctx context.Context, workflowID string) error {
		return runtime.Client.CancelWorkflow(ctx, workflowID, "")
	}

	l.Info("app - Run - agent framework worker started on task queue: %s", fwCfg.Temporal.TaskQueue)

	agentOSRuntime, err := agentostemporal.NewRuntimeWithClient(context.Background(), agentostemporal.RuntimeConfig{
		TemporalAddress:   fwCfg.Temporal.Address,
		TemporalNamespace: fwCfg.Temporal.Namespace,
		TemporalTaskQueue: fwCfg.Temporal.TaskQueue,
	}, runtime.Client)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos temporal runtime: %w", err))
	}

	return &temporalComponents{
		runtime:        runtime,
		agentOSRuntime: agentOSRuntime,
		batchWriter:    comp.batchWriter,
		agentUC:        comp.agentUC,
		triggerUC:      comp.triggerUC,
		toolRegistry:   comp.toolRegistry,
		cancelWorkflow: cancelWorkflow,
		signalWorkflow: signalWorkflow,
		llmProvider:    comp.llmProvider,
	}
}

// initAgentComponentsResult holds the results of initAgentComponents.
type initAgentComponentsResult struct {
	llmProvider      *webapi.BifrostProvider
	toolRegistry     *toolkit.ToolRegistry
	batchWriter      *pipelinepkg.BatchWriter
	agentUC          *agent.UseCase
	triggerUC        *triggerpkg.UseCase
	triggerScheduler *temporalrepo.TemporalTriggerScheduler
	activities       *orchestration.AgentActivities
}

func initAgentComponents(
	l *logger.Logger,
	cfg *config.Config,
	fwCfg *agentfwconfig.Config,
	pg *postgres.Postgres,
	rdb *goredis.Redis,
	runtime *agentfwruntime.TemporalRuntime,
	llmResult *webapi.LLMProvidersResult,
	messageRepo *temporalrepo.MessageRepo,
	agentRepo *cached.AgentRepo,
	templateRepo *temporalrepo.WorkflowTemplateRepo,
	triggerRepo *temporalrepo.TriggerRepo,
	eventStore stream.EventStore,
) *initAgentComponentsResult {
	llmProvider, err := initBifrostProvider(cfg, llmResult, l)
	if err != nil {
		l.Fatal(err.Error())
	}

	billingRepo := temporalrepo.NewBillingRepo(pg)
	billingUC := billingpkg.New(billingRepo, nil, billingRepo)

	toolRegistry := toolkit.NewRegistry()

	registerEnvTools(l, toolRegistry)

	toolPipe := tool.Pipeline{
		Executor: toolRegistry,
	}
	toolExecutor := framework.NewToolPipeline(&toolPipe)

	agentCompressor := compressor.New(compressor.Config{
		LLM: llmProvider,
	})

	wal := pipelinepkg.NewWriteAheadLog(rdb.GeneralClient)
	dlq := pipelinepkg.NewDeadLetterQueue(rdb.GeneralClient)

	batchWriter := pipelinepkg.NewBatchWriter(wal, dlq, messageRepo, l)

	batchWriter.Start()

	agentUC := agent.New(llmProvider, toolExecutor, wal, agentCompressor, toolRegistry, agentRepo)
	agentUC.SetLogger(l)

	triggerScheduler := temporalrepo.NewTemporalTriggerScheduler(runtime.Client, fwCfg.Temporal.TaskQueue)
	triggerUC := triggerpkg.New(triggerRepo, triggerScheduler, templateRepo)

	mcpManager := mcpRepo.NewManager()
	if toolRegistry != nil {
		mcpManager.SetRegistry(toolRegistry)
		l.Info("app - Run - mcp manager created for per-agent JIT tool registration")
	}

	activities := orchestration.NewAgentActivities(agentUC, eventStore, l).
		WithTemplateRepo(templateRepo).
		WithTriggerUC(triggerUC).
		WithMCPManager(mcpManager).
		WithBilling(billingUC)
	l.Info("app - Run - agent components initialized")

	return &initAgentComponentsResult{llmProvider, toolRegistry, batchWriter, agentUC, triggerUC, triggerScheduler, activities}
}

func registerToolsOnRegistry(l *logger.Logger, toolRegistry *toolkit.ToolRegistry, triggerUC *triggerpkg.UseCase, triggerScheduler *temporalrepo.TemporalTriggerScheduler) {
	agentCreator := func(_ context.Context, _, _, _, _ string, _ []string) error {
		return nil
	}
	if err := toolRegistry.Register(toolkit.NewAgentCreationTool(agentCreator)); err != nil {
		l.Warn("app - Run - register agent_creation_tool: %v", err)
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
	if err := toolRegistry.Register(toolkit.NewTriggerTool(triggerCreator, triggerScheduleFn)); err != nil {
		l.Warn("app - Run - register create_trigger: %v", err)
	}

	triggerLister := func(ctx context.Context, templateID string) ([]entity.TriggerSpec, error) {
		return triggerUC.ListByTemplate(ctx, templateID)
	}
	if err := toolRegistry.Register(toolkit.NewListTriggersTool(triggerLister)); err != nil {
		l.Warn("app - Run - register list_triggers: %v", err)
	}

	triggerToggler := func(ctx context.Context, triggerID string, isActive bool) (entity.TriggerSpec, error) {
		return triggerUC.Toggle(ctx, triggerID, isActive)
	}
	if err := toolRegistry.Register(toolkit.NewToggleTriggerTool(triggerToggler)); err != nil {
		l.Warn("app - Run - register toggle_trigger: %v", err)
	}

	triggerDeleter := func(ctx context.Context, triggerID string) error {
		return triggerUC.Delete(ctx, triggerID)
	}
	if err := toolRegistry.Register(toolkit.NewDeleteTriggerTool(triggerDeleter)); err != nil {
		l.Warn("app - Run - register delete_trigger: %v", err)
	}
}

func registerEnvTools(l *logger.Logger, toolRegistry *toolkit.ToolRegistry) {
	if apiKey := os.Getenv("TAVILY_API_KEY"); apiKey != "" {
		if err := toolRegistry.Register(toolkit.NewWebSearch(toolkit.WebSearchConfig{
			Provider: "tavily",
			APIKey:   apiKey,
		})); err != nil {
			l.Warn("app - initAgentComponents - register web_search: %v", err)
		}
	}

	if apiKey := os.Getenv("FIRECRAWL_API_KEY"); apiKey != "" {
		if err := toolRegistry.Register(toolkit.NewScrapeWebpage(toolkit.ScrapeWebpageConfig{
			APIKey: apiKey,
		})); err != nil {
			l.Warn("app - initAgentComponents - register scrape_webpage: %v", err)
		}
	}

	if endpoint := os.Getenv("CODE_INTERPRETER_ENDPOINT"); endpoint != "" {
		if err := toolRegistry.Register(toolkit.NewCodeInterpreter(toolkit.CodeInterpreterConfig{
			Endpoint: endpoint,
			APIKey:   os.Getenv("CODE_INTERPRETER_API_KEY"),
		})); err != nil {
			l.Warn("app - initAgentComponents - register code_interpreter: %v", err)
		}
	}
}

// initBifrostProvider creates the LLM provider and starts config file watching.
func initBifrostProvider(cfg *config.Config, llmResult *webapi.LLMProvidersResult, l *logger.Logger) (*webapi.BifrostProvider, error) {
	llmProvider, err := webapi.NewBifrost(&webapi.BifrostConfig{
		Providers:       llmResult.Providers,
		Scenarios:       llmResult.Scenarios,
		DefaultScenario: llmResult.DefaultScenario,
	})
	if err != nil {
		return nil, fmt.Errorf("app - Run - webapi.NewBifrost: %w", err)
	}

	if llmConfigPath := cfg.AgentFW.LLMConfigPath; llmConfigPath != "" {
		if err := webapi.WatchLLMConfig(llmConfigPath, func(updated *webapi.LLMConfigFile) {
			llmProvider.ReloadConfig(updated)
			l.Info("app - Run - LLM config reloaded from %s", llmConfigPath)
		}); err != nil {
			l.Warn("app - Run - LLM config watcher: %v", err)
		} else {
			l.Info("app - Run - LLM config watcher started for %s", llmConfigPath)
		}
	}

	return llmProvider, nil
}
