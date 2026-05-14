// Package app configures and runs application.
package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
	mcpRepo "github.com/TekkenSteve/GoAgent/internal/repo/mcp"
	"github.com/TekkenSteve/GoAgent/internal/repo/framework"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	pipelinepkg "github.com/TekkenSteve/GoAgent/internal/repo/pipeline"
	"github.com/TekkenSteve/GoAgent/internal/repo/toolkit"
	"github.com/TekkenSteve/GoAgent/internal/repo/webapi"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agent"
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
func Run(cfg *config.Config) { //nolint: gocyclo,cyclop,funlen,gocritic,nolintlint
	l := logger.New(cfg.Log.Level)
	var temporalRuntime *agentfwruntime.TemporalRuntime
	var batchWriter *pipelinepkg.BatchWriter
	var agentUC *agent.UseCase
	var wsHub *stream.WebSocketHub
	var cancelWorkflow restapiv1.CancelWorkflowFn
	var signalWorkflow restapiv1.SignalWorkflowFn
	var toolRegistry *toolkit.ToolRegistry
	var templateUC *templatepkg.UseCase
	var triggerUC *triggerpkg.UseCase

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
	eventSequencer := stream.NewRedisSequencer(rdb)
	eventStore := stream.NewRedisEventStore(rdb, eventSequencer)
	streamSubscriber := stream.NewRedisSubscriber(rdb.Hub())
	sseGateway := stream.NewSSEGateway()

	// WebSocket Hub for bidirectional streaming (Phase 4)
	wsHub = stream.NewWebSocketHub()

	if !controlDecision.UseTemporal {
		l.Info("app - Run - agent framework temporal worker skipped: %s", controlDecision.Reason)
	} else {
		runtime, err := agentfwruntime.NewTemporalRuntime(fwCfg.Temporal)
		if err != nil {
			l.Fatal(fmt.Errorf("app - Run - agentfw.NewTemporalRuntime: %w", err))
		}
		defer runtime.Close()

		// Build agent components when LLM is configured
		var activities *orchestration.AgentActivities
		var triggerScheduler *temporalrepo.TemporalTriggerScheduler
		if cfg.AgentFW.LLMAPIKey != "" {
			llmProvider := webapi.New(webapi.Config{
				BaseURL: cfg.AgentFW.LLMBaseURL,
				APIKey:  cfg.AgentFW.LLMAPIKey,
			})

			// Tool registry — register tools with env-sourced API keys
			toolRegistry = toolkit.NewRegistry()

			if apiKey := os.Getenv("TAVILY_API_KEY"); apiKey != "" {
				if err := toolRegistry.Register(toolkit.NewWebSearch(toolkit.WebSearchConfig{
					Provider: "tavily",
					APIKey:   apiKey,
				})); err != nil {
					l.Warn("app - Run - register web_search: %v", err)
				}
			}
			if apiKey := os.Getenv("FIRECRAWL_API_KEY"); apiKey != "" {
				if err := toolRegistry.Register(toolkit.NewScrapeWebpage(toolkit.ScrapeWebpageConfig{
					APIKey: apiKey,
				})); err != nil {
					l.Warn("app - Run - register scrape_webpage: %v", err)
				}
			}
			if endpoint := os.Getenv("CODE_INTERPRETER_ENDPOINT"); endpoint != "" {
				if err := toolRegistry.Register(toolkit.NewCodeInterpreter(toolkit.CodeInterpreterConfig{
					Endpoint: endpoint,
					APIKey:   os.Getenv("CODE_INTERPRETER_API_KEY"),
				})); err != nil {
					l.Warn("app - Run - register code_interpreter: %v", err)
				}
			}

			toolPipe := tool.Pipeline{
				Executor: toolRegistry,
			}
			toolExecutor := framework.NewToolPipeline(toolPipe)

			agentCompressor := compressor.New(compressor.Config{
				LLM: llmProvider,
			})

			// WAL + BatchWriter for async persistence (Redis Stream → Postgres)
			wal := pipelinepkg.NewWriteAheadLog(rdb.GeneralClient)
			dlq := pipelinepkg.NewDeadLetterQueue(rdb.GeneralClient)

			batchWriter = pipelinepkg.NewBatchWriter(wal, dlq, messageRepo, l)
			batchWriter.Start()
			defer batchWriter.Stop()

			agentUC = agent.New(llmProvider, toolExecutor, wal, agentCompressor, toolRegistry, agentRepo)

			// Trigger infrastructure — needed by both activities and tool registration
			triggerScheduler = temporalrepo.NewTemporalTriggerScheduler(runtime.Client, fwCfg.Temporal.TaskQueue)
			triggerUC = triggerpkg.New(triggerRepo, triggerScheduler, templateRepo)

			// MCP manager — empty, populated on demand during Prep
			// per-agent MCP server configs flow through ExecuteRequest.MCPServerConfigs
			// and are JIT-connected by EnsureConnected in PrepareActivity/InitStreamActivity.
			mcpManager := mcpRepo.NewManager()
			if toolRegistry != nil {
				mcpManager.SetRegistry(toolRegistry)
				l.Info("app - Run - mcp manager created for per-agent JIT tool registration")
			}

			activities = orchestration.NewAgentActivities(agentUC, eventStore, l).
				WithTemplateRepo(templateRepo).
				WithTriggerUC(triggerUC).
				WithMCPManager(mcpManager)
			l.Info("app - Run - agent components initialized")
		} else {
			l.Warn("app - Run - LLM API key not configured, agent execution will not be available")
		}

		registrar := agentfwruntime.NewDefaultRegistrar(activities)
		if err := agentfwruntime.StartWorker(runtime, registrar); err != nil {
			l.Fatal(fmt.Errorf("app - Run - agentfw.StartWorker: %w", err))
		}

		temporalRuntime = runtime


		// Register AgentCreationTool and TriggerTool
		if toolRegistry != nil {
			agentCreator := func(ctx context.Context, agentID, name, systemPrompt, modelRef string, tools []string) error {
				return nil
			}
			if err := toolRegistry.Register(toolkit.NewAgentCreationTool(agentCreator)); err != nil {
				l.Warn("app - Run - register agent_creation_tool: %v", err)
			}

			triggerCreator := func(ctx context.Context, templateID, name, cronExpression, agentPrompt string, templateVars []string, templateVarsVals map[string]string) (string, error) {
				t, err := triggerUC.Create(ctx, entity.CreateTriggerRequest{
					TemplateID:     templateID,
					Name:           name,
					TriggerType:    entity.TriggerSchedule,
					CronExpression: cronExpression,
					AgentPrompt:    agentPrompt,
					TemplateVars:   templateVars,
					TemplateVarsVals: templateVarsVals,
				})
				if err != nil {
					return "", err
				}
				return t.ID, nil
			}
			triggerScheduleFn := func(ctx context.Context, triggerID, cronExpression string) error {
				trigger, err := triggerUC.Get(ctx, triggerID)
				if err != nil {
					return err
				}
				return triggerScheduler.Schedule(ctx, trigger)
			}
			if err := toolRegistry.Register(toolkit.NewTriggerTool(triggerCreator, triggerScheduleFn)); err != nil {
				l.Warn("app - Run - register create_trigger: %v", err)
			}

			// list_triggers — list triggers by template
			triggerLister := func(ctx context.Context, templateID string) ([]entity.TriggerSpec, error) {
				return triggerUC.ListByTemplate(ctx, templateID)
			}
			if err := toolRegistry.Register(toolkit.NewListTriggersTool(triggerLister)); err != nil {
				l.Warn("app - Run - register list_triggers: %v", err)
			}

			// toggle_trigger — enable/disable a trigger
			triggerToggler := func(ctx context.Context, triggerID string, isActive bool) (entity.TriggerSpec, error) {
				return triggerUC.Toggle(ctx, triggerID, isActive)
			}
			if err := toolRegistry.Register(toolkit.NewToggleTriggerTool(triggerToggler)); err != nil {
				l.Warn("app - Run - register toggle_trigger: %v", err)
			}

			// delete_trigger — remove a trigger
			triggerDeleter := func(ctx context.Context, triggerID string) error {
				return triggerUC.Delete(ctx, triggerID)
			}
			if err := toolRegistry.Register(toolkit.NewDeleteTriggerTool(triggerDeleter)); err != nil {
				l.Warn("app - Run - register delete_trigger: %v", err)
			}
		}

		// Ensure default workflow template exists (code-defined default)
		if err := templateUC.EnsureDefault(ctx); err != nil {
			l.Warn("app - Run - ensure default template: %v", err)
		}

		// SignalWorkflow function for WS handler (Temporal mode) — pause/resume.
		signalWorkflow = func(ctx context.Context, workflowID, signalName string, arg interface{}) error {
			return temporalRuntime.Client.SignalWorkflow(ctx, workflowID, "", signalName, arg)
		}

		// CancelWorkflow function for WS handler (Temporal mode) — cancel.
		cancelWorkflow = func(ctx context.Context, workflowID string) error {
			return temporalRuntime.Client.CancelWorkflow(ctx, workflowID, "")
		}

		l.Info("app - Run - agent framework worker started on task queue: %s", fwCfg.Temporal.TaskQueue)
	}

	// Use-Case
	var agentExecutor usecase.AgentExecutor
	if temporalRuntime != nil {
		temporalRepo := temporalrepo.NewExecutorTemporal(temporalRuntime.Client, fwCfg.Temporal)
		agentExecutor = agentfwusecase.New(temporalRepo)
	} else {
		l.Warn("app - Run - agent executor is nil, agent endpoints will be unavailable")
	}

	historyUC := history.New(messageRepo)

	var streamExecutor usecase.StreamExecutor
	if temporalRuntime != nil {
		streamExecutor = temporalrepo.NewTemporalStreamExecutor(temporalRuntime.Client, fwCfg.Temporal.TaskQueue)
		l.Info("app - Run - stream executor: temporal")
	} else if agentUC != nil {
		streamExecutor = agentUC
		l.Info("app - Run - stream executor: in-process")
	} else {
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
	restapi.NewRouter(httpServer.App, cfg, agentExecutor, historyUC, streamExecutor, l, rdb,
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
