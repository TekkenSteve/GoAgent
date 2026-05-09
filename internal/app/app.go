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
	"github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	agentfwops "github.com/TekkenSteve/GoAgent/internal/agentfw/runtimeops"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/tool"
	amqp_rpc "github.com/TekkenSteve/GoAgent/internal/controller/amqp_rpc"
	"github.com/TekkenSteve/GoAgent/internal/controller/grpc"
	nats_rpc "github.com/TekkenSteve/GoAgent/internal/controller/nats_rpc"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi"
	pipelinepkg "github.com/TekkenSteve/GoAgent/internal/repo/pipeline"
	"github.com/TekkenSteve/GoAgent/internal/repo/compressor"
	"github.com/TekkenSteve/GoAgent/internal/repo/framework"
	"github.com/TekkenSteve/GoAgent/internal/repo/toolkit"
	"github.com/TekkenSteve/GoAgent/internal/repo/webapi"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agent"
	agentfwusecase "github.com/TekkenSteve/GoAgent/internal/usecase/executor"
	"github.com/TekkenSteve/GoAgent/internal/usecase/history"
	"github.com/TekkenSteve/GoAgent/pkg/grpcserver"
	"github.com/TekkenSteve/GoAgent/pkg/httpserver"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	natsRPCServer "github.com/TekkenSteve/GoAgent/pkg/nats/nats_rpc/server"
	"github.com/TekkenSteve/GoAgent/pkg/postgres"
	goredis "github.com/TekkenSteve/GoAgent/pkg/redis"
	rmqRPCServer "github.com/TekkenSteve/GoAgent/pkg/rabbitmq/rmq_rpc/server"
)

// Run creates objects via constructors.
func Run(cfg *config.Config) { //nolint: gocyclo,cyclop,funlen,gocritic,nolintlint
	l := logger.New(cfg.Log.Level)
	var temporalRuntime *agentfwruntime.TemporalRuntime
	var batchWriter *pipelinepkg.BatchWriter
	var agentUC *agent.UseCase

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
		if cfg.AgentFW.LLMAPIKey != "" {
			llmProvider := webapi.New(webapi.Config{
				BaseURL: cfg.AgentFW.LLMBaseURL,
				APIKey:  cfg.AgentFW.LLMAPIKey,
			})

			// Tool registry — register tools with env-sourced API keys
			toolRegistry := toolkit.NewRegistry()

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

			agentUC = agent.New(llmProvider, toolExecutor, wal, agentCompressor, toolRegistry)

			activities = orchestration.NewAgentActivities(agentUC, eventStore)
			l.Info("app - Run - agent components initialized")
		} else {
			l.Warn("app - Run - LLM API key not configured, agent execution will not be available")
		}

		registrar := agentfwruntime.NewDefaultRegistrar(activities)
		if err := agentfwruntime.StartWorker(runtime, registrar); err != nil {
			l.Fatal(fmt.Errorf("app - Run - agentfw.StartWorker: %w", err))
		}

		temporalRuntime = runtime
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
		eventStore, streamSubscriber, sseGateway)

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
