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
	"github.com/TekkenSteve/GoAgent/internal/agentfw/tool"
	amqp_rpc "github.com/TekkenSteve/GoAgent/internal/controller/amqp_rpc"
	"github.com/TekkenSteve/GoAgent/internal/controller/grpc"
	nats_rpc "github.com/TekkenSteve/GoAgent/internal/controller/nats_rpc"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi"
	pipelinepkg "github.com/TekkenSteve/GoAgent/internal/repo/pipeline"
	"github.com/TekkenSteve/GoAgent/internal/repo/compressor"
	"github.com/TekkenSteve/GoAgent/internal/repo/framework"
	"github.com/TekkenSteve/GoAgent/internal/repo/llm"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agent"
	agentfwusecase "github.com/TekkenSteve/GoAgent/internal/usecase/executor"
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

	ctx := context.Background()

	// Redis — for write-ahead log and warm-state persistence
	rdb, err := goredis.New(ctx, cfg.Redis.URL)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - redis.New: %w", err))
	}
	defer rdb.Close()

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
			llmProvider := llm.New(llm.Config{
				BaseURL: cfg.AgentFW.LLMBaseURL,
				APIKey:  cfg.AgentFW.LLMAPIKey,
			})

			toolPipe := tool.Pipeline{}
			toolExecutor := framework.NewToolPipeline(toolPipe)

			agentCompressor := compressor.New(compressor.Config{
				LLM: llmProvider,
			})

			// WAL + BatchWriter for async persistence (Redis Stream → Postgres)
			wal := pipelinepkg.NewWriteAheadLog(rdb.GeneralClient)
			dlq := pipelinepkg.NewDeadLetterQueue(rdb.GeneralClient)
			messageRepo := temporalrepo.NewMessageRepo(pg)

			batchWriter = pipelinepkg.NewBatchWriter(wal, dlq, messageRepo, l)
			batchWriter.Start()
			defer batchWriter.Stop()

			agentUC := agent.New(llmProvider, toolExecutor, wal, agentCompressor)

			activities = orchestration.NewAgentActivities(agentUC)
			l.Info("app - Run - agent components initialized (model: %s)", cfg.AgentFW.LLMModel)
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
	restapi.NewRouter(httpServer.App, cfg, agentExecutor, l)

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
