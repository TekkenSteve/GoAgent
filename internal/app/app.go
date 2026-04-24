// Package app configures and runs application.
package app

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/TekkenSteve/GoAgent/config"
	agentfwconfig "github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	agentfwruntime "github.com/TekkenSteve/GoAgent/internal/agentfw/runtime"
	agentfwops "github.com/TekkenSteve/GoAgent/internal/agentfw/runtimeops"
	amqprpc "github.com/TekkenSteve/GoAgent/internal/controller/amqp_rpc"
	"github.com/TekkenSteve/GoAgent/internal/controller/grpc"
	natsrpc "github.com/TekkenSteve/GoAgent/internal/controller/nats_rpc"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	"github.com/TekkenSteve/GoAgent/internal/repo/webapi"
	"github.com/TekkenSteve/GoAgent/internal/usecase/translation"
	"github.com/TekkenSteve/GoAgent/pkg/grpcserver"
	"github.com/TekkenSteve/GoAgent/pkg/httpserver"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	natsRPCServer "github.com/TekkenSteve/GoAgent/pkg/nats/nats_rpc/server"
	"github.com/TekkenSteve/GoAgent/pkg/postgres"
	rmqRPCServer "github.com/TekkenSteve/GoAgent/pkg/rabbitmq/rmq_rpc/server"
)

// Run creates objects via constructors.
func Run(cfg *config.Config) { //nolint: gocyclo,cyclop,funlen,gocritic,nolintlint
	l := logger.New(cfg.Log.Level)
	var temporalRuntime *agentfwruntime.TemporalRuntime

	if cfg.AgentFW.Enabled {
		fwCfg := agentfwconfig.FromAppConfig(cfg)
		selector := agentfwops.NewRolloutSelector(agentfwops.RolloutConfig{
			Mode:                agentfwops.RolloutMode(fwCfg.Rollout.Mode),
			Percent:             fwCfg.Rollout.Percent,
			AllowlistAccounts:   fwCfg.Rollout.AllowlistAccounts,
			RollbackForceLegacy: fwCfg.Rollout.RollbackForceLegacy,
			HashSalt:            fwCfg.Rollout.HashSalt,
		})
		controlDecision := selector.Decide("", "bootstrap")
		if !controlDecision.UseTemporal {
			l.Info("app - Run - agent framework temporal worker skipped: %s", controlDecision.Reason)
		} else {
			runtime, err := agentfwruntime.NewTemporalRuntime(fwCfg.Temporal)
			if err != nil {
				l.Fatal(fmt.Errorf("app - Run - agentfw.NewTemporalRuntime: %w", err))
			}
			defer runtime.Close()

			registrar := agentfwruntime.NewDefaultRegistrar()
			if err := agentfwruntime.StartWorker(runtime, registrar); err != nil {
				l.Fatal(fmt.Errorf("app - Run - agentfw.StartWorker: %w", err))
			}

			temporalRuntime = runtime
			l.Info("app - Run - agent framework worker started on task queue: %s", fwCfg.Temporal.TaskQueue)
		}
	}

	// Repository
	pg, err := postgres.New(cfg.PG.URL, postgres.MaxPoolSize(cfg.PG.PoolMax))
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - postgres.New: %w", err))
	}
	defer pg.Close()

	// Use-Case
	translationUseCase := translation.New(
		persistent.New(pg),
		webapi.New(),
	)

	// RabbitMQ RPC Server
	rmqRouter := amqprpc.NewRouter(translationUseCase, l)

	rmqServer, err := rmqRPCServer.New(cfg.RMQ.URL, cfg.RMQ.ServerExchange, rmqRouter, l)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - rmqServer - server.New: %w", err))
	}

	// NATS RPC Server
	natsRouter := natsrpc.NewRouter(translationUseCase, l)

	natsServer, err := natsRPCServer.New(cfg.NATS.URL, cfg.NATS.ServerExchange, natsRouter, l)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - natsServer - server.New: %w", err))
	}

	// gRPC Server
	grpcServer := grpcserver.New(l, grpcserver.Port(cfg.GRPC.Port))
	grpc.NewRouter(grpcServer.App, translationUseCase, l)

	// HTTP Server
	httpServer := httpserver.New(l, httpserver.Port(cfg.HTTP.Port), httpserver.Prefork(cfg.HTTP.UsePreforkMode))
	restapi.NewRouter(httpServer.App, cfg, translationUseCase, l)

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
