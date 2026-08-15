// Package app configures and runs application.
package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosplatform "github.com/TekkenSteve/GoAgent/agentos/platform"
	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
	agentostemporal "github.com/TekkenSteve/GoAgent/agentos/temporal"
	"github.com/TekkenSteve/GoAgent/config"
	agentfwconfig "github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/eventing"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	agentfwruntime "github.com/TekkenSteve/GoAgent/internal/agentfw/runtime"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/tool"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi"
	restapiv1 "github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1"
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
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agent"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosaction"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosbatch"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosledger"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosprojection"
	agentosruntime "github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
	billingpkg "github.com/TekkenSteve/GoAgent/internal/usecase/billing"
	agentfwusecase "github.com/TekkenSteve/GoAgent/internal/usecase/executor"
	templatepkg "github.com/TekkenSteve/GoAgent/internal/usecase/template"
	"github.com/TekkenSteve/GoAgent/pkg/httpserver"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

var (
	errAppRunAgentOSRuntimeRequired          = errors.New("app - Run - agentos runtime is required for agent execution")
	errAppRunPlanCmdRecoveryNoImplementation = errors.New("app - Run - agentos plan command recovery: plan runtime does not implement recovery")
	errAppRunPlanWorkerQueueUnconfigured     = errors.New("app - Run - agentos plan worker task queue is not configured")
	errAppProcessWorkerQueueUnconfigured     = errors.New("app - Run - agentos process worker task queue is not configured")
)

type appInfrastructure struct {
	pg              *postgres.Postgres
	rdb             *goredis.Redis
	eventIngest     *eventing.Service
	dataPlane       *agentostemporal.StreamingDataPlane
	messageRepo     *temporalrepo.MessageRepo
	agentRepo       *cached.AgentRepo
	templateRepo    *temporalrepo.WorkflowTemplateRepo
	runBackendIndex *temporalrepo.RunBackendIndexRepo
	templateUC      *templatepkg.UseCase
	fwCfg           agentfwconfig.Config
}

func initInfrastructure(cfg *config.Config, l *logger.Logger) *appInfrastructure {
	fwCfg := agentfwconfig.FromAppConfig(cfg)

	pg, err := postgres.New(cfg.PG.URL, postgres.MaxPoolSize(cfg.PG.PoolMax))
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - postgres.New: %w", err))
	}

	messageRepo := temporalrepo.NewMessageRepo(pg)
	persistentAgentRepo := temporalrepo.NewAgentRepo(pg)
	agentRepo := cached.NewAgentRepo(persistentAgentRepo)
	templateRepo := temporalrepo.NewWorkflowTemplateRepo(pg)
	runBackendIndex := temporalrepo.NewRunBackendIndexRepo(pg)
	templateUC := templatepkg.New(templateRepo)
	ctx := context.Background()

	rdb, err := goredis.New(ctx, cfg.Redis.URL)
	if err != nil {
		pg.Close()
		l.Fatal(fmt.Errorf("app - Run - redis.New: %w", err))
	}

	eventDedupeStore := repostream.NewEventDedupeStore(rdb)

	// The data plane is the single live transport: Centrifugo by default,
	// degrading to the in-process memstream bus when no base URL is configured
	// (a warning is logged). It feeds the run activities' publisher, the
	// run/plan subscriptions, and the durable projection.
	dataPlane, err := agentostemporal.NewStreamingDataPlane(context.Background(), agentostemporal.StreamCentrifugoConfig{
		BaseURL: cfg.StreamCentrifugo.BaseURL,
		APIKey:  cfg.StreamCentrifugo.APIKey,
	}, pg, l)
	if err != nil {
		if closeErr := rdb.Close(); closeErr != nil {
			l.Error("app - Run - close redis after data plane failure", closeErr)
		}

		pg.Close()
		l.Fatal(fmt.Errorf("app - Run - agentos data plane: %w", err))
	}

	eventIngest, err := eventing.NewService(dataPlane.Publisher, eventDedupeStore)
	if err != nil {
		dataPlane.Close()

		if closeErr := rdb.Close(); closeErr != nil {
			l.Error("app - Run - close redis after eventing.NewService failure", closeErr)
		}

		pg.Close()
		l.Fatal(fmt.Errorf("app - Run - eventing.NewService: %w", err))
	}

	return &appInfrastructure{
		pg: pg, rdb: rdb, eventIngest: eventIngest, dataPlane: dataPlane,
		messageRepo: messageRepo, agentRepo: agentRepo, templateRepo: templateRepo,
		runBackendIndex: runBackendIndex, templateUC: templateUC,
		fwCfg: fwCfg,
	}
}

// Run creates objects via constructors.
func Run(cfg *config.Config) {
	l := logger.New(cfg.Log.Level)

	var (
		temporalRuntime *agentfwruntime.TemporalRuntime
		agentOSRuntime  agentos.Runtime
		planRuntime     agentos.PlanRuntime
		platformRuntime agentosplatform.Runtime
		agentUC         *agent.UseCase
		cancelWorkflow  restapiv1.CancelWorkflowFn
		signalWorkflow  restapiv1.SignalWorkflowFn
	)

	infra := initInfrastructure(cfg, l)

	defer func() {
		// Stop the projection consumer before the Postgres pool it writes to.
		infra.dataPlane.Close()
		infra.pg.Close()

		if err := infra.rdb.Close(); err != nil {
			l.Error("app - Run - close redis", err)
		}
	}()

	tc := initTemporalComponents(l, cfg, &infra.fwCfg, infra.pg, infra.rdb, infra.messageRepo, infra.agentRepo, infra.runBackendIndex, infra.templateUC, infra.dataPlane)
	if tc != nil {
		defer tc.Stop(l)

		temporalRuntime = tc.runtime
		agentOSRuntime = tc.agentOSRuntime
		planRuntime = tc.planRuntime
		platformRuntime = tc.platformRuntime
		agentUC = tc.agentUC
		cancelWorkflow = tc.cancelWorkflow
		signalWorkflow = tc.signalWorkflow
	}

	// Use-Case
	agentExecutor, orchExecutor, err := initAgentExecutor(temporalRuntime, agentOSRuntime, &infra.fwCfg)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - init agent executor: %w", err))
	}

	if agentExecutor == nil {
		l.Fatal(errAppRunAgentOSRuntimeRequired)
	}

	switch {
	case temporalRuntime != nil:
		l.Info("app - Run - runtime: temporal")
	case agentUC != nil:
		l.Info("app - Run - runtime: in-process")
	default:
		l.Warn("app - Run - stream executor unavailable (agent usecase not initialized)")
	}

	runEventReader := temporalrepo.NewAgentOSRunEventRepo(infra.pg)
	runHTTPServer(cfg, l, agentExecutor, orchExecutor, cancelWorkflow, signalWorkflow, infra.templateUC, infra.eventIngest, agentOSRuntime, planRuntime, platformRuntime, temporalRuntime, runEventReader)
}

func initAgentExecutor(temporalRuntime *agentfwruntime.TemporalRuntime, agentOSRuntime agentos.Runtime, fwCfg *agentfwconfig.Config) (usecase.AgentExecutor, usecase.OrchestrationExecutor, error) {
	var (
		agentExecutor usecase.AgentExecutor
		orchExecutor  usecase.OrchestrationExecutor
	)

	if temporalRuntime != nil {
		temporalRepo, err := temporalrepo.NewExecutorTemporal(temporalRuntime.Client, &fwCfg.Temporal)
		if err != nil {
			return nil, nil, err
		}

		orchExecutor = agentfwusecase.New(temporalRepo)

		if agentOSRuntime != nil {
			agentExecutor = newAgentOSExecutor(agentOSRuntime)
		}
	}

	return agentExecutor, orchExecutor, nil
}

func runHTTPServer(cfg *config.Config, l *logger.Logger, agentExecutor usecase.AgentExecutor, orchExecutor usecase.OrchestrationExecutor, cancelWorkflow restapiv1.CancelWorkflowFn, signalWorkflow restapiv1.SignalWorkflowFn, templateUC *templatepkg.UseCase, eventIngest *eventing.Service, agentOSRuntime agentos.Runtime, planRuntime agentos.PlanRuntime, platformRuntime agentosplatform.Runtime, temporalRuntime *agentfwruntime.TemporalRuntime, runEventReader restapiv1.RunEventReader) {
	httpServer := httpserver.New(l, httpserver.Port(cfg.HTTP.Port), httpserver.Prefork(cfg.HTTP.UsePreforkMode))
	restapi.NewRouter(httpServer.App, cfg, agentExecutor, orchExecutor, l,
		cancelWorkflow, signalWorkflow, templateUC, eventIngest, agentOSRuntime, planRuntime, platformRuntime, runEventReader)
	httpServer.Start()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	select {
	case s := <-interrupt:
		l.Info("app - Run - signal: %s", s.String())
	case err := <-httpServer.Notify():
		l.Error(fmt.Errorf("app - Run - httpServer.Notify: %w", err))
	}

	if err := httpServer.Shutdown(); err != nil {
		l.Error(fmt.Errorf("app - Run - httpServer.Shutdown: %w", err))
	}

	if temporalRuntime != nil {
		agentfwruntime.StopWorker(temporalRuntime)
	}
}

// temporalComponents holds the initialized components returned by initTemporalComponents.
type temporalComponents struct {
	runtime             *agentfwruntime.TemporalRuntime
	agentOSRuntime      agentos.Runtime
	planRuntime         agentos.PlanRuntime
	platformRuntime     agentosplatform.Runtime
	closePlanRuntime    func() error
	closeProcessRuntime func() error
	planRecovery        *agentostemporal.PlanCommandRecoveryLoop
	planMetrics         *agentosplan.PlanMetricsExporterLoop
	batchWriter         *pipelinepkg.BatchWriter
	agentUC             *agent.UseCase
	toolRegistry        *toolkit.ToolRegistry
	cancelWorkflow      restapiv1.CancelWorkflowFn
	signalWorkflow      restapiv1.SignalWorkflowFn
	llmProvider         *webapi.BifrostProvider
}

// Stop releases background loops and Temporal-owned runtime resources created
// by initTemporalComponents. Stop order mirrors the previous stacked defers:
// stop producers first, then close plan/runtime clients.
func (c *temporalComponents) Stop(l logger.Interface) {
	if c == nil {
		return
	}

	if c.batchWriter != nil {
		c.batchWriter.Stop()
	}

	if c.planMetrics != nil {
		c.planMetrics.Stop()
	}

	if c.planRecovery != nil {
		c.planRecovery.Stop()
	}

	if c.closePlanRuntime != nil {
		if err := c.closePlanRuntime(); err != nil {
			l.Error(fmt.Errorf("app - Run - close agentos plan runtime: %w", err))
		}
	}

	if c.closeProcessRuntime != nil {
		if err := c.closeProcessRuntime(); err != nil {
			l.Error(fmt.Errorf("app - Run - close agentos process runtime: %w", err))
		}
	}

	if c.runtime != nil {
		c.runtime.Close()
	}
}

func initPlanInfrastructure(l *logger.Logger, cfg *config.Config, pg *postgres.Postgres, dataPlane *agentostemporal.StreamingDataPlane) (*temporalrepo.AgentOSPlanRepo, *planstream.PlanEventStream, *temporalrepo.AgentOSArtifactRepo, *temporalrepo.AgentOSCapabilityCatalogRepo, *temporalrepo.AgentOSArtifactSchemaCatalogRepo) {
	planStore := temporalrepo.NewAgentOSPlanRepo(pg)
	planEventStream := planstream.New(dataPlane.Publisher, dataPlane.Subscriber)
	artifactStoreCfg := cfg.AgentOS.ArtifactStoreConfig()
	blobCfg := appArtifactBlobConfig(&artifactStoreCfg)

	blobStore, err := artifactrepo.NewBlobStore(context.Background(), &blobCfg)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - artifact store: %w", err))
	}

	artifactStore := temporalrepo.NewAgentOSArtifactRepo(pg, blobStore)
	capabilityCatalog := temporalrepo.NewAgentOSCapabilityCatalogRepo(pg)
	artifactSchemaCatalog := temporalrepo.NewAgentOSArtifactSchemaCatalogRepo(pg)

	return planStore, planEventStream, artifactStore, capabilityCatalog, artifactSchemaCatalog
}

func registerAgentOSSchemas(l *logger.Logger, cfg *config.Config, capabilityCatalog *temporalrepo.AgentOSCapabilityCatalogRepo, artifactSchemaCatalog *temporalrepo.AgentOSArtifactSchemaCatalogRepo) {
	capabilities, err := cfg.AgentOS.Capabilities()
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos capabilities: %w", err))
	}

	if err := agentosplan.RegisterCapabilities(context.Background(), capabilityCatalog, agentostemporal.CapabilitiesWithDefaults(capabilities)); err != nil {
		l.Fatal(fmt.Errorf("app - Run - register agentos capabilities: %w", err))
	}

	artifactSchemas, err := cfg.AgentOS.ArtifactSchemas()
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos artifact schemas: %w", err))
	}

	if err := agentosplan.RegisterArtifactSchemas(context.Background(), artifactSchemaCatalog, artifactSchemas); err != nil {
		l.Fatal(fmt.Errorf("app - Run - register agentos artifact schemas: %w", err))
	}
}

type agentOSProcessPlatformRuntimes struct {
	processRuntime      agentosproc.Runtime
	ledgerRuntime       agentosproc.LedgerRuntime
	actionRuntime       agentosproc.GovernedActionRuntime
	worksetRuntime      agentosproc.BatchRuntime
	projectionRuntime   agentosproc.ProjectionRuntime
	closeProcessRuntime func() error
}

type agentOSProcessPlatformStores struct {
	processStore *temporalrepo.AgentOSProcessRepo
	ledgerStore  *temporalrepo.AgentOSLedgerRepo
	actionStore  *temporalrepo.AgentOSActionRepo
	worksetStore *temporalrepo.AgentOSWorksetRepo
}

func setupPlanRuntime(agentOSRuntime agentos.Runtime, planStore *temporalrepo.AgentOSPlanRepo, planEventStream *planstream.PlanEventStream, artifactStore *temporalrepo.AgentOSArtifactRepo, capabilityCatalog *temporalrepo.AgentOSCapabilityCatalogRepo, artifactSchemaCatalog *temporalrepo.AgentOSArtifactSchemaCatalogRepo, fwCfg *agentfwconfig.Config, cfg *config.Config, runtime *agentfwruntime.TemporalRuntime, comp *initAgentComponentsResult, l *logger.Logger) (*agentostemporal.PlanActivities, agentos.PlanRuntime, *agentOSProcessPlatformRuntimes, *agentostemporal.PlanCommandRecoveryLoop, *agentosplan.PlanMetricsExporterLoop) {
	planActivities, err := agentostemporal.NewPlanActivitiesWithCatalogAndSchemas(
		agentOSRuntime,
		capabilityCatalog,
		artifactSchemaCatalog,
		planStore,
		planEventStream,
		artifactStore,
	)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos plan activities: %w", err))
	}

	stores := newAgentOSProcessPlatformStores(planStore.Postgres)

	processActivities, err := agentostemporal.NewProcessActivities(stores.processStore, stores.processStore)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos process activities: %w", err))
	}

	planRuntimeCfg := cfg.AgentOS.ArtifactStoreConfig()
	planRuntimeConfig := agentostemporal.RuntimeConfig{
		TemporalAddress:     fwCfg.Temporal.Address,
		TemporalNamespace:   fwCfg.Temporal.Namespace,
		TemporalTaskQueues:  agentOSTemporalTaskQueues(&fwCfg.Temporal.TaskQueues),
		PostgresURL:         cfg.PG.URL,
		PostgresPoolMax:     cfg.PG.PoolMax,
		PlanEventPublisher:  planEventStream,
		PlanEventSubscriber: planEventStream,
		ArtifactStore:       appAgentOSArtifactStoreConfig(&planRuntimeCfg),
	}

	planRuntime, err := agentostemporal.NewPlanRuntimeWithClient(context.Background(), &planRuntimeConfig, runtime.Client)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos temporal plan runtime: %w", err))
	}

	processPlatform := newAgentOSProcessPlatformRuntimes(l, &planRuntimeConfig, runtime, stores)

	registrar := agentOSRegistrar{
		base:              agentfwruntime.NewDefaultRegistrar(comp.activities),
		planActivities:    planActivities,
		processActivities: processActivities,
	}
	if err := agentfwruntime.StartWorker(runtime, registrar); err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentfw.StartWorker: %w", err))
	}

	l.Info("app - Run - agent framework workers started on task queues: %v", fwCfg.Temporal.TaskQueues.QueueNames())
	planRecovery := startAgentOSPlanCommandRecovery(l, &cfg.AgentOS, planRuntime)
	planMetrics := startAgentOSPlanMetricsExporter(l, cfg, planStore)

	return planActivities, planRuntime, processPlatform, planRecovery, planMetrics
}

func newAgentOSProcessPlatformStores(pg *postgres.Postgres) agentOSProcessPlatformStores {
	return agentOSProcessPlatformStores{
		processStore: temporalrepo.NewAgentOSProcessRepo(pg),
		ledgerStore:  temporalrepo.NewAgentOSLedgerRepo(pg),
		actionStore:  temporalrepo.NewAgentOSActionRepo(pg),
		worksetStore: temporalrepo.NewAgentOSWorksetRepo(pg),
	}
}

func newAgentOSProcessPlatformRuntimes(l *logger.Logger, cfg *agentostemporal.RuntimeConfig, runtime *agentfwruntime.TemporalRuntime, stores agentOSProcessPlatformStores) *agentOSProcessPlatformRuntimes {
	processRuntime, err := agentostemporal.NewProcessRuntimeWithClient(context.Background(), cfg, runtime.Client)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos temporal process runtime: %w", err))
	}

	ledgerRuntime, err := agentosledger.NewRuntime(stores.ledgerStore)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos ledger runtime: %w", err))
	}

	actionRuntime, err := agentosaction.NewRuntime(stores.actionStore)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos action runtime: %w", err))
	}

	worksetRuntime, err := agentosbatch.NewRuntime(stores.worksetStore)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos workset runtime: %w", err))
	}

	projectionRuntime := newAgentOSProjectionRuntime(l, stores)

	return &agentOSProcessPlatformRuntimes{
		processRuntime:      processRuntime,
		ledgerRuntime:       ledgerRuntime,
		actionRuntime:       actionRuntime,
		worksetRuntime:      worksetRuntime,
		projectionRuntime:   projectionRuntime,
		closeProcessRuntime: closeAgentOSProcessRuntime(processRuntime),
	}
}

func newAgentOSProjectionRuntime(l *logger.Logger, stores agentOSProcessPlatformStores) agentosproc.ProjectionRuntime {
	projectionRuntime, err := agentosprojection.NewRuntime(agentosprojection.Config{
		Processes: stores.processStore,
		Ledger:    stores.ledgerStore,
		Actions:   stores.actionStore,
		Worksets:  stores.worksetStore,
	})
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos projection runtime: %w", err))
	}

	return projectionRuntime
}

func initTemporalComponents(l *logger.Logger, cfg *config.Config, fwCfg *agentfwconfig.Config, pg *postgres.Postgres, rdb *goredis.Redis,
	messageRepo *temporalrepo.MessageRepo, agentRepo *cached.AgentRepo,
	runBackendIndex *temporalrepo.RunBackendIndexRepo, templateUC *templatepkg.UseCase, dataPlane *agentostemporal.StreamingDataPlane,
) *temporalComponents {
	runtime, err := agentfwruntime.NewTemporalRuntime(&fwCfg.Temporal)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentfw.NewTemporalRuntime: %w", err))
	}

	llmResult, err := webapi.LoadLLMProviders(cfg.AgentFW.LLMConfigPath)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - LoadLLMProviders: %w", err))
	}

	comp := initAgentComponents(l, cfg, pg, rdb, llmResult, messageRepo, agentRepo, dataPlane)
	registerToolsOnRegistry(l, comp.toolRegistry)

	if err := templateUC.EnsureDefault(context.Background()); err != nil {
		l.Warn("app - Run - ensure default template: %v", err)
	}

	signalWorkflow := func(ctx context.Context, workflowID, signalName string, arg any) error {
		return runtime.Client.SignalWorkflow(ctx, workflowID, "", signalName, arg)
	}
	cancelWorkflow := func(ctx context.Context, workflowID string) error {
		return runtime.Client.CancelWorkflow(ctx, workflowID, "")
	}

	agentOSRuntime := newAgentOSControlRuntime(l, cfg, fwCfg, runtime, runBackendIndex, dataPlane)

	planStore, planEventStream, artifactStore, capabilityCatalog, artifactSchemaCatalog := initPlanInfrastructure(l, cfg, pg, dataPlane)
	registerAgentOSSchemas(l, cfg, capabilityCatalog, artifactSchemaCatalog)
	_, planRuntime, processPlatform, planRecovery, planMetrics := setupPlanRuntime(agentOSRuntime, planStore, planEventStream, artifactStore, capabilityCatalog, artifactSchemaCatalog, fwCfg, cfg, runtime, comp, l)

	return &temporalComponents{
		runtime:             runtime,
		agentOSRuntime:      agentOSRuntime,
		planRuntime:         planRuntime,
		platformRuntime:     processPlatform.platformRuntime(agentOSRuntime, planRuntime),
		closePlanRuntime:    closeAgentOSPlanRuntime(planRuntime),
		closeProcessRuntime: processPlatform.closeProcessRuntime,
		planRecovery:        planRecovery,
		planMetrics:         planMetrics,
		batchWriter:         comp.batchWriter,
		agentUC:             comp.agentUC,
		toolRegistry:        comp.toolRegistry,
		cancelWorkflow:      cancelWorkflow,
		signalWorkflow:      signalWorkflow,
		llmProvider:         comp.llmProvider,
	}
}

func newAgentOSControlRuntime(l logger.Interface, cfg *config.Config, fwCfg *agentfwconfig.Config, runtime *agentfwruntime.TemporalRuntime, runBackendIndex *temporalrepo.RunBackendIndexRepo, dataPlane *agentostemporal.StreamingDataPlane) agentos.Runtime {
	agentOSRuntime, err := agentostemporal.NewRuntimeWithClient(
		context.Background(), &agentostemporal.RuntimeConfig{
			TemporalAddress:          fwCfg.Temporal.Address,
			TemporalNamespace:        fwCfg.Temporal.Namespace,
			TemporalTaskQueues:       agentOSTemporalTaskQueues(&fwCfg.Temporal.TaskQueues),
			Subscriber:               dataPlane.Subscriber,
			Publisher:                dataPlane.Publisher,
			ProjectionController:     dataPlane.Projector,
			Logger:                   l,
			TemporalExternalBackends: temporalExternalBackends(l, cfg),
			HTTPBackends:             httpBackends(l, cfg),
			GRPCBackends:             grpcBackends(l, cfg),
		}, runtime.Client,
		agentostemporal.WithRunBackendIndex(runBackendIndex),
		agentostemporal.WithRunBackendSelector(backendSelector(l, cfg)),
	)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos temporal runtime: %w", err))
	}

	return agentOSRuntime
}

func startAgentOSPlanMetricsExporter(l logger.Interface, cfg *config.Config, planStore *temporalrepo.AgentOSPlanRepo) *agentosplan.PlanMetricsExporterLoop {
	if !cfg.Metrics.Enabled || !cfg.AgentOS.PlanMetricsExporterEnabled {
		return nil
	}

	exporter, err := agentosplan.NewPlanMetricsExporter(&agentosplan.PlanMetricsExporterConfig{
		ExporterID:  cfg.AgentOS.PlanMetricsExporterID,
		PlanRefs:    planStore,
		Plans:       planStore,
		PlanEvents:  planStore,
		Checkpoints: planStore,
		Sink:        planStore,
		BatchSize:   cfg.AgentOS.PlanMetricsExporterBatchSize,
	})
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos plan metrics exporter: %w", err))
	}

	loop, err := agentosplan.StartPlanMetricsExporterLoop(
		context.Background(),
		exporter,
		&agentosplan.PlanMetricsExporterLoopConfig{
			Interval: time.Duration(cfg.AgentOS.PlanMetricsExporterIntervalSeconds) * time.Second,
			Scope: agentosplan.PlanRefScope{
				Limit: cfg.AgentOS.PlanMetricsExporterPlanLimit,
			},
			ExportImmediately: cfg.AgentOS.PlanMetricsExporterImmediateOnWorkerRun,
		},
		planMetricsExporterLogger{logger: l},
	)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos plan metrics exporter: %w", err))
	}

	return loop
}

type planMetricsExporterLogger struct {
	logger logger.Interface
}

func (l planMetricsExporterLogger) PlanMetricsExportSucceeded(result agentosplan.PlanMetricsExportResult) {
	if result.PlansScanned == 0 && result.EventsScanned == 0 {
		l.logger.Debug("app - Run - agentos plan metrics exporter: no plan events to export")

		return
	}

	l.logger.Info(
		"app - Run - agentos plan metrics exporter: plans=%d events=%d samples=%d checkpoints=%d",
		result.PlansScanned,
		result.EventsScanned,
		result.SamplesRecorded,
		result.CheckpointsSaved,
	)
}

func (l planMetricsExporterLogger) PlanMetricsExportFailed(err error) {
	l.logger.Error(fmt.Errorf("app - Run - agentos plan metrics exporter: %w", err))
}

func startAgentOSPlanCommandRecovery(l logger.Interface, cfg *config.AgentOS, planRuntime agentos.PlanRuntime) *agentostemporal.PlanCommandRecoveryLoop {
	if !cfg.PlanCommandRecoveryEnabled {
		return nil
	}

	recoverer, ok := planRuntime.(agentostemporal.PlanCommandRecoverer)
	if !ok {
		l.Fatal(errAppRunPlanCmdRecoveryNoImplementation)
	}

	loop, err := agentostemporal.StartPlanCommandRecovery(
		context.Background(),
		recoverer,
		agentostemporal.PlanCommandRecoveryLoopConfig{
			Interval:           time.Duration(cfg.PlanCommandRecoveryIntervalSeconds) * time.Second,
			Limit:              cfg.PlanCommandRecoveryLimit,
			RecoverImmediately: cfg.PlanCommandRecoveryImmediateOnWorkerRun,
		},
		planCommandRecoveryLogger{logger: l},
	)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - agentos plan command recovery: %w", err))
	}

	return loop
}

type planCommandRecoveryLogger struct {
	logger logger.Interface
}

func (l planCommandRecoveryLogger) PlanCommandRecoverySucceeded(result agentostemporal.PlanCommandRecoveryResult) {
	if result.Scanned == 0 {
		l.logger.Debug("app - Run - agentos plan command recovery: no recoverable commands")

		return
	}

	l.logger.Info("app - Run - agentos plan command recovery: scanned=%d delivered=%d failed=%d", result.Scanned, result.Delivered, result.Failed)
}

func (l planCommandRecoveryLogger) PlanCommandRecoveryFailed(err error) {
	l.logger.Error(fmt.Errorf("app - Run - agentos plan command recovery: %w", err))
}

type agentOSRegistrar struct {
	base              agentfwruntime.DefaultRegistrar
	planActivities    *agentostemporal.PlanActivities
	processActivities *agentostemporal.ProcessActivities
}

func (r agentOSRegistrar) RegisterWorkflows(rt *agentfwruntime.TemporalRuntime) error {
	if err := r.base.RegisterWorkflows(rt); err != nil {
		return err
	}

	planWorker, ok := rt.WorkerFor(rt.TaskQueues.PlanControl)
	if !ok || planWorker == nil {
		return fmt.Errorf("%w: plan control task queue %q", errAppRunPlanWorkerQueueUnconfigured, rt.TaskQueues.PlanControl)
	}

	if err := agentostemporal.RegisterPlanWorkflow(planWorker); err != nil {
		return err
	}

	processWorker, ok := rt.WorkerFor(rt.TaskQueues.ProcessControl)
	if !ok || processWorker == nil {
		return fmt.Errorf("%w: process control task queue %q", errAppProcessWorkerQueueUnconfigured, rt.TaskQueues.ProcessControl)
	}

	return agentostemporal.RegisterProcessWorkflow(processWorker)
}

func (r agentOSRegistrar) RegisterActivities(rt *agentfwruntime.TemporalRuntime) error {
	if err := r.base.RegisterActivities(rt); err != nil {
		return err
	}

	planWorker, ok := rt.WorkerFor(rt.TaskQueues.PlanActivity)
	if !ok || planWorker == nil {
		return fmt.Errorf("%w: plan activity task queue %q", errAppRunPlanWorkerQueueUnconfigured, rt.TaskQueues.PlanActivity)
	}

	if err := agentostemporal.RegisterPlanActivities(planWorker, r.planActivities); err != nil {
		return err
	}

	processWorker, ok := rt.WorkerFor(rt.TaskQueues.ProcessActivity)
	if !ok || processWorker == nil {
		return fmt.Errorf("%w: process activity task queue %q", errAppProcessWorkerQueueUnconfigured, rt.TaskQueues.ProcessActivity)
	}

	return agentostemporal.RegisterProcessActivities(processWorker, r.processActivities)
}

func closeAgentOSPlanRuntime(planRuntime agentos.PlanRuntime) func() error {
	closeable, ok := planRuntime.(interface {
		Close() error
	})
	if !ok {
		return nil
	}

	return closeable.Close
}

func closeAgentOSProcessRuntime(processRuntime agentosproc.Runtime) func() error {
	closeable, ok := processRuntime.(interface {
		Close() error
	})
	if !ok {
		return nil
	}

	return closeable.Close
}

func agentOSTemporalTaskQueues(taskQueues *agentfwconfig.TaskQueues) agentostemporal.TaskQueues {
	return agentostemporal.TaskQueues{
		PlanControl:     taskQueues.PlanControl,
		PlanActivity:    taskQueues.PlanActivity,
		ProcessControl:  taskQueues.ProcessControl,
		ProcessActivity: taskQueues.ProcessActivity,
		NativeControl:   taskQueues.NativeControl,
		NativeLLM:       taskQueues.NativeLLM,
		NativeTool:      taskQueues.NativeTool,
		Stream:          taskQueues.Stream,
	}
}

func appArtifactBlobConfig(cfg *config.ArtifactStoreConfig) artifactrepo.Config {
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

func appAgentOSArtifactStoreConfig(cfg *config.ArtifactStoreConfig) agentostemporal.ArtifactStoreConfig {
	return agentostemporal.ArtifactStoreConfig{
		Backend: agentostemporal.ArtifactStoreBackend(cfg.Backend),
		Local: agentostemporal.LocalArtifactStoreConfig{
			Root: cfg.Local.Root,
		},
		S3: agentostemporal.S3ArtifactStoreConfig{
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

func httpBackends(l logger.Interface, cfg *config.Config) []agentostemporal.HTTPBackendConfig {
	backends, err := cfg.AgentFW.HTTPBackends()
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - http backends: %w", err))
	}

	result := make([]agentostemporal.HTTPBackendConfig, 0, len(backends))
	for _, backend := range backends {
		result = append(result, agentostemporal.HTTPBackendConfig{
			Name:     backend.Name,
			Endpoint: backend.Endpoint,
			Headers:  backend.Headers,
		})
	}

	return result
}

func grpcBackends(l logger.Interface, cfg *config.Config) []agentostemporal.GRPCBackendConfig {
	backends, err := cfg.AgentFW.GRPCBackends()
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - grpc backends: %w", err))
	}

	result := make([]agentostemporal.GRPCBackendConfig, 0, len(backends))
	for i := range backends {
		backend := &backends[i]
		result = append(result, agentostemporal.GRPCBackendConfig{
			Name:      backend.Name,
			Target:    backend.Target,
			Authority: backend.Authority,
			Insecure:  backend.Insecure,
			Service:   backend.Service,
			Methods: agentostemporal.GRPCMethodNames{
				Start:   backend.Methods.Start,
				Signal:  backend.Methods.Signal,
				Control: backend.Methods.Control,
				Status:  backend.Methods.Status,
			},
		})
	}

	return result
}

func backendSelector(l logger.Interface, cfg *config.Config) agentostemporal.RunBackendSelector {
	rules, err := cfg.AgentFW.BackendSelectionRules()
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - backend selection rules: %w", err))
	}

	if len(rules) == 0 {
		return nil
	}

	usecaseRules := make([]agentosruntime.BackendSelectionRule, 0, len(rules))
	for _, rule := range rules {
		usecaseRules = append(usecaseRules, agentosruntime.BackendSelectionRule{
			Name:     rule.Name,
			Backend:  rule.Backend,
			AgentID:  rule.AgentID,
			Metadata: rule.Metadata,
			Input:    rule.Input,
		})
	}

	selector, err := agentosruntime.NewRuleBackendSelector(usecaseRules)
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - backend selector: %w", err))
	}

	return selector
}

func temporalExternalBackends(l logger.Interface, cfg *config.Config) []agentostemporal.ExternalBackendConfig {
	backends, err := cfg.AgentFW.TemporalExternalBackends()
	if err != nil {
		l.Fatal(fmt.Errorf("app - Run - temporal external backends: %w", err))
	}

	result := make([]agentostemporal.ExternalBackendConfig, 0, len(backends))
	for _, backend := range backends {
		defaults := make(map[agentoscore.SignalType]string, len(backend.Signals.Defaults))
		for signalType, signalName := range backend.Signals.Defaults {
			defaults[agentoscore.SignalType(signalType)] = signalName
		}

		result = append(result, agentostemporal.ExternalBackendConfig{
			Name:         backend.Name,
			TaskQueue:    backend.TaskQueue,
			WorkflowType: backend.WorkflowType,
			QueryType:    backend.QueryType,
			Signals: agentostemporal.ExternalSignalNames{
				Pause:    backend.Signals.Pause,
				Resume:   backend.Signals.Resume,
				Cancel:   backend.Signals.Cancel,
				Defaults: defaults,
			},
		})
	}

	return result
}

// configureStreamingActivities wires the shared data plane onto the run
// activities: the publisher mirrors the AG-UI timeline and the projector
// controller attaches the durable milestone projection.
func configureStreamingActivities(activities *orchestration.AgentActivities, dataPlane *agentostemporal.StreamingDataPlane) {
	if dataPlane == nil {
		return
	}

	activities.WithStreamPublisher(dataPlane.Publisher)
	activities.WithRunProjectionController(dataPlane.Projector)
}

// initAgentComponentsResult holds the results of initAgentComponents.
type initAgentComponentsResult struct {
	llmProvider  *webapi.BifrostProvider
	toolRegistry *toolkit.ToolRegistry
	batchWriter  *pipelinepkg.BatchWriter
	agentUC      *agent.UseCase
	activities   *orchestration.AgentActivities
}

func initAgentComponents(
	l *logger.Logger,
	cfg *config.Config,
	pg *postgres.Postgres,
	rdb *goredis.Redis,
	llmResult *webapi.LLMProvidersResult,
	messageRepo *temporalrepo.MessageRepo,
	agentRepo *cached.AgentRepo,
	dataPlane *agentostemporal.StreamingDataPlane,
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

	agentCompressor := compressor.New(compressor.Config{LLM: llmProvider})
	wal := pipelinepkg.NewWriteAheadLog(rdb.GeneralClient)
	dlq := pipelinepkg.NewDeadLetterQueue(rdb.GeneralClient)

	batchWriter := pipelinepkg.NewBatchWriter(wal, dlq, messageRepo, l)
	batchWriter.Start()

	agentUC := agent.New(llmProvider, toolExecutor, wal, agentCompressor, toolRegistry, agentRepo)
	agentUC.SetLogger(l)

	mcpManager := mcpRepo.NewManager()
	if toolRegistry != nil {
		mcpManager.SetRegistry(toolRegistry)
		l.Info("app - Run - mcp manager created for per-agent JIT tool registration")
	}

	activities := orchestration.NewAgentActivities(agentUC, l).
		WithMCPManager(mcpManager).
		WithBilling(billingUC)
	configureStreamingActivities(activities, dataPlane)
	l.Info("app - Run - agent components initialized")

	return &initAgentComponentsResult{llmProvider, toolRegistry, batchWriter, agentUC, activities}
}

func registerToolsOnRegistry(l *logger.Logger, toolRegistry *toolkit.ToolRegistry) {
	agentCreator := func(_ context.Context, _, _, _, _ string, _ []string) error {
		return nil
	}
	if err := toolRegistry.Register(toolkit.NewAgentCreationTool(agentCreator)); err != nil {
		l.Warn("app - Run - register agent_creation_tool: %v", err)
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
