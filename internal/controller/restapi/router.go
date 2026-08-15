// Package restapi wires the HTTP router and its dependencies.
package restapi

import (
	"net/http"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentosplatform "github.com/TekkenSteve/GoAgent/agentos/platform"
	"github.com/TekkenSteve/GoAgent/config"
	_ "github.com/TekkenSteve/GoAgent/docs" // Swagger docs.
	"github.com/TekkenSteve/GoAgent/internal/agentfw/eventing"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/middleware"
	v1 "github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/swagger"
)

// NewRouter -.
// Swagger spec:
// @title       GoAgent API
// @description Agent Framework API
// @version     1.0
// @host        localhost:8080
// @BasePath    /v1
// Routes under /v1 expose the API; /healthz and /swagger serve probes and docs.
func NewRouter(app *fiber.App, cfg *config.Config, t usecase.AgentExecutor, o usecase.OrchestrationExecutor, l logger.Interface,
	cancelWorkflow v1.CancelWorkflowFn, signalWorkflow v1.SignalWorkflowFn,
	m usecase.TemplateManager,
	eventIngest *eventing.Service,
	agentOSRuntime agentos.Runtime,
	agentOSPlanRuntime agentos.PlanRuntime,
	agentOSPlatformRuntime agentosplatform.Runtime,
	runEventReader v1.RunEventReader,
) {
	// Options
	app.Use(middleware.Logger(l))
	app.Use(middleware.Recovery(l))

	// Prometheus metrics
	if cfg.Metrics.Enabled {
		prometheus := fiberprometheus.New("my-service-name")
		prometheus.RegisterAt(app, "/metrics")
		app.Use(prometheus.Middleware)
	}

	// Swagger
	if cfg.Swagger.Enabled {
		app.Get("/swagger/*", swagger.HandlerDefault)
	}

	// K8s probe
	app.Get("/healthz", func(ctx *fiber.Ctx) error { return ctx.SendStatus(http.StatusOK) })

	// Routers
	apiV1Group := app.Group("/v1")
	{
		v1.NewRoutes(apiV1Group, t, o, l, cancelWorkflow, signalWorkflow, m, eventIngest, agentOSRuntime, agentOSPlanRuntime, agentOSPlatformRuntime, runEventReader)
	}
}
