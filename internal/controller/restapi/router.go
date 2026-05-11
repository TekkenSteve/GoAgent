package restapi

import (
	"net/http"

	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/TekkenSteve/GoAgent/config"
	_ "github.com/TekkenSteve/GoAgent/docs" // Swagger docs.
	"github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/middleware"
	v1 "github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/TekkenSteve/GoAgent/pkg/redis"
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
func NewRouter(app *fiber.App, cfg *config.Config, t usecase.AgentExecutor, h usecase.HistoryQuery, s usecase.StreamExecutor, l logger.Interface, rdb *redis.Redis,
	eventStore stream.EventStore, subscriber stream.Subscriber, gateway stream.StatelessGateway,
	wsHub *stream.WebSocketHub,
	cancelWorkflow v1.CancelWorkflowFn, signalWorkflow v1.SignalWorkflowFn,
	m usecase.TemplateManager, eh usecase.TriggerEventHandler) {

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
		v1.NewRoutes(apiV1Group, t, h, s, l, rdb, eventStore, subscriber, gateway, wsHub, cancelWorkflow, signalWorkflow, m, eh)
	}
}
