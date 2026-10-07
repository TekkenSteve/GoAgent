// Package restapi wires the HTTP router and its dependencies.
package restapi

import (
	"net/http"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosplatform "github.com/TekkenSteve/GoAgent/agentos/platform"
	"github.com/TekkenSteve/GoAgent/config"
	_ "github.com/TekkenSteve/GoAgent/docs" // Swagger docs.
	"github.com/TekkenSteve/GoAgent/internal/agentfw/eventing"
	"github.com/TekkenSteve/GoAgent/internal/authn"
	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/middleware"
	v1 "github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/gofiber/contrib/otelfiber/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/swagger"
)

// AuthDeps carries the components the HTTP surface authenticates and
// authorizes with.
//
// Both are required. A router assembled without a verifier would serve the
// control plane anonymously and a router without an authorizer could not
// enforce the tenant boundary — the two failures this surface exists to
// prevent — so NewRouter refuses to assemble rather than degrade quietly.
type AuthDeps struct {
	// Verifier authenticates bearer credentials into a principal.
	Verifier *authn.Verifier
	// Authorizer decides whether a principal may act on an object.
	Authorizer agentoscore.Authorizer
	// PublicPaths lists the paths reachable without a credential. Empty means
	// the liveness probe alone.
	PublicPaths []string
}

// assertWired fails loudly on a missing authentication dependency. Startup is
// the only cheap moment to discover that the control plane would be open.
func (d AuthDeps) assertWired() {
	if d.Verifier == nil {
		panic("restapi: AuthDeps.Verifier is required — the HTTP surface must not be assembled without authentication")
	}

	if d.Authorizer == nil {
		panic("restapi: AuthDeps.Authorizer is required — the HTTP surface must not be assembled without authorization")
	}
}

// NewRouter -.
// Swagger spec:
// @title       GoAgent API
// @description Agent Framework API
// @version     1.0
// @host        localhost:8080
// @BasePath    /v1
// Routes under /v1 expose the API; /healthz and /swagger serve probes and docs.
func NewRouter(app *fiber.App, cfg *config.Config, l logger.Interface,
	cancelWorkflow v1.CancelWorkflowFn, signalWorkflow v1.SignalWorkflowFn,
	m usecase.TemplateManager,
	eventIngest *eventing.Service,
	agentOSRuntime agentos.Runtime,
	agentOSPlanRuntime agentos.PlanRuntime,
	agentOSPlatformRuntime agentosplatform.Runtime,
	runEventReader v1.RunEventReader,
	auth AuthDeps,
) {
	auth.assertWired()

	// Options
	app.Use(middleware.Logger(l))
	app.Use(middleware.Recovery(l))

	// Authentication is installed app-wide, before any route is registered, so
	// a route added later is protected by default rather than by remembering
	// to add middleware to the right group. The public list is explicit and
	// minimal (see config.Auth.ResolvedPublicPaths).
	app.Use(middleware.RequireAuth(auth.Verifier, auth.PublicPaths))

	// Prometheus metrics
	if cfg.Metrics.Enabled {
		prometheus := fiberprometheus.New(cfg.App.Name)
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
		if cfg.Tracing.Enabled {
			apiV1Group.Use(otelfiber.Middleware())
		}

		v1.NewRoutes(apiV1Group, l, cancelWorkflow, signalWorkflow, m, eventIngest, agentOSRuntime, agentOSPlanRuntime, agentOSPlatformRuntime, runEventReader, auth.Authorizer)
	}
}
