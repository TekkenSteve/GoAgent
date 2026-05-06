package v1

import (
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v2"
)

// NewAgentRoutes registers agent-related REST API endpoints under the /agent group.
func NewAgentRoutes(apiV1Group fiber.Router, t usecase.AgentExecutor, l logger.Interface) {
	r := &V1{t: t, l: l, v: validator.New(validator.WithRequiredStructEnabled())}

	agentGroup := apiV1Group.Group("/agent")

	{
		agentGroup.Post("/execute", r.execute)
		agentGroup.Get("/status/:run_id", r.status)
	}
}
