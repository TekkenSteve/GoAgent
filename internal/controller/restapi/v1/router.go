package v1

import (
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/TekkenSteve/GoAgent/pkg/redis"
	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v2"
)

// NewAgentRoutes registers agent-related REST API endpoints under the /agent group.
func NewAgentRoutes(apiV1Group fiber.Router, t usecase.AgentExecutor, h usecase.HistoryQuery, s usecase.StreamExecutor, l logger.Interface, rdb *redis.Redis) {
	r := &V1{t: t, h: h, s: s, l: l, v: validator.New(validator.WithRequiredStructEnabled()), rdb: rdb}

	agentGroup := apiV1Group.Group("/agent")

	{
		agentGroup.Post("/execute", r.execute)
		agentGroup.Get("/status/:run_id", r.status)
		agentGroup.Get("/stream", r.stream)
		agentGroup.Get("/:run_id/messages", r.listMessages)
		agentGroup.Get("/:run_id/tools", r.listToolResults)
	}
}
