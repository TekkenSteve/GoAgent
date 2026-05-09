package v1

import (
	"github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/TekkenSteve/GoAgent/pkg/redis"
	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v2"
)

// NewAgentRoutes registers agent-related REST API endpoints under the /agent group.
func NewAgentRoutes(apiV1Group fiber.Router, t usecase.AgentExecutor, h usecase.HistoryQuery, s usecase.StreamExecutor, l logger.Interface, rdb *redis.Redis,
	eventStore stream.EventStore, subscriber stream.Subscriber, gateway stream.StatelessGateway,
	wsHub *stream.WebSocketHub,
	cancelWorkflow CancelWorkflowFn, signalWorkflow SignalWorkflowFn) {

	r := &V1{
		t: t, h: h, s: s, l: l, v: validator.New(validator.WithRequiredStructEnabled()), rdb: rdb,
		eventStore: eventStore, subscriber: subscriber, gateway: gateway,
		wsHub: wsHub,
		cancelWorkflow: cancelWorkflow, signalWorkflow: signalWorkflow,
	}

	agentGroup := apiV1Group.Group("/agent")

	{
		agentGroup.Post("/execute", r.execute)
		agentGroup.Get("/status/:run_id", r.status)
		agentGroup.Get("/stream", r.stream)
		agentGroup.Get("/ws", r.ws)
		agentGroup.Get("/:run_id/messages", r.listMessages)
		agentGroup.Get("/:run_id/tools", r.listToolResults)
	}
}
