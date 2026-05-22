package amqp_rpc

import (
	v1 "github.com/TekkenSteve/GoAgent/internal/controller/amqp_rpc/v1"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/TekkenSteve/GoAgent/pkg/rabbitmq/rmq_rpc/server"
	"github.com/TekkenSteve/GoAgent/usecase"
)

// NewRouter -.
func NewRouter(t usecase.AgentExecutor, l logger.Interface) map[string]server.CallHandler {
	routes := make(map[string]server.CallHandler)

	{
		v1.NewAgentRoutes(routes, t, l)
	}

	return routes
}
