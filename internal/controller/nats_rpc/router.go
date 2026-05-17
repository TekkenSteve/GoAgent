package nats_rpc

import (
	v1 "github.com/TekkenSteve/GoAgent/internal/controller/nats_rpc/v1"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/TekkenSteve/GoAgent/pkg/nats/nats_rpc/server"
)

// NewRouter -.
func NewRouter(t usecase.AgentExecutor, l logger.Interface) map[string]server.CallHandler {
	routes := make(map[string]server.CallHandler)

	{
		v1.NewAgentRoutes(routes, t, l)
	}

	return routes
}
