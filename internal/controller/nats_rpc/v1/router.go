package v1

import (
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/TekkenSteve/GoAgent/pkg/nats/nats_rpc/server"
	"github.com/go-playground/validator/v10"
)

// NewAgentRoutes -.
func NewAgentRoutes(routes map[string]server.CallHandler, t usecase.AgentExecutor, l logger.Interface) {
	r := &V1{t: t, l: l, v: validator.New(validator.WithRequiredStructEnabled())}

	{
		routes["v1.execute"] = r.execute()
		routes["v1.getStatus"] = r.getStatus()
	}
}
