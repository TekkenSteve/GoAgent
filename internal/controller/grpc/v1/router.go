package v1

import (
	v1 "github.com/TekkenSteve/GoAgent/docs/proto/v1"
	"github.com/TekkenSteve/GoAgent/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	pbgrpc "google.golang.org/grpc"
)

// NewAgentRoutes registers the AgentService gRPC server.
func NewAgentRoutes(app *pbgrpc.Server, t usecase.AgentExecutor, l logger.Interface) {
	r := &V1{t: t, l: l}

	{
		v1.RegisterAgentServiceServer(app, r)
	}
}
