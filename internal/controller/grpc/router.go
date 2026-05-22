package grpc

import (
	v1 "github.com/TekkenSteve/GoAgent/internal/controller/grpc/v1"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/TekkenSteve/GoAgent/usecase"
	pbgrpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// NewRouter -.
func NewRouter(app *pbgrpc.Server, t usecase.AgentExecutor, l logger.Interface) {
	{
		v1.NewAgentRoutes(app, t, l)
	}

	reflection.Register(app)
}
