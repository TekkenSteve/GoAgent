package v1

import (
	v1 "github.com/TekkenSteve/GoAgent/docs/proto/v1"
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/go-playground/validator/v10"
	pbgrpc "google.golang.org/grpc"
)

// NewAgentRoutes -.
func NewAgentRoutes(app *pbgrpc.Server, t usecase.AgentExecutor, l logger.Interface) {
	r := &V1{t: t, l: l, v: validator.New(validator.WithRequiredStructEnabled())}

	{
		v1.RegisterTranslationServer(app, r)
	}
}
