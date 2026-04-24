package v1

import (
	"github.com/TekkenSteve/GoAgent/internal/usecase"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/TekkenSteve/GoAgent/pkg/rabbitmq/rmq_rpc/server"
	"github.com/go-playground/validator/v10"
)

// NewTranslationRoutes -.
func NewTranslationRoutes(routes map[string]server.CallHandler, t usecase.Translation, l logger.Interface) {
	r := &V1{t: t, l: l, v: validator.New(validator.WithRequiredStructEnabled())}

	{
		routes["v1.getHistory"] = r.getHistory()
	}
}
