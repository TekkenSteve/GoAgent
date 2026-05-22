package v1

import (
	"github.com/TekkenSteve/GoAgent/pkg/logger"
	"github.com/TekkenSteve/GoAgent/usecase"
	"github.com/go-playground/validator/v10"
)

// V1 -.
type V1 struct {
	t usecase.AgentExecutor
	l logger.Interface
	v *validator.Validate
}
