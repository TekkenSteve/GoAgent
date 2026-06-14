package agentos

import "errors"

var (
	ErrInvalidRunSpec          = errors.New("agentos: invalid run spec")
	ErrInvalidControlOperation = errors.New("agentos: invalid control operation")
	ErrInvalidStreamScope      = errors.New("agentos: invalid stream scope")
)
