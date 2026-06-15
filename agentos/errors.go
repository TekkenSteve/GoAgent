package agentos

import "errors"

var (
	ErrInvalidRunSpec          = errors.New("agentos: invalid run spec")
	ErrInvalidBackendRef       = errors.New("agentos: invalid backend ref")
	ErrBackendNotFound         = errors.New("agentos: backend not found")
	ErrRunRouteNotFound        = errors.New("agentos: run route not found")
	ErrInvalidSignal           = errors.New("agentos: invalid signal")
	ErrInvalidControlOperation = errors.New("agentos: invalid control operation")
	ErrInvalidStreamScope      = errors.New("agentos: invalid stream scope")
)
