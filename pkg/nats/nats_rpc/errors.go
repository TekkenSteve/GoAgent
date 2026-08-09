// Package natsrpc implements request-reply RPC over NATS.
package natsrpc

import "errors"

var (
	// ErrTimeout -.
	ErrTimeout = errors.New("timeout")
	// ErrInternalServer -.
	ErrInternalServer = errors.New("internal server error")
	// ErrBadHandler -.
	ErrBadHandler = errors.New("unregistered handler")
)

// Success -.
const Success = "success"
