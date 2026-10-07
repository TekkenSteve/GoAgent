package stream

import "errors"

var (
	// ErrInvalidStreamHandle reports a malformed stream handle.
	ErrInvalidStreamHandle = errors.New("agentos: invalid stream handle")
	// ErrInvalidEvent reports a malformed AG-UI stream event.
	ErrInvalidEvent = errors.New("agentos: invalid stream event")
	// ErrPublishFailed reports that an event could not be published to the bus.
	ErrPublishFailed = errors.New("agentos: stream publish failed")
	// ErrSubscriptionClosed reports use of a closed subscription.
	ErrSubscriptionClosed = errors.New("agentos: stream subscription closed")
)
