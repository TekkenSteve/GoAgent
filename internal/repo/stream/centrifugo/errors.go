package centrifugo

import "errors"

// Package-level sentinels so callers can classify transport failures without
// parsing error strings (err113: sentinels must not be created inline).
var (
	// ErrInvalidConfig reports a malformed Centrifugo configuration.
	ErrInvalidConfig = errors.New("centrifugo: invalid config")

	// ErrConnectFailed reports a failed websocket connection.
	ErrConnectFailed = errors.New("centrifugo: connect failed")

	// ErrSubscribeFailed reports a failed channel subscription.
	ErrSubscribeFailed = errors.New("centrifugo: subscribe failed")

	// ErrHistoryFailed reports a failed history fetch.
	ErrHistoryFailed = errors.New("centrifugo: history failed")

	// ErrPublishUnexpectedStatus reports a non-2xx server API response.
	ErrPublishUnexpectedStatus = errors.New("centrifugo: unexpected publish status")
)
