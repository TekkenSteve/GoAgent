package streamadapter

import "errors"

// Package-level sentinels so callers can classify mapping failures without
// parsing error strings (err113: sentinels must not be created inline).
var (
	// ErrUnsupportedStreamEvent reports an entity event kind the adapter
	// cannot map onto the AG-UI vocabulary.
	ErrUnsupportedStreamEvent = errors.New("streamadapter: unsupported stream event")
	// ErrRunFatal is wrapped into the RUN_ERROR event's error payload for an
	// unrecoverable agent error, so the message keeps dynamic context while the
	// error stays classified (err113).
	ErrRunFatal = errors.New("fatal agent error")
)
