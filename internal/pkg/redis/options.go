package redis

import "time"

// Option -.
type Option func(*Config)

// WithGeneralPoolSize sets the general connection pool size.
func WithGeneralPoolSize(n int) Option {
	return func(c *Config) { c.GeneralPoolSize = n }
}

// WithStreamPoolSize sets the stream connection pool size.
func WithStreamPoolSize(n int) Option {
	return func(c *Config) { c.StreamPoolSize = n }
}

// WithOpTimeout sets the timeout for basic operations.
func WithOpTimeout(d time.Duration) Option {
	return func(c *Config) { c.OpTimeout = d }
}

// WithStreamTimeout sets the timeout for stream operations.
func WithStreamTimeout(d time.Duration) Option {
	return func(c *Config) { c.StreamTimeout = d }
}

// WithSocketTimeout sets the socket read/write timeout.
func WithSocketTimeout(d time.Duration) Option {
	return func(c *Config) { c.SocketTimeout = d }
}

// WithConnectTimeout sets the connection dial timeout.
func WithConnectTimeout(d time.Duration) Option {
	return func(c *Config) { c.ConnectTimeout = d }
}
