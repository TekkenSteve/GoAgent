package jobgroup

// Option configures a Group at construction. Options apply in order, and a
// later option wins.
type Option func(*config)

// config holds the group-level settings an Option may set.
type config struct {
	observer      Observer
	recoverPanics bool
}

// WithObserver routes the group's job events to the observer. A nil
// observer is accepted and means silence.
func WithObserver(o Observer) Option {
	return func(c *config) {
		if o != nil {
			c.observer = o
		}
	}
}

// WithPanicRecovery converts a panicking job into a stopped job: the panic
// is reported through Observer.JobPanicked and returned as a failure
// wrapping ErrJobPanicked, instead of taking the process down. Reserve it
// for jobs whose survival matters more than the loudness of their crash.
func WithPanicRecovery() Option {
	return func(c *config) {
		c.recoverPanics = true
	}
}
