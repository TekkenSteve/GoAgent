package jobgroup

// Observer is the group's only reporting channel. The group itself neither
// logs nor touches metrics: an application that wants its jobs visible
// passes an observer at New, and a nil observer is a silent one.
type Observer interface {
	// JobStarted reports that a named job's function has begun running.
	JobStarted(name string)

	// JobStopped reports that a named job's function has returned, with the
	// error it returned — nil for a clean exit. A recovered panic is
	// reported as JobPanicked first, then JobStopped with the panic
	// converted to an error.
	JobStopped(name string, err error)

	// JobPanicked reports a job that panicked under WithPanicRecovery.
	// Without that option a panicking job is not recovered and takes the
	// process down, as any unrecovered panic does.
	JobPanicked(name string, recovered any)
}

// silentObserver is the default: it receives everything and reports nothing.
type silentObserver struct{}

func (silentObserver) JobStarted(string)        {}
func (silentObserver) JobStopped(string, error) {}
func (silentObserver) JobPanicked(string, any)  {}
