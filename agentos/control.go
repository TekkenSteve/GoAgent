package agentos

// ControlOperation defines an external run-control intent.
type ControlOperation string

const (
	ControlPause  ControlOperation = "pause"
	ControlResume ControlOperation = "resume"
	ControlCancel ControlOperation = "cancel"
)
