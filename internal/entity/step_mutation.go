package entity

import "time"

// StepMutation describes a runtime modification to the step queue.
// Applied by either:
//   - OnResult callback from a completed step
//   - External Signal "step-modify"
type StepMutation struct {
	AppendAfter string   `json:"append_after,omitempty"` // insert after this step ID
	InsertSteps []Step   `json:"insert_steps"`           // steps to insert
	ModifyStep  string   `json:"modify_step,omitempty"`  // replace this step ID
	DeleteSteps []string `json:"delete_steps,omitempty"` // delete these step IDs
}

// IsEmpty reports whether the mutation changes nothing.
//
// A mutation that says nothing is a mistake rather than a no-op: applying it
// would leave the queue exactly as it was and report success, which hides the
// caller's error until the run does not do what they asked.
func (m *StepMutation) IsEmpty() bool {
	return m.AppendAfter == "" && len(m.InsertSteps) == 0 && m.ModifyStep == "" && len(m.DeleteSteps) == 0
}

// WaitCondition configures a StepWait for HITL or external events.
type WaitCondition struct {
	SignalName string         `json:"signal_name,omitempty"` // Temporal Signal to wait for
	Timeout    *time.Duration `json:"timeout,omitempty"`     // max wait duration
	OnTimeout  string         `json:"on_timeout,omitempty"`  // "timeout" | "skip" | "fail"
}
