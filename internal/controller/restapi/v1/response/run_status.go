package response

import "github.com/TekkenSteve/GoAgent/internal/entity"

// RunStatus -.
type RunStatus struct {
	RunID          string `json:"run_id"`
	LifecycleState string `json:"lifecycle_state"`
	Step            int32  `json:"step"`
	Reason          string `json:"reason,omitempty"`
	UpdatedAt       int64  `json:"updated_at"`
}

// NewRunStatus -.
func NewRunStatus(s entity.RunStatus) RunStatus {
	return RunStatus{
		RunID:          s.RunID,
		LifecycleState: s.LifecycleState,
		Step:            s.Step,
		Reason:          s.Reason,
		UpdatedAt:       s.UpdatedAt.Unix(),
	}
}
