package response

import "github.com/TekkenSteve/GoAgent/entity"

// TriggerFireResult -.
type TriggerFireResult struct {
	TriggerID string `json:"trigger_id"`
	RunID     string `json:"run_id"`
	Name      string `json:"name"`
	FiredAt   int64  `json:"fired_at"`
}

// NewTriggerFireResult -.
func NewTriggerFireResult(r *entity.TriggerFireResult) TriggerFireResult {
	return TriggerFireResult{
		TriggerID: r.TriggerID,
		RunID:     r.RunID,
		Name:      r.Name,
		FiredAt:   r.FiredAt.Unix(),
	}
}

// EventWebhookResponse -.
type EventWebhookResponse struct {
	EventSlug string              `json:"event_slug"`
	Fired     []TriggerFireResult `json:"fired"`
	Count     int                 `json:"count"`
}
