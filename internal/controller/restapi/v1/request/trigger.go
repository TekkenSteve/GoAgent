package request

// EventWebhook -.
type EventWebhook struct {
	EventSlug string            `json:"event_slug" validate:"required" example:"order.created"`
	Payload   map[string]string `json:"payload,omitempty"`
}
