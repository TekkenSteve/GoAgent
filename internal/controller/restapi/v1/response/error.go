// Package response defines the v1 API response payloads.
package response

// Error is the standard v1 API error response payload.
type Error struct {
	Error string `json:"error" example:"message"`
}
