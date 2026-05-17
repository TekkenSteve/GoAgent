package response

// Error is the error response for NATS RPC.
type Error struct {
	Error string `json:"error"`
}
