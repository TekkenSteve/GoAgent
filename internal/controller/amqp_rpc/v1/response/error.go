package response

// Error is the error response for AMQP RPC.
type Error struct {
	Error string `json:"error"`
}
