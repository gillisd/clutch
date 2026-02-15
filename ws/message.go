package ws

import "encoding/json"

// Message is the envelope sent over the WebSocket connection.
type Message struct {
	ID      uint64          `json:"id"`
	Method  string          `json:"method,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// request is an internal envelope pairing a message with a response channel.
type request struct {
	msg    Message
	respCh chan Message
}
