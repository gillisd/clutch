package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/gorilla/websocket"
)

// Client sends request/response pairs over a single WebSocket connection.
// It uses an actor-based concurrency model: the readLoop owns all mutable
// state (the pending-request map), and communication happens exclusively
// through channels.
type Client struct {
	conn     *websocket.Conn
	nextID   atomic.Uint64
	requests chan request // unbuffered — registration before write
	writes   chan Message // buffered 64 — decouple callers from writer
	done     chan struct{}
}

// NewClient wraps an existing WebSocket connection and starts the
// read and write actor goroutines.
func NewClient(conn *websocket.Conn) *Client {
	c := &Client{
		conn:     conn,
		requests: make(chan request),
		writes:   make(chan Message, 64),
		done:     make(chan struct{}),
	}
	go c.readLoop()
	go c.writeLoop()
	return c
}

// Request sends a method call over the WebSocket and blocks until a response
// with the matching ID arrives or the context is cancelled.
func (c *Client) Request(ctx context.Context, method string, payload any) (*Message, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	id := c.nextID.Add(1)
	req := request{
		msg: Message{
			ID:      id,
			Method:  method,
			Payload: json.RawMessage(raw),
		},
		respCh: make(chan Message, 1), // buffered 1 — reader never blocks
	}

	// Register the request with the reader actor. The unbuffered requests
	// channel guarantees registration completes before we send the write.
	select {
	case c.requests <- req:
	case <-c.done:
		return nil, errors.New("client closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Hand the message to the writer actor.
	select {
	case c.writes <- req.msg:
	case <-c.done:
		return nil, errors.New("client closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Wait for the response.
	select {
	case resp := <-req.respCh:
		if resp.Error != "" {
			return nil, errors.New(resp.Error)
		}
		return &resp, nil
	case <-c.done:
		return nil, errors.New("client closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// readLoop is the reader actor. It exclusively owns the pending map and
// multiplexes over new registrations, incoming WS messages, read errors,
// and the done signal.
func (c *Client) readLoop() {
	pending := make(map[uint64]request)

	incoming := make(chan Message)    // unbuffered — tight backpressure
	errs := make(chan error, 1)       // buffered 1 — pump never blocks

	// WS read pump goroutine.
	go func() {
		for {
			var msg Message
			if err := c.conn.ReadJSON(&msg); err != nil {
				errs <- err
				return
			}
			select {
			case incoming <- msg:
			case <-c.done:
				return
			}
		}
	}()

	defer func() {
		// Signal all goroutines that the client is done.
		select {
		case <-c.done:
		default:
			close(c.done)
		}
		// Drain all pending requests with an error.
		for _, req := range pending {
			req.respCh <- Message{Error: "client closed"}
		}
	}()

	for {
		select {
		case req := <-c.requests:
			pending[req.msg.ID] = req

		case msg := <-incoming:
			if req, ok := pending[msg.ID]; ok {
				req.respCh <- msg
				delete(pending, msg.ID)
			}

		case <-errs:
			return

		case <-c.done:
			return
		}
	}
}

// writeLoop is the writer actor. It ranges over the writes channel and
// serialises messages onto the WebSocket connection.
func (c *Client) writeLoop() {
	for {
		select {
		case msg := <-c.writes:
			if err := c.conn.WriteJSON(msg); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

// Close shuts down the client, signalling both actor goroutines to exit
// and closing the underlying WebSocket connection.
func (c *Client) Close() error {
	select {
	case <-c.done:
		// Already closed.
		return nil
	default:
		close(c.done)
	}
	return c.conn.Close()
}
