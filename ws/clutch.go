package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

// Conn is the interface for the underlying WebSocket connection.
// *websocket.Conn from gorilla/websocket satisfies this implicitly.
type Conn interface {
	ReadMessage() (messageType int, p []byte, err error)
	WriteMessage(messageType int, data []byte) error
	Close() error
}

// Clutch correlates request/response pairs over a single WebSocket connection.
// It uses an actor-based concurrency model: the readLoop owns all mutable
// state (the pending-request map), and communication happens exclusively
// through channels. The caller owns the wire format — Clutch only manages
// the correlation ID field.
type Clutch struct {
	dial     func() (Conn, error) // nil = no reconnection
	conn     Conn
	idField  string
	nextID   atomic.Uint64
	requests chan request          // unbuffered — registration before write
	writes   chan json.RawMessage  // buffered 64 — decouple callers from writer
	cancels  chan uint64           // buffered 64 — cancel notifications never block callers
	done     chan struct{}
}

// Option configures a Clutch.
type Option func(*Clutch)

// WithDialer enables automatic reconnection. When the underlying connection
// drops, Clutch will call dial to establish a new connection, retrying with
// exponential backoff. Pending requests at the time of disconnection receive
// an error; new requests made after reconnection succeed normally.
func WithDialer(dial func() (Conn, error)) Option {
	return func(c *Clutch) {
		c.dial = dial
	}
}

// NewClutch wraps an existing WebSocket connection and starts the
// read and write actor goroutines. The idField parameter names the
// top-level JSON field used for request/response correlation.
func NewClutch(conn Conn, idField string, opts ...Option) *Clutch {
	c := &Clutch{
		conn:     conn,
		idField:  idField,
		requests: make(chan request),
		writes:   make(chan json.RawMessage, 64),
		cancels:  make(chan uint64, 64),
		done:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	go c.readLoop()
	return c
}

// Request sends raw JSON over the WebSocket, injecting a correlation ID,
// and blocks until a response with the matching ID arrives or the context
// is cancelled. The caller owns the wire format — the library does not
// interpret the response contents.
func (c *Clutch) Request(ctx context.Context, msg json.RawMessage) (json.RawMessage, error) {
	id := c.nextID.Add(1)

	raw, err := injectID(c.idField, msg, id)
	if err != nil {
		return nil, fmt.Errorf("inject ID: %w", err)
	}

	req := request{
		id:     id,
		raw:    raw,
		respCh: make(chan json.RawMessage, 1), // buffered 1 — reader never blocks
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
	case c.writes <- req.raw:
	case <-c.done:
		return nil, errors.New("client closed")
	case <-ctx.Done():
		select {
		case c.cancels <- id:
		default:
		}
		return nil, ctx.Err()
	}

	// Wait for the response.
	select {
	case resp, ok := <-req.respCh:
		if !ok {
			return nil, errors.New("client closed")
		}
		return resp, nil
	case <-c.done:
		return nil, errors.New("client closed")
	case <-ctx.Done():
		select {
		case c.cancels <- id:
		default:
		}
		return nil, ctx.Err()
	}
}

// readLoop is the reader actor. It exclusively owns the pending map and
// manages per-connection goroutines (read pump + writeLoop). When the
// connection drops and a dial function is configured, it reconnects
// automatically with exponential backoff.
func (c *Clutch) readLoop() {
	pending := make(map[uint64]request)

	defer func() {
		select {
		case <-c.done:
		default:
			close(c.done)
		}
		c.conn.Close()
		for _, req := range pending {
			close(req.respCh)
		}
	}()

	for {
		incoming := make(chan json.RawMessage) // unbuffered — tight backpressure
		errs := make(chan error, 1)            // buffered 1 — pump never blocks
		connDone := make(chan struct{})

		// WS read pump goroutine.
		go func() {
			for {
				_, data, err := c.conn.ReadMessage()
				if err != nil {
					errs <- err
					return
				}
				select {
				case incoming <- json.RawMessage(data):
				case <-c.done:
					return
				case <-connDone:
					return
				}
			}
		}()

		// Writer goroutine (per-connection).
		go c.writeLoop(c.conn, connDone)

		shouldReconnect := false
	loop:
		for {
			select {
			case req := <-c.requests:
				pending[req.id] = req

			case raw := <-incoming:
				id, err := extractID(c.idField, raw)
				if err != nil {
					slog.Warn("dropping message: cannot extract correlation ID",
						"error", err,
						"idField", c.idField,
					)
					continue
				}
				if req, ok := pending[id]; ok {
					req.respCh <- raw
					delete(pending, id)
				}

			case id := <-c.cancels:
				delete(pending, id)

			case <-errs:
				shouldReconnect = c.dial != nil
				break loop

			case <-connDone:
				shouldReconnect = c.dial != nil
				break loop

			case <-c.done:
				return
			}
		}

		// Stop per-connection goroutines.
		select {
		case <-connDone:
		default:
			close(connDone)
		}
		c.conn.Close()

		// Fail all pending requests (server lost their state).
		for _, req := range pending {
			close(req.respCh)
		}
		pending = make(map[uint64]request)

		// Drain buffered writes from the dead connection.
	drain:
		for {
			select {
			case <-c.writes:
			default:
				break drain
			}
		}

		if !shouldReconnect {
			return
		}

		if !c.reconnect() {
			return
		}
	}
}

// writeLoop is the writer actor for a single connection. It sends raw JSON
// messages and signals connDone if a write error occurs.
func (c *Clutch) writeLoop(conn Conn, connDone chan struct{}) {
	defer func() {
		select {
		case <-connDone:
		default:
			close(connDone)
		}
	}()
	for {
		select {
		case raw := <-c.writes:
			if err := conn.WriteMessage(1, raw); err != nil { // 1 = text frame
				return
			}
		case <-connDone:
			return
		case <-c.done:
			return
		}
	}
}

// reconnect dials a new connection with exponential backoff.
// Returns true on success (c.conn is replaced), false if c.done was closed.
func (c *Clutch) reconnect() bool {
	backoff := 250 * time.Millisecond
	maxBackoff := 10 * time.Second

	for {
		select {
		case <-time.After(backoff):
		case <-c.done:
			return false
		}

		conn, err := c.dial()
		if err != nil {
			slog.Warn("reconnect failed", "error", err)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		c.conn = conn
		slog.Info("reconnected")
		return true
	}
}

// Close shuts down the clutch, signalling both actor goroutines to exit.
// The underlying connection is closed by readLoop when it detects the
// shutdown signal. Close is idempotent.
func (c *Clutch) Close() error {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	return nil
}
