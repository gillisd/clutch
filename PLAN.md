# WebSocket Request/Response Correlator — Implementation Plan

## Overview

A Go library and HTTP API server that correlates request/response pairs over a WebSocket connection. The library (`ws.Clutch`) is wire-format agnostic — it injects a correlation ID into outbound JSON, extracts it from inbound JSON, and returns the raw response. The caller owns the wire format.

## Architecture

```
curl POST ──► HTTP API Server ──► ws.Clutch ──► WebSocket Server
                                      │
                                (injects ID, matches
                                 response by ID,
                                 returns raw JSON)
```

## Core Design: Actor-Based, No Mutexes

The `Clutch` uses a pure channel-based actor model. No `sync.Mutex` anywhere.

### Goroutines (Actors)

There are 4 goroutines:

1. **Caller goroutine** — creates a `request` struct with its own `respCh`, sends sequentially to reader then writer, blocks on `respCh`.
2. **Reader actor** — owns the `pending` map (private state). Receives registration requests from callers, incoming WS messages, and cancel notifications. Matches responses to pending requests by ID.
3. **Writer actor** — owns the write side of the WS connection. Receives raw JSON from a buffered channel and writes it to the socket.
4. **WS read pump** — a helper goroutine that turns blocking `conn.ReadMessage()` calls into channel sends, so the reader actor can `select` over both registrations and incoming messages.

### Channel Topology

```
Caller ──(unbuffered)──► Reader actor
Caller ──(buffered)────► Writer actor
Caller ──(buffered)────► Reader actor (cancels)
WS read pump ──(unbuffered)──► Reader actor
```

- `requests chan request` — **unbuffered**. Caller sends to reader. Unbuffered guarantees the reader has registered the pending request before the caller proceeds. This is a happens-before edge.
- `writes chan json.RawMessage` — **buffered** (64). Caller hands outbound messages to the writer actor. Multiple callers can enqueue without blocking each other.
- `cancels chan uint64` — **buffered** (64). Caller sends cancelled request IDs for cleanup by the reader actor. Non-blocking best-effort send prevents callers from hanging.
- `incoming chan json.RawMessage` — **unbuffered**. WS read pump sends raw messages here for the reader to dispatch.
- `errs chan error` — **buffered (1)**. WS read pump sends connection errors here.
- `req.respCh chan json.RawMessage` — **buffered (1)** per request. Reader sends the matched response here. Closed on shutdown.

### Ordering Guarantee (Why No Race)

The caller sends to `requests` (unbuffered) BEFORE the message reaches the writer. Because the channel is unbuffered, the send blocks until the reader picks it up. The reader registers in `pending`, then the caller sends to `writes`. So registration is structurally guaranteed to happen-before the write. Even if the WS server responds instantly, the reader already has the registration.

### Caller Flow (3-Select Design)

The caller performs three sequential selects:
1. **Register** — send to unbuffered `requests` channel (happens-before write).
2. **Write** — send to buffered `writes` channel (handed to writer actor).
3. **Wait** — block on per-request `respCh` for the response.

Cancel paths after step 1 send the request ID to the `cancels` channel (best-effort, non-blocking) so the reader actor can clean up the pending map entry.

```go
func (c *Clutch) Request(ctx context.Context, msg json.RawMessage) (json.RawMessage, error) {
    id := c.nextID.Add(1)
    raw, err := injectID(c.idField, msg, id)
    if err != nil {
        return nil, fmt.Errorf("inject ID: %w", err)
    }

    req := request{
        id:     id,
        raw:    raw,
        respCh: make(chan json.RawMessage, 1),
    }

    // Step 1: Register with the reader actor.
    select {
    case c.requests <- req:
    case <-c.done:
        return nil, errors.New("client closed")
    case <-ctx.Done():
        return nil, ctx.Err()
    }

    // Step 2: Hand the message to the writer actor.
    select {
    case c.writes <- req.raw:
    case <-c.done:
        return nil, errors.New("client closed")
    case <-ctx.Done():
        select { case c.cancels <- id: default: }
        return nil, ctx.Err()
    }

    // Step 3: Wait for the response.
    select {
    case resp, ok := <-req.respCh:
        if !ok {
            return nil, errors.New("client closed")
        }
        return resp, nil
    case <-c.done:
        return nil, errors.New("client closed")
    case <-ctx.Done():
        select { case c.cancels <- id: default: }
        return nil, ctx.Err()
    }
}
```

### Reader Actor

The reader actor owns the `pending` map and handles the `cancels` channel to clean up entries for cancelled requests. On exit, its defer closes `c.done` (if not already closed) and drains all pending requests by closing their response channels.

Messages with unparseable or missing IDs are logged and dropped.

```go
func (c *Clutch) readLoop() {
    pending := make(map[uint64]request)
    incoming := make(chan json.RawMessage)
    errs := make(chan error, 1)

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
            }
        }
    }()

    defer func() {
        select {
        case <-c.done:
        default:
            close(c.done)
        }
        for _, req := range pending {
            close(req.respCh)
        }
    }()

    for {
        select {
        case req := <-c.requests:
            pending[req.id] = req

        case raw := <-incoming:
            id, err := extractID(c.idField, raw)
            if err != nil {
                slog.Warn("dropping message: cannot extract correlation ID", ...)
                continue
            }
            if req, ok := pending[id]; ok {
                req.respCh <- raw
                delete(pending, id)
            }

        case id := <-c.cancels:
            delete(pending, id)

        case <-errs:
            return

        case <-c.done:
            return
        }
    }
}
```

### Writer Actor

The writer actor closes `c.done` on exit (via defer) so that a write error propagates to the rest of the system. This mirrors the reader actor's defer pattern and prevents callers from hanging indefinitely when the write side dies.

```go
func (c *Clutch) writeLoop() {
    defer func() {
        select {
        case <-c.done:
        default:
            close(c.done)
        }
    }()
    for {
        select {
        case raw := <-c.writes:
            if err := c.conn.WriteMessage(websocket.TextMessage, raw); err != nil {
                return
            }
        case <-c.done:
            return
        }
    }
}
```

## Project Structure

```
clutch/
├── go.mod
├── go.sum
├── ws/
│   ├── clutch.go       # Clutch struct, NewClutch, Request, Close
│   ├── clutch_test.go  # Unit tests
│   └── message.go      # Internal helpers: injectID, extractID, request struct
├── cmd/
│   ├── apiserver/
│   │   └── main.go     # HTTP API server (raw JSON passthrough)
│   └── wsecho/
│       └── main.go     # Echo WebSocket server (for testing)
└── test/
    └── integration.sh  # Integration test script
```

## Types

### `ws/message.go`

```go
package ws

type request struct {
    id     uint64
    raw    json.RawMessage
    respCh chan json.RawMessage
}

func injectID(idField string, data json.RawMessage, id uint64) (json.RawMessage, error)
func extractID(idField string, data json.RawMessage) (uint64, error)
```

### `ws/clutch.go`

```go
package ws

type Clutch struct {
    conn     *websocket.Conn
    idField  string
    nextID   atomic.Uint64
    requests chan request          // unbuffered
    writes   chan json.RawMessage  // buffered 64
    cancels  chan uint64           // buffered 64
    done     chan struct{}
}
```

Methods:
- `NewClutch(conn *websocket.Conn, idField string) *Clutch` — starts readLoop and writeLoop goroutines.
- `Request(ctx context.Context, msg json.RawMessage) (json.RawMessage, error)` — injects correlation ID, sends, and blocks until response.
- `Close() error` — idempotent shutdown.

## HTTP API Server (`cmd/apiserver/main.go`)

- Dials the WebSocket server, creates a `ws.Clutch` with `"id"` as the correlation field.
- Single endpoint: `POST /request` — reads raw JSON body, passes through to `clutch.Request()`, writes raw response.
- Pure transparent proxy — knows nothing about the application-level schema.

## Echo WebSocket Server (`cmd/wsecho/main.go`)

- Defines its own local `message` struct (independent of the `ws` package).
- Reads messages, echoes them back with the same `id`, wrapping the payload in `{"echo": <original>}`.

## Shutdown / Cleanup

- `Clutch.Close()` closes `c.done` and the underlying WS connection.
- This causes `conn.ReadMessage()` in the read pump to return an error.
- The reader actor exits, closing all pending response channels.
- Both actor goroutines exit cleanly via their defer blocks.
- Write failure: writer goroutine exits and closes `c.done` via its defer, which causes the reader to exit. All callers unblock promptly.

## Error Handling

- WS connection drops: all pending callers get "client closed" error (via channel close).
- Context cancellation: caller's `select` picks up `ctx.Done()` and returns `ctx.Err()`. Pending entry is cleaned up via `cancels` channel.
- Write failure: writer closes `c.done`, reader exits, all callers unblock.
- Application-level errors: the library does not interpret them. Response JSON is returned as-is.

## Dependencies

- `github.com/gorilla/websocket` — WebSocket implementation (caller's dependency).
- Standard library only otherwise.

## Summary of Key Design Decisions

1. **Wire-format agnostic.** The library works with raw `json.RawMessage`. The caller owns the wire format. Only the correlation ID field name is configured.
2. **No mutexes.** All shared state is owned by a single goroutine (the reader actor) and accessed only through channels.
3. **Unbuffered registration channel.** Provides a structural happens-before guarantee that registration precedes the write.
4. **Buffered write channel.** Decouples callers from the writer so they don't block each other.
5. **Per-request response channel (buffered 1).** Each caller blocks on its own channel. Closed on shutdown to unblock callers.
6. **Gorilla as external dependency.** The caller owns dialing and brings their own `*websocket.Conn`. Connection lifecycle stays out of the library.
