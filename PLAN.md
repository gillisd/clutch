# WebSocket Request/Response Bridge — Implementation Plan

## Overview

Build a Go library and HTTP API server that bridges HTTP request/response semantics onto a WebSocket connection. A caller makes a `curl POST` with a JSON body, the server forwards it over a persistent WebSocket connection, and returns the WebSocket reply as the HTTP response. Requests and responses are correlated by an `id` field in the message envelope.

## Architecture

```
curl POST ──► HTTP API Server ──► WS Client ──► WebSocket Server
                                    │
                              (matches response
                               by id, returns
                               to HTTP handler)
```

## Core Design: Actor-Based, No Mutexes

The WebSocket client uses a pure channel-based actor model. No `sync.Mutex` anywhere.

### Goroutines (Actors)

There are 4 goroutines:

1. **Caller goroutine** (HTTP handler) — creates a `request` struct with its own `respCh`, sends sequentially to reader then writer, blocks on `respCh`.
2. **Reader actor** — owns the `pending` map (private state). Receives registration requests from callers AND incoming WS messages. Matches responses to pending requests by ID.
3. **Writer actor** — owns the write side of the WS connection. Receives messages from a buffered channel and writes them to the socket one at a time.
4. **WS read pump** — a helper goroutine that turns blocking `conn.ReadJSON()` calls into channel sends, so the reader actor can `select` over both registrations and incoming messages.

### Channel Topology

```
Caller ──(unbuffered)──► Reader actor ──(buffered)──► Writer actor
                            ▲
                            │ (unbuffered)
                        WS read pump
```

- `requests chan request` — **unbuffered**. Caller sends to reader. Unbuffered guarantees the reader has registered the pending request before the caller proceeds to do anything else. This is a happens-before edge.
- `writes chan Message` — **buffered** (e.g. 64). Reader forwards outbound messages here after registering. Multiple callers can enqueue without blocking each other.
- `incoming chan Message` — **unbuffered**. WS read pump sends decoded messages here for the reader to dispatch.
- `errs chan error` — **buffered (1)**. WS read pump sends connection errors here.
- `req.respCh chan Message` — **buffered (1)** per request. Reader sends the matched response here. Buffer of 1 ensures reader never blocks on send.

### Ordering Guarantee (Why No Race)

The caller sends to `requests` (unbuffered) BEFORE the message reaches the writer. Because the channel is unbuffered, the send blocks until the reader picks it up. The reader registers in `pending`, THEN forwards to `writes`. So registration is structurally guaranteed to happen-before the write. Even if the WS server responds instantly, the reader already has the registration.

### Caller Flow

```go
func (c *Client) Request(ctx context.Context, method string, payload any) (*Message, error) {
    id := c.nextID.Add(1)
    data, _ := json.Marshal(payload)

    req := request{
        msg:    Message{ID: id, Method: method, Payload: data},
        respCh: make(chan Message, 1),
    }

    // Sequential send to unbuffered channel — blocks until reader registers it.
    select {
    case c.requests <- req:
    case <-ctx.Done():
        return nil, ctx.Err()
    }

    // Block until response or timeout.
    select {
    case resp := <-req.respCh:
        if resp.Error != "" {
            return nil, fmt.Errorf("remote: %s", resp.Error)
        }
        return &resp, nil
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}
```

### Reader Actor

```go
func (c *Client) readLoop() {
    pending := make(map[uint64]request)  // private state, no mutex
    incoming := make(chan Message)
    errs := make(chan error, 1)

    // WS read pump — turns blocking reads into channel sends.
    go func() {
        for {
            var msg Message
            if err := c.conn.ReadJSON(&msg); err != nil {
                errs <- err
                return
            }
            incoming <- msg
        }
    }()

    for {
        select {
        case req := <-c.requests:
            pending[req.msg.ID] = req
            c.writes <- req.msg  // forward to writer

        case msg := <-incoming:
            if req, ok := pending[msg.ID]; ok {
                req.respCh <- msg
                delete(pending, msg.ID)
            }

        case err := <-errs:
            for _, req := range pending {
                req.respCh <- Message{Error: err.Error()}
            }
            return
        }
    }
}
```

### Writer Actor

```go
func (c *Client) writeLoop() {
    for msg := range c.writes {
        if err := c.conn.WriteJSON(msg); err != nil {
            return  // connection dead
        }
    }
}
```

## Project Structure

```
wsreq/
├── go.mod
├── go.sum
├── ws/
│   ├── client.go       # Client struct, NewClient, Request, Close
│   └── message.go      # Message envelope type
├── cmd/
│   ├── apiserver/
│   │   └── main.go     # HTTP API server
│   └── wsecho/
│       └── main.go     # Echo WebSocket server (for testing)
└── test/
    └── integration.sh  # Integration test script
```

## Types

### `ws/message.go`

```go
package ws

import "encoding/json"

type Message struct {
    ID      uint64          `json:"id"`
    Method  string          `json:"method,omitempty"`
    Payload json.RawMessage `json:"payload,omitempty"`
    Error   string          `json:"error,omitempty"`
}

type request struct {
    msg    Message
    respCh chan Message
}
```

### `ws/client.go`

```go
package ws

type Client struct {
    conn     *websocket.Conn
    nextID   atomic.Uint64
    requests chan request    // unbuffered
    writes   chan Message    // buffered
    done     chan struct{}   // signals shutdown
}
```

Methods:
- `NewClient(conn *websocket.Conn) *Client` — starts readLoop and writeLoop goroutines.
- `Request(ctx context.Context, method string, payload any) (*Message, error)` — sends a request and blocks until response.
- `Close() error` — closes the WS connection, which causes readLoop to exit via the error path, which unblocks all pending callers.

## HTTP API Server (`cmd/apiserver/main.go`)

- Accepts a flag or env var for the WS server URL to connect to (default: `ws://localhost:9090/ws`).
- Accepts a flag for HTTP listen address (default: `:8080`).
- On startup: dials the WebSocket server, creates a `ws.Client`.
- Single endpoint: `POST /request`
  - Reads JSON body as `json.RawMessage`.
  - Optionally reads `method` from a query param or JSON field.
  - Calls `client.Request(ctx, method, payload)` with the request's context (so client disconnects = cancellation).
  - Returns the response payload as JSON with appropriate status code.

Example usage:
```bash
curl -X POST http://localhost:8080/request \
  -H "Content-Type: application/json" \
  -d '{"method": "echo", "payload": {"hello": "world"}}'
```

## Echo WebSocket Server (`cmd/wsecho/main.go`)

A minimal WebSocket server for testing:
- Upgrades HTTP connections to WebSocket at `/ws`.
- Reads messages, echoes them back with the same `id` but wraps the payload in `{"echo": <original_payload>}`.
- This is only for testing; the real WS server will be external.

## Integration Test (`test/integration.sh`)

**Critical requirement:** All server processes MUST be launched via process substitution so they die when the script exits. Do NOT use `&` and `wait`.

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_DIR"

# Build binaries
go build -o bin/wsecho ./cmd/wsecho
go build -o bin/apiserver ./cmd/apiserver

# Start echo WS server via process substitution (auto-killed on script exit)
exec 3< <(./bin/wsecho -addr :9090)
# Give it a moment to start
sleep 1

# Start API server via process substitution
exec 4< <(./bin/apiserver -addr :8080 -ws ws://localhost:9090/ws)
sleep 1

# Test 1: Basic echo
echo "=== Test 1: Basic echo ==="
RESPONSE=$(curl -s -X POST http://localhost:8080/request \
  -H "Content-Type: application/json" \
  -d '{"method": "echo", "payload": {"hello": "world"}}')

echo "Response: $RESPONSE"

# Validate response contains expected data
if echo "$RESPONSE" | grep -q '"hello"'; then
  echo "PASS"
else
  echo "FAIL"
  exit 1
fi

# Test 2: Concurrent requests
echo "=== Test 2: Concurrent requests ==="
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8080/request \
    -H "Content-Type: application/json" \
    -d "{\"method\": \"echo\", \"payload\": {\"n\": $i}}" &
done

# Wait for all curl processes (these are fine to wait on — they're curl, not servers)
wait

echo "=== All tests passed ==="
```

**Important notes for the integration test:**
- Servers are started with `exec N< <(./command)` — process substitution. When the shell exits, the subshells running the servers are killed.
- Do NOT use `./server &` — this risks calling `wait` later and hanging forever.
- Only `wait` on the curl background jobs in Test 2, never on server processes.
- The `sleep 1` calls give servers time to bind their ports. A more robust version could poll with retries.

## Shutdown / Cleanup

- `Client.Close()` closes the underlying WS connection.
- This causes `conn.ReadJSON()` in the read pump to return an error.
- The read pump sends the error to `errs` and exits.
- The reader actor receives from `errs`, sends error responses to all pending callers, and exits.
- The API server should `defer client.Close()` and can close the `writes` channel to signal the writer to exit via `range`.

## Error Handling

- WS connection drops: all pending callers get an error response via their `respCh`.
- Context cancellation: caller's `select` picks up `ctx.Done()` and returns `ctx.Err()`.
- Write failure: writer goroutine exits. Subsequent sends to `writes` will block/deadlock unless the reader also shuts down. Consider having the writer signal the reader to shut down on write error (e.g. close a shared `done` channel).

## Dependencies

- `github.com/gorilla/websocket` — WebSocket implementation.
- Standard library only otherwise.

## Summary of Key Design Decisions

1. **No mutexes.** All shared state is owned by a single goroutine (the reader actor) and accessed only through channels.
2. **Unbuffered registration channel.** Provides a structural happens-before guarantee that registration precedes the write.
3. **Buffered write channel.** Decouples callers from the writer so they don't block each other.
4. **Per-request response channel (buffered 1).** Each caller blocks on its own channel. Buffer of 1 means the reader never blocks when dispatching.
5. **Process substitution in tests.** Servers auto-terminate when the test script exits. No `&` + `wait` on servers.
