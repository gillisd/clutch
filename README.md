# clutch

Request/response correlation over a single WebSocket connection.

`clutch` provides `ws.Clutch` — a transport-layer correlator that pairs outbound JSON messages with their inbound responses using a configurable ID field. The caller owns the wire format. The library only manages the correlation ID.

## Architecture

```
curl POST ──> HTTP API Server ──> ws.Clutch ──> WebSocket Server
                                      │
                                (injects ID, matches
                                 response by ID,
                                 returns raw JSON)
```

`ws.Clutch` uses an actor-based concurrency model with zero mutexes. All mutable state (the pending-request map) is owned exclusively by the reader goroutine, and communication happens through channels.

## Project Structure

```
clutch/
├── ws/
│   ├── clutch.go        # Clutch, NewClutch, Request, Close
│   ├── clutch_test.go   # Unit tests
│   └── message.go       # Internal helpers (injectID, extractID)
├── cmd/
│   ├── apiserver/       # HTTP-to-WebSocket bridge
│   └── wsecho/          # Echo WebSocket server (for testing)
└── test/
    └── integration.sh   # End-to-end integration tests
```

## Usage

### As a library

```go
import (
    "github.com/gorilla/websocket"
    "clutch/ws"
)

// Caller owns dialing.
conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:9090/ws", nil)
if err != nil {
    log.Fatal(err)
}

// "id" is the top-level JSON field used for correlation.
clutch := ws.NewClutch(conn, "id")
defer clutch.Close()

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// Send any JSON. The library injects the correlation ID automatically.
resp, err := clutch.Request(ctx, json.RawMessage(`{
    "method": "greet",
    "payload": {"name": "world"}
}`))
if err != nil {
    log.Fatal(err)
}

// resp is raw JSON — the full response from the server.
fmt.Println(string(resp))
```

Concurrent calls to `Request` are safe — all requests share a single WebSocket connection.

### Different ID field

```go
// Server uses "request_id" instead of "id":
clutch := ws.NewClutch(conn, "request_id")
```

### Wire format

The wire format is caller-defined. The library only adds the correlation ID:

```
→  {"id":1,"method":"greet","payload":{"name":"world"}}
←  {"id":1,"payload":{"greeting":"hello, world"}}
```

Application-level errors in the response JSON are the caller's responsibility to interpret.

### Running the servers

Start the echo WebSocket server:

```bash
go run ./cmd/wsecho -addr :9090
```

Start the HTTP API bridge:

```bash
go run ./cmd/apiserver -addr :8080 -ws ws://localhost:9090/ws
```

Make a request:

```bash
curl -s -X POST http://localhost:8080/request \
  -H 'Content-Type: application/json' \
  -d '{"method":"greet","payload":{"msg":"hello"}}'
```

## Testing

Unit tests (with race detector):

```bash
go test -race ./...
```

Integration tests:

```bash
bash test/integration.sh
```

## Dependencies

- [gorilla/websocket](https://github.com/gorilla/websocket) -- WebSocket implementation (caller's dependency)
- Standard library only otherwise
