# clutch

Request/response correlation over a single WebSocket connection.

`clutch` ships two things: **`clutchpedal`**, an HTTP-to-WebSocket bridge CLI with auto-reconnection, and **`ws.Clutch`**, the Go library it's built on. The library pairs outbound JSON messages with their inbound responses using a configurable ID field — the caller owns the wire format, the library only manages correlation.

## Architecture

```
curl POST ──> clutchpedal ──> ws.Clutch ──> WebSocket Server
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
│   ├── clutchpedal/     # HTTP-to-WebSocket bridge CLI
│   ├── apiserver/       # Reference HTTP-to-WS bridge
│   └── wsecho/          # Echo WebSocket server (for testing)
└── test/
    └── integration.sh   # End-to-end integration tests
```

## Usage

### clutchpedal

`clutchpedal` is a standalone HTTP-to-WebSocket bridge CLI with auto-reconnection and graceful shutdown.

```
clutchpedal <bind> [--upstream ws://...] [--id-field id] [--insecure]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--upstream` | _(stdio)_ | WebSocket server URL; omit for stdin/stdout mode |
| `--id-field` | `id` | Correlation ID field name |
| `--insecure` | `false` | Skip TLS certificate verification for `wss://` endpoints |

Build it:

```bash
go build -o bin/clutchpedal ./cmd/clutchpedal
```

Or build all binaries at once:

```bash
./build.sh
```

**WebSocket mode** — proxy HTTP requests to an upstream WebSocket server:

```bash
bin/clutchpedal localhost:8080 --upstream ws://localhost:9090/ws

curl -s -X POST http://localhost:8080/ \
  -H 'Content-Type: application/json' \
  -d '{"method":"greet","payload":{"msg":"hello"}}'
```

The connection automatically reconnects with exponential backoff if the upstream server restarts.

**Stdio mode** — omit `--upstream` to read/write newline-delimited JSON on stdin/stdout:

```bash
bin/clutchpedal localhost:8080
```

In this mode, `clutchpedal` bridges HTTP requests to a process connected via pipes rather than a WebSocket server.

**TLS** — use `--insecure` to skip certificate verification for `wss://` endpoints:

```bash
bin/clutchpedal localhost:8080 --upstream wss://server:9090/ws --insecure
```

**Custom ID field**:

```bash
bin/clutchpedal localhost:8080 --upstream ws://localhost:9090/ws --id-field request_id
```

`clutchpedal` shuts down gracefully on SIGINT/SIGTERM.

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
./test/integration.sh
```

## Dependencies

- [gorilla/websocket](https://github.com/gorilla/websocket) -- WebSocket implementation (caller's dependency)
- Standard library only otherwise
