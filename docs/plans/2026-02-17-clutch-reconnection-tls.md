# Clutch Reconnection & TLS Support Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix two bugs — Clutch cannot survive upstream restarts, and clutchpedal errors on wss:// endpoints.

**Architecture:** Add an optional `WithDialer` option to `NewClutch` that enables automatic reconnection with exponential backoff. Refactor the actor loops so `readLoop` manages per-connection read-pump and write goroutines, coordinated through a per-connection `connDone` channel. For TLS, switch clutchpedal from `websocket.DefaultDialer` to a custom `websocket.Dialer` with configurable `TLSClientConfig`, exposed via an `--insecure` flag.

**Tech Stack:** Go 1.23, gorilla/websocket, `net/http/httptest` (TLS test servers)

---

### Task 1: TLS round-trip test (Issue #2)

Prove that Clutch works over `wss://` by adding a test that uses `httptest.NewTLSServer`.

**Files:**
- Modify: `ws/clutch_test.go`

**Step 1: Write the TLS test helpers and test**

Add after the existing `dialClutch` helper (~line 60):

```go
func echoTLSServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		for {
			var msg testMessage
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}
			if err := conn.WriteJSON(msg); err != nil {
				return
			}
		}
	}))
	return srv, srv.Close
}

func dialClutchTLS(t *testing.T, srv *httptest.Server) *ws.Clutch {
	t.Helper()
	url := "wss" + strings.TrimPrefix(srv.URL, "https")
	dialer := websocket.Dialer{
		TLSClientConfig: srv.Client().Transport.(*http.Transport).TLSClientConfig,
	}
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial tls: %v", err)
	}
	return ws.NewClutch(conn, "id")
}
```

Add the test after the existing tests:

```go
func TestRequest_TLS(t *testing.T) {
	srv, cleanup := echoTLSServer(t)
	defer cleanup()

	clutch := dialClutchTLS(t, srv)
	defer clutch.Close()

	resp, err := clutch.Request(context.Background(), json.RawMessage(`{"method":"tls.echo","payload":{"secure":true}}`))
	if err != nil {
		t.Fatalf("request over TLS: %v", err)
	}

	var got map[string]json.RawMessage
	if err := json.Unmarshal(resp, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var method string
	if err := json.Unmarshal(got["method"], &method); err != nil {
		t.Fatalf("unmarshal method: %v", err)
	}
	if method != "tls.echo" {
		t.Errorf("method = %q, want %q", method, "tls.echo")
	}
}
```

Also add `"crypto/tls"` to the import block (needed later for the insecure dialer tests, and to ensure the `net/http` TLS types resolve).

**Step 2: Run the test**

```bash
cd /workspace/wsreq && go test ./ws/ -run TestRequest_TLS -v
```

Expected: PASS — the Clutch library itself is TLS-agnostic (it just uses the `Conn` interface). This test confirms wss:// works when the dialer is configured correctly.

**Step 3: Commit**

```bash
git add ws/clutch_test.go
git commit -m "test: add TLS round-trip test for wss:// connections"
```

---

### Task 2: Fix clutchpedal for wss:// (Issue #2)

Replace `websocket.DefaultDialer` with a custom dialer that has TLS configuration. Add `--insecure` flag.

**Files:**
- Modify: `cmd/clutchpedal/main.go`

**Step 1: Add --insecure flag and custom dialer**

Add `"crypto/tls"` to the import block.

Replace lines 26-45 (flag parsing through connection setup):

```go
	upstream := flag.String("upstream", "", "WebSocket URL (omit for stdio)")
	idField := flag.String("id-field", "id", "correlation ID field name")
	insecure := flag.Bool("insecure", false, "skip TLS certificate verification (for wss://)")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: clutchpedal <bind> [--upstream ws://...] [--id-field id] [--insecure]\n")
		os.Exit(1)
	}
	bind := flag.Arg(0)

	var conn ws.Conn
	if *upstream != "" {
		dialer := &websocket.Dialer{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: *insecure,
			},
		}
		wc, _, err := dialer.Dial(*upstream, nil)
		if err != nil {
			log.Fatalf("dial ws: %v", err)
		}
		conn = wc
	} else {
		conn = ws.NewStdioConn(os.Stdin, os.Stdout)
	}
```

**Step 2: Verify it builds**

```bash
cd /workspace/wsreq && go build ./cmd/clutchpedal/
```

Expected: Builds without errors.

**Step 3: Commit**

```bash
git add cmd/clutchpedal/main.go
git commit -m "fix: support wss:// endpoints with --insecure flag in clutchpedal"
```

---

### Task 3: Add Option type and WithDialer API

Add the functional options pattern to `NewClutch` so callers can provide a dial function for reconnection.

**Files:**
- Modify: `ws/clutch.go`
- Modify: `ws/clutch_test.go`

**Step 1: Write failing test that uses WithDialer**

Add to `clutch_test.go`:

```go
func TestNewClutch_WithDialer(t *testing.T) {
	srv, cleanup := echoServer(t)
	defer cleanup()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	dial := func() (ws.Conn, error) {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		return conn, err
	}

	conn, err := dial()
	if err != nil {
		t.Fatalf("initial dial: %v", err)
	}

	clutch := ws.NewClutch(conn, "id", ws.WithDialer(dial))
	defer clutch.Close()

	resp, err := clutch.Request(context.Background(), json.RawMessage(`{"method":"test.dialer"}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	var got map[string]json.RawMessage
	if err := json.Unmarshal(resp, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var method string
	if err := json.Unmarshal(got["method"], &method); err != nil {
		t.Fatalf("unmarshal method: %v", err)
	}
	if method != "test.dialer" {
		t.Errorf("method = %q, want %q", method, "test.dialer")
	}
}
```

**Step 2: Run it to verify it fails**

```bash
cd /workspace/wsreq && go test ./ws/ -run TestNewClutch_WithDialer -v
```

Expected: FAIL — `ws.WithDialer` does not exist.

**Step 3: Add Option type, WithDialer, and update NewClutch**

In `ws/clutch.go`, add after the `Clutch` struct definition (after line 33):

```go
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
```

Add the `dial` field to the `Clutch` struct (after `conn` on line 26):

```go
type Clutch struct {
	dial     func() (Conn, error) // nil = no reconnection
	conn     Conn
	idField  string
	nextID   atomic.Uint64
	requests chan request
	writes   chan json.RawMessage
	cancels  chan uint64
	done     chan struct{}
}
```

Update `NewClutch` signature (line 38) to accept options:

```go
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
	go c.writeLoop()
	return c
}
```

**Step 4: Run the test**

```bash
cd /workspace/wsreq && go test ./ws/ -run TestNewClutch_WithDialer -v
```

Expected: PASS.

**Step 5: Run all existing tests for regression**

```bash
cd /workspace/wsreq && go test ./ws/ -v -count=1
```

Expected: All tests PASS (existing callers unaffected by the variadic option).

**Step 6: Commit**

```bash
git add ws/clutch.go ws/clutch_test.go
git commit -m "feat: add WithDialer option to NewClutch for reconnection support"
```

---

### Task 4: Refactor loops for per-connection lifecycle

Restructure `readLoop` and `writeLoop` so that:
- `readLoop` is the outer lifecycle manager (handles reconnection)
- `writeLoop` is started per-connection from `readLoop` with a `connDone` channel
- Write errors signal `connDone` (not `c.done`), allowing reconnection instead of global shutdown

This is a pure refactor — no reconnection logic yet. Existing tests must still pass.

**Files:**
- Modify: `ws/clutch.go`

**Step 1: Run existing tests (baseline)**

```bash
cd /workspace/wsreq && go test ./ws/ -v -count=1 -race
```

Expected: All PASS.

**Step 2: Refactor readLoop and writeLoop**

Replace the entire `readLoop` method (lines 114-178) and `writeLoop` method (lines 182-200) with:

```go
func (c *Clutch) readLoop() {
	pending := make(map[uint64]request)

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
		incoming := make(chan json.RawMessage)
		errs := make(chan error, 1)
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

		// Fail all pending requests.
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

		// Reconnection will be added in the next task.
		return
	}
}

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
			if err := conn.WriteMessage(1, raw); err != nil {
				return
			}
		case <-connDone:
			return
		case <-c.done:
			return
		}
	}
}
```

Update `NewClutch` to only start `readLoop` (remove the `writeLoop` goroutine since `readLoop` now starts it):

```go
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
```

**Step 3: Run all tests**

```bash
cd /workspace/wsreq && go test ./ws/ -v -count=1 -race
```

Expected: All PASS. This is a pure refactor — behavior is identical because `shouldReconnect` is always false without `WithDialer` (falls through to `return`).

**Step 4: Commit**

```bash
git add ws/clutch.go
git commit -m "refactor: per-connection lifecycle in readLoop/writeLoop for reconnection support"
```

---

### Task 5: Implement reconnection

Add the `reconnect` method with exponential backoff and wire it into the `readLoop` outer loop.

**Files:**
- Modify: `ws/clutch.go`
- Modify: `ws/clutch_test.go`

**Step 1: Write the failing reconnection test**

Add to `clutch_test.go`:

```go
func TestRequest_ReconnectAfterServerRestart(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var msg testMessage
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}
			if err := conn.WriteJSON(msg); err != nil {
				return
			}
		}
	})

	// Start first server.
	srv1 := httptest.NewServer(handler)
	url1 := "ws" + strings.TrimPrefix(srv1.URL, "http")

	var mu sync.Mutex
	currentURL := url1

	dial := func() (ws.Conn, error) {
		mu.Lock()
		u := currentURL
		mu.Unlock()
		conn, _, err := websocket.DefaultDialer.Dial(u, nil)
		return conn, err
	}

	conn, err := dial()
	if err != nil {
		t.Fatalf("initial dial: %v", err)
	}

	clutch := ws.NewClutch(conn, "id", ws.WithDialer(dial))
	defer clutch.Close()

	// First request should work.
	resp, err := clutch.Request(context.Background(), json.RawMessage(`{"method":"before.restart"}`))
	if err != nil {
		t.Fatalf("request before restart: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(resp, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Kill server 1, start server 2.
	srv1.Close()

	srv2 := httptest.NewServer(handler)
	defer srv2.Close()

	mu.Lock()
	currentURL = "ws" + strings.TrimPrefix(srv2.URL, "http")
	mu.Unlock()

	// Request after reconnection should succeed.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err = clutch.Request(ctx, json.RawMessage(`{"method":"after.restart"}`))
	if err != nil {
		t.Fatalf("request after reconnect: %v", err)
	}

	if err := json.Unmarshal(resp, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var method string
	if err := json.Unmarshal(got["method"], &method); err != nil {
		t.Fatalf("unmarshal method: %v", err)
	}
	if method != "after.restart" {
		t.Errorf("method = %q, want %q", method, "after.restart")
	}
}
```

**Step 2: Run it to verify it fails**

```bash
cd /workspace/wsreq && go test ./ws/ -run TestRequest_ReconnectAfterServerRestart -v -timeout 10s
```

Expected: FAIL — reconnection is not yet implemented (readLoop returns after `shouldReconnect` check).

**Step 3: Implement the reconnect method**

Add `"time"` to the import block in `ws/clutch.go`.

Add after the `writeLoop` method:

```go
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
```

**Step 4: Wire reconnection into readLoop**

Replace the placeholder `return` at the end of `readLoop` (the line after `if !shouldReconnect { return }`):

```go
		if !shouldReconnect {
			return
		}

		if !c.reconnect() {
			return
		}
```

(This replaces the two lines `// Reconnection will be added in the next task.` and `return`.)

**Step 5: Run the reconnection test**

```bash
cd /workspace/wsreq && go test ./ws/ -run TestRequest_ReconnectAfterServerRestart -v -timeout 10s
```

Expected: PASS.

**Step 6: Run all tests**

```bash
cd /workspace/wsreq && go test ./ws/ -v -count=1 -race -timeout 30s
```

Expected: All PASS.

**Step 7: Commit**

```bash
git add ws/clutch.go ws/clutch_test.go
git commit -m "feat: automatic reconnection with exponential backoff via WithDialer"
```

---

### Task 6: Reconnection edge-case tests

Add thorough tests covering reconnection edge cases.

**Files:**
- Modify: `ws/clutch_test.go`

**Step 1: Write test — pending requests fail on disconnect**

```go
func TestRequest_PendingRequestsFailOnDisconnect(t *testing.T) {
	// Server that reads but never responds, then closes.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))

	// Second server that actually echoes (for post-reconnect).
	srv2, cleanup2 := echoServer(t)
	defer cleanup2()

	var mu sync.Mutex
	currentURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	dial := func() (ws.Conn, error) {
		mu.Lock()
		u := currentURL
		mu.Unlock()
		conn, _, err := websocket.DefaultDialer.Dial(u, nil)
		return conn, err
	}

	conn, err := dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	clutch := ws.NewClutch(conn, "id", ws.WithDialer(dial))
	defer clutch.Close()

	// Send a request that will never be answered.
	errCh := make(chan error, 1)
	go func() {
		_, err := clutch.Request(context.Background(), json.RawMessage(`{"method":"will.fail"}`))
		errCh <- err
	}()

	// Let the request reach the server.
	time.Sleep(50 * time.Millisecond)

	// Point reconnection at the echo server, then kill server 1.
	mu.Lock()
	currentURL = "ws" + strings.TrimPrefix(srv2.URL, "http")
	mu.Unlock()
	srv.Close()

	// The pending request should fail (server lost state).
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error for pending request, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending request did not fail within timeout")
	}

	// New requests after reconnect should work.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := clutch.Request(ctx, json.RawMessage(`{"method":"after.reconnect"}`))
	if err != nil {
		t.Fatalf("request after reconnect: %v", err)
	}

	var got map[string]json.RawMessage
	if err := json.Unmarshal(resp, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var method string
	if err := json.Unmarshal(got["method"], &method); err != nil {
		t.Fatalf("unmarshal method: %v", err)
	}
	if method != "after.reconnect" {
		t.Errorf("method = %q, want %q", method, "after.reconnect")
	}
}
```

**Step 2: Write test — Close stops reconnection loop**

```go
func TestRequest_CloseStopsReconnect(t *testing.T) {
	// Server that immediately closes the connection.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conn.Close()
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	dial := func() (ws.Conn, error) {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		return conn, err
	}

	conn, err := dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	clutch := ws.NewClutch(conn, "id", ws.WithDialer(dial))

	// Give reconnection loop time to start.
	time.Sleep(100 * time.Millisecond)

	// Close should stop the reconnection loop and return promptly.
	done := make(chan struct{})
	go func() {
		clutch.Close()
		close(done)
	}()

	select {
	case <-done:
		// Good — Close returned.
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2 seconds — reconnection loop may be stuck")
	}
}
```

**Step 3: Write test — no reconnection without dialer**

```go
func TestRequest_NoReconnectWithoutDialer(t *testing.T) {
	// Server that closes after one message.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var msg testMessage
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}
		conn.WriteJSON(msg) // echo once
		// Close — next read will error.
	}))
	defer srv.Close()

	clutch := dialClutch(t, srv)
	defer clutch.Close()

	// First request works.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	_, err := clutch.Request(ctx1, json.RawMessage(`{"method":"first"}`))
	if err != nil {
		t.Fatalf("first request: %v", err)
	}

	// Second request should fail permanently (no reconnection).
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	_, err = clutch.Request(ctx2, json.RawMessage(`{"method":"second"}`))
	if err == nil {
		t.Fatal("expected error on second request, got nil")
	}
	if ctx2.Err() != nil {
		t.Fatalf("should fail promptly, not timeout: %v", ctx2.Err())
	}
}
```

**Step 4: Run all tests**

```bash
cd /workspace/wsreq && go test ./ws/ -v -count=1 -race -timeout 30s
```

Expected: All PASS.

**Step 5: Commit**

```bash
git add ws/clutch_test.go
git commit -m "test: add reconnection edge-case tests (pending fail, close stops loop, no-dialer)"
```

---

### Task 7: Update clutchpedal for reconnection

Wire `WithDialer` into clutchpedal so upstream restarts are handled automatically.

**Files:**
- Modify: `cmd/clutchpedal/main.go`

**Step 1: Update the upstream connection block**

Replace the upstream connection block (the `if *upstream != ""` branch, currently ~lines 37-45) with:

```go
	var clutch *ws.Clutch
	if *upstream != "" {
		dialer := &websocket.Dialer{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: *insecure,
			},
		}
		dial := func() (ws.Conn, error) {
			c, _, err := dialer.Dial(*upstream, nil)
			return c, err
		}
		conn, err := dial()
		if err != nil {
			log.Fatalf("dial ws: %v", err)
		}
		clutch = ws.NewClutch(conn, *idField, ws.WithDialer(dial))
	} else {
		conn := ws.NewStdioConn(os.Stdin, os.Stdout)
		clutch = ws.NewClutch(conn, *idField)
	}
```

Remove the old `var conn ws.Conn` declaration and the separate `clutch := ws.NewClutch(conn, *idField)` line. The `clutch` variable is now declared and assigned inside the if/else.

**Step 2: Verify it builds**

```bash
cd /workspace/wsreq && go build ./cmd/clutchpedal/
```

Expected: Builds without errors.

**Step 3: Commit**

```bash
git add cmd/clutchpedal/main.go
git commit -m "feat: enable automatic reconnection and TLS support in clutchpedal"
```

---

### Task 8: Final verification

**Step 1: Run full test suite with race detector**

```bash
cd /workspace/wsreq && go test ./... -v -count=1 -race -timeout 60s
```

Expected: All PASS, no races.

**Step 2: Build all binaries**

```bash
cd /workspace/wsreq && go build ./cmd/clutchpedal/ && go build ./cmd/wsecho/ && go build ./cmd/apiserver/
```

Expected: All build cleanly.

**Step 3: Run integration tests (if they work in this environment)**

```bash
cd /workspace/wsreq && bash test/integration.sh
```

Expected: PASS (or skip if environment doesn't support it).

**Step 4: Update CLAUDE.md — remove resolved issues**

Replace the contents of `CLAUDE.md` to remove the two fixed issues, or mark them as resolved.
