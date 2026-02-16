package ws_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"clutch/ws"
)

var upgrader = websocket.Upgrader{}

// testMessage is a local struct for test echo servers.
type testMessage struct {
	ID      uint64          `json:"id"`
	Method  string          `json:"method,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// echoServer reads JSON messages and echoes them back with the same ID.
func echoServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

func dialClutch(t *testing.T, srv *httptest.Server) *ws.Clutch {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return ws.NewClutch(conn, "id")
}

func TestRequest_BasicRoundTrip(t *testing.T) {
	srv, cleanup := echoServer(t)
	defer cleanup()

	clutch := dialClutch(t, srv)
	defer clutch.Close()

	resp, err := clutch.Request(context.Background(), json.RawMessage(`{"method":"test.echo","payload":{"hello":"world"}}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	var got map[string]json.RawMessage
	if err := json.Unmarshal(resp, &got); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	var method string
	if err := json.Unmarshal(got["method"], &method); err != nil {
		t.Fatalf("unmarshal method: %v", err)
	}
	if method != "test.echo" {
		t.Errorf("method = %q, want %q", method, "test.echo")
	}

	var payload map[string]string
	if err := json.Unmarshal(got["payload"], &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["hello"] != "world" {
		t.Errorf("payload = %v, want hello=world", payload)
	}
}

func TestRequest_ConcurrentRequests(t *testing.T) {
	srv, cleanup := echoServer(t)
	defer cleanup()

	clutch := dialClutch(t, srv)
	defer clutch.Close()

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg := json.RawMessage(fmt.Sprintf(`{"method":"op.%d","payload":{"i":%d}}`, i, i))
			resp, err := clutch.Request(context.Background(), msg)
			if err != nil {
				errs <- fmt.Errorf("request %d: %w", i, err)
				return
			}
			var got map[string]json.RawMessage
			if err := json.Unmarshal(resp, &got); err != nil {
				errs <- fmt.Errorf("unmarshal %d: %w", i, err)
				return
			}
			var payload map[string]int
			if err := json.Unmarshal(got["payload"], &payload); err != nil {
				errs <- fmt.Errorf("unmarshal payload %d: %w", i, err)
				return
			}
			if payload["i"] != i {
				errs <- fmt.Errorf("request %d: got i=%d", i, payload["i"])
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestRequest_ContextCancellation(t *testing.T) {
	// Server that never responds — just reads and discards.
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
	defer srv.Close()

	clutch := dialClutch(t, srv)
	defer clutch.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := clutch.Request(ctx, json.RawMessage(`{"method":"slow.op"}`))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if ctx.Err() == nil {
		t.Fatal("expected context to be done")
	}
}

func TestRequest_ResponsePassthrough(t *testing.T) {
	// Server that replies with an "error" field in the JSON.
	// The library should NOT interpret this — it should pass through raw.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			reply := testMessage{
				ID:    msg.ID,
				Error: "something went wrong",
			}
			if err := conn.WriteJSON(reply); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	clutch := dialClutch(t, srv)
	defer clutch.Close()

	resp, err := clutch.Request(context.Background(), json.RawMessage(`{"method":"fail.op"}`))
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}

	// The response should contain the error field as raw JSON — not interpreted.
	var got map[string]json.RawMessage
	if err := json.Unmarshal(resp, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var errMsg string
	if err := json.Unmarshal(got["error"], &errMsg); err != nil {
		t.Fatalf("unmarshal error field: %v", err)
	}
	if errMsg != "something went wrong" {
		t.Errorf("error = %q, want %q", errMsg, "something went wrong")
	}
}

func TestRequest_ConnectionClose(t *testing.T) {
	// Server that closes the WS connection on signal.
	closeConn := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		<-closeConn
		conn.Close()
	}))
	defer srv.Close()

	clutch := dialClutch(t, srv)
	defer clutch.Close()

	// Signal server to close the WS connection.
	close(closeConn)

	// Give the read pump a moment to notice.
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := clutch.Request(ctx, json.RawMessage(`{"method":"op"}`))
	if err == nil {
		t.Fatal("expected error after connection close, got nil")
	}
}

func TestWriteLoop_ErrorPropagation(t *testing.T) {
	// Server accepts connection, reads one message, then closes.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// Read exactly one message, then close.
		var msg testMessage
		if err := conn.ReadJSON(&msg); err != nil {
			conn.Close()
			return
		}
		conn.WriteJSON(msg) // echo the first one back
		conn.Close()
	}))
	defer srv.Close()

	clutch := dialClutch(t, srv)
	defer clutch.Close()

	// First request should succeed.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	_, err := clutch.Request(ctx1, json.RawMessage(`{"method":"first","payload":{"n":"1"}}`))
	if err != nil {
		t.Fatalf("first request: %v", err)
	}

	// Second request must error promptly (not hang until timeout).
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	_, err = clutch.Request(ctx2, json.RawMessage(`{"method":"second","payload":{"n":"2"}}`))
	if err == nil {
		t.Fatal("expected error on second request after server close, got nil")
	}
	if ctx2.Err() != nil {
		t.Fatalf("second request timed out instead of erroring promptly: %v", ctx2.Err())
	}
}

func TestClose_Idempotent(t *testing.T) {
	srv, cleanup := echoServer(t)
	defer cleanup()

	clutch := dialClutch(t, srv)

	err1 := clutch.Close()
	if err1 != nil {
		t.Fatalf("first close: %v", err1)
	}

	err2 := clutch.Close()
	if err2 != nil {
		t.Fatalf("second close should return nil, got: %v", err2)
	}
}

func TestGoroutineLeak_AfterClose(t *testing.T) {
	srv, cleanup := echoServer(t)
	defer cleanup()

	before := runtime.NumGoroutine()

	clutch := dialClutch(t, srv)
	clutch.Close()

	// Poll for goroutine count to return to baseline.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("goroutine leak: before=%d, after=%d", before, runtime.NumGoroutine())
		default:
		}
		if runtime.NumGoroutine() <= before+1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestRequest_ConcurrentClose(t *testing.T) {
	srv, cleanup := echoServer(t)
	defer cleanup()

	clutch := dialClutch(t, srv)

	const n = 10
	var wg sync.WaitGroup

	// Launch n goroutines making requests.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			// Errors expected — we just need no panic or deadlock.
			clutch.Request(ctx, json.RawMessage(`{"method":"op"}`))
		}()
	}

	// Close mid-flight after a brief delay.
	time.Sleep(10 * time.Millisecond)
	clutch.Close()

	// All goroutines must return within 2 seconds.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock: goroutines did not return within 2 seconds")
	}
}
