package ws_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"wsreq/ws"
)

var upgrader = websocket.Upgrader{}

// echoServer reads Messages and echoes them back with the same ID.
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
			var msg ws.Message
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

func dialClient(t *testing.T, srv *httptest.Server) *ws.Client {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return ws.NewClient(conn)
}

func TestRequest_BasicRoundTrip(t *testing.T) {
	srv, cleanup := echoServer(t)
	defer cleanup()

	client := dialClient(t, srv)
	defer client.Close()

	payload := map[string]string{"hello": "world"}
	resp, err := client.Request(context.Background(), "test.echo", payload)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if resp.Method != "test.echo" {
		t.Errorf("method = %q, want %q", resp.Method, "test.echo")
	}

	var got map[string]string
	if err := json.Unmarshal(resp.Payload, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got["hello"] != "world" {
		t.Errorf("payload = %v, want hello=world", got)
	}
}

func TestRequest_ConcurrentRequests(t *testing.T) {
	srv, cleanup := echoServer(t)
	defer cleanup()

	client := dialClient(t, srv)
	defer client.Close()

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := map[string]int{"i": i}
			resp, err := client.Request(context.Background(), fmt.Sprintf("op.%d", i), payload)
			if err != nil {
				errs <- fmt.Errorf("request %d: %w", i, err)
				return
			}
			var got map[string]int
			if err := json.Unmarshal(resp.Payload, &got); err != nil {
				errs <- fmt.Errorf("unmarshal %d: %w", i, err)
				return
			}
			if got["i"] != i {
				errs <- fmt.Errorf("request %d: got i=%d", i, got["i"])
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

	client := dialClient(t, srv)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := client.Request(ctx, "slow.op", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if ctx.Err() == nil {
		t.Fatal("expected context to be done")
	}
}

func TestRequest_RemoteError(t *testing.T) {
	// Server that replies with an error field.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var msg ws.Message
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}
			reply := ws.Message{
				ID:    msg.ID,
				Error: "something went wrong",
			}
			if err := conn.WriteJSON(reply); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	client := dialClient(t, srv)
	defer client.Close()

	_, err := client.Request(context.Background(), "fail.op", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "something went wrong" {
		t.Errorf("error = %q, want %q", err.Error(), "something went wrong")
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

	client := dialClient(t, srv)
	defer client.Close()

	// Signal server to close the WS connection.
	close(closeConn)

	// Give the read pump a moment to notice.
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := client.Request(ctx, "op", nil)
	if err == nil {
		t.Fatal("expected error after connection close, got nil")
	}
}
