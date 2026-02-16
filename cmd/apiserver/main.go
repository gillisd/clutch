package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"clutch/ws"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	wsURL := flag.String("ws", "ws://localhost:9090/ws", "WebSocket server URL")
	flag.Parse()

	conn, _, err := websocket.DefaultDialer.Dial(*wsURL, nil)
	if err != nil {
		log.Fatalf("dial ws: %v", err)
	}
	clutch := ws.NewClutch(conn, "id")
	defer clutch.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/request", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := clutch.Request(r.Context(), body)
		if err != nil {
			if r.Context().Err() != nil {
				http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
				return
			}
			http.Error(w, "bad gateway: "+err.Error(), http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(resp)
		w.Write([]byte("\n"))
	})

	srv := &http.Server{Addr: *addr, Handler: mux}

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("shutting down...")
		clutch.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	log.Printf("apiserver listening on %s (ws: %s)", *addr, *wsURL)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
