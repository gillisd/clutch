package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"clutch/ws"

	"github.com/gorilla/websocket"
)

func main() {
	// Go's flag package stops at the first non-flag arg. Reorder so
	// flags come first, allowing: clutchpedal <bind> --upstream ws://...
	reorderArgs()

	upstream := flag.String("upstream", "", "WebSocket URL (omit for stdio)")
	idField := flag.String("id-field", "id", "correlation ID field name")
	insecure := flag.Bool("insecure", false, "skip TLS certificate verification (for wss://)")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: clutchpedal <bind> [--upstream ws://...] [--id-field id] [--insecure]\n")
		os.Exit(1)
	}
	bind := flag.Arg(0)

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
	defer clutch.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
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

	srv := &http.Server{Addr: bind, Handler: mux}

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

	log.Printf("clutchpedal listening on %s", bind)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

// reorderArgs moves flag arguments before positional arguments in os.Args
// so Go's flag package parses them correctly.
func reorderArgs() {
	var flags, pos []string
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		if strings.HasPrefix(args[i], "-") {
			flags = append(flags, args[i])
			// Consume the next arg as the flag value if it uses --key value form.
			if !strings.Contains(args[i], "=") && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
		} else {
			pos = append(pos, args[i])
		}
	}
	os.Args = append(append([]string{os.Args[0]}, flags...), pos...)
}
