package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

type message struct {
	ID      uint64          `json:"id"`
	Method  string          `json:"method,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func main() {
	addr := flag.String("addr", ":9090", "listen address")
	flag.Parse()

	http.HandleFunc("/ws", handleWS)

	log.Printf("wsecho listening on %s", *addr)
	if err := http.ListenAndServe(*addr, nil); err != nil {
		log.Fatal(err)
	}
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade: %v", err)
		return
	}
	defer conn.Close()
	log.Printf("new connection from %s", r.RemoteAddr)

	for {
		var msg message
		if err := conn.ReadJSON(&msg); err != nil {
			log.Printf("read: %v", err)
			return
		}

		// Wrap the original payload as {"echo": <original>}.
		echo, err := json.Marshal(map[string]json.RawMessage{"echo": msg.Payload})
		if err != nil {
			log.Printf("marshal echo: %v", err)
			continue
		}
		reply := message{
			ID:      msg.ID,
			Payload: echo,
		}

		if err := conn.WriteJSON(reply); err != nil {
			log.Printf("write: %v", err)
			return
		}
	}
}
