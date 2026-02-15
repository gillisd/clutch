package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"wsreq/ws"
)

var upgrader = websocket.Upgrader{}

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
		var msg ws.Message
		if err := conn.ReadJSON(&msg); err != nil {
			log.Printf("read: %v", err)
			return
		}

		// Wrap the original payload as {"echo": <original>}.
		echo, _ := json.Marshal(map[string]json.RawMessage{"echo": msg.Payload})
		reply := ws.Message{
			ID:      msg.ID,
			Payload: echo,
		}

		if err := conn.WriteJSON(reply); err != nil {
			log.Printf("write: %v", err)
			return
		}
	}
}
