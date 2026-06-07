package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Client struct {
	conn *websocket.Conn
	room string
}

var rooms = make(map[string]map[*Client]bool)
var roomsMutex sync.RWMutex

func main() {
	http.HandleFunc("/ws", handleWebSocket)
	log.Println("Signaling server starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	client := &Client{conn: conn}

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			handleDisconnect(client)
			break
		}

		var msg map[string]interface{}
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}

		switch msg["type"] {
		case "join":
			room := msg["room"].(string)
			client.room = room
			joinRoom(client, room)
		case "offer", "answer", "ice-candidate":
			broadcastToRoom(client.room, message, client)
		}
	}
}

func joinRoom(client *Client, room string) {
	roomsMutex.Lock()
	defer roomsMutex.Unlock()

	if rooms[room] == nil {
		rooms[room] = make(map[*Client]bool)
	}
	rooms[room][client] = true

	fmt.Printf("Client joined room: %s\n", room)
}

func broadcastToRoom(room string, message []byte, sender *Client) {
	roomsMutex.RLock()
	defer roomsMutex.RUnlock()

	for c := range rooms[room] {
		if c != sender {
			c.conn.WriteMessage(websocket.TextMessage, message)
		}
	}
}

func handleDisconnect(client *Client) {
	roomsMutex.Lock()
	defer roomsMutex.Unlock()

	if client.room != "" {
		delete(rooms[client.room], client)
		if len(rooms[client.room]) == 0 {
			delete(rooms, client.room)
		}
	}
}
