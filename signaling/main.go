package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// ---------- Upgrader (Critical fix #11: ALLOWED_ORIGIN env var check) ----------

var allowedOrigin = os.Getenv("ALLOWED_ORIGIN") // empty = allow all (dev mode)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		if allowedOrigin == "" {
			return true // dev mode: accept any origin
		}
		return r.Header.Get("Origin") == allowedOrigin
	},
}

// ---------- Types ----------

// Client represents a connected WebSocket peer.
// Critical fix #4: added id field so targeted messages can be routed.
type Client struct {
	id   string
	conn *websocket.Conn
	room string
	mu   sync.Mutex // guards conn.WriteMessage calls
}

// ---------- Global state ----------

var (
	rooms       = make(map[string]map[*Client]bool) // room → set of clients
	clientsById = make(map[string]*Client)           // Critical fix #4: id → client
	globalMu    sync.RWMutex
)

// ---------- Entry point ----------

func main() {
	http.HandleFunc("/ws", handleWebSocket)
	addr := ":8080"
	log.Printf("Signaling server starting on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

// ---------- WebSocket handler ----------

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}
	defer conn.Close()

	// Critical fix #4: assign a unique ID to each connected client
	client := &Client{
		id:   uuid.New().String(),
		conn: conn,
	}

	// Register client globally
	globalMu.Lock()
	clientsById[client.id] = client
	globalMu.Unlock()

	// Send the client its own peer ID so it can include it in signaling messages
	sendJSON(client, map[string]string{"type": "self-id", "id": client.id})

	defer func() {
		globalMu.Lock()
		delete(clientsById, client.id)
		globalMu.Unlock()
		handleDisconnect(client)
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var msg map[string]interface{}
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}

		msgType, _ := msg["type"].(string)

		switch msgType {
		case "join":
			room, _ := msg["room"].(string)
			if room == "" {
				continue
			}
			client.room = room
			joinRoom(client, room)

		// Critical fix #3 & #12: route offer/answer/ICE to the specific target peer only
		case "offer", "answer", "ice-candidate":
			target, _ := msg["target"].(string)
			if target == "" {
				// No specific target — broadcast to room (fallback)
				broadcastToRoom(client.room, message, client)
				continue
			}
			// Inject sender ID so the receiver knows who sent it
			msg["from"] = client.id
			enriched, err := json.Marshal(msg)
			if err != nil {
				continue
			}
			sendToTarget(target, enriched)
		}
	}
}

// ---------- Room management ----------

func joinRoom(client *Client, room string) {
	globalMu.Lock()

	if rooms[room] == nil {
		rooms[room] = make(map[*Client]bool)
	}

	// Collect existing peers before adding the new one
	existingPeers := make([]string, 0, len(rooms[room]))
	for c := range rooms[room] {
		existingPeers = append(existingPeers, c.id)
	}

	rooms[room][client] = true
	globalMu.Unlock()

	fmt.Printf("Client %s joined room: %s (peers: %d)\n", client.id, room, len(existingPeers))

	// Tell the new client about existing peers
	sendJSON(client, map[string]interface{}{
		"type":  "room-peers",
		"peers": existingPeers,
	})

	// Critical fix: notify existing peers that a new peer joined
	peerJoinedMsg, _ := json.Marshal(map[string]string{
		"type": "peer-joined",
		"id":   client.id,
	})
	globalMu.RLock()
	for c := range rooms[room] {
		if c != client {
			writeMessage(c, peerJoinedMsg)
		}
	}
	globalMu.RUnlock()
}

// ---------- Messaging helpers ----------

// sendToTarget routes a message to a specific peer by ID.
// Critical fix #12: replaces the incorrect broadcastToRoom for targeted messages.
func sendToTarget(targetID string, message []byte) {
	globalMu.RLock()
	target, ok := clientsById[targetID]
	globalMu.RUnlock()

	if !ok {
		log.Printf("sendToTarget: unknown target %s", targetID)
		return
	}
	writeMessage(target, message)
}

// broadcastToRoom sends a message to all peers in a room except the sender.
func broadcastToRoom(room string, message []byte, sender *Client) {
	globalMu.RLock()
	defer globalMu.RUnlock()

	for c := range rooms[room] {
		if c != sender {
			writeMessage(c, message)
		}
	}
}

// writeMessage is a thread-safe write with dead-client cleanup.
// High fix #13: error-checks WriteMessage and removes dead clients.
func writeMessage(c *Client, message []byte) {
	c.mu.Lock()
	err := c.conn.WriteMessage(websocket.TextMessage, message)
	c.mu.Unlock()

	if err != nil {
		log.Printf("writeMessage error to %s: %v — removing", c.id, err)
		go handleDisconnect(c) // clean up asynchronously
	}
}

// sendJSON marshals v and writes it to a single client.
func sendJSON(c *Client, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	writeMessage(c, data)
}

// ---------- Disconnect ----------

func handleDisconnect(client *Client) {
	globalMu.Lock()

	if client.room == "" {
		globalMu.Unlock()
		return
	}

	room := client.room
	delete(rooms[room], client)

	// Collect peers to notify while holding the lock
	peersToNotify := make([]*Client, 0, len(rooms[room]))
	for c := range rooms[room] {
		peersToNotify = append(peersToNotify, c)
	}

	if len(rooms[room]) == 0 {
		delete(rooms, room)
	}

	client.room = ""
	globalMu.Unlock()

	fmt.Printf("Client %s disconnected\n", client.id)

	// Notify remaining peers OUTSIDE the lock to avoid deadlock.
	// Use rawWrite which doesn't trigger recursive handleDisconnect.
	leaveMsg, _ := json.Marshal(map[string]string{
		"type": "peer-left",
		"id":   client.id,
	})
	for _, c := range peersToNotify {
		rawWrite(c, leaveMsg)
	}
}

// rawWrite is a thread-safe write that logs errors but does NOT
// trigger handleDisconnect, preventing recursive lock acquisition.
func rawWrite(c *Client, message []byte) {
	c.mu.Lock()
	err := c.conn.WriteMessage(websocket.TextMessage, message)
	c.mu.Unlock()

	if err != nil {
		log.Printf("rawWrite error to %s: %v", c.id, err)
	}
}
