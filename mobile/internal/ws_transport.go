package internal

import (
	"encoding/json"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type WSTransport struct {
	conn      *websocket.Conn
	mu        sync.Mutex
	clientId  string
	onMessage func(map[string]interface{})
	onBinary  func([]byte)
}

func NewWSTransport(signalingURL, roomId, clientId, connectToken string) (*WSTransport, error) {
	base := strings.TrimRight(signalingURL, "/")
	fullURL := base + "/ws/" + url.PathEscape(roomId) + "?cid=" + url.QueryEscape(clientId)
	if connectToken != "" {
		fullURL += "&token=" + url.QueryEscape(connectToken)
	}

	conn, _, err := websocket.DefaultDialer.Dial(fullURL, nil)
	if err != nil {
		return nil, err
	}
	ws := &WSTransport{conn: conn, clientId: clientId}
	go ws.readLoop()
	go ws.heartbeat(20 * time.Second)
	return ws, nil
}

func (ws *WSTransport) readLoop() {
	for {
		msgType, data, err := ws.conn.ReadMessage()
		if err != nil {
			log.Printf("[WS] 读取错误: %v", err)
			return
		}
		if msgType == websocket.TextMessage {
			var msg map[string]interface{}
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			if ws.onMessage != nil {
				ws.onMessage(msg)
			}
		} else if msgType == websocket.BinaryMessage {
			if ws.onBinary != nil {
				ws.onBinary(data)
			}
		}
	}
}

func (ws *WSTransport) heartbeat(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		if err := ws.Send(map[string]interface{}{
			"type": "ping",
			"ts":   time.Now().Unix(),
		}); err != nil {
			return
		}
	}
}

func (ws *WSTransport) Send(msg map[string]interface{}) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	data, _ := json.Marshal(msg)
	return ws.conn.WriteMessage(websocket.TextMessage, data)
}

func (ws *WSTransport) SendBinary(data []byte) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (ws *WSTransport) Close() error {
	return ws.conn.Close()
}
