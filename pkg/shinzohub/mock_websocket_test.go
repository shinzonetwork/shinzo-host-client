package shinzohub

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// MockWebSocketServer simulates a ShinzoHub WebSocket server for testing.
type MockWebSocketServer struct {
	server   *http.Server
	upgrader websocket.Upgrader
	conn     *websocket.Conn
	connMu   sync.Mutex // protects concurrent writes to conn
	done     chan struct{}
	port     int
	ready    chan struct{} // signals when all subscriptions have been processed
}

// NewMockWebSocketServer creates a new mock WebSocket server.
func NewMockWebSocketServer() *MockWebSocketServer {
	mux := http.NewServeMux()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("Failed to create listener: %v", err))
	}

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: defaultTimeout,
	}

	mock := &MockWebSocketServer{
		server: server,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(_ *http.Request) bool {
				return true
			},
		},
		done:  make(chan struct{}),
		ready: make(chan struct{}),
		port:  listener.Addr().(*net.TCPAddr).Port,
	}

	mux.HandleFunc("/websocket", mock.handleWebSocket)

	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("Mock WebSocket server error: %v\n", err)
		}
	}()

	return mock
}

// WebsocketURL returns the WebSocket URL for the mock server.
func (m *MockWebSocketServer) WebsocketURL() string {
	return fmt.Sprintf("ws://127.0.0.1:%d/websocket", m.port)
}

// Close shuts down the mock server.
func (m *MockWebSocketServer) Close() {
	close(m.done)
	if m.server != nil {
		defer func() { _ = m.server.Close() }()
	}
	m.connMu.Lock()
	if m.conn != nil {
		_ = m.conn.Close()
	}
	m.connMu.Unlock()
}

// SendEvent sends a mock event to the connected client.
// Waits briefly for subscriptions to be processed before sending.
func (m *MockWebSocketServer) SendEvent(event RPCResponse) {
	// Wait for the handler to finish processing subscriptions
	select {
	case <-m.ready:
	case <-time.After(2 * time.Second):
	}

	m.connMu.Lock()
	defer m.connMu.Unlock()

	if m.conn == nil {
		return
	}

	if err := m.conn.WriteJSON(event); err != nil {
		fmt.Printf("Failed to send event: %v\n", err)
		return
	}
}

// handleWebSocket handles WebSocket connections.
func (m *MockWebSocketServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("Failed to upgrade WebSocket: %v\n", err)
		return
	}

	m.connMu.Lock()
	m.conn = conn
	m.connMu.Unlock()
	defer func() { _ = conn.Close() }()

	subscriptionCount := 0

	for {
		select {
		case <-m.done:
			return
		default:
			var msg map[string]any
			if err := conn.ReadJSON(&msg); err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					fmt.Printf("WebSocket error: %v\n", err)
				}
				return
			}

			if method, ok := msg["method"].(string); ok && method == "subscribe" {
				m.connMu.Lock()
				response := map[string]any{
					"jsonrpc": "2.0",
					"id":      msg["id"],
					"result": map[string]any{
						"query": "tm.event='Tx' AND (Registered.key EXISTS OR EntityRegistered.key EXISTS)",
					},
				}
				if err := conn.WriteJSON(response); err != nil {
					m.connMu.Unlock()
					fmt.Printf("Failed to send subscription response: %v\n", err)
					return
				}
				m.connMu.Unlock()

				subscriptionCount++
				// Signal ready after all 3 subscriptions are processed
				if subscriptionCount >= 3 {
					select {
					case <-m.ready:
						// already closed
					default:
						close(m.ready)
					}
				}
			}
		}
	}
}
