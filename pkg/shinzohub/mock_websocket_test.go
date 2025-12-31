package shinzohub

import (
	"fmt"
	"net"
	"net/http"

	"github.com/gorilla/websocket"
)

// MockWebSocketServer simulates a ShinzoHub WebSocket server for testing
type MockWebSocketServer struct {
	server   *http.Server
	upgrader websocket.Upgrader
	conn     *websocket.Conn
	done     chan struct{}
	port     int
}

// NewMockWebSocketServer creates a new mock WebSocket server
func NewMockWebSocketServer() *MockWebSocketServer {
	mux := http.NewServeMux()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("Failed to create listener: %v", err))
	}
	
	server := &http.Server{Handler: mux}
	
	mock := &MockWebSocketServer{
		server: server,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for testing
			},
		},
		done: make(chan struct{}),
		port: listener.Addr().(*net.TCPAddr).Port,
	}

	mux.HandleFunc("/websocket", mock.handleWebSocket)
	
	// Start the server in a goroutine
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Mock WebSocket server error: %v\n", err)
		}
	}()

	return mock
}

// WebsocketURL returns the WebSocket URL for the mock server
func (m *MockWebSocketServer) WebsocketURL() string {
	return fmt.Sprintf("ws://127.0.0.1:%d/websocket", m.port)
}

// Close shuts down the mock server
func (m *MockWebSocketServer) Close() {
	close(m.done)
	if m.server != nil {
		m.server.Close()
	}
	if m.conn != nil {
		m.conn.Close()
	}
}

// SendEvent sends a mock event to the connected client
func (m *MockWebSocketServer) SendEvent(event RPCResponse) {
	if m.conn == nil {
		return
	}

	// Send subscription response first (simulating successful subscription)
	subscriptionResponse := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"result": map[string]interface{}{
			"query": "tm.event='Tx' AND (Registered.key EXISTS OR EntityRegistered.key EXISTS)",
		},
	}

	if err := m.conn.WriteJSON(subscriptionResponse); err != nil {
		fmt.Printf("Failed to send subscription response: %v\n", err)
		return
	}

	// Send the actual event
	if err := m.conn.WriteJSON(event); err != nil {
		fmt.Printf("Failed to send event: %v\n", err)
		return
	}
}

// handleWebSocket handles WebSocket connections
func (m *MockWebSocketServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("Failed to upgrade WebSocket: %v\n", err)
		return
	}

	m.conn = conn
	defer conn.Close()

	// Handle WebSocket messages
	for {
		select {
		case <-m.done:
			return
		default:
			var msg map[string]interface{}
			if err := conn.ReadJSON(&msg); err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					fmt.Printf("WebSocket error: %v\n", err)
				}
				return
			}

			// Handle subscription message
			if method, ok := msg["method"].(string); ok && method == "subscribe" {
				// Send subscription confirmation
				response := map[string]interface{}{
					"jsonrpc": "2.0",
					"id":      msg["id"],
					"result": map[string]interface{}{
						"query": "tm.event='Tx' AND (Registered.key EXISTS OR EntityRegistered.key EXISTS)",
					},
				}
				if err := conn.WriteJSON(response); err != nil {
					fmt.Printf("Failed to send subscription response: %v\n", err)
					return
				}
			}
		}
	}
}
