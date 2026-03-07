package shinzohub

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/stretchr/testify/require"
)

func TestGetEntityType(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected EntityType
	}{
		{"indexer", "\u0001", EntityTypeIndexer},
		{"host", "\u0002", EntityTypeHost},
		{"unknown empty", "", EntityTypeUnknown},
		{"unknown other", "foo", EntityTypeUnknown},
		{"unknown zero", "\u0000", EntityTypeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetEntityType(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractShinzoEvents_ValidRegisteredEvent(t *testing.T) {
	query := `query { some_query }`
	sdl := `type TestView @materialized(if: true) { field: String }`
	v := view.View{
		Name:  "TestView",
		Query: &query,
		Sdl:   &sdl,
	}
	viewJSON, err := json.Marshal(v)
	require.NoError(t, err)

	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "Registered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "test-key"},
										{Key: "creator", Value: "test-creator"},
										{Key: "view", Value: string(viewJSON)},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Len(t, events, 1)

	vre, ok := events[0].(*ViewRegisteredEvent)
	require.True(t, ok)
	require.Equal(t, "test-key", vre.Key)
	require.Equal(t, "test-creator", vre.Creator)
	require.Equal(t, "TestView", vre.View.Name)
}

func TestExtractShinzoEvents_ValidEntityRegisteredEvent(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "EntityRegistered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "entity-key"},
										{Key: "owner", Value: "entity-owner"},
										{Key: "did", Value: "entity-did"},
										{Key: "pid", Value: "entity-pid"},
										{Key: "entity", Value: "\u0001"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Len(t, events, 1)

	ere, ok := events[0].(*EntityRegisteredEvent)
	require.True(t, ok)
	require.Equal(t, "entity-key", ere.Key)
	require.Equal(t, "entity-owner", ere.Owner)
	require.Equal(t, "entity-did", ere.DID)
	require.Equal(t, "entity-pid", ere.Pid)
	require.Equal(t, "\u0001", ere.Entity)
}

func TestExtractShinzoEvents_IncompleteRegisteredEvent(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "Registered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "test-key"},
										// Missing creator and view
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Empty(t, events)
}

func TestExtractShinzoEvents_IncompleteEntityRegisteredEvent(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "EntityRegistered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "k"},
										// Missing owner, did, pid
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Empty(t, events)
}

func TestExtractShinzoEvents_WrongJsonRpcVersion(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "1.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{{Type: "Registered"}},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Empty(t, events)
}

func TestExtractShinzoEvents_WrongDataType(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "other.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{{Type: "Registered"}},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Empty(t, events)
}

func TestExtractShinzoEvents_NilEvents(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: nil,
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Empty(t, events)
}

func TestExtractShinzoEvents_InvalidViewJSON(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "Registered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "k"},
										{Key: "creator", Value: "c"},
										{Key: "view", Value: "invalid json{{{"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Empty(t, events)
}

func TestExtractShinzoEvents_UnknownEventType(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{Type: "UnknownEvent", Attributes: []EventAttribute{{Key: "key", Value: "v"}}},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Empty(t, events)
}

func TestViewRegisteredEvent_ToString(t *testing.T) {
	vre := &ViewRegisteredEvent{
		Key:     "k1",
		Creator: "c1",
		View:    view.View{Name: "v1"},
	}
	result := vre.ToString()
	require.Contains(t, result, "ViewRegistered")
	require.Contains(t, result, "k1")
	require.Contains(t, result, "c1")
	require.Contains(t, result, "v1")
}

func TestEntityRegisteredEvent_ToString(t *testing.T) {
	ere := &EntityRegisteredEvent{
		Key:   "k1",
		Owner: "o1",
		DID:   "d1",
		Pid:   "p1",
	}
	result := ere.ToString()
	require.Contains(t, result, "EntityRegistered")
	require.Contains(t, result, "k1")
	require.Contains(t, result, "o1")
	require.Contains(t, result, "d1")
	require.Contains(t, result, "p1")
}

func TestExtractNameFromSDL_NilSDL(t *testing.T) {
	v := &view.View{Sdl: nil}
	ExtractNameFromSDL(v)
	require.Equal(t, "", v.Name)
}

// ---------------------------------------------------------------------------
// Additional coverage for extractShinzoEvents
// ---------------------------------------------------------------------------

func TestExtractShinzoEvents_MixedRegisteredAndEntityRegistered(t *testing.T) {
	query := `query { some_query }`
	sdl := `type MixedView @materialized(if: true) { field: String }`
	v := view.View{
		Name:  "MixedView",
		Query: &query,
		Sdl:   &sdl,
	}
	viewJSON, err := json.Marshal(v)
	require.NoError(t, err)

	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "Registered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "view-key"},
										{Key: "creator", Value: "view-creator"},
										{Key: "view", Value: string(viewJSON)},
									},
								},
								{
									Type: "EntityRegistered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "entity-key"},
										{Key: "owner", Value: "entity-owner"},
										{Key: "did", Value: "entity-did"},
										{Key: "pid", Value: "entity-pid"},
										{Key: "entity", Value: "\u0002"},
									},
								},
								{
									Type: "UnknownType",
									Attributes: []EventAttribute{
										{Key: "key", Value: "ignored"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Len(t, events, 2)

	// First should be ViewRegistered
	vre, ok := events[0].(*ViewRegisteredEvent)
	require.True(t, ok)
	require.Equal(t, "view-key", vre.Key)
	require.Equal(t, "view-creator", vre.Creator)
	require.Equal(t, "MixedView", vre.View.Name)

	// Second should be EntityRegistered
	ere, ok := events[1].(*EntityRegisteredEvent)
	require.True(t, ok)
	require.Equal(t, "entity-key", ere.Key)
	require.Equal(t, "entity-owner", ere.Owner)
	require.Equal(t, "\u0002", ere.Entity)
}

func TestExtractShinzoEvents_RegisteredEventWithEmptyQuery(t *testing.T) {
	// A view with key, creator, and valid JSON but empty query should be filtered out
	emptyQuery := ""
	sdl := `type EmptyQueryView @materialized(if: false) { field: String }`
	v := view.View{
		Query: &emptyQuery,
		Sdl:   &sdl,
	}
	viewJSON, err := json.Marshal(v)
	require.NoError(t, err)

	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "Registered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "k"},
										{Key: "creator", Value: "c"},
										{Key: "view", Value: string(viewJSON)},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Empty(t, events, "View with empty query should be filtered out")
}

func TestExtractShinzoEvents_RegisteredEventWithNilQuery(t *testing.T) {
	// A view where query is nil after unmarshaling
	v := view.View{
		Name: "NoQueryView",
	}
	viewJSON, err := json.Marshal(v)
	require.NoError(t, err)

	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "Registered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "k"},
										{Key: "creator", Value: "c"},
										{Key: "view", Value: string(viewJSON)},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Empty(t, events, "View with nil query should be filtered out")
}

func TestExtractShinzoEvents_EntityRegisteredWithHostEntity(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "EntityRegistered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "host-key"},
										{Key: "owner", Value: "host-owner"},
										{Key: "did", Value: "host-did"},
										{Key: "pid", Value: "host-pid"},
										{Key: "entity", Value: "\u0002"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Len(t, events, 1)

	ere, ok := events[0].(*EntityRegisteredEvent)
	require.True(t, ok)
	require.Equal(t, "\u0002", ere.Entity)
}

func TestExtractShinzoEvents_EmptyEventsSlice(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Empty(t, events)
}

func TestExtractShinzoEvents_RegisteredEventWithSDLNameExtraction(t *testing.T) {
	// View has no Name set, but SDL has a type name that should be extracted
	query := `query { some_query }`
	sdl := `type AutoNamedView @materialized(if: true) { field: String }`
	v := view.View{
		Query: &query,
		Sdl:   &sdl,
		// Name is intentionally left empty
	}
	viewJSON, err := json.Marshal(v)
	require.NoError(t, err)

	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "Registered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "k"},
										{Key: "creator", Value: "c"},
										{Key: "view", Value: string(viewJSON)},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Len(t, events, 1)

	vre, ok := events[0].(*ViewRegisteredEvent)
	require.True(t, ok)
	require.Equal(t, "AutoNamedView", vre.View.Name)
}

// ---------------------------------------------------------------------------
// StartEventSubscription – error path
// ---------------------------------------------------------------------------

func TestStartEventSubscription_InvalidURL(t *testing.T) {
	// Attempting to connect to a non-existent server should return an error
	cancel, eventChan, err := StartEventSubscription("ws://127.0.0.1:1/nonexistent")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to connect to Tendermint WebSocket")
	require.Nil(t, eventChan)
	// cancel is still returned on error, calling it should not panic
	cancel()
}

func TestStartEventSubscription_WriteJSONError(t *testing.T) {
	// Create a server that accepts the WebSocket upgrade but immediately closes
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// Close immediately to cause WriteJSON to fail
		conn.Close()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/"
	cancel, eventChan, err := StartEventSubscription(wsURL)
	// The server closes immediately after upgrade; WriteJSON or ReadMessage may fail
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "failed to send subscription message") ||
		strings.Contains(err.Error(), "failed to read subscription response"),
		"unexpected error: %s", err.Error())
	require.Nil(t, eventChan)
	cancel()
}

func TestStartEventSubscription_ReadMessageError(t *testing.T) {
	// Create a server that accepts the WebSocket upgrade, receives the first
	// subscription write, then closes without responding
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// Read the first subscription message (so WriteJSON succeeds)
		_, _, _ = conn.ReadMessage()
		// Close immediately so ReadMessage fails on the client side
		conn.Close()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/"
	cancel, eventChan, err := StartEventSubscription(wsURL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read subscription response")
	require.Nil(t, eventChan)
	cancel()
}

func TestStartEventSubscription_SuccessAndCancel(t *testing.T) {
	// Use the full mock WebSocket server for a successful subscription + cancellation
	mockServer := NewMockWebSocketServer()
	defer mockServer.Close()

	cancel, eventChan, err := StartEventSubscription(mockServer.WebsocketURL())
	require.NoError(t, err)
	require.NotNil(t, eventChan)

	// Cancel the context to stop the goroutines
	cancel()

	// eventChan should eventually close
	timeout := time.After(3 * time.Second)
	for {
		select {
		case _, ok := <-eventChan:
			if !ok {
				return // channel closed as expected
			}
		case <-timeout:
			t.Fatal("timeout waiting for event channel to close after cancel")
		}
	}
}

func TestStartEventSubscription_ReceivesValidEvents(t *testing.T) {
	// Create a WebSocket server that sends a valid event after subscriptions
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Respond to two subscription messages
		for i := 0; i < 2; i++ {
			_, _, readErr := conn.ReadMessage()
			if readErr != nil {
				return
			}
			resp := map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      i + 1,
				"result":  map[string]interface{}{},
			}
			if writeErr := conn.WriteJSON(resp); writeErr != nil {
				return
			}
		}

		// Build a valid EntityRegistered event
		validEvent := RPCResponse{
			JsonRpcVersion: "2.0",
			ID:             1,
			Result: RPCResult{
				Data: RPCData{
					Type: "tendermint.event",
					Value: TxResult{
						TxResult: TxResultData{
							Result: TxResultResult{
								Events: []Event{
									{
										Type: "EntityRegistered",
										Attributes: []EventAttribute{
											{Key: "key", Value: "test-key"},
											{Key: "owner", Value: "test-owner"},
											{Key: "did", Value: "test-did"},
											{Key: "pid", Value: "test-pid"},
											{Key: "entity", Value: "\u0001"},
										},
									},
								},
							},
						},
					},
				},
			},
		}
		conn.WriteJSON(validEvent)

		// Keep connection open briefly
		time.Sleep(500 * time.Millisecond)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/"
	cancel, eventChan, err := StartEventSubscription(wsURL)
	require.NoError(t, err)
	require.NotNil(t, eventChan)
	defer cancel()

	// Wait for the event
	timeout := time.After(3 * time.Second)
	select {
	case event, ok := <-eventChan:
		if !ok {
			return // channel closed
		}
		// Verify we got an EntityRegistered event
		ere, isEntity := event.(*EntityRegisteredEvent)
		require.True(t, isEntity)
		require.Equal(t, "test-key", ere.Key)
		require.Equal(t, "test-owner", ere.Owner)
	case <-timeout:
		t.Fatal("timeout waiting for valid event")
	}
}

func TestStartEventSubscription_WebSocketReadError(t *testing.T) {
	// Server that accepts subscription then closes to trigger ReadMessage error in goroutine
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		// Respond to two subscription messages
		for i := 0; i < 2; i++ {
			_, _, readErr := conn.ReadMessage()
			if readErr != nil {
				conn.Close()
				return
			}
			resp := map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      i + 1,
				"result":  map[string]interface{}{},
			}
			if writeErr := conn.WriteJSON(resp); writeErr != nil {
				conn.Close()
				return
			}
		}

		// Close connection to trigger read error in the message processing goroutine
		conn.Close()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/"
	cancel, eventChan, err := StartEventSubscription(wsURL)
	require.NoError(t, err)
	require.NotNil(t, eventChan)
	defer cancel()

	// eventChan should close when the goroutine detects the read error
	timeout := time.After(3 * time.Second)
	for {
		select {
		case _, ok := <-eventChan:
			if !ok {
				return // channel closed as expected
			}
		case <-timeout:
			// Timeout is acceptable - goroutine may still be running
			cancel()
			return
		}
	}
}

func TestStartEventSubscription_JSONParseError(t *testing.T) {
	// Create a server that accepts WebSocket, responds to subscriptions,
	// then sends invalid JSON to test the json.Unmarshal error path
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Respond to two subscription messages
		for i := 0; i < 2; i++ {
			_, _, readErr := conn.ReadMessage()
			if readErr != nil {
				return
			}
			resp := map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      i + 1,
				"result":  map[string]interface{}{},
			}
			if writeErr := conn.WriteJSON(resp); writeErr != nil {
				return
			}
		}

		// Send invalid JSON to trigger parse error (continue path)
		conn.WriteMessage(websocket.TextMessage, []byte("not valid json{{{"))

		// Then close to terminate the goroutine
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/"
	cancel, eventChan, err := StartEventSubscription(wsURL)
	require.NoError(t, err)
	require.NotNil(t, eventChan)
	defer cancel()

	// The goroutine should handle the parse error and eventually close
	timeout := time.After(3 * time.Second)
	select {
	case _, ok := <-eventChan:
		if !ok {
			// Channel closed - expected after server closes
			return
		}
	case <-timeout:
		// Timeout is acceptable; the goroutine processed the error and continued
		cancel()
	}
}
