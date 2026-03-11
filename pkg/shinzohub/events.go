package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"


	"github.com/gorilla/websocket"
)

type RPCResponse struct {
	JsonRpcVersion string    `json:"jsonrpc"`
	ID             int       `json:"id"`
	Result         RPCResult `json:"result"`
}

type RPCResult struct {
	Query string  `json:"query"`
	Data  RPCData `json:"data"`
}

type RPCData struct {
	Type  string   `json:"type"`
	Value TxResult `json:"value"`
}

type TxResult struct {
	TxResult TxResultData `json:"TxResult"`
}

type TxResultData struct {
	Height string         `json:"height"`
	Tx     string         `json:"tx"`
	Result TxResultResult `json:"result"`
	Events []Event        `json:"events"`
}

type TxResultResult struct {
	Data      string  `json:"data"`
	Events    []Event `json:"events"`
	GasUsed   string  `json:"gas_used"`
	GasWanted string  `json:"gas_wanted"`
}

type Event struct {
	Type       string           `json:"type"`
	Attributes []EventAttribute `json:"attributes"`
}

type EventAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Index bool   `json:"index"`
}

type ShinzoEvent interface {
	ToString() string
}

// EntityType represents the type of entity
type EntityType string

const (
	EntityTypeIndexer EntityType = "Indexer"
	EntityTypeHost    EntityType = "Host"
	EntityTypeUnknown EntityType = "Unknown"
)

// GetEntityType determines the entity type from the entity field value
func GetEntityType(entityValue string) EntityType {
	switch entityValue {
	case "\u0001":
		return EntityTypeIndexer
	case "\u0002":
		return EntityTypeHost
	default:
		return EntityTypeUnknown
	}
}

// StartEventSubscription starts the event subscription and returns a context for cancellation and event channel
func StartEventSubscription(tendermintURL string) (context.CancelFunc, <-chan ShinzoEvent, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Keep trying to connect until cancelled
	var conn *websocket.Conn
	var err error
	
	for {
		select {
		case <-ctx.Done():
			cancel()
			return cancel, nil, fmt.Errorf("connection cancelled")
		default:
			conn, _, err = websocket.DefaultDialer.Dial(tendermintURL, nil)
			if err == nil {
				// Connected successfully - break out of the loop
				goto Connected
			}
			// Wait 5 seconds before retrying
			time.Sleep(5 * time.Second)
		}
	}
	
Connected:

	// Create an unbuffered channel for events (direct processing)
	eventChan := make(chan ShinzoEvent)

	// Subscribe to Registered and EntityRegistered events separately because Tendermint doesn't support OR logic
	queries := []string{
		"tm.event='Tx' AND Registered.key EXISTS",
		"tm.event='Tx' AND EntityRegistered.key EXISTS",
	}

	// Send all subscriptions first
	for i, query := range queries {
		subscribeMsg := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "subscribe",
			"id":      i + 1,
			"params": map[string]interface{}{
				"query": query,
			},
		}

		fmt.Printf("Sending subscription: %+v\n", subscribeMsg)
		if err := conn.WriteJSON(subscribeMsg); err != nil {
			conn.Close()
			cancel()
			return cancel, nil, fmt.Errorf("failed to send subscription message: %w", err)
		}

		// Read the subscription response
		_, response, err := conn.ReadMessage()
		if err != nil {
			conn.Close()
			cancel()
			return cancel, nil, fmt.Errorf("failed to read subscription response: %w", err)
		}
		fmt.Printf("Subscription response: %s\n", string(response))
	}

	// Start ping goroutine to keep connection alive
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Send a JSON-RPC request that Tendermint will respond to
				pingMsg := map[string]interface{}{
					"jsonrpc": "2.0",
					"method":  "status",
					"id":      999,
					"params":  map[string]interface{}{},
				}
				if err := conn.WriteJSON(pingMsg); err != nil {
					return
				}
			}
		}
	}()

	// Start goroutine for message processing
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("WebSocket goroutine recovered from panic: %v\n", r)
			}
		}()
		defer conn.Close()
		defer cancel()
		defer close(eventChan)

		// Heartbeat ticker
		heartbeat := time.NewTicker(30 * time.Second)
		defer heartbeat.Stop()

		for {
			select {
			case <-ctx.Done():
				fmt.Printf("Context cancelled, stopping message loop\n")
				return
			case <-heartbeat.C:
				// Send ping to keep connection alive
				if err := conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
					fmt.Printf("WebSocket ping failed: %v\n", err)
					return
				}
				fmt.Printf("WebSocket ping sent\n")
			default:
				// Set read deadline to detect dead connections
				conn.SetReadDeadline(time.Now().Add(35 * time.Second))

				// Read raw message first to see what we're getting
				messageType, message, err := conn.ReadMessage()
				if err != nil {
					fmt.Printf("WebSocket connection failed: %v\n", err)
					return // This WILL close the goroutine via defers
				}

				// Log that we received a WebSocket message
				if messageType == websocket.TextMessage {
					fmt.Printf("📥 WebSocket: Received text message (%d bytes)\n", len(message))
				} else if messageType == websocket.BinaryMessage {
					fmt.Printf("📥 WebSocket: Received binary message (%d bytes)\n", len(message))
				}

				// Handle pong responses
				if messageType == websocket.PongMessage {
					fmt.Printf("WebSocket pong received\n")
					continue
				}

				// Now try to parse it
				var msg RPCResponse
				if err := json.Unmarshal(message, &msg); err != nil {
					fmt.Printf("Failed to parse JSON: %v\n", err)
					continue
				}

				// Debug: Log the parsed message structure
				fmt.Printf("🔍 WebSocket: Parsed RPCResponse - Version: %s, ID: %d\n", msg.JsonRpcVersion, msg.ID)
				if msg.Result.Data.Type != "" {
					fmt.Printf("🔍 WebSocket: Event type: %s\n", msg.Result.Data.Type)
				}

				// Look for Registered and EntityRegistered events and send them to the channel
				events := extractShinzoEvents(msg)
				if len(events) > 0 {
					fmt.Printf("🌐 WebSocket: Received %d ShinzoHub event(s) from Tendermint\n", len(events))
				} else {
					fmt.Printf("🔍 WebSocket: No ShinzoHub events found in this message\n")
				}
				for _, event := range events {
					// Send event to channel (this will block if channel is full)
					select {
					case eventChan <- event:
						fmt.Printf("📤 WebSocket: Sent %s event to processing channel\n", event.ToString())
					case <-ctx.Done():
						// Context cancelled, stop sending
						return
					}
				}
			}
		}
	}()

	return cancel, eventChan, nil
}

// extractShinzoEvents extracts both Registered and EntityRegistered events from the RPC message
// and processes them for view registration and indexer/host registration
func extractShinzoEvents(msg RPCResponse) []ShinzoEvent {
	var events []ShinzoEvent

	// Debug: Log validation steps
	fmt.Printf("🔍 extractShinzoEvents: Version=%s, Type=%s\n", msg.JsonRpcVersion, msg.Result.Data.Type)

	// Validate message structure
	if msg.JsonRpcVersion != "2.0" {
		fmt.Printf("🔍 extractShinzoEvents: Invalid JSON-RPC version\n")
		return events
	}
	if msg.Result.Data.Type != "tendermint.event" {
		fmt.Printf("🔍 extractShinzoEvents: Not a tendermint.event type\n")
		return events
	}
	if msg.Result.Data.Value.TxResult.Result.Events == nil {
		fmt.Printf("🔍 extractShinzoEvents: No events array in TxResult\n")
		return events
	}

	fmt.Printf("🔍 extractShinzoEvents: Found %d events in TxResult\n", len(msg.Result.Data.Value.TxResult.Result.Events))

	// Navigate through the nested structure to find events
	for _, event := range msg.Result.Data.Value.TxResult.Result.Events {
		switch event.Type {
		case "Registered":
			registeredEvent := ViewRegisteredEvent{}

			// Extract attributes
			for _, attr := range event.Attributes {
				switch attr.Key {
				case "key":
					registeredEvent.Key = attr.Value
				case "creator":
					registeredEvent.Creator = attr.Value
				case "view":
					// Process the view using the new view processor
					processor := NewViewProcessor(nil) // We don't need defraNode for extraction
					newView, err := processor.ProcessViewFromWire(context.Background(), attr.Value)
					if err != nil {
						fmt.Printf("Failed to process view from wire: %v\n", err)
						continue
					}

					registeredEvent.View = *newView
				}
			}

			// Only add if we have all required fields
			if registeredEvent.View.Data.Query != "" {
				fmt.Printf("📋 Registered View: %s (creator: %s)\n", registeredEvent.View.Name, registeredEvent.Creator)
			}
			if registeredEvent.Key != "" && registeredEvent.Creator != "" && registeredEvent.View.Data.Query != "" {
				fmt.Printf("🔍 View %s registered for monitoring\n", registeredEvent.View.Name)

				events = append(events, &registeredEvent)
			} else {
				fmt.Printf("Incomplete Registered event: %+v\n", registeredEvent)
			}

		case "EntityRegistered":
			entityRegisteredEvent := EntityRegisteredEvent{}

			// Extract attributes
			for _, attr := range event.Attributes {
				switch attr.Key {
				case "key":
					entityRegisteredEvent.Key = attr.Value
				case "owner":
					entityRegisteredEvent.Owner = attr.Value
				case "did":
					entityRegisteredEvent.DID = attr.Value
				case "pid":
					entityRegisteredEvent.Pid = attr.Value
				case "entity":
					entityRegisteredEvent.Entity = attr.Value
				}
			}

			// Only add if we have all required fields
			if entityRegisteredEvent.Key != "" && entityRegisteredEvent.Owner != "" && entityRegisteredEvent.DID != "" && entityRegisteredEvent.Pid != "" {
				// Determine entity type
				entityType := "Unknown"
				switch entityRegisteredEvent.Entity {
				case "\u0001":
					entityType = "Indexer"
				case "\u0002":
					entityType = "Host"
				}

				fmt.Printf("🎯 Processing EntityRegistered: type=%s, key=%s, owner=%s, did=%s, pid=%s\n",
					entityType, entityRegisteredEvent.Key, entityRegisteredEvent.Owner, entityRegisteredEvent.DID, entityRegisteredEvent.Pid)

				events = append(events, &entityRegisteredEvent)
			} else {
				fmt.Printf("Incomplete EntityRegistered event: %+v\n", entityRegisteredEvent)
			}
		}
	}

	return events
}
