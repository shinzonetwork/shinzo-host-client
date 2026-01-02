package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
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

	conn, _, err := websocket.DefaultDialer.Dial(tendermintURL, nil)
	if err != nil {
		cancel()
		return cancel, nil, fmt.Errorf("failed to connect to Tendermint WebSocket: %w", err)
	}

	// Create an unbuffered channel for events (direct processing)
	eventChan := make(chan ShinzoEvent)

	// Subscribe to Registered and EntityRegistered events separately because Tendermint doesn't support OR logic
	queries := []string{
		"tm.event='Tx' AND Registered.key EXISTS",
		"tm.event='Tx' AND EntityRegistered.key EXISTS",
	}

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

		for {
			select {
			case <-ctx.Done():
				fmt.Printf("Context cancelled, stopping message loop\n")
				return
			default:
				// Set a deadline for the next read
				if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
					fmt.Printf("Failed to set read deadline: %v\n", err)
					return
				}

				// Read raw message first to see what we're getting
				_, message, err := conn.ReadMessage()
				if err != nil {
					// Check if it's a timeout, which is expected - continue the loop
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						select {
						case <-ctx.Done():
							return
						default:
							continue
						}
					}
					fmt.Printf("WebSocket error, stopping: %v\n", err)
					return
				}

				// Now try to parse it
				var msg RPCResponse
				if err := json.Unmarshal(message, &msg); err != nil {
					fmt.Printf("Failed to parse JSON: %v\n", err)
					continue
				}

				// Look for Registered and EntityRegistered events and send them to the channel
				events := extractShinzoEvents(msg)
				for _, event := range events {
					// Send event to channel (this will block if channel is full)
					select {
					case eventChan <- event:
						// Event sent successfully
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

	// Validate message structure
	if msg.JsonRpcVersion != "2.0" ||
		msg.Result.Data.Type != "tendermint.event" ||
		msg.Result.Data.Value.TxResult.Result.Events == nil {
		return events
	}

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
					// Parse the view JSON string into View struct
					var view view.View
					if err := json.Unmarshal([]byte(attr.Value), &view); err != nil {
						fmt.Printf("Failed to parse view JSON: %v, value: %s\n", err, attr.Value)
						continue
					}
					ExtractNameFromSDL(&view)

					registeredEvent.View = view
				}
			}

			// Only add if we have all required fields
			if registeredEvent.Key != "" && registeredEvent.Creator != "" && registeredEvent.View.Query != nil && *registeredEvent.View.Query != "" {
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