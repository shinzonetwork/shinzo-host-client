package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"

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

// EventSubscription defines the interface for event subscriptions
type EventSubscription interface {
	StartEventSubscription(tendermintURL string) (context.CancelFunc, <-chan ShinzoEvent, error)
}

// StartEventSubscription starts the event subscription and returns a context for cancellation and event channel
func StartEventSubscription(tendermintURL string) (context.CancelFunc, <-chan ShinzoEvent, error) {
	ctx, cancel := context.WithCancel(context.Background())

	conn, _, err := websocket.DefaultDialer.Dial(tendermintURL, nil)
	if err != nil {
		cancel()
		return cancel, nil, fmt.Errorf("failed to connect to Tendermint WebSocket: %w", err)
	}

	// Create a buffered channel for events
	eventChan := make(chan ShinzoEvent, 100) // Buffer up to 100 events

	// Subscribe to Registered events
	subscribeMsg := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "subscribe",
		"id":      1,
		"params": map[string]interface{}{
			"query": "tm.event='Tx' AND Registered.key EXISTS",
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

	// Start goroutine for message processing
	go func() {
		defer conn.Close()
		defer cancel()
		defer close(eventChan)

		for {
			select {
			case <-ctx.Done():
				fmt.Printf("Context cancelled, stopping message loop\n")
				return
			default:
				// Read raw message first to see what we're getting
				_, message, err := conn.ReadMessage()
				if err != nil {
					fmt.Printf("Failed to read message: %v\n", err)
					return
				}

				// Now try to parse it
				var msg RPCResponse
				if err := json.Unmarshal(message, &msg); err != nil {
					fmt.Printf("Failed to parse JSON: %v\n", err)
					continue
				}

				// Look for Registered events and send them to the channel
				registeredEvents := extractRegisteredEvents(msg)
				for _, event := range registeredEvents {
					// Send event to channel (this will block if channel is full)
					select {
					case eventChan <- &event:
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

// extractRegisteredEvents extracts Registered events from the RPC message
func extractRegisteredEvents(msg RPCResponse) []ViewRegisteredEvent {
	var registeredEvents []ViewRegisteredEvent

	// Navigate through the nested structure to find events
	for _, event := range msg.Result.Data.Value.TxResult.Result.Events {
		if event.Type == "Registered" {
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
				registeredEvents = append(registeredEvents, registeredEvent)
			} else {
				fmt.Printf("Incomplete Registered event: %+v\n", registeredEvent)
			}
		}
	}

	return registeredEvents
}
