package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
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

	// Create an unbuffered channel for events (direct processing)
	eventChan := make(chan ShinzoEvent)

	// Subscribe to Registered and EntityRegistered events
	subscribeMsg := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "subscribe",
		"id":      1,
		"params": map[string]interface{}{
			"query": "tm.event='Tx' AND (Registered.key EXISTS OR EntityRegistered.key EXISTS)",
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

				entityRegisteredEvents := extractEntityRegisteredEvents(msg)
				for _, event := range entityRegisteredEvents {
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
// and processes them for view registration with block range monitoring
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

					// Save WASM blob data to file system if lenses exist
					if len(view.Transform.Lenses) > 0 {
						fmt.Printf("📦 Saving WASM blob data for view %s with %d lenses\n", view.Name, len(view.Transform.Lenses))
						if err := view.PostWasmToFile(context.Background(), "./.lens"); err != nil {
							fmt.Printf("Failed to save WASM blob for view %s: %v\n", view.Name, err)
						}
					}

					registeredEvent.View = view
				}
			}

			// Only add if we have all required fields
			if registeredEvent.Key != "" && registeredEvent.Creator != "" && registeredEvent.View.Query != nil && *registeredEvent.View.Query != "" {
				// Analyze query to determine block range requirements
				blockRangeInfo := analyzeViewQueryForBlockRange(registeredEvent.View)
				fmt.Printf("🔍 View %s requires monitoring: %s\n", registeredEvent.View.Name, blockRangeInfo)

				registeredEvents = append(registeredEvents, registeredEvent)
			} else {
				fmt.Printf("Incomplete Registered event: %+v\n", registeredEvent)
			}
		}
	}

	return registeredEvents
}

// extractEntityRegisteredEvents extracts EntityRegistered events from the RPC message
// and processes them for indexer/host registration
func extractEntityRegisteredEvents(msg RPCResponse) []EntityRegisteredEvent {
	var entityRegisteredEvents []EntityRegisteredEvent

	// Navigate through the nested structure to find events
	for _, event := range msg.Result.Data.Value.TxResult.Result.Events {
		if event.Type == "EntityRegistered" {
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

				entityRegisteredEvents = append(entityRegisteredEvents, entityRegisteredEvent)
			} else {
				fmt.Printf("Incomplete EntityRegistered event: %+v\n", entityRegisteredEvent)
			}
		}
	}

	return entityRegisteredEvents
}

// BlockRangeRequirement describes what block ranges a view needs to monitor
type BlockRangeRequirement struct {
	Collections    []string // ["Log", "Transaction", "Block"]
	RequiresBlocks bool     // true if view needs block number filtering
	StartFromBlock uint64   // minimum block to start monitoring from
	Description    string   // human-readable description
}

// analyzeViewQueryForBlockRange parses a view query to determine block monitoring requirements
func analyzeViewQueryForBlockRange(viewDef view.View) string {
	if viewDef.Query == nil {
		return "No query specified"
	}

	query := *viewDef.Query
	requirement := BlockRangeRequirement{
		Collections:    []string{},
		RequiresBlocks: false,
		StartFromBlock: 0,
	}

	// Check for collection references
	collections := constants.AllCollections
	for _, collection := range collections {
		if strings.Contains(query, collection) {
			requirement.Collections = append(requirement.Collections, collection)
			requirement.RequiresBlocks = true
		}
	}

	// Check for block number references in query
	blockNumberPatterns := []string{"blockNumber", "number", "block"}
	for _, pattern := range blockNumberPatterns {
		if strings.Contains(strings.ToLower(query), strings.ToLower(pattern)) {
			requirement.RequiresBlocks = true
			break
		}
	}

	// Build description
	if len(requirement.Collections) > 0 {
		requirement.Description = fmt.Sprintf("Collections: %v, Requires block monitoring: %v",
			requirement.Collections, requirement.RequiresBlocks)
	} else {
		requirement.Description = "No blockchain collections detected"
	}

	return requirement.Description
}

// ViewRegistrationHandler manages view registration from Shinzo Hub events
type ViewRegistrationHandler struct {
	viewManager ViewManagerInterface // Will be injected from host
	startHeight uint64               // Starting block height for new views
}

// NewViewRegistrationHandler creates a new view registration handler
func NewViewRegistrationHandler(startHeight uint64) *ViewRegistrationHandler {
	return &ViewRegistrationHandler{
		startHeight: startHeight,
	}
}

// SetViewManager injects the ViewManager dependency
func (vrh *ViewRegistrationHandler) SetViewManager(vm ViewManagerInterface) {
	vrh.viewManager = vm
}

// ProcessRegisteredEvent handles a view registration event from Shinzo Hub
func (vrh *ViewRegistrationHandler) ProcessRegisteredEvent(ctx context.Context, event ViewRegisteredEvent) error {
	if vrh.viewManager == nil {
		return fmt.Errorf("ViewManager not set - cannot process view registration")
	}

	fmt.Printf("🎯 Processing view registration: %s (creator: %s)\n", event.View.Name, event.Creator)

	// Register view with ViewManager (which handles ViewMatcher and ViewRangeFinder)
	err := vrh.viewManager.RegisterView(ctx, event.View)
	if err != nil {
		return fmt.Errorf("failed to register view %s: %w", event.View.Name, err)
	}

	fmt.Printf("✅ Successfully registered view %s for block range monitoring\n", event.View.Name)
	return nil
}

// ViewManagerInterface to avoid circular imports
type ViewManagerInterface interface {
	RegisterView(ctx context.Context, viewDef view.View) error
}
