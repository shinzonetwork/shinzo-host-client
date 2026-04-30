package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

// RPCResponse represents the structure of a JSON-RPC response received from the Tendermint WebSocket.
type RPCResponse struct {
	JSONRPCVersion string    `json:"jsonrpc"`
	ID             int       `json:"id"`
	Result         RPCResult `json:"result"`
}

// RPCResult represents the structure of a Tendermint event received from the WebSocket.
type RPCResult struct {
	Query string  `json:"query"`
	Data  RPCData `json:"data"`
}

// RPCData represents the data structure of a Tendermint event received from the WebSocket.
type RPCData struct {
	Type  string   `json:"type"`
	Value TxResult `json:"value"`
}

// TxResult represents the structure of a transaction result received from Tendermint.
type TxResult struct {
	TxResult TxResultData `json:"TxResult"`
}

// TxResultData represents the data structure of a transaction result in Tendermint.
type TxResultData struct {
	Height string         `json:"height"`
	Tx     string         `json:"tx"`
	Result TxResultResult `json:"result"`
	Events []Event        `json:"events"`
}

// TxResultResult represents the result of a transaction execution in Tendermint.
type TxResultResult struct {
	Data      string  `json:"data"`
	Events    []Event `json:"events"`
	GasUsed   string  `json:"gas_used"`
	GasWanted string  `json:"gas_wanted"`
}

// Event represents a Tendermint event with a type and attributes.
type Event struct {
	Type       string           `json:"type"`
	Attributes []EventAttribute `json:"attributes"`
}

// EventAttribute represents a key-value pair attribute of a Tendermint event, with an index flag.
type EventAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Index bool   `json:"index"`
}

// ShinzoEvent is an interface that all Shinzo events implement, allowing them to be processed uniformly.
type ShinzoEvent interface {
	ToString() string
}

// StartEventSubscription connects to Tendermint and returns a channel of ShinzoEvents.
func StartEventSubscription(tendermintURL string) (context.CancelFunc, <-chan ShinzoEvent, error) {
	ctx, cancel := context.WithCancel(context.Background())

	conn, err := dialWebSocket(tendermintURL, cancel)
	if err != nil {
		return cancel, nil, err
	}

	if err := sendSubscriptions(conn, cancel); err != nil {
		return cancel, nil, err
	}

	eventChan := make(chan ShinzoEvent)
	go runPingLoop(ctx, conn)
	go runMessageLoop(ctx, cancel, conn, eventChan)

	return cancel, eventChan, nil
}

// dialWebSocket connects to the Tendermint WebSocket endpoint.
func dialWebSocket(tendermintURL string, cancel context.CancelFunc) (*websocket.Conn, error) {
	conn, resp, err := websocket.DefaultDialer.Dial(tendermintURL, nil)
	if resp != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to Tendermint WebSocket: %w", err)
	}
	if conn == nil {
		cancel()
		return nil, ErrTendermintConnection
	}
	return conn, nil
}

// sendSubscriptions sends all event subscription messages to the WebSocket.
func sendSubscriptions(conn *websocket.Conn, cancel context.CancelFunc) error {
	queries := []string{
		"tm.event='Tx' AND ViewRegistered.view_address EXISTS",
		"tm.event='Tx' AND HostRegistered.owner EXISTS",
		"tm.event='Tx' AND IndexerRegistered.owner EXISTS",
	}

	for i, query := range queries {
		subscribeMsg := map[string]any{
			"jsonrpc": "2.0",
			"method":  "subscribe",
			"id":      i + 1,
			"params":  map[string]any{"query": query},
		}

		fmt.Printf("Sending subscription: %+v\n", subscribeMsg)
		if err := conn.WriteJSON(subscribeMsg); err != nil {
			_ = conn.Close()
			cancel()
			return fmt.Errorf("failed to send subscription message: %w", err)
		}

		_, response, err := conn.ReadMessage()
		if err != nil {
			_ = conn.Close()
			cancel()
			return fmt.Errorf("failed to read subscription response: %w", err)
		}
		fmt.Printf("Subscription response: %s\n", string(response))
	}

	return nil
}

// runPingLoop sends periodic pings to keep the WebSocket connection alive.
func runPingLoop(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(pingIntervalSecs * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingMsg := map[string]any{
				"jsonrpc": "2.0",
				"method":  "status",
				"id":      pingMessageID,
				"params":  map[string]any{},
			}
			if err := conn.WriteJSON(pingMsg); err != nil {
				return
			}
		}
	}
}

// runMessageLoop reads messages from the WebSocket and dispatches events to eventChan.
func runMessageLoop(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn, eventChan chan ShinzoEvent) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("WebSocket goroutine recovered from panic: %v\n", r)
		}
	}()
	defer func() { _ = conn.Close() }()
	defer cancel()
	defer close(eventChan)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Context canceled, stopping message loop\n")
			return
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				fmt.Printf("WebSocket connection failed: %v\n", err)
				return
			}

			var msg RPCResponse
			if err := json.Unmarshal(message, &msg); err != nil {
				fmt.Printf("Failed to parse JSON: %v\n", err)
				continue
			}

			for _, event := range extractShinzoEvents(msg) {
				select {
				case eventChan <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// extractShinzoEvents extracts typed events from an RPC response.
func extractShinzoEvents(msg RPCResponse) []ShinzoEvent {
	if msg.JSONRPCVersion != "2.0" ||
		msg.Result.Data.Type != "tendermint.event" ||
		msg.Result.Data.Value.TxResult.Result.Events == nil {
		return nil
	}

	var events []ShinzoEvent
	for _, event := range msg.Result.Data.Value.TxResult.Result.Events {
		switch event.Type {
		case "ViewRegistered":
			if e := extractViewRegisteredEvent(event); e != nil {
				events = append(events, e)
			}
		case "HostRegistered":
			if e := extractHostRegisteredEvent(event); e != nil {
				events = append(events, e)
			}
		case "IndexerRegistered":
			if e := extractIndexerRegisteredEvent(event); e != nil {
				events = append(events, e)
			}
		}
	}

	return events
}

// extractViewRegisteredEvent parses a ViewRegistered event from attributes.
func extractViewRegisteredEvent(event Event) ShinzoEvent {
	e := &ViewRegisteredEvent{}
	for _, attr := range event.Attributes {
		switch attr.Key {
		case "view_address":
			e.ViewAddress = attr.Value
		case "view_name":
			e.ViewName = attr.Value
		case "creator":
			e.Creator = attr.Value
		case "data":
			newView, err := ProcessViewFromWireFormat(attr.Value)
			if err != nil {
				fmt.Printf("Failed to process view from wire: %v\n", err)
				continue
			}
			e.View = newView
		}
	}

	if e.ViewAddress != "" && e.Creator != "" && e.View.Data.Query != "" {
		fmt.Printf("View %s registered for monitoring\n", e.View.Name)
		return e
	}

	fmt.Printf("Incomplete ViewRegistered event: %+v\n", e)
	return nil
}

// extractHostRegisteredEvent parses a HostRegistered event from attributes.
func extractHostRegisteredEvent(event Event) ShinzoEvent {
	e := &HostRegisteredEvent{}
	for _, attr := range event.Attributes {
		switch attr.Key {
		case "owner":
			e.Owner = attr.Value
		case "did":
			e.DID = attr.Value
		case "connection_string":
			e.ConnectionString = attr.Value
		}
	}

	if e.Owner != "" && e.DID != "" {
		fmt.Printf("Host registered: owner=%s, did=%s, conn=%s\n", e.Owner, e.DID, e.ConnectionString)
		return e
	}

	fmt.Printf("Incomplete HostRegistered event: %+v\n", e)
	return nil
}

// extractIndexerRegisteredEvent parses an IndexerRegistered event from attributes.
func extractIndexerRegisteredEvent(event Event) ShinzoEvent {
	e := &IndexerRegisteredEvent{}
	for _, attr := range event.Attributes {
		switch attr.Key {
		case "owner":
			e.Owner = attr.Value
		case "did":
			e.DID = attr.Value
		case "connection_string":
			e.ConnectionString = attr.Value
		case "source_chain":
			e.SourceChain = attr.Value
		case "source_chain_id":
			e.SourceChainID = attr.Value
		}
	}

	if e.Owner != "" && e.DID != "" {
		fmt.Printf("Indexer registered: owner=%s, did=%s, chain=%s/%s\n",
			e.Owner, e.DID, e.SourceChain, e.SourceChainID)
		return e
	}

	fmt.Printf("Incomplete IndexerRegistered event: %+v\n", e)
	return nil
}
