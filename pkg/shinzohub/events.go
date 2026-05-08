package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"go.uber.org/zap"
)

// log returns a non-nil SugaredLogger. Falls back to a nop logger when
// the global logger hasn't been initialized yet (e.g. in unit tests).
func log() *zap.SugaredLogger {
	if logger.Sugar != nil {
		return logger.Sugar
	}
	return zap.NewNop().Sugar()
}

// RPCResponse represents the structure of a RPCResponse.
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

// getSubscriptionQueries returns the CometBFT event queries the host subscribes to.
// Tendermint doesn't support OR logic, so each event type gets its own subscription.
// CometBFT's EXISTS clause requires an attribute key (<event_type>.<attribute_key>),
// not just the type; view_id is always present on these events.
func getSubscriptionQueries() []string {
	return []string{
		"tm.event='Tx' AND view.view_registered.view_id EXISTS",
		"tm.event='Tx' AND view.view_registration_failed.view_id EXISTS",
		"tm.event='Tx' AND view.view_registration_timed_out.view_id EXISTS",
	}
}

// StartEventSubscription connects to the CometBFT WebSocket, subscribes to
// registry events, and pushes parsed events to the returned channel. The
// connection is re-established automatically on drops with exponential
// backoff (1s, 2s, 4s, ..., capped at 60s). The channel stays open across
// reconnections; it is only closed when ctx is cancelled.
func StartEventSubscription(tendermintURL string) (context.CancelFunc, <-chan ShinzoEvent, error) {
	ctx, cancel := context.WithCancel(context.Background())
	eventChan := make(chan ShinzoEvent, 16) //nolint:mnd

	// Verify the URL is reachable before returning to the caller. This
	// catches typos and DNS failures at startup instead of silently
	// looping in the background.
	conn, err := dialAndSubscribe(tendermintURL)
	if err != nil {
		cancel()
		return cancel, nil, fmt.Errorf("initial WebSocket connection failed: %w", err)
	}

	go eventLoop(ctx, tendermintURL, conn, eventChan)

	return cancel, eventChan, nil
}

// dialAndSubscribe opens a WebSocket to the CometBFT node and sends all
// subscription requests. Returns the live connection or an error.
func dialAndSubscribe(url string) (*websocket.Conn, error) {
	conn, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if resp != nil {
		if cerr := resp.Body.Close(); cerr != nil {
			log().Warnf("failed to close dial response body: %v", cerr)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", url, err)
	}

	for i, query := range getSubscriptionQueries() {
		msg := map[string]any{
			"jsonrpc": "2.0",
			"method":  "subscribe",
			"id":      i + 1,
			"params":  map[string]any{"query": query},
		}
		if err := conn.WriteJSON(msg); err != nil {
			if err := conn.Close(); err != nil {
				log().Warnf("failed to close websocket connection: %v", err)
			}
			return nil, fmt.Errorf("subscribe query %d: %w", i, err)
		}
		_, resp, err := conn.ReadMessage()
		if err != nil {
			if err := conn.Close(); err != nil {
				log().Warnf("failed to close websocket connection: %v", err)
			}
			return nil, fmt.Errorf("subscribe response %d: %w", i, err)
		}
		log().Debugf("subscription %d accepted: %s", i+1, string(resp))
	}

	log().Infof("WebSocket connected to %s with %d subscriptions", url, len(getSubscriptionQueries()))
	return conn, nil
}

// eventLoop reads events from the WebSocket and reconnects on failure. It
// owns the connection lifecycle: when a read fails, it closes the old
// connection, waits with backoff, dials a new one, re-subscribes, and
// resumes reading. The channel is closed only when ctx is cancelled.
func eventLoop(ctx context.Context, url string, conn *websocket.Conn, out chan<- ShinzoEvent) {
	defer close(out)
	defer recoverPanic()

	backoff := time.Second
	const maxBackoff = 60 * time.Second
	pingStop := startPing(ctx, conn)
	connectedAt := time.Now()

	for {
		if ctx.Err() != nil {
			closeConn(conn)
			return
		}

		conn, pingStop, connectedAt, backoff = processNextMessage(
			ctx, url, conn, out, pingStop, connectedAt, backoff, maxBackoff,
		)
		if conn == nil {
			return
		}
	}
}

// recoverPanic from event loop.
func recoverPanic() {
	if r := recover(); r != nil {
		log().Errorf("WebSocket event loop recovered from panic: %v", r)
	}
}

// closeConn from event loop.
func closeConn(conn *websocket.Conn) {
	if err := conn.Close(); err != nil {
		log().Warnf("failed to close websocket connection: %v", err)
	}
}

// processNextMessage in event loop.
func processNextMessage(
	ctx context.Context,
	url string,
	conn *websocket.Conn,
	out chan<- ShinzoEvent,
	pingStop func(),
	connectedAt time.Time,
	backoff, maxBackoff time.Duration,
) (*websocket.Conn, func(), time.Time, time.Duration) {
	const readTimeout = 30 * time.Second

	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		log().Warnf("failed to set read deadline: %v", err)
	}

	_, message, err := conn.ReadMessage()
	if err != nil {
		return handleReadError(ctx, url, conn, pingStop, backoff, maxBackoff, err)
	}

	if time.Since(connectedAt) > 60*time.Second {
		backoff = time.Second
	}

	conn = dispatchEvents(ctx, conn, out, message)
	return conn, pingStop, connectedAt, backoff
}

// handleReadError in event loop.
func handleReadError(
	ctx context.Context,
	url string,
	conn *websocket.Conn,
	pingStop func(),
	backoff, maxBackoff time.Duration,
	err error,
) (*websocket.Conn, func(), time.Time, time.Duration) {
	if ctx.Err() != nil {
		closeConn(conn)
		return nil, nil, time.Time{}, backoff
	}
	if isTimeoutError(err) {
		return conn, pingStop, time.Now(), backoff
	}
	pingStop()
	closeConn(conn)
	log().Warnf("WebSocket read failed: %v; reconnecting in ~%v", err, backoff)
	conn = reconnect(ctx, url, &backoff, maxBackoff)
	if conn == nil {
		return nil, nil, time.Time{}, backoff
	}
	return conn, startPing(ctx, conn), time.Now(), backoff
}

// dispatchEvents from event loop.
func dispatchEvents(ctx context.Context, conn *websocket.Conn, out chan<- ShinzoEvent, message []byte) *websocket.Conn {
	var msg RPCResponse
	if err := json.Unmarshal(message, &msg); err != nil {
		return conn
	}
	for _, ev := range extractShinzoEvents(msg) {
		select {
		case out <- ev:
		case <-ctx.Done():
			closeConn(conn)
			return nil
		}
	}
	return conn
}

// isTimeoutError checks whether the error is a read deadline timeout.
func isTimeoutError(err error) bool {
	if netErr, ok := err.(interface{ Timeout() bool }); ok {
		return netErr.Timeout()
	}
	return false
}

// reconnect blocks until a new connection is established or ctx is cancelled.
// Uses exponential backoff with jitter to avoid thundering herd when multiple
// hosts reconnect after a CometBFT node restart.
func reconnect(ctx context.Context, url string, backoff *time.Duration, maxBackoff time.Duration) *websocket.Conn {
	for {
		// Add jitter: sleep for backoff +/- 25% to spread reconnection attempts.
		jitter := time.Duration(rand.Int64N(int64(*backoff) / 2)) //nolint:gosec,mnd
		wait := *backoff + jitter - (*backoff / 4)                // nolint:mnd

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}

		conn, err := dialAndSubscribe(url)
		if err != nil {
			log().Warnf("WebSocket reconnect failed: %v; retrying in ~%v", err, *backoff)
			*backoff *= 2
			if *backoff > maxBackoff {
				*backoff = maxBackoff
			}
			continue
		}

		log().Infof("WebSocket reconnected to %s", url)
		return conn
	}
}

// startPing sends periodic JSON-RPC status requests to keep the WebSocket
// alive. Returns a stop function that must be called before closing the
// connection.
func startPing(ctx context.Context, conn *websocket.Conn) context.CancelFunc {
	pingCtx, pingCancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(15 * time.Second) // nolint:mnd
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				msg := map[string]any{
					"jsonrpc": "2.0",
					"method":  "status",
					"id":      999, // nolint:mnd
					"params":  map[string]any{},
				}
				if err := conn.WriteJSON(msg); err != nil {
					return
				}
			}
		}
	}()
	return pingCancel
}

// extractShinzoEvents extracts typed events from an RPC response.
func extractShinzoEvents(msg RPCResponse) []ShinzoEvent {
	var events []ShinzoEvent
	if msg.JSONRPCVersion != "2.0" ||
		msg.Result.Data.Type != "tendermint/event/Tx" ||
		msg.Result.Data.Value.TxResult.Result.Events == nil {
		return nil
	}

	for _, event := range msg.Result.Data.Value.TxResult.Result.Events {
		switch event.Type {
		case "ViewRegistered":
			if e := parseViewRegisteredEvent(event); e != nil {
				events = append(events, e)
			}
		case "HostRegistered":
			if e := parseHostRegisteredEvent(event); e != nil {
				events = append(events, e)
			}
		case "IndexerRegistered":
			if e := parseIndexerRegisteredEvent(event); e != nil {
				events = append(events, e)
			}
		}
	}

	return events
}

// parseViewRegisteredEvent parses view registration events.
func parseViewRegisteredEvent(event Event) ShinzoEvent {
	e := ViewRegisteredEvent{}
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
				log().Warnf("failed to process view from wire: %v", err)
				continue
			}
			e.View = newView
		}
	}
	if e.ViewAddress != "" && e.Creator != "" && e.View.Data.Query != "" {
		log().Infof("ViewRegistered event received: %s", e.View.Name)
		return &e
	}
	log().Debugf("incomplete ViewRegistered event: %+v", e)
	return nil
}

// parseHostRegisteredEvent parse host registration events.
func parseHostRegisteredEvent(event Event) ShinzoEvent {
	e := HostRegisteredEvent{}
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
		log().Infof("HostRegistered event received: owner=%s did=%s", e.Owner, e.DID)
		return &e
	}
	log().Debugf("incomplete HostRegistered event: %+v", e)
	return nil
}

// parseIndexerRegisteredEvent parse indexer registration events.
func parseIndexerRegisteredEvent(event Event) ShinzoEvent {
	e := IndexerRegisteredEvent{}
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
		log().Infof("IndexerRegistered event received: owner=%s did=%s chain=%s/%s",
			e.Owner, e.DID, e.SourceChain, e.SourceChainID)
		return &e
	}
	log().Debugf("incomplete IndexerRegistered event: %+v", e)
	return nil
}
