package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
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

// subscriptionQueries are the CometBFT event queries the host subscribes to.
// Tendermint doesn't support OR logic, so each event type gets its own subscription.
var subscriptionQueries = []string{
	"tm.event='Tx' AND ViewRegistered.view_address EXISTS",
	"tm.event='Tx' AND HostRegistered.owner EXISTS",
	"tm.event='Tx' AND IndexerRegistered.owner EXISTS",
}

// StartEventSubscription connects to the CometBFT WebSocket, subscribes to
// registry events, and pushes parsed events to the returned channel. The
// connection is re-established automatically on drops with exponential
// backoff (1s, 2s, 4s, ..., capped at 60s). The channel stays open across
// reconnections; it is only closed when ctx is cancelled.
func StartEventSubscription(tendermintURL string) (context.CancelFunc, <-chan ShinzoEvent, error) {
	ctx, cancel := context.WithCancel(context.Background())
	eventChan := make(chan ShinzoEvent, 16)

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
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", url, err)
	}

	for i, query := range subscriptionQueries {
		msg := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "subscribe",
			"id":      i + 1,
			"params":  map[string]interface{}{"query": query},
		}
		if err := conn.WriteJSON(msg); err != nil {
			conn.Close()
			return nil, fmt.Errorf("subscribe query %d: %w", i, err)
		}
		_, resp, err := conn.ReadMessage()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("subscribe response %d: %w", i, err)
		}
		log().Debugf("subscription %d accepted: %s", i+1, string(resp))
	}

	log().Infof("WebSocket connected to %s with %d subscriptions", url, len(subscriptionQueries))
	return conn, nil
}

// eventLoop reads events from the WebSocket and reconnects on failure. It
// owns the connection lifecycle: when a read fails, it closes the old
// connection, waits with backoff, dials a new one, re-subscribes, and
// resumes reading. The channel is closed only when ctx is cancelled.
func eventLoop(ctx context.Context, url string, conn *websocket.Conn, out chan<- ShinzoEvent) {
	defer close(out)
	defer func() {
		if r := recover(); r != nil {
			log().Errorf("WebSocket event loop recovered from panic: %v", r)
		}
	}()

	backoff := time.Second
	const maxBackoff = 60 * time.Second

	pingStop := startPing(ctx, conn)
	connectedAt := time.Now()

	// ReadMessage blocks until data arrives. A read deadline lets the loop
	// wake up periodically to check ctx cancellation during graceful shutdown.
	const readTimeout = 30 * time.Second

	for {
		if ctx.Err() != nil {
			conn.Close()
			return
		}

		conn.SetReadDeadline(time.Now().Add(readTimeout))
		_, message, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				conn.Close()
				return
			}
			if isTimeoutError(err) {
				continue
			}

			// Connection failure (reset by peer, server close, etc). Reconnect.
			pingStop()
			conn.Close()
			log().Warnf("WebSocket read failed: %v; reconnecting in ~%v", err, backoff)

			conn = reconnect(ctx, url, &backoff, maxBackoff)
			if conn == nil {
				return
			}
			pingStop = startPing(ctx, conn)
			connectedAt = time.Now()
			continue
		}

		// Reset backoff only after the connection has been stable for a while.
		// This prevents a flapping connection from resetting to 1s on every
		// brief reconnect.
		if time.Since(connectedAt) > 60*time.Second {
			backoff = time.Second
		}

		var msg RPCResponse
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}

		for _, ev := range extractShinzoEvents(msg) {
			select {
			case out <- ev:
			case <-ctx.Done():
				conn.Close()
				return
			}
		}
	}
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
		jitter := time.Duration(rand.Int64N(int64(*backoff) / 2))
		wait := *backoff + jitter - (*backoff / 4)

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
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				msg := map[string]interface{}{
					"jsonrpc": "2.0",
					"method":  "status",
					"id":      999,
					"params":  map[string]interface{}{},
				}
				if err := conn.WriteJSON(msg); err != nil {
					return
				}
			}
		}
	}()
	return pingCancel
}

// extractShinzoEvents extracts ViewRegistered, HostRegistered, and IndexerRegistered events from an RPC message.
func extractShinzoEvents(msg RPCResponse) []ShinzoEvent {
	var events []ShinzoEvent
	if msg.JsonRpcVersion != "2.0" ||
		msg.Result.Data.Type != "tendermint.event" ||
		msg.Result.Data.Value.TxResult.Result.Events == nil {
		return events
	}

	for _, event := range msg.Result.Data.Value.TxResult.Result.Events {
		switch event.Type {
		case "ViewRegistered":
			registeredEvent := ViewRegisteredEvent{}

			for _, attr := range event.Attributes {
				switch attr.Key {
				case "view_address":
					registeredEvent.ViewAddress = attr.Value
				case "view_name":
					registeredEvent.ViewName = attr.Value
				case "creator":
					registeredEvent.Creator = attr.Value
				case "data":
					newView, err := ProcessViewFromWireFormat(attr.Value)
					if err != nil {
						log().Warnf("failed to process view from wire: %v", err)
						continue
					}
					registeredEvent.View = newView
				}
			}

			if registeredEvent.ViewAddress != "" && registeredEvent.Creator != "" && registeredEvent.View.Data.Query != "" {
				log().Infof("ViewRegistered event received: %s", registeredEvent.View.Name)
				events = append(events, &registeredEvent)
			} else {
				log().Debugf("incomplete ViewRegistered event: %+v", registeredEvent)
			}

		case "HostRegistered":
			hostEvent := HostRegisteredEvent{}

			for _, attr := range event.Attributes {
				switch attr.Key {
				case "owner":
					hostEvent.Owner = attr.Value
				case "did":
					hostEvent.DID = attr.Value
				case "connection_string":
					hostEvent.ConnectionString = attr.Value
				}
			}

			if hostEvent.Owner != "" && hostEvent.DID != "" {
				log().Infof("HostRegistered event received: owner=%s did=%s",
					hostEvent.Owner, hostEvent.DID)
				events = append(events, &hostEvent)
			} else {
				log().Debugf("incomplete HostRegistered event: %+v", hostEvent)
			}

		case "IndexerRegistered":
			indexerEvent := IndexerRegisteredEvent{}

			for _, attr := range event.Attributes {
				switch attr.Key {
				case "owner":
					indexerEvent.Owner = attr.Value
				case "did":
					indexerEvent.DID = attr.Value
				case "connection_string":
					indexerEvent.ConnectionString = attr.Value
				case "source_chain":
					indexerEvent.SourceChain = attr.Value
				case "source_chain_id":
					indexerEvent.SourceChainID = attr.Value
				}
			}

			if indexerEvent.Owner != "" && indexerEvent.DID != "" {
				log().Infof("IndexerRegistered event received: owner=%s did=%s chain=%s/%s",
					indexerEvent.Owner, indexerEvent.DID, indexerEvent.SourceChain, indexerEvent.SourceChainID)
				events = append(events, &indexerEvent)
			} else {
				log().Debugf("incomplete IndexerRegistered event: %+v", indexerEvent)
			}
		}
	}

	return events
}
