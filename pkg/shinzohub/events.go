package shinzohub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"go.uber.org/zap"
)

// Live LCD hydration retries while the hub RPC pod catches up to the
// registering block.
var (
	hydrateMaxAttempts = 3
	hydrateBaseDelay   = 100 * time.Millisecond
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
		"tm.event='Tx' AND " + eventTypeViewRegistered + "." + attrViewID + " EXISTS",
		"tm.event='Tx' AND " + eventTypeViewRegistrationFailed + "." + attrViewID + " EXISTS",
		"tm.event='Tx' AND " + eventTypeViewRegistrationTimedOut + "." + attrViewID + " EXISTS",
	}
}

// StartEventSubscription connects to the CometBFT WebSocket, subscribes to
// registry events, and pushes parsed events to the returned channel. The
// connection is re-established automatically on drops with exponential
// backoff (1s, 2s, 4s, ..., capped at 60s). The channel stays open across
// reconnections; it is only closed when ctx is cancelled.
//
// lcd is used to hydrate view-registration events with the bundle wire bytes
// the CometBFT event payload doesn't carry. Pass nil to disable hydration;
// any view-registration events received then get dropped with a warning.
func StartEventSubscription(tendermintURL string, lcd *RPCClient) (context.CancelFunc, <-chan ShinzoEvent, error) {
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

	go eventLoop(ctx, tendermintURL, conn, eventChan, lcd)

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
			jsonRPCMsgKey: jsonRPCVersion,
			"method":      "subscribe",
			"id":          i + 1,
			"params":      map[string]any{"query": query},
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
//
// lcd is consulted to hydrate view-registration events before they're pushed
// onto the channel; see hydrateViewBundle. nil disables hydration.
func eventLoop(ctx context.Context, url string, conn *websocket.Conn, out chan<- ShinzoEvent, lcd *RPCClient) {
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
			ctx, url, conn, out, lcd, pingStop, connectedAt, backoff, maxBackoff,
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
	lcd *RPCClient,
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

	conn = dispatchEvents(ctx, conn, out, lcd, message)
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

// dispatchEvents decodes the WebSocket message, hydrates any view-registration
// events from the hub's REST gateway, and pushes them onto out. Events whose
// hydration fails are dropped with a warn log; the loop continues.
func dispatchEvents(ctx context.Context, conn *websocket.Conn, out chan<- ShinzoEvent, lcd *RPCClient, message []byte) *websocket.Conn {
	var msg RPCResponse
	if err := json.Unmarshal(message, &msg); err != nil {
		return conn
	}
	for _, ev := range extractShinzoEvents(msg) {
		if vre, ok := ev.(*ViewRegisteredEvent); ok && vre.View.Data.Query == "" {
			if err := hydrateViewBundle(ctx, lcd, vre); err != nil {
				log().Warnf("dropping view.view_registered for contract %s: %v", vre.ContractAddress, err)
				continue
			}
		}
		select {
		case out <- ev:
		case <-ctx.Done():
			closeConn(conn)
			return nil
		}
	}
	return conn
}

// hydrateViewBundle fetches the wire bundle from the hub's view-registry REST
// endpoint and decodes it onto vre.View. CometBFT event payloads carry only
// identifiers (view_id, contract_address, creator); the bundle has to be
// resolved separately before the consumer can register the view in DefraDB.
func hydrateViewBundle(ctx context.Context, lcd *RPCClient, vre *ViewRegisteredEvent) error {
	if lcd == nil {
		return ErrLCDNotConfigured
	}
	if vre.ContractAddress == "" {
		return ErrEventNoContract
	}

	wireBase64, err := fetchBundleWithRetry(ctx, lcd, vre.ContractAddress)
	if err != nil {
		return fmt.Errorf("fetch bundle: %w", err)
	}

	v, err := ProcessViewFromWireFormat(wireBase64)
	if err != nil {
		return fmt.Errorf("decode bundle: %w", err)
	}
	v.ContractAddress = vre.ContractAddress
	vre.View = v
	return nil
}

// fetchBundleWithRetry calls GetViewBundle with exponential backoff on
// transient failures (see isTransientHydrationErr).
func fetchBundleWithRetry(ctx context.Context, lcd *RPCClient, contractAddr string) (string, error) {
	delay := hydrateBaseDelay
	var lastErr error
	for attempt := 1; attempt <= hydrateMaxAttempts; attempt++ {
		wireBase64, err := lcd.GetViewBundle(ctx, contractAddr)
		if err == nil {
			if attempt > 1 {
				log().Infof("view bundle for contract %s hydrated on attempt %d", contractAddr, attempt)
			}
			return wireBase64, nil
		}
		if !isTransientHydrationErr(err) {
			return "", err
		}
		lastErr = err
		if attempt == hydrateMaxAttempts {
			break
		}
		jitter := time.Duration(rand.Int64N(int64(delay) / 2)) //nolint:gosec,mnd
		wait := delay + jitter - delay/4                       //nolint:mnd
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(wait):
		}
		delay *= 2
	}
	return "", fmt.Errorf("gave up after %d attempts: %w", hydrateMaxAttempts, lastErr)
}

// isTransientHydrationErr identifies failures that the hub RPC pod's
// replication lag can produce.
func isTransientHydrationErr(err error) bool {
	if errors.Is(err, ErrLCDHTTPStatus) || errors.Is(err, ErrLCDEmptyData) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
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
					jsonRPCMsgKey: jsonRPCVersion,
					"method":      "status",
					"id":          pingMessageID,
					"params":      map[string]any{},
				}
				if err := conn.WriteJSON(msg); err != nil {
					return
				}
			}
		}
	}()
	return pingCancel
}

// extractShinzoEvents converts the CometBFT tx events in msg into typed
// registry events. The view-registration cases (view.view_registered and the
// terminal-failure counterparts) carry view_id and contract_address; the
// bundle wire bytes are fetched separately by downstream hydration.
func extractShinzoEvents(msg RPCResponse) []ShinzoEvent {
	var events []ShinzoEvent
	if msg.JSONRPCVersion != jsonRPCVersion ||
		msg.Result.Data.Type != tmEventTxType ||
		msg.Result.Data.Value.TxResult.Result.Events == nil {
		return nil
	}

	for _, event := range msg.Result.Data.Value.TxResult.Result.Events {
		switch event.Type {
		case eventTypeViewRegistered:
			registeredEvent := ViewRegisteredEvent{}

			for _, attr := range event.Attributes {
				switch attr.Key {
				case attrViewID:
					registeredEvent.ViewID = attr.Value
				case attrContractAddress:
					registeredEvent.ContractAddress = attr.Value
				case attrCreator:
					registeredEvent.Creator = attr.Value
				}
			}

			if registeredEvent.ViewID != "" && registeredEvent.ContractAddress != "" && registeredEvent.Creator != "" {
				log().Infof("view.view_registered event received: id=%s contract=%s creator=%s",
					registeredEvent.ViewID, registeredEvent.ContractAddress, registeredEvent.Creator)
				events = append(events, &registeredEvent)
			} else {
				log().Debugf("incomplete view.view_registered event: %+v", registeredEvent)
			}

		case eventTypeViewRegistrationFailed, eventTypeViewRegistrationTimedOut:
			// Logged for visibility and dropped: the host keeps no pending-view state
			// locally, so there is nothing to roll back.
			var viewID, contractAddr, errMsg string
			for _, attr := range event.Attributes {
				switch attr.Key {
				case attrViewID:
					viewID = attr.Value
				case attrContractAddress:
					contractAddr = attr.Value
				case attrError:
					errMsg = attr.Value
				}
			}
			log().Warnf("%s: id=%s contract=%s error=%q", event.Type, viewID, contractAddr, errMsg)
		}
	}

	return events
}
