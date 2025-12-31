package shinzohub

import (
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/stretchr/testify/require"
)

// TestEntityRegisteredEventSubscription tests the subscription to EntityRegistered events
func TestEntityRegisteredEventSubscription(t *testing.T) {
	// Create a mock WebSocket server for testing
	mockServer := NewMockWebSocketServer()
	defer mockServer.Close()

	// Start the event subscription with the mock server
	cancel, eventChan, err := StartEventSubscription(mockServer.WebsocketURL())
	require.NoError(t, err)
	defer cancel()

	// Send a mock EntityRegistered event
	mockEvent := RPCResponse{
		JsonRpcVersion: "2.0",
		ID:             1,
		Result: RPCResult{
			Query: "tm.event='Tx' AND EntityRegistered.key EXISTS",
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Height: "12345",
						Tx:     "0x1234567890abcdef",
						Result: TxResultResult{
							Data: "success",
							Events: []Event{
								{
									Type: "EntityRegistered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "0x2dabf350a86364713863b2bc7bf59029bb7a87aceb2633c5b2a8b733c16f5e86", Index: true},
										{Key: "owner", Value: "shinzo10hphdkj8srj7afwezpu4m3puugj8lrywgswzpy", Index: true},
										{Key: "did", Value: "did:key:zQ3shbKR7JqKU3SVfMvxHv5N8UtV4EzbChhJXFyprouANr9mt", Index: true},
										{Key: "pid", Value: "12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", Index: true},
										{Key: "entity", Value: "\u0002", Index: true},
										{Key: "msg_index", Value: "0", Index: true},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Send the mock event to the subscription
	mockServer.SendEvent(mockEvent)

	// Wait for the event to be processed
	timeout := time.After(5 * time.Second)
	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				t.Fatal("Event channel closed unexpectedly")
			}

			// Check if we received an EntityRegistered event
			if entityEvent, ok := event.(*EntityRegisteredEvent); ok {
				require.Equal(t, "0x2dabf350a86364713863b2bc7bf59029bb7a87aceb2633c5b2a8b733c16f5e86", entityEvent.Key)
				require.Equal(t, "shinzo10hphdkj8srj7afwezpu4m3puugj8lrywgswzpy", entityEvent.Owner)
				require.Equal(t, "did:key:zQ3shbKR7JqKU3SVfMvxHv5N8UtV4EzbChhJXFyprouANr9mt", entityEvent.DID)
				require.Equal(t, "12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", entityEvent.Pid)
				require.Equal(t, "\u0002", entityEvent.Entity)
				
				t.Logf("✅ Successfully received EntityRegistered event: %s", entityEvent.ToString())
				return
			}

			// If it's a ViewRegistered event, continue waiting
			if _, ok := event.(*ViewRegisteredEvent); ok {
				t.Log("Received ViewRegistered event, waiting for EntityRegistered...")
				continue
			}

			t.Fatalf("Unexpected event type: %T", event)

		case <-timeout:
			t.Fatal("Timeout waiting for EntityRegistered event")
		}
	}
}

// TestExtractEntityRegisteredEvents tests the extraction function directly
func TestExtractEntityRegisteredEvents(t *testing.T) {
	// Create a test RPC response with EntityRegistered events
	testMsg := RPCResponse{
		JsonRpcVersion: "2.0",
		ID:             1,
		Result: RPCResult{
			Query: "tm.event='Tx' AND EntityRegistered.key EXISTS",
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Height: "12345",
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "EntityRegistered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "0x2dabf350a86364713863b2bc7bf59029bb7a87aceb2633c5b2a8b733c16f5e86", Index: true},
										{Key: "owner", Value: "shinzo10hphdkj8srj7afwezpu4m3puugj8lrywgswzpy", Index: true},
										{Key: "did", Value: "did:key:zQ3shbKR7JqKU3SVfMvxHv5N8UtV4EzbChhJXFyprouANr9mt", Index: true},
										{Key: "pid", Value: "12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", Index: true},
										{Key: "entity", Value: "\u0002", Index: true},
									},
								},
								{
									Type: "EntityRegistered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "0x1234567890abcdef", Index: true},
										{Key: "owner", Value: "shinzo1abcdef", Index: true},
										// Missing some attributes - should be filtered out
									},
								},
								{
									Type: "OtherEvent", // Should be ignored
									Attributes: []EventAttribute{
										{Key: "key", Value: "0xdeadbeef", Index: true},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Extract events
	allEvents := extractShinzoEvents(testMsg)
	
	// Filter for EntityRegistered events only
	var events []EntityRegisteredEvent
	for _, event := range allEvents {
		if entityEvent, ok := event.(*EntityRegisteredEvent); ok {
			events = append(events, *entityEvent)
		}
	}

	// Should only get the complete EntityRegistered event (the second one is incomplete)
	require.Len(t, events, 1, "Should extract exactly 1 complete EntityRegistered event")

	event := events[0]
	require.Equal(t, "0x2dabf350a86364713863b2bc7bf59029bb7a87aceb2633c5b2a8b733c16f5e86", event.Key)
	require.Equal(t, "shinzo10hphdkj8srj7afwezpu4m3puugj8lrywgswzpy", event.Owner)
	require.Equal(t, "did:key:zQ3shbKR7JqKU3SVfMvxHv5N8UtV4EzbChhJXFyprouANr9mt", event.DID)
	require.Equal(t, "12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", event.Pid)
	require.Equal(t, "\u0002", event.Entity)

	t.Logf("✅ Successfully extracted EntityRegistered event: %s", event.ToString())
}

// TestMixedEventSubscription tests receiving both ViewRegistered and EntityRegistered events
func TestMixedEventSubscription(t *testing.T) {
	// Create a mock WebSocket server for testing
	mockServer := NewMockWebSocketServer()
	defer mockServer.Close()

	// Start the event subscription with the mock server
	cancel, eventChan, err := StartEventSubscription(mockServer.WebsocketURL())
	require.NoError(t, err)
	defer cancel()

	// Test data
	query := "Ethereum__Mainnet__Log {address topics data transactionHash blockNumber}"
	sdl := "type FilteredAndDecodedLogs_0xdc0812f6a7ea5d7b3bf2ee7362e4ed87e7c070eb6d2852c7aaa9589a85dcdd85 @materialized(if: false) {transactionHash: String}"
	expectedView := view.View{
		Query: &query,
		Sdl:   &sdl,
		Name:  "FilteredAndDecodedLogs_0xdc0812f6a7ea5d7b3bf2ee7362e4ed87e7c070eb6d2852c7aaa9589a85dcdd85",
	}

	// Send a mixed event with both ViewRegistered and EntityRegistered
	mixedEvent := RPCResponse{
		JsonRpcVersion: "2.0",
		ID:             1,
		Result: RPCResult{
			Query: "tm.event='Tx' AND (Registered.key EXISTS OR EntityRegistered.key EXISTS)",
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Height: "12345",
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "Registered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "0xdc0812f6a7ea5d7b3bf2ee7362e4ed87e7c070eb6d2852c7aaa9589a85dcdd85", Index: true},
										{Key: "creator", Value: "shinzo140fehngcrxvhdt84x729p3f0qmkmea8nq3rk92", Index: true},
										{Key: "view", Value: `{"query":"Ethereum__Mainnet__Log {address topics data transactionHash blockNumber}","sdl":"type FilteredAndDecodedLogs_0xdc0812f6a7ea5d7b3bf2ee7362e4ed87e7c070eb6d2852c7aaa9589a85dcdd85 @materialized(if: false) {transactionHash: String}","transform":{"lenses":[]}}`, Index: true},
									},
								},
								{
									Type: "EntityRegistered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "0x2dabf350a86364713863b2bc7bf59029bb7a87aceb2633c5b2a8b733c16f5e86", Index: true},
										{Key: "owner", Value: "shinzo10hphdkj8srj7afwezpu4m3puugj8lrywgswzpy", Index: true},
										{Key: "did", Value: "did:key:zQ3shbKR7JqKU3SVfMvxHv5N8UtV4EzbChhJXFyprouANr9mt", Index: true},
										{Key: "pid", Value: "12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", Index: true},
										{Key: "entity", Value: "\u0002", Index: true},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Send the mixed event
	mockServer.SendEvent(mixedEvent)

	// Wait for both events
	receivedView := false
	receivedEntity := false
	timeout := time.After(5 * time.Second)

	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				t.Fatal("Event channel closed unexpectedly")
			}

			switch e := event.(type) {
			case *ViewRegisteredEvent:
				if !receivedView {
					require.Equal(t, "0xdc0812f6a7ea5d7b3bf2ee7362e4ed87e7c070eb6d2852c7aaa9589a85dcdd85", e.Key)
					require.Equal(t, "shinzo140fehngcrxvhdt84x729p3f0qmkmea8nq3rk92", e.Creator)
					require.Equal(t, expectedView.Query, e.View.Query)
					receivedView = true
					t.Log("✅ Received ViewRegistered event")
				}

			case *EntityRegisteredEvent:
				if !receivedEntity {
					require.Equal(t, "0x2dabf350a86364713863b2bc7bf59029bb7a87aceb2633c5b2a8b733c16f5e86", e.Key)
					require.Equal(t, "shinzo10hphdkj8srj7afwezpu4m3puugj8lrywgswzpy", e.Owner)
					require.Equal(t, "did:key:zQ3shbKR7JqKU3SVfMvxHv5N8UtV4EzbChhJXFyprouANr9mt", e.DID)
					receivedEntity = true
					t.Log("✅ Received EntityRegistered event")
				}

			default:
				t.Fatalf("Unexpected event type: %T", event)
			}

			// If we've received both events, test is successful
			if receivedView && receivedEntity {
				t.Log("✅ Successfully received both ViewRegistered and EntityRegistered events")
				return
			}

		case <-timeout:
			t.Fatal("Timeout waiting for events")
		}
	}
}

// TestEntityRegisteredEventToString tests the string representation
func TestEntityRegisteredEventToString(t *testing.T) {
	event := EntityRegisteredEvent{
		Key:    "0x2dabf350a86364713863b2bc7bf59029bb7a87aceb2633c5b2a8b733c16f5e86",
		Owner:  "shinzo10hphdkj8srj7afwezpu4m3puugj8lrywgswzpy",
		DID:    "did:key:zQ3shbKR7JqKU3SVfMvxHv5N8UtV4EzbChhJXFyprouANr9mt",
		Pid:    "12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo",
		Entity: "\u0002",
	}

	expected := "EntityRegistered: key=0x2dabf350a86364713863b2bc7bf59029bb7a87aceb2633c5b2a8b733c16f5e86, owner=shinzo10hphdkj8srj7afwezpu4m3puugj8lrywgswzpy, did=did:key:zQ3shbKR7JqKU3SVfMvxHv5N8UtV4EzbChhJXFyprouANr9mt, pid=12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo"
	require.Equal(t, expected, event.ToString())
}
