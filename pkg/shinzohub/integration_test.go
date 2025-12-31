package shinzohub

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEntityRegisteredToIntegrationFlow tests the complete flow from EntityRegistered event to indexer addition
func TestEntityRegisteredToIntegrationFlow(t *testing.T) {
	// Create a mock indexer manager
	indexerManager := NewMockIndexerManager()
	
	// Create the entity registration handler
	entityHandler := NewEntityRegistrationHandler(indexerManager)

	// Create a mock WebSocket server
	mockServer := NewMockWebSocketServer()
	defer mockServer.Close()

	// Start the event subscription
	cancel, eventChan, err := StartEventSubscription(mockServer.WebsocketURL())
	require.NoError(t, err)
	defer cancel()

	// Create a test EntityRegistered event
	testEvent := RPCResponse{
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
							},
						},
					},
				},
			},
		},
	}

	// Send the event
	mockServer.SendEvent(testEvent)

	// Process the event
	timeout := time.After(5 * time.Second)
	eventProcessed := false

	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				t.Fatal("Event channel closed unexpectedly")
			}

			if entityEvent, ok := event.(*EntityRegisteredEvent); ok {
				// Process the EntityRegistered event
				err := entityHandler.ProcessEntityRegisteredEvent(context.Background(), *entityEvent)
				require.NoError(t, err)
				
				eventProcessed = true
				t.Log("✅ Successfully processed EntityRegistered event")
			}

		case <-timeout:
			if !eventProcessed {
				t.Fatal("Timeout waiting for EntityRegistered event")
			}
			// Continue to verification
		}

		if eventProcessed {
			break
		}
	}

	// Verify the indexer was added
	indexer, exists := indexerManager.GetIndexer("0x2dabf350a86364713863b2bc7bf59029bb7a87aceb2633c5b2a8b733c16f5e86")
	require.True(t, exists, "Indexer should exist after EntityRegistered event")

	require.Equal(t, "0x2dabf350a86364713863b2bc7bf59029bb7a87aceb2633c5b2a8b733c16f5e86", indexer.Key)
	require.Equal(t, "shinzo10hphdkj8srj7afwezpu4m3puugj8lrywgswzpy", indexer.Owner)
	require.Equal(t, "did:key:zQ3shbKR7JqKU3SVfMvxHv5N8UtV4EzbChhJXFyprouANr9mt", indexer.DID)
	require.Equal(t, "12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", indexer.Pid)
	require.True(t, indexer.Active)

	// Verify indexer count
	require.Equal(t, 1, indexerManager.GetIndexerCount())

	t.Log("✅ Integration test completed successfully - indexer added from EntityRegistered event")
}

// TestMultipleEntityRegisteredEvents tests handling multiple EntityRegistered events
func TestMultipleEntityRegisteredEvents(t *testing.T) {
	// Create a mock indexer manager
	indexerManager := NewMockIndexerManager()
	
	// Create the entity registration handler
	entityHandler := NewEntityRegistrationHandler(indexerManager)

	// Create a mock WebSocket server
	mockServer := NewMockWebSocketServer()
	defer mockServer.Close()

	// Start the event subscription
	cancel, eventChan, err := StartEventSubscription(mockServer.WebsocketURL())
	require.NoError(t, err)
	defer cancel()

	// Test data for multiple events
	testEvents := []RPCResponse{
		{
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
											{Key: "key", Value: "0x1111111111111111", Index: true},
											{Key: "owner", Value: "shinzo1user1", Index: true},
											{Key: "did", Value: "did:key:1111", Index: true},
											{Key: "pid", Value: "12D3KooW111111111111", Index: true},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			JsonRpcVersion: "2.0",
			ID:             2,
			Result: RPCResult{
				Query: "tm.event='Tx' AND EntityRegistered.key EXISTS",
				Data: RPCData{
					Type: "tendermint.event",
					Value: TxResult{
						TxResult: TxResultData{
							Height: "12346",
							Result: TxResultResult{
								Events: []Event{
									{
										Type: "EntityRegistered",
										Attributes: []EventAttribute{
											{Key: "key", Value: "0x2222222222222222", Index: true},
											{Key: "owner", Value: "shinzo1user2", Index: true},
											{Key: "did", Value: "did:key:2222", Index: true},
											{Key: "pid", Value: "12D3KooW222222222222", Index: true},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Send multiple events
	for i, event := range testEvents {
		mockServer.SendEvent(event)
		
		// Wait for and process each event
		timeout := time.After(2 * time.Second)
		eventProcessed := false

		for {
			select {
			case receivedEvent, ok := <-eventChan:
				if !ok {
					t.Fatal("Event channel closed unexpectedly")
				}

				if entityEvent, ok := receivedEvent.(*EntityRegisteredEvent); ok {
					err := entityHandler.ProcessEntityRegisteredEvent(context.Background(), *entityEvent)
					require.NoError(t, err)
					
					eventProcessed = true
					t.Logf("✅ Processed event %d: %s", i+1, entityEvent.Key)
				}

			case <-timeout:
				if !eventProcessed {
					t.Fatalf("Timeout waiting for EntityRegistered event %d", i+1)
				}
			}

			if eventProcessed {
				break
			}
		}
	}

	// Verify all indexers were added
	require.Equal(t, 2, indexerManager.GetIndexerCount())

	// Check first indexer
	indexer1, exists1 := indexerManager.GetIndexer("0x1111111111111111")
	require.True(t, exists1)
	require.Equal(t, "shinzo1user1", indexer1.Owner)

	// Check second indexer
	indexer2, exists2 := indexerManager.GetIndexer("0x2222222222222222")
	require.True(t, exists2)
	require.Equal(t, "shinzo1user2", indexer2.Owner)

	t.Log("✅ Multiple EntityRegistered events processed successfully")
}

// TestIndexerManagerFunctionality tests the indexer manager directly
func TestIndexerManagerFunctionality(t *testing.T) {
	manager := NewMockIndexerManager()

	// Test adding indexers
	err := manager.AddIndexer(context.Background(), "test-key-1", "owner1", "did1", "pid1")
	require.NoError(t, err)

	err = manager.AddIndexer(context.Background(), "test-key-2", "owner2", "did2", "pid2")
	require.NoError(t, err)

	// Test getting indexers
	indexer1, exists1 := manager.GetIndexer("test-key-1")
	require.True(t, exists1)
	require.Equal(t, "owner1", indexer1.Owner)

	indexer2, exists2 := manager.GetIndexer("test-key-2")
	require.True(t, exists2)
	require.Equal(t, "owner2", indexer2.Owner)

	// Test non-existent indexer
	_, exists3 := manager.GetIndexer("non-existent")
	require.False(t, exists3)

	// Test listing indexers
	indexers := manager.ListIndexers()
	require.Len(t, indexers, 2)

	// Test removing indexer
	err = manager.RemoveIndexer("test-key-1")
	require.NoError(t, err)

	_, exists4 := manager.GetIndexer("test-key-1")
	require.False(t, exists4)

	require.Equal(t, 1, manager.GetIndexerCount())

	t.Log("✅ Indexer manager functionality test passed")
}
