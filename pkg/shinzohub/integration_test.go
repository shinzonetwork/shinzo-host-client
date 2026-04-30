package shinzohub

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestIndexerRegisteredToIntegrationFlow tests the complete flow from IndexerRegistered event to indexer addition.
func TestIndexerRegisteredToIntegrationFlow(t *testing.T) {
	indexerManager := NewMockIndexerManager()
	handler := NewRegistrationHandler(indexerManager)

	mockServer := NewMockWebSocketServer()
	defer mockServer.Close()

	cancel, eventChan, err := StartEventSubscription(mockServer.WebsocketURL())
	require.NoError(t, err)
	defer cancel()

	testEvent := RPCResponse{
		JSONRPCVersion: "2.0",
		ID:             3,
		Result: RPCResult{
			Query: "tm.event='Tx' AND IndexerRegistered.owner EXISTS",
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Height: "12345",
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "IndexerRegistered",
									Attributes: []EventAttribute{
										{Key: "owner", Value: "shinzo1idx123", Index: true},
										{Key: "did", Value: "did:key:z7r8idx", Index: true},
										{Key: "connection_string", Value: "/ip4/10.0.0.50/tcp/9090/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r", Index: true},
										{Key: "source_chain", Value: "ethereum", Index: true},
										{Key: "source_chain_id", Value: "1", Index: true},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	mockServer.SendEvent(testEvent)

	timeout := time.After(5 * time.Second)
	eventProcessed := false

	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				t.Fatal("Event channel closed unexpectedly")
			}

			if indexerEvent, ok := event.(*IndexerRegisteredEvent); ok {
				err := handler.ProcessIndexerRegisteredEvent(context.Background(), *indexerEvent)
				require.NoError(t, err)

				eventProcessed = true
				t.Log("Successfully processed IndexerRegistered event")
			}

		case <-timeout:
			if !eventProcessed {
				t.Fatal("Timeout waiting for IndexerRegistered event")
			}
		}

		if eventProcessed {
			break
		}
	}

	// Verify the indexer was added (keyed by owner)
	indexer, exists := indexerManager.GetIndexer("shinzo1idx123")
	require.True(t, exists, "Indexer should exist after IndexerRegistered event")

	require.Equal(t, "shinzo1idx123", indexer.Owner)
	require.Equal(t, "did:key:z7r8idx", indexer.DID)
	require.Equal(t, "/ip4/10.0.0.50/tcp/9090/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r", indexer.ConnectionString)
	require.Equal(t, "ethereum", indexer.SourceChain)
	require.Equal(t, "1", indexer.SourceChainID)
	require.True(t, indexer.Active)

	require.Equal(t, 1, indexerManager.GetIndexerCount())
}

// TestMultipleIndexerRegisteredEvents tests handling multiple IndexerRegistered events.
func TestMultipleIndexerRegisteredEvents(t *testing.T) {
	indexerManager := NewMockIndexerManager()
	handler := NewRegistrationHandler(indexerManager)

	mockServer := NewMockWebSocketServer()
	defer mockServer.Close()

	cancel, eventChan, err := StartEventSubscription(mockServer.WebsocketURL())
	require.NoError(t, err)
	defer cancel()

	testEvents := []RPCResponse{
		{
			JSONRPCVersion: "2.0",
			ID:             3,
			Result: RPCResult{
				Query: "tm.event='Tx' AND IndexerRegistered.owner EXISTS",
				Data: RPCData{
					Type: "tendermint.event",
					Value: TxResult{
						TxResult: TxResultData{
							Height: "12345",
							Result: TxResultResult{
								Events: []Event{
									{
										Type: "IndexerRegistered",
										Attributes: []EventAttribute{
											{Key: "owner", Value: "shinzo1idx1", Index: true},
											{Key: "did", Value: "did:key:1111", Index: true},
											{Key: "connection_string", Value: "/ip4/10.0.0.1/tcp/9090/p2p/12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", Index: true},
											{Key: "source_chain", Value: "ethereum", Index: true},
											{Key: "source_chain_id", Value: "1", Index: true},
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
			JSONRPCVersion: "2.0",
			ID:             3,
			Result: RPCResult{
				Query: "tm.event='Tx' AND IndexerRegistered.owner EXISTS",
				Data: RPCData{
					Type: "tendermint.event",
					Value: TxResult{
						TxResult: TxResultData{
							Height: "12346",
							Result: TxResultResult{
								Events: []Event{
									{
										Type: "IndexerRegistered",
										Attributes: []EventAttribute{
											{Key: "owner", Value: "shinzo1idx2", Index: true},
											{Key: "did", Value: "did:key:2222", Index: true},
											{Key: "connection_string", Value: "/ip4/10.0.0.2/tcp/9090/p2p/12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN", Index: true},
											{Key: "source_chain", Value: "polygon", Index: true},
											{Key: "source_chain_id", Value: "137", Index: true},
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

	for i, event := range testEvents {
		mockServer.SendEvent(event)

		timeout := time.After(2 * time.Second)
		eventProcessed := false

		for {
			select {
			case receivedEvent, ok := <-eventChan:
				if !ok {
					t.Fatal("Event channel closed unexpectedly")
				}

				if indexerEvent, ok := receivedEvent.(*IndexerRegisteredEvent); ok {
					err := handler.ProcessIndexerRegisteredEvent(context.Background(), *indexerEvent)
					require.NoError(t, err)

					eventProcessed = true
					t.Logf("Processed event %d: %s", i+1, indexerEvent.Owner)
				}

			case <-timeout:
				if !eventProcessed {
					t.Fatalf("Timeout waiting for IndexerRegistered event %d", i+1)
				}
			}

			if eventProcessed {
				break
			}
		}
	}

	require.Equal(t, 2, indexerManager.GetIndexerCount())

	indexer1, exists1 := indexerManager.GetIndexer("shinzo1idx1")
	require.True(t, exists1)
	require.Equal(t, "ethereum", indexer1.SourceChain)

	indexer2, exists2 := indexerManager.GetIndexer("shinzo1idx2")
	require.True(t, exists2)
	require.Equal(t, "polygon", indexer2.SourceChain)
}

// TestIndexerManagerFunctionality tests the indexer manager directly.
func TestIndexerManagerFunctionality(t *testing.T) {
	manager := NewMockIndexerManager()

	err := manager.AddIndexer(context.Background(), "shinzo1owner1", "did1", "/ip4/10.0.0.1/tcp/9090/p2p/12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", "ethereum", "1")
	require.NoError(t, err)

	err = manager.AddIndexer(context.Background(), "shinzo1owner2", "did2", "/ip4/10.0.0.2/tcp/9090/p2p/12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN", "polygon", "137")
	require.NoError(t, err)

	indexer1, exists1 := manager.GetIndexer("shinzo1owner1")
	require.True(t, exists1)
	require.Equal(t, "did1", indexer1.DID)
	require.Equal(t, "ethereum", indexer1.SourceChain)

	indexer2, exists2 := manager.GetIndexer("shinzo1owner2")
	require.True(t, exists2)
	require.Equal(t, "did2", indexer2.DID)
	require.Equal(t, "polygon", indexer2.SourceChain)

	_, exists3 := manager.GetIndexer("non-existent")
	require.False(t, exists3)

	indexers := manager.ListIndexers()
	require.Len(t, indexers, 2)

	err = manager.RemoveIndexer("shinzo1owner1")
	require.NoError(t, err)

	_, exists4 := manager.GetIndexer("shinzo1owner1")
	require.False(t, exists4)

	require.Equal(t, 1, manager.GetIndexerCount())
}
