package shinzohub

import (
	"context"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/stretchr/testify/require"
)

// TestUnifiedEventHandler_IndexerRegistration tests indexer registration flow
func TestUnifiedEventHandler_IndexerRegistration(t *testing.T) {
	// Create mock managers
	indexerManager := NewMockIndexerManager()
	peerManager := NewMockPeerManager()
	blockProcessor := NewMockBlockProcessor()
	
	// Create unified event handler
	handler := NewUnifiedEventHandler(indexerManager, nil, peerManager, blockProcessor)

	// Create an indexer EntityRegistered event
	indexerEvent := EntityRegisteredEvent{
		Key:    "0x2dabf350a86364713863b2bc7bf59029bb7a87aceb2633c5b2a8b733c16f5e86",
		Owner:  "shinzo10hphdkj8srj7afwezpu4m3puugj8lrywgswzpy",
		DID:    "did:key:zQ3shbKR7JqKU3SVfMvxHv5N8UtV4EzbChhJXFyprouANr9mt",
		Pid:    "12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo",
		Entity: "\u0001", // Indexer
	}

	// Process the event
	err := handler.ProcessEvent(context.Background(), &indexerEvent)
	require.NoError(t, err)

	// Verify indexer was added
	indexer, exists := indexerManager.GetIndexer(indexerEvent.Key)
	require.True(t, exists)
	require.Equal(t, indexerEvent.Owner, indexer.Owner)

	// Verify peer was added
	peer, exists := peerManager.GetPeer(indexerEvent.Pid)
	require.True(t, exists)
	require.Equal(t, indexerEvent.Owner, peer.Owner)
	require.Equal(t, indexerEvent.DID, peer.DID)

	t.Log("✅ Indexer registration test passed")
}

// TestUnifiedEventHandler_HostRegistration tests host registration flow
func TestUnifiedEventHandler_HostRegistration(t *testing.T) {
	// Create mock managers
	indexerManager := NewMockIndexerManager()
	peerManager := NewMockPeerManager()
	blockProcessor := NewMockBlockProcessor()
	
	// Create unified event handler
	handler := NewUnifiedEventHandler(indexerManager, nil, peerManager, blockProcessor)

	// Create a host EntityRegistered event
	hostEvent := EntityRegisteredEvent{
		Key:    "0x1234567890abcdef",
		Owner:  "shinzo1hostowner",
		DID:    "did:key:zQ3shbKR7JqKU3SVfMvxHv5N8UtV4EzbChhJXFyprouANr9mt",
		Pid:    "12D3KooWHostPeer123456789",
		Entity: "\u0002", // Host
	}

	// Process the event
	err := handler.ProcessEvent(context.Background(), &hostEvent)
	require.NoError(t, err)

	// Verify peer was added (hosts are peers but not indexers)
	peer, exists := peerManager.GetPeer(hostEvent.Pid)
	require.True(t, exists)
	require.Equal(t, hostEvent.Owner, peer.Owner)
	require.Equal(t, hostEvent.DID, peer.DID)

	// Verify indexer was NOT added (hosts are not indexers)
	_, exists = indexerManager.GetIndexer(hostEvent.Key)
	require.False(t, exists)

	t.Log("✅ Host registration test passed")
}

// TestUnifiedEventHandler_ViewRegistration tests view registration flow
func TestUnifiedEventHandler_ViewRegistration(t *testing.T) {
	// Create mock managers
	indexerManager := NewMockIndexerManager()
	peerManager := NewMockPeerManager()
	blockProcessor := NewMockBlockProcessor()
	mockViewManager := &MockViewManager{
		views: make(map[string]view.View),
	}
	
	// Create unified event handler
	handler := NewUnifiedEventHandler(indexerManager, mockViewManager, peerManager, blockProcessor)

	// Create a ViewRegistered event
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type FilteredAndDecodedLogs_0xdc0812f6a7ea5d7b3bf2ee7362e4ed87e7c070eb6d2852c7aaa9589a85dcdd85 @materialized(if: false) {transactionHash: String}"
	viewEvent := ViewRegisteredEvent{
		Key:     "0xdc0812f6a7ea5d7b3bf2ee7362e4ed87e7c070eb6d2852c7aaa9589a85dcdd85",
		Creator: "shinzo140fehngcrxvhdt84x729p3f0qmkmea8nq3rk92",
		View: view.View{
			Query: &query,
			Sdl:   &sdl,
			Name:  "FilteredAndDecodedLogs_0xdc0812f6a7ea5d7b3bf2ee7362e4ed87e7c070eb6d2852c7aaa9589a85dcdd85",
		},
	}

	// Process the event
	err := handler.ProcessEvent(context.Background(), &viewEvent)
	require.NoError(t, err)

	// Verify view was applied to blocks
	require.Equal(t, 1, blockProcessor.GetActiveViewCount())
	activeViews := blockProcessor.GetActiveViews()
	require.Contains(t, activeViews, viewEvent.Key)

	t.Log("✅ View registration test passed")
}

// TestUnifiedEventHandler_MixedEvents tests handling mixed event types
func TestUnifiedEventHandler_MixedEvents(t *testing.T) {
	// Create variables for string literals
	blockQuery := "Block {number hash}"
	testViewSdl := "type TestView { number: String }"

	// Create mock managers
	indexerManager := NewMockIndexerManager()
	peerManager := NewMockPeerManager()
	blockProcessor := NewMockBlockProcessor()
	mockViewManager := &MockViewManager{
		views: make(map[string]view.View),
	}
	
	// Create unified event handler
	handler := NewUnifiedEventHandler(indexerManager, mockViewManager, peerManager, blockProcessor)

	// Create multiple events
	events := []ShinzoEvent{
		&EntityRegisteredEvent{
			Key:    "0xindexer1",
			Owner:  "shinzo1indexer1",
			DID:    "did:key:indexer1",
			Pid:    "12D3KooWIndexer1",
			Entity: "\u0001", // Indexer
		},
		&EntityRegisteredEvent{
			Key:    "0xhost1",
			Owner:  "shinzo1host1",
			DID:    "did:key:host1",
			Pid:    "12D3KooWHost1",
			Entity: "\u0002", // Host
		},
		&ViewRegisteredEvent{
			Key:     "0xview1",
			Creator: "shinzo1creator1",
			View: view.View{
				Query: &blockQuery,
				Sdl:   &testViewSdl,
				Name:  "TestView",
			},
		},
	}

	// Process all events
	for _, event := range events {
		err := handler.ProcessEvent(context.Background(), event)
		require.NoError(t, err)
	}

	// Verify indexer was added
	indexer, exists := indexerManager.GetIndexer("0xindexer1")
	require.True(t, exists)
	require.Equal(t, "shinzo1indexer1", indexer.Owner)

	// Verify all peers were added (indexer + host = 2 peers)
	require.Equal(t, 2, peerManager.GetPeerCount())

	// Verify view was applied
	require.Equal(t, 1, blockProcessor.GetActiveViewCount())

	t.Log("✅ Mixed events test passed")
}

// TestGetEntityType tests entity type detection
func TestGetEntityType(t *testing.T) {
	tests := []struct {
		name     string
		entity   string
		expected EntityType
	}{
		{"Indexer", "\u0001", EntityTypeIndexer},
		{"Host", "\u0002", EntityTypeHost},
		{"Unknown", "\u0003", EntityTypeUnknown},
		{"Empty", "", EntityTypeUnknown},
		{"Invalid", "invalid", EntityTypeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetEntityType(tt.entity)
			require.Equal(t, tt.expected, result)
		})
	}

	t.Log("✅ Entity type detection test passed")
}

// TestUnifiedEventHandler_IntegrationFlow tests the complete integration flow
func TestUnifiedEventHandler_IntegrationFlow(t *testing.T) {
	// Create mock managers
	indexerManager := NewMockIndexerManager()
	peerManager := NewMockPeerManager()
	blockProcessor := NewMockBlockProcessor()
	mockViewManager := &MockViewManager{
		views: make(map[string]view.View),
	}
	
	// Create unified event handler
	handler := NewUnifiedEventHandler(indexerManager, mockViewManager, peerManager, blockProcessor)

	// Create a mock WebSocket server
	mockServer := NewMockWebSocketServer()
	defer mockServer.Close()

	// Start the event subscription
	cancel, eventChan, err := StartEventSubscription(mockServer.WebsocketURL())
	require.NoError(t, err)
	defer cancel()

	// Create a mixed event with indexer registration
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
									Type: "EntityRegistered",
									Attributes: []EventAttribute{
										{Key: "key", Value: "0xtest-indexer", Index: true},
										{Key: "owner", Value: "shinzo1testowner", Index: true},
										{Key: "did", Value: "did:key:test", Index: true},
										{Key: "pid", Value: "12D3KooWTestPeer", Index: true},
										{Key: "entity", Value: "\u0001", Index: true}, // Indexer
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
	mockServer.SendEvent(mixedEvent)

	// Process the event
	timeout := time.After(5 * time.Second)
	eventProcessed := false

	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				t.Fatal("Event channel closed unexpectedly")
			}

			// Process the event with unified handler
			err := handler.ProcessEvent(context.Background(), event)
			require.NoError(t, err)
			
			eventProcessed = true
			t.Log("✅ Event processed with unified handler")

		case <-timeout:
			if !eventProcessed {
				t.Fatal("Timeout waiting for event")
			}
		}

		if eventProcessed {
			break
		}
	}

	// Verify the indexer was added and peer was added
	indexer, exists := indexerManager.GetIndexer("0xtest-indexer")
	require.True(t, exists)
	require.Equal(t, "shinzo1testowner", indexer.Owner)

	peer, exists := peerManager.GetPeer("12D3KooWTestPeer")
	require.True(t, exists)
	require.Equal(t, "shinzo1testowner", peer.Owner)

	t.Log("✅ Integration flow test completed successfully")
}

// MockViewManager is a mock implementation of ViewManagerInterface
type MockViewManager struct {
	views map[string]view.View
}

// NewMockViewManager creates a new mock view manager
func NewMockViewManager() *MockViewManager {
	return &MockViewManager{
		views: make(map[string]view.View),
	}
}

// RegisterView registers a new view
func (mvm *MockViewManager) RegisterView(ctx context.Context, viewDef view.View) error {
	mvm.views[viewDef.Name] = viewDef
	return nil
}

// GetView retrieves a view by name
func (mvm *MockViewManager) GetView(name string) (view.View, bool) {
	view, exists := mvm.views[name]
	return view, exists
}

// GetViewCount returns the number of views
func (mvm *MockViewManager) GetViewCount() int {
	return len(mvm.views)
}
