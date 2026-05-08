package shinzohub

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/shinzonetwork/viewbundle-go"
	"github.com/stretchr/testify/require"
)

// TestHostRegisteredEventSubscription tests the subscription to HostRegistered events via WebSocket.
func TestHostRegisteredEventSubscription(t *testing.T) {
	mockServer := NewMockWebSocketServer()
	defer mockServer.Close()

	cancel, eventChan, err := StartEventSubscription(mockServer.WebsocketURL(), nil)
	require.NoError(t, err)
	defer cancel()

	mockEvent := RPCResponse{
		JSONRPCVersion: "2.0",
		ID:             2,
		Result: RPCResult{
			Query: "tm.event='Tx' AND HostRegistered.owner EXISTS",
			Data: RPCData{
				Type: "tendermint/event/Tx",
				Value: TxResult{
					TxResult: TxResultData{
						Height: "12345",
						Tx:     "0x1234567890abcdef",
						Result: TxResultResult{
							Data: "success",
							Events: []Event{
								{
									Type: "HostRegistered",
									Attributes: []EventAttribute{
										{Key: "owner", Value: "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm", Index: true},
										{Key: "did", Value: "did:key:z7r8orQ6nZti55L4MsGEfJcpMBpjFAHResJ2wscTo77VFAdot5JNZ8Bz87hCMZ8XLwgA6YYMyQ521AXaEGYb9BSYfKf7i", Index: true},
										{Key: "connection_string", Value: "/ip4/192.168.1.100/tcp/9171/p2p/12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", Index: true},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	mockServer.SendEvent(mockEvent)

	timeout := time.After(5 * time.Second)
	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				t.Fatal("Event channel closed unexpectedly")
			}

			if hostEvent, ok := event.(*HostRegisteredEvent); ok {
				require.Equal(t, "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm", hostEvent.Owner)
				require.Equal(t, "did:key:z7r8orQ6nZti55L4MsGEfJcpMBpjFAHResJ2wscTo77VFAdot5JNZ8Bz87hCMZ8XLwgA6YYMyQ521AXaEGYb9BSYfKf7i", hostEvent.DID)
				require.Equal(t, "/ip4/192.168.1.100/tcp/9171/p2p/12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", hostEvent.ConnectionString)

				t.Logf("Successfully received HostRegistered event: %s", hostEvent.ToString())
				return
			}

			// Skip other event types (ViewRegistered, etc.)
			continue

		case <-timeout:
			t.Fatal("Timeout waiting for HostRegistered event")
		}
	}
}

// TestExtractHostAndIndexerRegisteredEvents tests the extraction function directly.
func TestExtractHostAndIndexerRegisteredEvents(t *testing.T) {
	testMsg := RPCResponse{
		JSONRPCVersion: "2.0",
		ID:             1,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint/event/Tx",
				Value: TxResult{
					TxResult: TxResultData{
						Height: "12345",
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "HostRegistered",
									Attributes: []EventAttribute{
										{Key: "owner", Value: "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm", Index: true},
										{Key: "did", Value: "did:key:z7r8orQ6nZti55L4MsGEfJcpMBpjFAHResJ2wscTo77VFAdot5JNZ8Bz87hCMZ8XLwgA6YYMyQ521AXaEGYb9BSYfKf7i", Index: true},
										{Key: "connection_string", Value: "/ip4/192.168.1.100/tcp/8080/p2p/12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", Index: true},
									},
								},
								{
									Type: "IndexerRegistered",
									Attributes: []EventAttribute{
										{Key: "owner", Value: "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm", Index: true},
										{Key: "did", Value: "did:key:z7r8op4kfY1gpaF3x3uPBT2VJEsCEJiMGq8EG9u9DXzKzRv2jzG2T4d8ictygKyMCVDSsYSwNreSyiepJfeajFfZSkRQb", Index: true},
										{Key: "connection_string", Value: "10.0.0.50:9090", Index: true},
										{Key: "source_chain", Value: "ethereum", Index: true},
										{Key: "source_chain_id", Value: "1", Index: true},
									},
								},
								{
									Type: "HostRegistered",
									Attributes: []EventAttribute{
										{Key: "owner", Value: "shinzo1incomplete"},
										// Missing DID: should be filtered out
									},
								},
								{
									Type: "OtherEvent",
									Attributes: []EventAttribute{
										{Key: "key", Value: "0xdeadbeef"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	allEvents := extractShinzoEvents(testMsg)

	// Should get 1 host + 1 indexer (incomplete host and other event filtered out)
	require.Len(t, allEvents, 2, "Should extract exactly 2 complete events")

	// First event: HostRegistered
	hostEvent, ok := allEvents[0].(*HostRegisteredEvent)
	require.True(t, ok, "First event should be HostRegisteredEvent")
	require.Equal(t, "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm", hostEvent.Owner)
	require.Equal(t, "did:key:z7r8orQ6nZti55L4MsGEfJcpMBpjFAHResJ2wscTo77VFAdot5JNZ8Bz87hCMZ8XLwgA6YYMyQ521AXaEGYb9BSYfKf7i", hostEvent.DID)
	require.Equal(t, "/ip4/192.168.1.100/tcp/8080/p2p/12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", hostEvent.ConnectionString)

	// Second event: IndexerRegistered
	indexerEvent, ok := allEvents[1].(*IndexerRegisteredEvent)
	require.True(t, ok, "Second event should be IndexerRegisteredEvent")
	require.Equal(t, "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm", indexerEvent.Owner)
	require.Equal(t, "did:key:z7r8op4kfY1gpaF3x3uPBT2VJEsCEJiMGq8EG9u9DXzKzRv2jzG2T4d8ictygKyMCVDSsYSwNreSyiepJfeajFfZSkRQb", indexerEvent.DID)
	require.Equal(t, "10.0.0.50:9090", indexerEvent.ConnectionString)
	require.Equal(t, "ethereum", indexerEvent.SourceChain)
	require.Equal(t, "1", indexerEvent.SourceChainID)
}

// TestMixedEventSubscription tests receiving ViewRegistered and HostRegistered events together.
func TestMixedEventSubscription(t *testing.T) {
	mockServer := NewMockWebSocketServer()
	defer mockServer.Close()

	cancel, eventChan, err := StartEventSubscription(mockServer.WebsocketURL(), nil)
	require.NoError(t, err)
	defer cancel()

	// Create valid WASM magic number + some data
	wasmData := []byte{0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB, 0xCC, 0xDD}
	wasmBase64 := base64.StdEncoding.EncodeToString(wasmData)
	query := "Ethereum__Mainnet__Log {address topics data transactionHash blockNumber}"
	expectedView := view.View{
		Data: viewbundle.View{
			Query: query,
			Sdl:   "type FilteredAndDecodedLogs @materialized(if: false) {transactionHash: String}",
			Transform: viewbundle.Transform{
				Lenses: []viewbundle.Lens{
					{Path: wasmBase64},
				},
			},
		},
	}

	// Create proper wire format and encode to base64
	bundler := viewbundle.NewBundler()
	wire, err := bundler.BundleView(expectedView.Data)
	require.NoError(t, err)
	viewBase64 := base64.StdEncoding.EncodeToString(wire)

	// Send a mixed event with ViewRegistered and HostRegistered
	mixedEvent := RPCResponse{
		JSONRPCVersion: "2.0",
		ID:             1,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint/event/Tx",
				Value: TxResult{
					TxResult: TxResultData{
						Height: "12345",
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "ViewRegistered",
									Attributes: []EventAttribute{
										{Key: "view_address", Value: "0xf00DFed28B5304251f271c6474dF260067ee6BDa", Index: true},
										{Key: "view_name", Value: "FilteredAndDecodedLogs", Index: true},
										{Key: "creator", Value: "shinzo140fehngcrxvhdt84x729p3f0qmkmea8nq3rk92", Index: true},
										{Key: "data", Value: viewBase64, Index: true},
									},
								},
								{
									Type: "HostRegistered",
									Attributes: []EventAttribute{
										{Key: "owner", Value: "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm", Index: true},
										{Key: "did", Value: "did:key:z7r8host", Index: true},
										{Key: "connection_string", Value: "/ip4/192.168.1.100/tcp/9171/p2p/12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", Index: true},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	mockServer.SendEvent(mixedEvent)

	receivedView := false
	receivedHost := false
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
					require.Equal(t, "0xf00DFed28B5304251f271c6474dF260067ee6BDa", e.ContractAddress)
					require.Equal(t, "shinzo140fehngcrxvhdt84x729p3f0qmkmea8nq3rk92", e.Creator)
					require.Equal(t, expectedView.Data.Query, e.View.Data.Query)
					receivedView = true
					t.Log("Received ViewRegistered event")
				}

			case *HostRegisteredEvent:
				if !receivedHost {
					require.Equal(t, "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm", e.Owner)
					require.Equal(t, "did:key:z7r8host", e.DID)
					require.Equal(t, "/ip4/192.168.1.100/tcp/9171/p2p/12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo", e.ConnectionString)
					receivedHost = true
					t.Log("Received HostRegistered event")
				}

			default:
				t.Fatalf("Unexpected event type: %T", event)
			}

			if receivedView && receivedHost {
				t.Log("Successfully received both ViewRegistered and HostRegistered events")
				return
			}

		case <-timeout:
			t.Fatalf("Timeout waiting for events (view=%v, host=%v)", receivedView, receivedHost)
		}
	}
}

// TestHostRegisteredEventToString tests the string representation.
func TestHostRegisteredEventToString(t *testing.T) {
	event := HostRegisteredEvent{
		Owner:            "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm",
		DID:              "did:key:z7r8orQ6nZti55L4MsGEfJcpMBpjFAHResJ2wscTo77VFAdot5JNZ8Bz87hCMZ8XLwgA6YYMyQ521AXaEGYb9BSYfKf7i",
		ConnectionString: "/ip4/192.168.1.100/tcp/8080/p2p/12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo",
	}

	expected := "HostRegistered: owner=shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm, did=did:key:z7r8orQ6nZti55L4MsGEfJcpMBpjFAHResJ2wscTo77VFAdot5JNZ8Bz87hCMZ8XLwgA6YYMyQ521AXaEGYb9BSYfKf7i, conn=/ip4/192.168.1.100/tcp/8080/p2p/12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo"
	require.Equal(t, expected, event.ToString())
}

// TestIndexerRegisteredEventToString tests the string representation.
func TestIndexerRegisteredEventToString(t *testing.T) {
	event := IndexerRegisteredEvent{
		Owner:            "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm",
		DID:              "did:key:z7r8op4kfY1gpaF3x3uPBT2VJEsCEJiMGq8EG9u9DXzKzRv2jzG2T4d8ictygKyMCVDSsYSwNreSyiepJfeajFfZSkRQb",
		ConnectionString: "/ip4/10.0.0.50/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r",
		SourceChain:      "ethereum",
		SourceChainID:    "1",
	}

	expected := "IndexerRegistered: owner=shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm, did=did:key:z7r8op4kfY1gpaF3x3uPBT2VJEsCEJiMGq8EG9u9DXzKzRv2jzG2T4d8ictygKyMCVDSsYSwNreSyiepJfeajFfZSkRQb, conn=/ip4/10.0.0.50/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r, chain=ethereum/1"
	require.Equal(t, expected, event.ToString())
}
