package shinzohub

import (
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/shinzonetwork/viewbundle-go"
	"github.com/stretchr/testify/require"
)

// Real devnet values used across tests
const (
	testHostAddress    = "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm"
	testHostDID        = "did:key:z7r8orQ6nZti55L4MsGEfJcpMBpjFAHResJ2wscTo77VFAdot5JNZ8Bz87hCMZ8XLwgA6YYMyQ521AXaEGYb9BSYfKf7i"
	testHostConnString = "/ip4/192.168.1.100/tcp/9171/p2p/12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo"

	testIndexerAddress    = "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm"
	testIndexerDID        = "did:key:z7r8op4kfY1gpaF3x3uPBT2VJEsCEJiMGq8EG9u9DXzKzRv2jzG2T4d8ictygKyMCVDSsYSwNreSyiepJfeajFfZSkRQb"
	testIndexerConnString = "/ip4/10.0.0.50/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r"

	testViewCreator  = "shinzo1cg7rssfan6duymqrvhhce2ldyycwvqll0z338z"
	testViewAddress1 = "0x6814589574569e6c25e72EB52f62aFaDDf5eF14D"
	testViewAddress2 = "0xf00DFed28B5304251f271c6474dF260067ee6BDa"
)

func TestViewRegisteredEvent_ToString(t *testing.T) {
	event := ViewRegisteredEvent{
		ViewAddress: testViewAddress2,
		ViewName:    "StablecoinEvent",
		Creator:     testViewCreator,
		View: view.View{
			Name: "StablecoinEvent",
		},
	}

	expected := "ViewRegistered: address=" + testViewAddress2 + ", name=StablecoinEvent, creator=" + testViewCreator
	actual := event.ToString()

	require.Equal(t, expected, actual)
}

func TestHostRegisteredEvent_ToString(t *testing.T) {
	event := HostRegisteredEvent{
		Owner:            testHostAddress,
		DID:              testHostDID,
		ConnectionString: testHostConnString,
	}

	expected := "HostRegistered: owner=" + testHostAddress + ", did=" + testHostDID + ", conn=" + testHostConnString
	actual := event.ToString()

	require.Equal(t, expected, actual)
}

func TestIndexerRegisteredEvent_ToString(t *testing.T) {
	event := IndexerRegisteredEvent{
		Owner:            testIndexerAddress,
		DID:              testIndexerDID,
		ConnectionString: testIndexerConnString,
		SourceChain:      "ethereum",
		SourceChainID:    "1",
	}

	expected := "IndexerRegistered: owner=" + testIndexerAddress + ", did=" + testIndexerDID + ", conn=" + testIndexerConnString + ", chain=ethereum/1"
	actual := event.ToString()

	require.Equal(t, expected, actual)
}

func TestExtractViewFromEvent(t *testing.T) {
	event := ViewRegisteredEvent{
		ViewAddress: testViewAddress1,
		ViewName:    "SimpleLog",
		Creator:     testViewCreator,
		View: view.View{
			Name: "SimpleLog",
			Data: viewbundle.View{
				Query: "Ethereum__Testnet__Log { address topics data transactionHash blockNumber }",
				Sdl:   "type SimpleLog @materialized(if: false) { transactionHash: String }",
			},
		},
	}

	require.Equal(t, testViewAddress1, event.ViewAddress)
	require.Equal(t, "SimpleLog", event.ViewName)
	require.Equal(t, testViewCreator, event.Creator)
	require.Equal(t, "SimpleLog", event.View.Name)
	require.Equal(t, "Ethereum__Testnet__Log { address topics data transactionHash blockNumber }", event.View.Data.Query)
}

func TestEventStructures(t *testing.T) {
	viewEvent := ViewRegisteredEvent{
		ViewAddress: testViewAddress2,
		ViewName:    "StablecoinEvent",
		Creator:     testViewCreator,
		View: view.View{
			Name: "StablecoinEvent",
			Data: viewbundle.View{
				Query: "Ethereum__Testnet__Log { address topics data }",
				Sdl:   "type StablecoinEvent @materialized(if: false) { address: String }",
			},
		},
	}

	require.Equal(t, testViewAddress2, viewEvent.ViewAddress)
	require.Equal(t, "StablecoinEvent", viewEvent.ViewName)
	require.Equal(t, testViewCreator, viewEvent.Creator)

	hostEvent := HostRegisteredEvent{
		Owner:            testHostAddress,
		DID:              testHostDID,
		ConnectionString: testHostConnString,
	}

	require.Equal(t, testHostAddress, hostEvent.Owner)
	require.Equal(t, testHostDID, hostEvent.DID)
	require.Equal(t, testHostConnString, hostEvent.ConnectionString)

	indexerEvent := IndexerRegisteredEvent{
		Owner:            testIndexerAddress,
		DID:              testIndexerDID,
		ConnectionString: testIndexerConnString,
		SourceChain:      "ethereum",
		SourceChainID:    "1",
	}

	require.Equal(t, testIndexerAddress, indexerEvent.Owner)
	require.Equal(t, testIndexerDID, indexerEvent.DID)
	require.Equal(t, testIndexerConnString, indexerEvent.ConnectionString)
	require.Equal(t, "ethereum", indexerEvent.SourceChain)
	require.Equal(t, "1", indexerEvent.SourceChainID)
}

func TestEventInterface(t *testing.T) {
	var viewEvent ShinzoEvent = &ViewRegisteredEvent{
		ViewAddress: testViewAddress1,
		Creator:     testViewCreator,
		View:        view.View{Name: "SimpleLog"},
	}
	require.NotPanics(t, func() { _ = viewEvent.ToString() })

	var hostEvent ShinzoEvent = &HostRegisteredEvent{
		Owner: testHostAddress,
		DID:   testHostDID,
	}
	require.NotPanics(t, func() { _ = hostEvent.ToString() })

	var indexerEvent ShinzoEvent = &IndexerRegisteredEvent{
		Owner:       testIndexerAddress,
		DID:         testIndexerDID,
		SourceChain: "ethereum",
	}
	require.NotPanics(t, func() { _ = indexerEvent.ToString() })
}

func TestExtractShinzoEvents_ViewRegistered(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		ID:             1,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "ViewRegistered",
									Attributes: []EventAttribute{
										{Key: "view_address", Value: testViewAddress1},
										{Key: "view_name", Value: "SimpleLog"},
										{Key: "creator", Value: testViewCreator},
										// "data" omitted: wire format would need real viewbundle
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Len(t, events, 0, "should reject ViewRegistered event without valid view data")
}

func TestExtractShinzoEvents_HostRegistered(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		ID:             2,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "HostRegistered",
									Attributes: []EventAttribute{
										{Key: "owner", Value: testHostAddress},
										{Key: "did", Value: testHostDID},
										{Key: "connection_string", Value: testHostConnString},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Len(t, events, 1)

	hostEvent, ok := events[0].(*HostRegisteredEvent)
	require.True(t, ok, "event should be HostRegisteredEvent")
	require.Equal(t, testHostAddress, hostEvent.Owner)
	require.Equal(t, testHostDID, hostEvent.DID)
	require.Equal(t, testHostConnString, hostEvent.ConnectionString)
}

func TestExtractShinzoEvents_IndexerRegistered(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		ID:             3,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "IndexerRegistered",
									Attributes: []EventAttribute{
										{Key: "owner", Value: testIndexerAddress},
										{Key: "did", Value: testIndexerDID},
										{Key: "connection_string", Value: testIndexerConnString},
										{Key: "source_chain", Value: "ethereum"},
										{Key: "source_chain_id", Value: "1"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Len(t, events, 1)

	indexerEvent, ok := events[0].(*IndexerRegisteredEvent)
	require.True(t, ok, "event should be IndexerRegisteredEvent")
	require.Equal(t, testIndexerAddress, indexerEvent.Owner)
	require.Equal(t, testIndexerDID, indexerEvent.DID)
	require.Equal(t, testIndexerConnString, indexerEvent.ConnectionString)
	require.Equal(t, "ethereum", indexerEvent.SourceChain)
	require.Equal(t, "1", indexerEvent.SourceChainID)
}

func TestExtractShinzoEvents_IncompleteHost(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		ID:             2,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "HostRegistered",
									Attributes: []EventAttribute{
										{Key: "owner", Value: testHostAddress},
										// missing did
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Len(t, events, 0, "should reject HostRegistered event missing DID")
}

func TestExtractShinzoEvents_IgnoresUnknownEvents(t *testing.T) {
	msg := RPCResponse{
		JsonRpcVersion: "2.0",
		ID:             1,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint.event",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "SomeOtherEvent",
									Attributes: []EventAttribute{
										{Key: "foo", Value: "bar"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	events := extractShinzoEvents(msg)
	require.Len(t, events, 0)
}
