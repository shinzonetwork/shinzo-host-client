package shinzohub

import (
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/shinzonetwork/viewbundle-go"
	"github.com/stretchr/testify/require"
)

// Real devnet values used across tests.
const (
	testHostAddress    = "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm"
	testHostDID        = "did:key:z7r8orQ6nZti55L4MsGEfJcpMBpjFAHResJ2wscTo77VFAdot5JNZ8Bz87hCMZ8XLwgA6YYMyQ521AXaEGYb9BSYfKf7i"
	testHostConnString = "/ip4/192.168.1.100/tcp/9171/p2p/12D3KooWQuQrFFtJ7dNi4R69MaEjrJ7dKxiwjKAhLgzqxjC1ntbo"

	testIndexerAddress    = "shinzo1k2e3g3696x7ycdz5tlqslplpgyh3dwy7e7jarm"
	testIndexerDID        = "did:key:z7r8op4kfY1gpaF3x3uPBT2VJEsCEJiMGq8EG9u9DXzKzRv2jzG2T4d8ictygKyMCVDSsYSwNreSyiepJfeajFfZSkRQb"
	testIndexerConnString = "/ip4/10.0.0.50/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r"

	testViewCreator      = "shinzo1cg7rssfan6duymqrvhhce2ldyycwvqll0z338z"
	testContractAddress1 = "0x6814589574569e6c25e72EB52f62aFaDDf5eF14D"
	testContractAddress2 = "0xf00DFed28B5304251f271c6474dF260067ee6BDa"
)

func TestViewRegisteredEvent_ToString(t *testing.T) {
	event := ViewRegisteredEvent{
		ViewID:          "view-abc123",
		ContractAddress: testContractAddress2,
		ViewName:        "StablecoinEvent",
		Creator:         testViewCreator,
		View: view.View{
			Name: "StablecoinEvent",
		},
	}

	expected := "ViewRegistered: id=view-abc123, address=" + testContractAddress2 + ", creator=" + testViewCreator
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
		ContractAddress: testContractAddress1,
		ViewName:        "SimpleLog",
		Creator:         testViewCreator,
		View: view.View{
			Name: "SimpleLog",
			Data: viewbundle.View{
				Query: "Ethereum__Mainnet__Log { address topics data transactionHash blockNumber }",
				Sdl:   "type SimpleLog @materialized(if: false) { transactionHash: String }",
			},
		},
	}

	require.Equal(t, testContractAddress1, event.ContractAddress)
	require.Equal(t, "SimpleLog", event.ViewName)
	require.Equal(t, testViewCreator, event.Creator)
	require.Equal(t, "SimpleLog", event.View.Name)
	require.Equal(t, "Ethereum__Mainnet__Log { address topics data transactionHash blockNumber }", event.View.Data.Query)
}

func TestEventStructures(t *testing.T) {
	viewEvent := ViewRegisteredEvent{
		ContractAddress: testContractAddress2,
		ViewName:        "StablecoinEvent",
		Creator:         testViewCreator,
		View: view.View{
			Name: "StablecoinEvent",
			Data: viewbundle.View{
				Query: "Ethereum__Mainnet__Log { address topics data }",
				Sdl:   "type StablecoinEvent @materialized(if: false) { address: String }",
			},
		},
	}

	require.Equal(t, testContractAddress2, viewEvent.ContractAddress)
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
		ContractAddress: testContractAddress1,
		Creator:         testViewCreator,
		View:            view.View{Name: "SimpleLog"},
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

// Covers the legacy single-word ViewRegistered case the parser still matches.
// See the TODO in events.go for cleanup context.
func TestExtractShinzoEvents_ViewRegistered(t *testing.T) {
	msg := RPCResponse{
		JSONRPCVersion: "2.0",
		ID:             1,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint/event/Tx",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "ViewRegistered",
									Attributes: []EventAttribute{
										{Key: "view_address", Value: testContractAddress1},
										{Key: "view_name", Value: "SimpleLog"},
										{Key: "creator", Value: testViewCreator},
										// "data" omitted: wire format would need a real viewbundle
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
		JSONRPCVersion: "2.0",
		ID:             2,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint/event/Tx",
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
		JSONRPCVersion: "2.0",
		ID:             3,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint/event/Tx",
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
		JSONRPCVersion: "2.0",
		ID:             2,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint/event/Tx",
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
		JSONRPCVersion: "2.0",
		ID:             1,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint/event/Tx",
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

// view.view_registered fills view_id, contract_address, creator on the
// emitted event. View stays empty; downstream hydration populates it.
func TestExtractShinzoEvents_ViewViewRegistered(t *testing.T) {
	msg := RPCResponse{
		JSONRPCVersion: "2.0",
		ID:             1,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint/event/Tx",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "view.view_registered",
									Attributes: []EventAttribute{
										{Key: "view_id", Value: "view-id-abc"},
										{Key: "contract_address", Value: testContractAddress1},
										{Key: "creator", Value: testViewCreator},
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

	vre, ok := events[0].(*ViewRegisteredEvent)
	require.True(t, ok, "event should be ViewRegisteredEvent")
	require.Equal(t, "view-id-abc", vre.ViewID)
	require.Equal(t, testContractAddress1, vre.ContractAddress)
	require.Equal(t, testViewCreator, vre.Creator)
	require.Empty(t, vre.View.Data.Query, "View bundle is hydrated downstream, not in the event")
}

// A view.view_registered event missing contract_address can't be hydrated,
// so the parser drops it.
func TestExtractShinzoEvents_ViewViewRegistered_Incomplete(t *testing.T) {
	msg := RPCResponse{
		JSONRPCVersion: "2.0",
		ID:             1,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint/event/Tx",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								{
									Type: "view.view_registered",
									Attributes: []EventAttribute{
										{Key: "view_id", Value: "view-id-abc"},
										// contract_address omitted
										{Key: "creator", Value: testViewCreator},
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

// view.view_registration_failed and _timed_out are logged and not emitted
// onto the channel.
func TestExtractShinzoEvents_ViewViewRegistrationFailed(t *testing.T) {
	for _, eventType := range []string{
		"view.view_registration_failed",
		"view.view_registration_timed_out",
	} {
		msg := RPCResponse{
			JSONRPCVersion: "2.0",
			ID:             1,
			Result: RPCResult{
				Data: RPCData{
					Type: "tendermint/event/Tx",
					Value: TxResult{
						TxResult: TxResultData{
							Result: TxResultResult{
								Events: []Event{
									{
										Type: eventType,
										Attributes: []EventAttribute{
											{Key: "view_id", Value: "view-id-failed"},
											{Key: "contract_address", Value: testContractAddress1},
											{Key: "creator", Value: testViewCreator},
											{Key: "error", Value: "ack returned with status FAILURE"},
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
		require.Len(t, events, 0, "%s should not be emitted as a ShinzoEvent", eventType)
	}
}

// A single Cosmos tx routinely carries many events: the registry event we care
// about plus the ICA bookkeeping and any unrelated module emissions on the
// same tx. The parser must walk the full Events list and return exactly the
// matching well-formed registry events, regardless of order or interleaving.
//
// What this catches: a parser that short-circuits on the first matching event
// and ignores the rest; a parser that fails to drop a malformed
// view.view_registered (incomplete attributes); a panic on an unrelated event
// type; a regression where view.view_registration_failed accidentally lands
// on the channel instead of being logged-and-dropped.
func TestExtractShinzoEvents_MultiEventTx(t *testing.T) {
	msg := RPCResponse{
		JSONRPCVersion: "2.0",
		ID:             1,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint/event/Tx",
				Value: TxResult{
					TxResult: TxResultData{
						Result: TxResultResult{
							Events: []Event{
								// Unrelated event — should be skipped, not crash.
								{
									Type: "transfer",
									Attributes: []EventAttribute{
										{Key: "amount", Value: "1000ushinzo"},
									},
								},
								// Well-formed view.view_registered — the only event
								// we expect to see emitted.
								{
									Type: "view.view_registered",
									Attributes: []EventAttribute{
										{Key: "view_id", Value: "good-view-id"},
										{Key: "contract_address", Value: testContractAddress1},
										{Key: "creator", Value: testViewCreator},
									},
								},
								// Malformed view.view_registered — missing
								// contract_address. Hydration would fail later
								// anyway; the parser should drop it now.
								{
									Type: "view.view_registered",
									Attributes: []EventAttribute{
										{Key: "view_id", Value: "bad-view-id"},
										{Key: "creator", Value: testViewCreator},
									},
								},
								// Terminal failure event — must be logged but
								// not emitted; the host has no pending state to
								// roll back.
								{
									Type: "view.view_registration_failed",
									Attributes: []EventAttribute{
										{Key: "view_id", Value: "failed-view-id"},
										{Key: "contract_address", Value: testContractAddress2},
										{Key: "error", Value: "ack returned FAILURE"},
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
	require.Len(t, events, 1, "exactly one well-formed view.view_registered should pass")

	vre, ok := events[0].(*ViewRegisteredEvent)
	require.True(t, ok, "expected *ViewRegisteredEvent, got %T", events[0])
	require.Equal(t, "good-view-id", vre.ViewID)
	require.Equal(t, testContractAddress1, vre.ContractAddress)
	require.Equal(t, testViewCreator, vre.Creator)
}
