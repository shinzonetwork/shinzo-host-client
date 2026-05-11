package shinzohub

import (
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/shinzonetwork/viewbundle-go"
	"github.com/stretchr/testify/require"
)

// Real devnet values used across tests.
const (
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
