package shinzohub

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stubLCDForBundle returns an httptest server that responds to the single-view
// REST endpoint for `contract` with `bundleBase64`, and 404s every other path.
// Used to simulate the hub's view-registry REST gateway during event hydration.
func stubLCDForBundle(t *testing.T, contract, bundleBase64 string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/shinzonetwork/view/v1/views/"+contract {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Query().Get("include_data") != "true" {
			t.Errorf("expected include_data=true, got %q", r.URL.Query().Get("include_data"))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			lcdFieldView: LCDView{
				Name:            "stub",
				Creator:         testViewCreator,
				ContractAddress: contract,
				Data:            bundleBase64,
				Height:          "1",
			},
		})
	}))
}

// txResponseFor wraps a single Cosmos event into the CometBFT WS RPCResponse
// envelope the host's eventLoop expects.
func txResponseFor(events ...Event) RPCResponse {
	return RPCResponse{
		JSONRPCVersion: jsonRPCVersion,
		ID:             1,
		Result: RPCResult{
			Data: RPCData{
				Type: tmEventTxType,
				Value: TxResult{
					TxResult: TxResultData{
						Height: "100",
						Result: TxResultResult{Events: events},
					},
				},
			},
		},
	}
}

// view.view_registered events carry only identifiers; the bundle has to be
// fetched separately from the hub's REST gateway. This test verifies that the
// full live path (CometBFT WS → parser → LCD hydration → channel) emits a view
// whose Data is populated from the LCD-served bundle.
//
// What this catches: a parser regression that fails to extract contract_address;
// a missing or misordered call to hydrateViewBundle in eventLoop; a mismatch
// between the LCD URL the client constructs and the path the gateway actually
// exposes; a regression in ProcessViewFromWireFormat that fails to decode a
// well-formed bundle.
func TestEventSubscription_HydratesAndEmits(t *testing.T) {
	const contract = "0x6814589574569e6c25e72EB52f62aFaDDf5eF14D"
	const expectedQuery = "Ethereum__Mainnet__Log { address }"
	bundleBase64 := makeTestBundle(t, "TestHydration")

	lcdSrv := stubLCDForBundle(t, contract, bundleBase64)
	defer lcdSrv.Close()
	lcd := NewRPCClient(lcdSrv.URL, nil)

	wsSrv := NewMockWebSocketServer()
	defer wsSrv.Close()

	cancel, ch, err := StartEventSubscription(wsSrv.WebsocketURL(), lcd)
	require.NoError(t, err)
	defer cancel()

	wsSrv.SendEvent(txResponseFor(Event{
		Type: eventTypeViewRegistered,
		Attributes: []EventAttribute{
			{Key: attrViewID, Value: "TestHydration_" + contract},
			{Key: attrContractAddress, Value: contract},
			{Key: attrCreator, Value: testViewCreator},
		},
	}))

	select {
	case ev, ok := <-ch:
		require.True(t, ok, "event channel closed unexpectedly")
		vre, ok := ev.(*ViewRegisteredEvent)
		require.True(t, ok, "expected *ViewRegisteredEvent, got %T", ev)
		require.Equal(t, contract, vre.ContractAddress)
		require.Equal(t, testViewCreator, vre.Creator)
		// The point of the test: the View on the emitted event has been
		// hydrated from the LCD response, not left as the empty struct that
		// extractShinzoEvents constructs.
		require.Equal(t, "TestHydration", vre.View.Name, "view name extracted from SDL")
		require.Equal(t, expectedQuery, vre.View.Data.Query, "view query hydrated from bundle")
		require.NotEmpty(t, vre.View.Data.Sdl, "view SDL hydrated from bundle")
		require.Len(t, vre.View.Data.Transform.Lenses, 1, "lens chain decoded from bundle")
		// The contract address is an event attribute, not part of the bundle.
		// Hydration must copy it onto the View so the decoded value carries
		// the on-chain identity end-to-end.
		require.Equal(t, contract, vre.View.ContractAddress, "view contract address propagated to View struct")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for hydrated view.view_registered event")
	}
}

// When the LCD returns a transient 404 before the registering block has
// propagated to the LCD-backing RPC pod, hydration must retry rather than
// drop. This test stubs an LCD that 404s the first two requests and then
// serves the bundle; the event must be emitted on the channel with its
// View populated.
//
// What this catches: a regression where the retry is bypassed or counted
// wrong; a mismatch between the retry classifier and what GetViewBundle
// returns on 404; a deadlock between the retry sleep and the WS read
// goroutine; the channel-send racing the WARN drop.
func TestEventSubscription_HydratesAfterTransientLCD(t *testing.T) {
	useFastHydrateRetries(t, 5, 1*time.Millisecond)

	const contract = "0x6814589574569e6c25e72EB52f62aFaDDf5eF14D"
	const expectedQuery = "Ethereum__Mainnet__Log { address }"
	bundleBase64 := makeTestBundle(t, "TestHydration")

	var hits atomic.Int64
	lcdSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/shinzonetwork/view/v1/views/"+contract {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		count := hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if count <= 2 {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":5,"message":"view not found"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			lcdFieldView: LCDView{
				Name:            "stub",
				Creator:         testViewCreator,
				ContractAddress: contract,
				Data:            bundleBase64,
				Height:          "1",
			},
		})
	}))
	defer lcdSrv.Close()
	lcd := NewRPCClient(lcdSrv.URL, nil)

	wsSrv := NewMockWebSocketServer()
	defer wsSrv.Close()

	cancel, ch, err := StartEventSubscription(wsSrv.WebsocketURL(), lcd)
	require.NoError(t, err)
	defer cancel()

	wsSrv.SendEvent(txResponseFor(Event{
		Type: eventTypeViewRegistered,
		Attributes: []EventAttribute{
			{Key: attrViewID, Value: "TestHydration_" + contract},
			{Key: attrContractAddress, Value: contract},
			{Key: attrCreator, Value: testViewCreator},
		},
	}))

	select {
	case ev, ok := <-ch:
		require.True(t, ok, "event channel closed unexpectedly")
		vre, ok := ev.(*ViewRegisteredEvent)
		require.True(t, ok, "expected *ViewRegisteredEvent, got %T", ev)
		require.Equal(t, contract, vre.ContractAddress)
		require.Equal(t, expectedQuery, vre.View.Data.Query, "view query hydrated from bundle on retry")
		require.EqualValues(t, 3, hits.Load(), "two transient failures plus one success")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for hydrated view.view_registered event")
	}
}

// When the LCD returns 404 (e.g. the registry hasn't caught up to the just-
// fired event, or the operator points at a stale gateway), hydration must
// fail closed: the unhydrated event must NOT reach the channel. Otherwise the
// downstream consumer would try to register a view whose Data fields are
// empty and the host's local state diverges silently.
//
// What this catches: a regression that emits the event despite a hydration
// error (e.g. someone "helpfully" forwards on warning instead of dropping);
// a panic on LCD non-200; a ctx-cancel race that closes the channel early.
func TestEventSubscription_HydrationFailure_DropsEvent(t *testing.T) {
	const contract = "0xf00DFed28B5304251f271c6474dF260067ee6BDa"

	lcdSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":5,"message":"view not found"}`))
	}))
	defer lcdSrv.Close()
	lcd := NewRPCClient(lcdSrv.URL, nil)

	wsSrv := NewMockWebSocketServer()
	defer wsSrv.Close()

	cancel, ch, err := StartEventSubscription(wsSrv.WebsocketURL(), lcd)
	require.NoError(t, err)
	defer cancel()

	wsSrv.SendEvent(txResponseFor(Event{
		Type: eventTypeViewRegistered,
		Attributes: []EventAttribute{
			{Key: attrViewID, Value: "Missing_" + contract},
			{Key: attrContractAddress, Value: contract},
			{Key: attrCreator, Value: testViewCreator},
		},
	}))

	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("channel closed unexpectedly; expected the event to be dropped, not the channel")
		}
		t.Fatalf("expected no event after LCD hydration failure, got %T (%+v)", ev, ev)
	case <-time.After(500 * time.Millisecond):
		// Pass: event was dropped.
	}
}
