package shinzohub

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
			"view": LCDView{
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
		JSONRPCVersion: "2.0",
		ID:             1,
		Result: RPCResult{
			Data: RPCData{
				Type: "tendermint/event/Tx",
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
		Type: "view.view_registered",
		Attributes: []EventAttribute{
			{Key: "view_id", Value: "TestHydration_" + contract},
			{Key: "contract_address", Value: contract},
			{Key: "creator", Value: testViewCreator},
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

	lcdSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		Type: "view.view_registered",
		Attributes: []EventAttribute{
			{Key: "view_id", Value: "Missing_" + contract},
			{Key: "contract_address", Value: contract},
			{Key: "creator", Value: testViewCreator},
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
