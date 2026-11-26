package host

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/pruner"
)

// The verifier lists the documents to repair from the chain's own generators.
func TestVerifierHooksListTheChainsGenerators(t *testing.T) {
	listed := map[string][]string{"Bsc__Mainnet__Header": {"h1"}}
	generator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != documentsEndpoint {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(listed)
	}))
	t.Cleanup(generator.Close)
	c := chain.Config{Prefix: "Bsc__Mainnet", Generators: []chain.Generator{{URL: generator.URL}}}

	got, err := (&Host{}).verifierHooks(c, http.DefaultClient).Documents(t.Context(), 10, 12)

	require.NoError(t, err)
	require.Equal(t, listed, got)
}

// The verifier's lowest unverified height raises its own chain's prune floor.
func TestVerifierHooksRaiseTheFloor(t *testing.T) {
	floors := map[string]*pruner.Floor{"Bsc__Mainnet": {}, "Testchain__Devnet": {}}
	h := &Host{floors: floors}

	h.verifierHooks(chain.Config{Prefix: "Bsc__Mainnet"}, nil).Unverified(101)

	require.Equal(t, int64(101), floors["Bsc__Mainnet"].Load())
	require.Zero(t, floors["Testchain__Devnet"].Load())
	// A chain without a floor, as in a host built without pruning, is skipped.
	(&Host{}).verifierHooks(chain.Config{Prefix: "Bsc__Mainnet"}, nil).Unverified(101)
}
