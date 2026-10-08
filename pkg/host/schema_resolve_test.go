package host

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	localschema "github.com/shinzonetwork/shinzo-host-client/pkg/schema"
)

// generatorSchema returns a schema as a generator serves it for Ethereum mainnet.
func generatorSchema(t *testing.T) string {
	t.Helper()
	sdl, err := os.ReadFile(filepath.Join("..", "schema", "testdata", "generator_schema.graphql"))
	require.NoError(t, err)
	return string(sdl)
}

// schemaHandler answers with a schema response for network.
func schemaHandler(t *testing.T, network string) http.HandlerFunc {
	sdl := generatorSchema(t)
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(localschema.Response{Network: network, Schema: sdl}))
	}
}

func TestResolveSchema(t *testing.T) {
	valid := schemaHandler(t, chain.EthereumMainnet)
	wrongNetwork := schemaHandler(t, "Ethereum__Sepolia")
	failing := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }
	// The schema client times out after one second.
	slow := func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		valid(w, r)
	}

	cases := []struct {
		desc        string
		generators  []http.HandlerFunc
		wantBuiltIn bool
	}{
		{desc: "no generators", wantBuiltIn: true},
		{desc: "a valid generator", generators: []http.HandlerFunc{valid}},
		{desc: "the first generator fails", generators: []http.HandlerFunc{failing, valid}},
		{desc: "the first generator times out", generators: []http.HandlerFunc{slow, valid}},
		{desc: "every generator fails", generators: []http.HandlerFunc{failing, wrongNetwork}, wantBuiltIn: true},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			served := chain.Config{Prefix: chain.EthereumMainnet}
			for _, handler := range c.generators {
				srv := httptest.NewServer(handler)
				t.Cleanup(srv.Close)
				served.Generators = append(served.Generators, chain.Generator{URL: srv.URL})
			}
			schemaCfg := config.SchemaConfig{IndexerSchemaEndpoint: config.DefaultIndexerSchemaEndpoint, HTTPClientTimeoutSecs: 1}

			got := resolveSchema(context.Background(), schemaCfg, served, testCollections)

			if c.wantBuiltIn {
				require.Equal(t, localschema.GetSchema(), got)
			} else {
				require.NotEqual(t, localschema.GetSchema(), got)
				require.Contains(t, got, testCollections.AttestationRecord.Name)
			}
		})
	}
}

func TestResolveSchemaUsesFirstUsableGenerator(t *testing.T) {
	first := httptest.NewServer(schemaHandler(t, chain.EthereumMainnet))
	t.Cleanup(first.Close)
	var secondCalled atomic.Bool
	second := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { secondCalled.Store(true) }))
	t.Cleanup(second.Close)

	served := chain.Config{Prefix: chain.EthereumMainnet, Generators: []chain.Generator{{URL: first.URL}, {URL: second.URL}}}
	schemaCfg := config.SchemaConfig{IndexerSchemaEndpoint: config.DefaultIndexerSchemaEndpoint, HTTPClientTimeoutSecs: 1}

	resolveSchema(context.Background(), schemaCfg, served, testCollections)
	require.False(t, secondCalled.Load(), "a generator after the first usable one was contacted")
}
