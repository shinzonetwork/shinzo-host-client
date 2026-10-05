package host

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

// schemaHandler serves the fixture schema renamed to prefix, as a generator for that chain would.
func schemaHandler(t *testing.T, prefix string) http.HandlerFunc {
	sdl := strings.ReplaceAll(generatorSchema(t), chain.EthereumMainnet, prefix)
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(localschema.Response{Network: prefix, Schema: sdl}))
	}
}

func TestResolveSchema(t *testing.T) {
	const other = "Testchain__Devnet"
	valid := schemaHandler(t, chain.EthereumMainnet)
	otherValid := schemaHandler(t, other)
	wrongNetwork := schemaHandler(t, "Ethereum__Sepolia")
	failing := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }
	// The schema client times out after one second.
	slow := func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		valid(w, r)
	}

	cases := []struct {
		desc        string
		prefix      string
		generators  []http.HandlerFunc
		wantBuiltIn bool
		wantErr     error
	}{
		{desc: "no generators", prefix: chain.EthereumMainnet, wantBuiltIn: true},
		{desc: "a valid generator", prefix: chain.EthereumMainnet, generators: []http.HandlerFunc{valid}},
		{desc: "the first generator fails", prefix: chain.EthereumMainnet, generators: []http.HandlerFunc{failing, valid}},
		{desc: "the first generator times out", prefix: chain.EthereumMainnet, generators: []http.HandlerFunc{slow, valid}},
		{desc: "every generator fails", prefix: chain.EthereumMainnet, generators: []http.HandlerFunc{failing, wrongNetwork}, wantErr: errNoGeneratorSchema},
		{desc: "another chain with no generators", prefix: other, wantErr: errNoChainSchema},
		{desc: "another chain whose generators fail", prefix: other, generators: []http.HandlerFunc{failing, valid}, wantErr: errNoGeneratorSchema},
		{desc: "another chain with a valid generator", prefix: other, generators: []http.HandlerFunc{otherValid}},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			served := chain.Config{Prefix: c.prefix}
			for _, handler := range c.generators {
				srv := httptest.NewServer(handler)
				t.Cleanup(srv.Close)
				served.Generators = append(served.Generators, chain.Generator{URL: srv.URL})
			}
			collections := chain.EVM(c.prefix)
			schemaCfg := config.SchemaConfig{IndexerSchemaEndpoint: config.DefaultIndexerSchemaEndpoint, HTTPClientTimeoutSecs: 1}

			got, err := resolveSchema(context.Background(), schemaCfg, served, collections)

			switch {
			case c.wantErr != nil:
				require.ErrorIs(t, err, c.wantErr)
			case c.wantBuiltIn:
				require.NoError(t, err)
				require.Equal(t, localschema.GetSchema(), got)
			default:
				require.NoError(t, err)
				require.NotEqual(t, localschema.GetSchema(), got)
				require.Contains(t, got, collections.Block.Name)
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

	_, err := resolveSchema(context.Background(), schemaCfg, served, testCollections)
	require.NoError(t, err)
	require.False(t, secondCalled.Load(), "a generator after the first usable one was contacted")
}
