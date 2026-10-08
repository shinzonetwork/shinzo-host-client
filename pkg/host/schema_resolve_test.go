package host

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
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

func TestServedSchemas(t *testing.T) {
	valid := schemaHandler(t, chain.EthereumMainnet)
	wrongNetwork := schemaHandler(t, "Ethereum__Sepolia")
	failing := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }
	// A schema for the chain that the fetch checks would refuse: it lacks most of the tables.
	partialSDL := "type " + testCollections.Block.Name + " { number: Int }"
	partial := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(localschema.Response{Network: chain.EthereumMainnet, Schema: partialSDL}))
	}

	// answer is a schema servedSchemas yielded, with the index of the generator that served it.
	type answer struct {
		generator int
		schema    string
	}
	cases := []struct {
		desc       string
		generators []http.HandlerFunc
		want       []answer
	}{
		{desc: "no generators"},
		{
			desc:       "schemas are yielded unchecked, in order",
			generators: []http.HandlerFunc{partial, valid},
			want:       []answer{{generator: 0, schema: partialSDL}, {generator: 1, schema: generatorSchema(t)}},
		},
		{
			desc:       "a failing generator and one for another chain are skipped",
			generators: []http.HandlerFunc{failing, wrongNetwork, valid},
			want:       []answer{{generator: 2, schema: generatorSchema(t)}},
		},
		{desc: "no generator answers", generators: []http.HandlerFunc{failing, wrongNetwork}},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			served := chain.Config{Prefix: chain.EthereumMainnet}
			var urls []string
			for _, handler := range c.generators {
				srv := httptest.NewServer(handler)
				t.Cleanup(srv.Close)
				served.Generators = append(served.Generators, chain.Generator{URL: srv.URL})
				urls = append(urls, srv.URL)
			}
			schemaCfg := config.SchemaConfig{IndexerSchemaEndpoint: config.DefaultIndexerSchemaEndpoint, HTTPClientTimeoutSecs: 1}

			var got []answer
			for url, sdl := range servedSchemas(context.Background(), schemaCfg, served) {
				got = append(got, answer{generator: slices.Index(urls, url), schema: sdl})
			}

			require.Equal(t, c.want, got)
		})
	}
}

func TestServedSchemasStopsEarly(t *testing.T) {
	first := httptest.NewServer(schemaHandler(t, chain.EthereumMainnet))
	t.Cleanup(first.Close)
	var secondCalled atomic.Bool
	second := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { secondCalled.Store(true) }))
	t.Cleanup(second.Close)

	served := chain.Config{Prefix: chain.EthereumMainnet, Generators: []chain.Generator{{URL: first.URL}, {URL: second.URL}}}
	schemaCfg := config.SchemaConfig{IndexerSchemaEndpoint: config.DefaultIndexerSchemaEndpoint, HTTPClientTimeoutSecs: 1}

	for range servedSchemas(context.Background(), schemaCfg, served) {
		break
	}
	require.False(t, secondCalled.Load(), "a generator was contacted after the caller stopped")
}

func TestChainsApplier(t *testing.T) {
	const other = "Testchain__Devnet"
	ctx := context.Background()
	schemaCfg := config.SchemaConfig{IndexerSchemaEndpoint: config.DefaultIndexerSchemaEndpoint, HTTPClientTimeoutSecs: 1}
	var chains []chain.Config
	for _, prefix := range []string{chain.EthereumMainnet, other} {
		generator := httptest.NewServer(schemaHandler(t, prefix))
		t.Cleanup(generator.Close)
		chains = append(chains, chain.Config{Prefix: prefix, Generators: []chain.Generator{{URL: generator.URL}}})
	}

	node, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, chainsApplier{schemaCfg: schemaCfg, chains: chains})
	require.NoError(t, err)
	defer func() { _ = node.Close(ctx) }()
	for _, c := range chains {
		collections := chain.EVM(c.Prefix)
		for _, col := range append(collections.Generated(), collections.AttestationRecord) {
			_, err := node.DB.GetCollectionByName(ctx, col.Name)
			require.NoError(t, err, col.Name)
		}
	}

	// Every stored chain is listed, so a restart is accepted.
	require.NoError(t, chainsApplier{schemaCfg: schemaCfg, chains: chains}.ApplySchema(ctx, node))

	// A view named like a BlockSignature table does not mark a chain.
	_, err = node.DB.AddView(ctx, chain.EVM(other).Block.Name+" { number }", "type Unlisted__BlockSignature { number: Int }")
	require.NoError(t, err)
	require.NoError(t, chainsApplier{schemaCfg: schemaCfg, chains: chains}.ApplySchema(ctx, node))

	// The store holds a chain the config does not list, and the config lists a chain the store does
	// not hold yet. The unlisted chain is refused before any table is created.
	const third = "Third__Chain"
	err = chainsApplier{schemaCfg: schemaCfg, chains: []chain.Config{chains[0], {Prefix: third}}}.ApplySchema(ctx, node)
	require.ErrorIs(t, err, errUnlistedChain)
	_, err = node.DB.GetCollectionByName(ctx, chain.EVM(third).Block.Name)
	require.Error(t, err)
}
