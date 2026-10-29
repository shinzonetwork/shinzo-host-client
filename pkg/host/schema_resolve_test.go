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
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
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
		mode        string
		generators  []http.HandlerFunc
		wantBuiltIn bool
		wantErr     error
	}{
		{desc: "no generators", prefix: chain.EthereumMainnet, wantBuiltIn: true},
		{desc: "Ethereum mainnet in tokens mode with no generators", prefix: chain.EthereumMainnet, mode: chain.ModeTokens, wantErr: errNoChainSchema},
		{desc: "a valid generator", prefix: chain.EthereumMainnet, generators: []http.HandlerFunc{valid}},
		{desc: "the first generator fails", prefix: chain.EthereumMainnet, generators: []http.HandlerFunc{failing, valid}},
		{desc: "the first generator times out", prefix: chain.EthereumMainnet, generators: []http.HandlerFunc{slow, valid}},
		{desc: "every generator fails or times out", prefix: chain.EthereumMainnet, generators: []http.HandlerFunc{failing, slow}, wantErr: errNoGeneratorAnswered},
		{desc: "one generator serves another chain and the next fails", prefix: chain.EthereumMainnet, generators: []http.HandlerFunc{wrongNetwork, failing}, wantErr: errNoGeneratorSchema},
		{desc: "another chain with no generators", prefix: other, wantErr: errNoChainSchema},
		{desc: "another chain whose generators fail or serve Ethereum mainnet", prefix: other, generators: []http.HandlerFunc{failing, valid}, wantErr: errNoGeneratorSchema},
		{desc: "another chain with a valid generator", prefix: other, generators: []http.HandlerFunc{otherValid}},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			served := chain.Config{Prefix: c.prefix, Mode: c.mode}
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
	const sepolia = "Ethereum__Sepolia"
	valid := schemaHandler(t, chain.EthereumMainnet)
	wrongNetwork := schemaHandler(t, sepolia)
	failing := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }
	serving := func(sdl string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(localschema.Response{Network: chain.EthereumMainnet, Schema: sdl}))
		}
	}
	// A schema for the chain that the fetch checks would refuse: it lacks most of the tables.
	partialSDL := "type " + testCollections.Block.Name + " { number: Int }"

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
			generators: []http.HandlerFunc{serving(partialSDL), valid},
			want:       []answer{{generator: 0, schema: partialSDL}, {generator: 1, schema: generatorSchema(t)}},
		},
		{
			desc:       "a failing generator is skipped and one for another chain is yielded",
			generators: []http.HandlerFunc{failing, wrongNetwork, valid},
			want: []answer{
				{generator: 1, schema: strings.ReplaceAll(generatorSchema(t), chain.EthereumMainnet, sepolia)},
				{generator: 2, schema: generatorSchema(t)},
			},
		},
		{
			desc:       "a generator serving a schema that does not parse is skipped",
			generators: []http.HandlerFunc{serving("type {"), valid},
			want:       []answer{{generator: 1, schema: generatorSchema(t)}},
		},
		{desc: "no generator answers", generators: []http.HandlerFunc{failing, serving("type {")}},
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
			for url, resp := range servedSchemas(context.Background(), schemaCfg, served) {
				got = append(got, answer{generator: slices.Index(urls, url), schema: resp.Schema})
			}

			require.Equal(t, c.want, got)
		})
	}
}

func TestServedSchemasShareOneDeadline(t *testing.T) {
	// The first generator holds the request until the host gives up on it.
	hanging := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	t.Cleanup(hanging.Close)
	var secondCalled atomic.Bool
	valid := schemaHandler(t, chain.EthereumMainnet)
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalled.Store(true)
		valid(w, r)
	}))
	t.Cleanup(second.Close)

	served := chain.Config{Prefix: chain.EthereumMainnet, Generators: []chain.Generator{{URL: hanging.URL}, {URL: second.URL}}}
	schemaCfg := config.SchemaConfig{IndexerSchemaEndpoint: config.DefaultIndexerSchemaEndpoint, HTTPClientTimeoutSecs: 1}

	for generator := range servedSchemas(context.Background(), schemaCfg, served) {
		t.Fatalf("generator %s answered after the first used up the deadline", generator)
	}
	require.False(t, secondCalled.Load(), "a generator was asked after the first used up the deadline")
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
	const (
		other = "Testchain__Devnet"
		third = "Third__Chain"
	)
	ctx := context.Background()
	schemaCfg := config.SchemaConfig{IndexerSchemaEndpoint: config.DefaultIndexerSchemaEndpoint, HTTPClientTimeoutSecs: 1}
	var chains []chain.Config
	for _, prefix := range []string{chain.EthereumMainnet, other} {
		generator := httptest.NewServer(schemaHandler(t, prefix))
		t.Cleanup(generator.Close)
		chains = append(chains, chain.Config{Prefix: prefix, Generators: []chain.Generator{{URL: generator.URL}}})
	}

	// The store holds the first chain before the other is added, as when a chain is added to the
	// config of a host that already serves one.
	node, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, &chainsApplier{schemaCfg: schemaCfg, chains: chains[:1]})
	require.NoError(t, err)
	defer func() { _ = node.Close(ctx) }()
	_, err = node.DB.GetCollectionByName(ctx, chain.EVM(other).Block.Name)
	require.Error(t, err)

	applier := &chainsApplier{schemaCfg: schemaCfg, chains: chains}
	require.NoError(t, applier.ApplySchema(ctx, node))
	var subscribed []string
	for _, c := range chains {
		collections := chain.EVM(c.Prefix)
		for _, col := range append(collections.Generated(), collections.AttestationRecord) {
			_, err := node.DB.GetCollectionByName(ctx, col.Name)
			require.NoError(t, err, col.Name)
		}
		subscribed = append(subscribed, collections.Subscribed()...)
	}
	require.Equal(t, chains, applier.enabled)
	got, err := node.DB.ListP2PCollections(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, subscribed, got)

	// Every stored chain is listed, so a restart is accepted.
	require.NoError(t, applier.ApplySchema(ctx, node))

	// A view named like a BlockSignature table does not mark a chain.
	_, err = node.DB.AddView(ctx, chain.EVM(other).Block.Name+" { number }", "type Unlisted__BlockSignature { number: Int }")
	require.NoError(t, err)
	require.NoError(t, applier.ApplySchema(ctx, node))

	// A chain the store holds no tables for, whose generator does not answer, is disabled with nothing
	// stored or subscribed for it, and the other chains are still served.
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(down.Close)
	unanswered := chain.Config{Prefix: third, Generators: []chain.Generator{{URL: down.URL}}}
	withUnanswered := &chainsApplier{schemaCfg: schemaCfg, chains: append(slices.Clone(chains), unanswered)}
	require.NoError(t, withUnanswered.ApplySchema(ctx, node))
	require.Equal(t, chains, withUnanswered.enabled)
	require.Equal(t, map[string]string{third: server.ReasonNoGeneratorAnswered}, withUnanswered.reasons)
	_, err = node.DB.GetCollectionByName(ctx, chain.EVM(third).Block.Name)
	require.Error(t, err)
	got, err = node.DB.ListP2PCollections(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, subscribed, got)

	// A stored chain the config no longer lists is unsubscribed and keeps its tables, and listing it
	// again serves it again.
	withoutOther := &chainsApplier{schemaCfg: schemaCfg, chains: chains[:1]}
	require.NoError(t, withoutOther.ApplySchema(ctx, node))
	require.Equal(t, chains[:1], withoutOther.enabled)
	got, err = node.DB.ListP2PCollections(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, chain.EVM(chain.EthereumMainnet).Subscribed(), got)
	_, err = node.DB.GetCollectionByName(ctx, chain.EVM(other).Block.Name)
	require.NoError(t, err)
	// A later start without it succeeds and still reports it removed.
	require.NoError(t, withoutOther.ApplySchema(ctx, node))
	require.Equal(t, []string{other}, withoutOther.removed)
	require.NoError(t, applier.ApplySchema(ctx, node))
	got, err = node.DB.ListP2PCollections(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, subscribed, got)

	// The store holds a table the other chain's generator does not serve, so that chain is disabled
	// and unsubscribed, and the first chain is still served.
	_, err = node.DB.AddCollection(ctx, "type "+other+"__Extra { a: Int }")
	require.NoError(t, err)
	require.NoError(t, applier.ApplySchema(ctx, node))
	require.Equal(t, chains[:1], applier.enabled)
	got, err = node.DB.ListP2PCollections(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, chain.EVM(chain.EthereumMainnet).Subscribed(), got)

	// Drift disables a chain wherever it is listed.
	driftedFirst := &chainsApplier{schemaCfg: schemaCfg, chains: []chain.Config{chains[1], chains[0]}}
	require.NoError(t, driftedFirst.ApplySchema(ctx, node))
	require.Equal(t, chains[:1], driftedFirst.enabled)

	// A chain with no generators, or whose generator serves another chain's schema, is disabled with
	// nothing stored, and Ethereum is still served.
	refusing := httptest.NewServer(schemaHandler(t, chain.EthereumMainnet))
	t.Cleanup(refusing.Close)
	for _, c := range []struct {
		config chain.Config
		reason string
	}{
		{config: chain.Config{Prefix: third}, reason: server.ReasonNoGeneratorConfigured},
		{config: chain.Config{Prefix: third, Generators: []chain.Generator{{URL: refusing.URL}}}, reason: server.ReasonSchemaRefused},
	} {
		withThird := &chainsApplier{schemaCfg: schemaCfg, chains: []chain.Config{chains[0], chains[1], c.config}}
		require.NoError(t, withThird.ApplySchema(ctx, node))
		require.Equal(t, chains[:1], withThird.enabled)
		require.Equal(t, map[string]string{other: server.ReasonSchemaDrift, third: c.reason}, withThird.reasons)
		_, err = node.DB.GetCollectionByName(ctx, chain.EVM(third).Block.Name)
		require.Error(t, err)
	}

	// A chain with only some of its tables stored is disabled, and its stored table is unsubscribed.
	partial := chain.EVM(third).Block.Name
	_, err = node.DB.AddCollection(ctx, "type "+partial+" { number: Int }")
	require.NoError(t, err)
	require.NoError(t, node.DB.AddP2PCollections(ctx, []string{partial}))
	withPartial := &chainsApplier{schemaCfg: schemaCfg, chains: []chain.Config{chains[0], chains[1], {Prefix: third}}}
	require.NoError(t, withPartial.ApplySchema(ctx, node))
	require.Equal(t, chains[:1], withPartial.enabled)
	require.Equal(t, map[string]string{other: server.ReasonSchemaDrift, third: server.ReasonTablesPartiallyStored}, withPartial.reasons)
	got, err = node.DB.ListP2PCollections(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, chain.EVM(chain.EthereumMainnet).Subscribed(), got)

	// A chain with only some of its tables stored can still be removed.
	_, err = node.DB.AddCollection(ctx, "type "+chain.EVM(third).BlockSignature.Name+" { number: Int }")
	require.NoError(t, err)
	require.NoError(t, applier.ApplySchema(ctx, node))
	require.Equal(t, []string{third}, applier.removed)

	// When drift disables every chain, the host does not start, and the error names each chain.
	_, err = node.DB.AddCollection(ctx, "type "+chain.EthereumMainnet+"__Extra { a: Int }")
	require.NoError(t, err)
	err = applier.ApplySchema(ctx, node)
	require.ErrorIs(t, err, errNoChainEnabled)
	require.ErrorIs(t, err, localschema.ErrSchemaDrift)
	require.ErrorContains(t, err, chain.EthereumMainnet)
	require.ErrorContains(t, err, other)
}

// A tokens-mode chain stores and subscribes to exactly its tables, leaving out the tables of other
// contracts its generator serves, and removing it from the config unsubscribes all of them.
func TestChainsApplierTokensMode(t *testing.T) {
	const (
		prefix        = "Bsc__Mainnet"
		otherContract = prefix + "__ContractLog_55d398326f99059ff775485246999027b3197955"
	)
	ctx := context.Background()
	schemaCfg := config.SchemaConfig{IndexerSchemaEndpoint: config.DefaultIndexerSchemaEndpoint, HTTPClientTimeoutSecs: 1}
	sdl, err := os.ReadFile(filepath.Join("..", "schema", "testdata", "generator_tokens_schema.graphql"))
	require.NoError(t, err)
	served := string(sdl) + "\ntype " + otherContract + " {\n    blockNumber: Int @index\n    address: String\n}\n"
	tokensGenerator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(localschema.Response{Network: prefix, Schema: served}))
	}))
	t.Cleanup(tokensGenerator.Close)
	ethereumGenerator := httptest.NewServer(schemaHandler(t, chain.EthereumMainnet))
	t.Cleanup(ethereumGenerator.Close)
	ethereum := chain.Config{Prefix: chain.EthereumMainnet, Generators: []chain.Generator{{URL: ethereumGenerator.URL}}}
	tokens := chain.Config{
		Prefix:     prefix,
		Mode:       chain.ModeTokens,
		Contracts:  []string{"0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d"},
		Generators: []chain.Generator{{URL: tokensGenerator.URL}},
	}

	applier := &chainsApplier{schemaCfg: schemaCfg, chains: []chain.Config{ethereum, tokens}}
	node, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, applier)
	require.NoError(t, err)
	defer func() { _ = node.Close(ctx) }()
	require.Equal(t, []chain.Config{ethereum, tokens}, applier.enabled)
	collections := tokens.Collections()
	for _, col := range append(collections.Generated(), collections.AttestationRecord) {
		_, err := node.DB.GetCollectionByName(ctx, col.Name)
		require.NoError(t, err, col.Name)
	}
	_, err = node.DB.GetCollectionByName(ctx, otherContract)
	require.Error(t, err)
	got, err := node.DB.ListP2PCollections(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, append(ethereum.Collections().Subscribed(), collections.Subscribed()...), got)

	// A restart compares the stored tables with the served schema, which still has the other table.
	require.NoError(t, applier.ApplySchema(ctx, node))
	require.Equal(t, []chain.Config{ethereum, tokens}, applier.enabled)

	withoutTokens := &chainsApplier{schemaCfg: schemaCfg, chains: []chain.Config{ethereum}}
	require.NoError(t, withoutTokens.ApplySchema(ctx, node))
	require.Equal(t, []string{prefix}, withoutTokens.removed)
	got, err = node.DB.ListP2PCollections(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, ethereum.Collections().Subscribed(), got)
}

// When no generator answers for the only chain, StartDefraInstance returns errNoChainEnabled.
func TestChainsApplierNoChainAnswered(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(down.Close)
	schemaCfg := config.SchemaConfig{IndexerSchemaEndpoint: config.DefaultIndexerSchemaEndpoint, HTTPClientTimeoutSecs: 1}
	applier := &chainsApplier{schemaCfg: schemaCfg, chains: []chain.Config{{Prefix: chain.EthereumMainnet, Generators: []chain.Generator{{URL: down.URL}}}}}

	_, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, applier)
	require.ErrorIs(t, err, errNoChainEnabled)
	require.ErrorIs(t, err, errNoGeneratorAnswered)
}
