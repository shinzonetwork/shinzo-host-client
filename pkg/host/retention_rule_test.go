package host

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sourcenetwork/defradb/client"
	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/pruner"
)

// namedCollection carries only what ResolveCollections reads: its identity and its fields.
type namedCollection struct {
	client.Collection

	name   string
	id     string
	fields []client.CollectionFieldDescription
}

func (c namedCollection) Name() string         { return c.name }
func (c namedCollection) CollectionID() string { return c.id }
func (c namedCollection) Version() client.CollectionVersion {
	return client.CollectionVersion{Fields: c.fields}
}

func intField(name string) []client.CollectionFieldDescription {
	return []client.CollectionFieldDescription{{Name: name, Kind: client.FieldKind_NILLABLE_INT}}
}

func TestRetentionRuleFloor(t *testing.T) {
	cols := []client.Collection{
		namedCollection{name: testCollections.Block.Name, id: "block-id", fields: intField("number")},
		namedCollection{name: testCollections.Log.Name, id: "log-id", fields: intField("blockNumber")},
		namedCollection{name: testCollections.SnapshotSignature.Name, id: "snapshot-id", fields: intField("endBlock")},
		namedCollection{name: testCollections.Transaction.Name, id: "tx-id", fields: intField("blocknumber")},
		namedCollection{name: chain.EthereumMainnet, id: "chain-id"},
	}
	tests := []struct {
		name         string
		cutoff       int64
		unresolved   bool
		collectionID string
		wantField    string
		wantOK       bool
	}{
		{name: "block", cutoff: 100, collectionID: "block-id", wantField: "number", wantOK: true},
		{name: "dependent", cutoff: 100, collectionID: "log-id", wantField: "blockNumber", wantOK: true},
		{name: "dependent on its own height field", cutoff: 100, collectionID: "snapshot-id", wantField: "endBlock", wantOK: true},
		{name: "no cutoff set", collectionID: "block-id", wantField: "number", wantOK: true},
		{name: "collections not resolved", cutoff: 100, unresolved: true, collectionID: "block-id"},
		{name: "collection not pruned", cutoff: 100, collectionID: "chain-id"},
		{name: "height field missing", cutoff: 100, collectionID: "tx-id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cutoff := &pruner.Cutoff{}
			r := NewRetentionRule([]prunedChain{{collections: pruner.CollectionConfigFor(testCollections), cutoff: cutoff}})
			if !tt.unresolved {
				require.Equal(t,
					[]string{testCollections.Block.Name, testCollections.Log.Name, testCollections.SnapshotSignature.Name},
					r.ResolveCollections(cols), "Chain is not pruned, and Transaction has no blockNumber field")
			}
			cutoff.Raise(tt.cutoff)

			field, floor, ok := r.RetentionFloor(tt.collectionID)

			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantField, field)
			if tt.wantOK {
				require.Equal(t, tt.cutoff, floor)
			}
		})
	}
}

// Each configured chain's pruner publishes its own cutoff to the retention rule, a drift-disabled
// chain's included, and Close stops every pruner. With max_blocks 2, blocks 1-10 give a cutoff of 8
// and blocks 1-20 a cutoff of 18.
func TestStartHostingPrunesEachChainAgainstItsOwnCutoff(t *testing.T) {
	const other = "Testchain__Devnet"
	ctx := context.Background()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	require.NoError(t, listener.Close())

	cfg := *DefaultConfig
	cfg.DefraDB.Store.Path = t.TempDir()
	cfg.DefraDB.URL = "127.0.0.1:0"
	cfg.DefraDB.P2P.ListenAddr = "/ip4/127.0.0.1/tcp/0"
	cfg.HostConfig.HealthServerPort = addr.Port
	cfg.Pruner = pruner.Config{Enabled: true, MaxBlocks: 2, IntervalSeconds: 1, PruneHistory: true}
	cfg.Chains = nil
	for _, prefix := range []string{chain.EthereumMainnet, other} {
		generator := httptest.NewServer(schemaHandler(t, prefix))
		t.Cleanup(generator.Close)
		cfg.Chains = append(cfg.Chains, chain.Config{Prefix: prefix, Generators: []chain.Generator{{URL: generator.URL}}})
	}

	h, err := StartHosting(&cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, h.Close(closeCtx))
	})

	blocks := map[string]int{chain.EthereumMainnet: 10, other: 20}
	blockTables := make(map[string]string, len(blocks))
	for prefix, n := range blocks {
		col, err := h.DefraNode.DB.GetCollectionByName(ctx, chain.EVM(prefix).Block.Name)
		require.NoError(t, err)
		for i := 1; i <= n; i++ {
			doc, err := client.NewDocFromMap(ctx, map[string]any{"number": i, "hash": fmt.Sprintf("h%d", i)}, col.Version())
			require.NoError(t, err)
			require.NoError(t, col.AddDocument(ctx, doc))
		}
		blockTables[prefix] = col.CollectionID()
	}
	want := map[string]int64{chain.EthereumMainnet: 8, other: 18}
	requireFloors := func(h *Host) {
		t.Helper()
		require.Eventually(t, func() bool {
			for prefix, floor := range want {
				field, got, ok := h.DefraNode.RetentionRule.RetentionFloor(blockTables[prefix])
				if !ok || field != "number" || got != floor {
					return false
				}
			}
			return true
		}, 10*time.Second, 50*time.Millisecond)
	}
	requireFloors(h)

	// The other chain's stored tables now differ from what its generator serves.
	_, err = h.DefraNode.DB.AddCollection(ctx, "type "+other+"__Extra { a: Int }")
	require.NoError(t, err)
	pruners := h.pruners
	require.Len(t, pruners, 2)
	closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, h.Close(closeCtx))
	for _, p := range pruners {
		require.False(t, p.GetMetrics().IsRunning)
	}

	// After a restart the other chain is disabled, and its pruner sets its cutoff again.
	h, err = StartHosting(&cfg)
	require.NoError(t, err)
	requireFloors(h)
}
