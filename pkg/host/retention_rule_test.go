package host

import (
	"context"
	"fmt"
	"net"
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
			r := NewRetentionRule(pruner.CollectionConfigFor(chain.EVM(chain.EthereumMainnet)), cutoff)
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

// A host with the pruner enabled installs the retention rule on its node and the pruner publishes its
// cutoff to that rule. With blocks 1-10 and max_blocks 2, the cutoff is 10 - 2 = 8.
func TestStartHostingSharesThePrunerCutoffWithTheRule(t *testing.T) {
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

	h, err := StartHosting(&cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, h.Close(closeCtx))
	})

	col, err := h.DefraNode.DB.GetCollectionByName(ctx, testCollections.Block.Name)
	require.NoError(t, err)
	for i := 1; i <= 10; i++ {
		doc, err := client.NewDocFromMap(ctx, map[string]any{"number": i, "hash": fmt.Sprintf("h%d", i)}, col.Version())
		require.NoError(t, err)
		require.NoError(t, col.AddDocument(ctx, doc))
	}

	rule := h.DefraNode.RetentionRule
	require.NotNil(t, rule)
	require.Eventually(t, func() bool {
		field, floor, ok := rule.RetentionFloor(col.CollectionID())
		return ok && field == "number" && floor == 8
	}, 10*time.Second, 50*time.Millisecond)
}
