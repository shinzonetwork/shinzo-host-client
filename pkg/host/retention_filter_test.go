package host

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/sourcenetwork/defradb/client"
	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/pruner"
)

// namedCollection carries only the identity ResolveCollections reads.
type namedCollection struct {
	client.Collection

	name string
	id   string
}

func (c namedCollection) Name() string         { return c.name }
func (c namedCollection) CollectionID() string { return c.id }

func TestRetentionFilterAllowReplication(t *testing.T) {
	cols := []client.Collection{
		namedCollection{name: constants.CollectionBlock, id: "block-id"},
		namedCollection{name: constants.CollectionLog, id: "log-id"},
		namedCollection{name: constants.CollectionSnapshotSignature, id: "snapshot-id"},
		namedCollection{name: constants.CollectionChain, id: "chain-id"},
	}
	tests := []struct {
		name         string
		cutoff       int64
		unresolved   bool
		collectionID string
		fields       map[string]any
		want         bool
		counted      string
	}{
		{
			name:         "block at the cutoff",
			cutoff:       100,
			collectionID: "block-id",
			fields:       map[string]any{"number": uint64(100)},
			want:         false,
			counted:      "rejected",
		},
		{
			name:         "block above the cutoff",
			cutoff:       100,
			collectionID: "block-id",
			fields:       map[string]any{"number": uint64(101)},
			want:         true,
			counted:      "allowed",
		},
		{
			name:         "dependent below the cutoff",
			cutoff:       100,
			collectionID: "log-id",
			fields:       map[string]any{"blockNumber": uint64(99)},
			want:         false,
			counted:      "rejected",
		},
		{
			name:         "dependent on its own height field",
			cutoff:       100,
			collectionID: "snapshot-id",
			fields:       map[string]any{"endBlock": uint64(100)},
			want:         false,
			counted:      "rejected",
		},
		{
			name:         "no cutoff set",
			collectionID: "block-id",
			fields:       map[string]any{"number": uint64(0)},
			want:         true,
			counted:      "cutoffUnset",
		},
		{
			name:         "collections not resolved",
			cutoff:       100,
			unresolved:   true,
			collectionID: "block-id",
			fields:       map[string]any{"number": uint64(1)},
			want:         true,
			counted:      "unmapped",
		},
		{
			name:         "collection not pruned",
			cutoff:       100,
			collectionID: "chain-id",
			fields:       map[string]any{"blockNumber": uint64(1)},
			want:         true,
			counted:      "unmapped",
		},
		{
			name:         "height not among the fields",
			cutoff:       100,
			collectionID: "log-id",
			fields:       map[string]any{"address": "0xabc"},
			want:         true,
			counted:      "noHeight",
		},
		{
			name:         "no field values before the fetch",
			cutoff:       100,
			collectionID: "log-id",
			fields:       map[string]any{},
			want:         true,
			counted:      "beforeFetch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cutoff := &pruner.Cutoff{}
			f := NewRetentionFilter(pruner.DefaultCollectionConfig(), cutoff)
			if !tt.unresolved {
				require.Equal(t,
					[]string{constants.CollectionBlock, constants.CollectionLog, constants.CollectionSnapshotSignature},
					f.ResolveCollections(cols), "Chain is not pruned")
			}
			cutoff.Raise(tt.cutoff)

			require.Equal(t, tt.want, f.AllowReplication(context.Background(), tt.collectionID, "doc", tt.fields))

			stats := f.stats()
			for outcome, count := range map[string]int64{
				"beforeFetch": stats.beforeFetch,
				"cutoffUnset": stats.cutoffUnset,
				"unmapped":    stats.unmapped,
				"noHeight":    stats.noHeight,
				"rejected":    stats.rejected,
				"allowed":     stats.allowed,
			} {
				want := int64(0)
				if outcome == tt.counted {
					want = 1
				}
				require.Equal(t, want, count, outcome)
			}
		})
	}
}

// fixedFilter answers every document the same way.
type fixedFilter bool

func (f fixedFilter) AllowReplication(context.Context, string, string, map[string]any) bool {
	return bool(f)
}

func TestReplicationFiltersAllowReplication(t *testing.T) {
	tests := []struct {
		name    string
		filters replicationFilters
		want    bool
	}{
		{name: "every filter allows", filters: replicationFilters{fixedFilter(true), fixedFilter(true)}, want: true},
		{name: "last filter rejects", filters: replicationFilters{fixedFilter(true), fixedFilter(false)}, want: false},
		{name: "first filter rejects", filters: replicationFilters{fixedFilter(false), fixedFilter(true)}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.filters.AllowReplication(context.Background(), "col", "doc", nil))
		})
	}
}

// A host with the pruner enabled installs the retention filter on its node and the pruner publishes
// its cutoff to that filter. With blocks 1-10 and max_blocks 2, the cutoff is 10 - 2 = 8.
func TestStartHostingSharesThePrunerCutoffWithTheFilter(t *testing.T) {
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

	col, err := h.DefraNode.DB.GetCollectionByName(ctx, constants.CollectionBlock)
	require.NoError(t, err)
	for i := 1; i <= 10; i++ {
		doc, err := client.NewDocFromMap(ctx, map[string]any{"number": i, "hash": fmt.Sprintf("h%d", i)}, col.Version())
		require.NoError(t, err)
		require.NoError(t, col.AddDocument(ctx, doc))
	}

	require.Eventually(t, func() bool { return h.retentionFilter.stats().cutoff == 8 }, 10*time.Second, 50*time.Millisecond)

	filter := h.DefraNode.ReplicationFilter
	require.NotNil(t, filter)
	require.False(t, filter.AllowReplication(ctx, col.CollectionID(), "doc", map[string]any{"number": uint64(8)}))
	require.True(t, filter.AllowReplication(ctx, col.CollectionID(), "doc", map[string]any{"number": uint64(9)}))
}
