package pruner

import (
	"context"
	"fmt"
	"testing"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

const (
	blockCollection    = "Ethereum__Mainnet__Block"
	logCollection      = "Ethereum__Mainnet__Log"
	txCollection       = "Ethereum__Mainnet__Transaction"
	aleCollection      = "Ethereum__Mainnet__AccessListEntry"
	attRecCollection   = "Ethereum__Mainnet__AttestationRecord"
	snapshotCollection = "Ethereum__Mainnet__SnapshotSignature"
)

// heightTestSchema covers the cases the sweep tells apart: a block collection with its own height
// field, dependents on blockNumber, one on a differently named field, one unindexed, and one with
// no height field at all.
const heightTestSchema = `
type Ethereum__Mainnet__Block {
	number: Int @index
	hash: String
}
type Ethereum__Mainnet__Log {
	blockNumber: Int @index
	address: String
}
type Ethereum__Mainnet__Transaction {
	blockNumber: Int @index
	hash: String
}
type Ethereum__Mainnet__SnapshotSignature {
	endBlock: Int @index
	merkleRoot: String
}
type Ethereum__Mainnet__AccessListEntry {
	blockNumber: Int
	address: String
}
type Ethereum__Mainnet__AttestationRecord {
	attested_doc: String
}
`

func heightTestCollections() CollectionConfig {
	return CollectionConfig{
		Block: CollectionHeight{Name: blockCollection, HeightField: blockHeightField},
		Dependents: []CollectionHeight{
			{Name: aleCollection, HeightField: dependentHeightField},
			{Name: logCollection, HeightField: dependentHeightField},
			{Name: txCollection, HeightField: dependentHeightField},
			{Name: attRecCollection, HeightField: dependentHeightField},
			{Name: snapshotCollection, HeightField: snapshotHeightField},
		},
	}
}

// newHeightTestPruner starts a DefraDB node on a temp store and returns a pruner wired to it.
func newHeightTestPruner(t *testing.T, cfg *Config) (*Pruner, *node.Node) {
	t.Helper()
	ctx := context.Background()

	nb := options.Node().SetDisableAPI(true).SetDisableP2P(true)
	nb.Store().SetPath(t.TempDir())

	n, err := node.New(ctx, nb)
	require.NoError(t, err)
	require.NoError(t, n.Start(ctx))
	t.Cleanup(func() { _ = n.Close(ctx) })

	_, err = n.DB.AddCollection(ctx, heightTestSchema)
	require.NoError(t, err)

	cfg.SetDefaults()
	p := NewPruner(cfg, n, heightTestCollections())
	p.heightPrunable = p.resolveHeightPrunable(ctx)
	return p, n
}

func addHeightDoc(t *testing.T, n *node.Node, collection string, fields map[string]any) {
	t.Helper()
	ctx := context.Background()
	col, err := n.DB.GetCollectionByName(ctx, collection)
	require.NoError(t, err)
	doc, err := client.NewDocFromMap(ctx, fields, col.Version())
	require.NoError(t, err)
	require.NoError(t, col.AddDocument(ctx, doc))
}

// blockNumbers returns fieldName across a collection, so a test can assert which documents
// survived rather than only how many.
func blockNumbers(t *testing.T, n *node.Node, collection, fieldName string) []int64 {
	t.Helper()
	res := n.DB.ExecRequest(context.Background(),
		fmt.Sprintf("query { %s(order: {%s: ASC}) { %s } }", collection, fieldName, fieldName))
	require.Empty(t, res.GQL.Errors)

	data, ok := res.GQL.Data.(map[string]any)
	require.True(t, ok)

	var out []int64
	switch docs := data[collection].(type) {
	case []map[string]any:
		for _, d := range docs {
			n, parsed := parseBlockNumber(d[fieldName])
			require.True(t, parsed)
			out = append(out, n)
		}
	case []any:
		for _, raw := range docs {
			d, ok := raw.(map[string]any)
			require.True(t, ok)
			n, parsed := parseBlockNumber(d[fieldName])
			require.True(t, parsed)
			out = append(out, n)
		}
	}
	return out
}

func countHeightDocs(t *testing.T, n *node.Node, collection string) int {
	t.Helper()
	res := n.DB.ExecRequest(context.Background(), fmt.Sprintf("query { %s { _docID } }", collection))
	require.Empty(t, res.GQL.Errors)
	data, ok := res.GQL.Data.(map[string]any)
	require.True(t, ok)
	switch docs := data[collection].(type) {
	case []map[string]any:
		return len(docs)
	case []any:
		return len(docs)
	}
	return 0
}

// seedHeightBlocks writes one Block and one Log per block number in [from, to].
func seedHeightBlocks(t *testing.T, n *node.Node, from, to int) {
	t.Helper()
	for i := from; i <= to; i++ {
		addHeightDoc(t, n, blockCollection, map[string]any{blockHeightField: i, "hash": fmt.Sprintf("h%d", i)})
		addHeightDoc(t, n, logCollection, map[string]any{dependentHeightField: i, "address": fmt.Sprintf("a%d", i)})
	}
}

// The block collection and its dependents are both trimmed to the retention window.
func TestPruneTrimsBlocksAndDependentsToTheWindow(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})

	seedHeightBlocks(t, n, 1, 20)

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, logCollection, dependentHeightField))
}

// A dependent collection can hold blocks the block collection has already dropped.
func TestPruneRemovesDependentTailBelowTheWindow(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})

	for i := 16; i <= 20; i++ {
		addHeightDoc(t, n, blockCollection, map[string]any{blockHeightField: i, "hash": fmt.Sprintf("h%d", i)})
	}
	for i := 1; i <= 20; i++ {
		addHeightDoc(t, n, logCollection, map[string]any{dependentHeightField: i, "address": fmt.Sprintf("a%d", i)})
	}

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, logCollection, dependentHeightField))
}

// Block zero is a real block number, not an empty collection.
func TestPruneHandlesBlockZero(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})

	seedHeightBlocks(t, n, 0, 20)

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, logCollection, dependentHeightField))
}

// The sweep stops once the cycle's budget is spent, however far below the window the store is.
func TestHeightSweepStopsAtTheCycleBudget(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{
		Enabled: true, MaxBlocks: 5, MaxDocsPerCycle: 3,
	})

	seedHeightBlocks(t, n, 1, 20)

	require.NoError(t, p.runPrune(context.Background()))

	require.Len(t, blockNumbers(t, n, logCollection, dependentHeightField), 17)
	require.Len(t, blockNumbers(t, n, blockCollection, blockHeightField), 20)
}

// The budget is spent across collections in order: a collection that needs less than the remainder
// leaves the rest for the next one.
func TestHeightSweepBudgetIsSharedAcrossCollections(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{
		Enabled: true, MaxBlocks: 5, MaxDocsPerCycle: 5,
	})

	for i := 1; i <= 20; i++ {
		addHeightDoc(t, n, blockCollection, map[string]any{blockHeightField: i, "hash": fmt.Sprintf("h%d", i)})
		addHeightDoc(t, n, txCollection, map[string]any{dependentHeightField: i, "hash": fmt.Sprintf("t%d", i)})
	}
	// Only two Log rows sit below the cutoff of 15, so Log cannot use the whole budget.
	for _, i := range []int{14, 15} {
		addHeightDoc(t, n, logCollection, map[string]any{dependentHeightField: i, "address": fmt.Sprintf("a%d", i)})
	}

	require.NoError(t, p.runPrune(context.Background()))

	require.Empty(t, blockNumbers(t, n, logCollection, dependentHeightField))
	require.Equal(t, []int64{4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		blockNumbers(t, n, txCollection, dependentHeightField))
	require.Len(t, blockNumbers(t, n, blockCollection, blockHeightField), 20)
}

// Zero is unlimited to the query planner, so a spent budget must remove nothing rather than
// everything.
func TestPurgeCollectionBelowRemovesNothingWithoutBudget(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
	seedHeightBlocks(t, n, 1, 20)

	purged, err := p.purgeCollectionBelow(context.Background(), logCollection, dependentHeightField, 15, 0)
	require.NoError(t, err)
	require.Zero(t, purged)
	require.Len(t, blockNumbers(t, n, logCollection, dependentHeightField), 20)
}

// A collection with no height field cannot be ordered by height, so it is left alone.
func TestHeightPruneSkipsCollectionWithoutHeightField(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})

	require.NotContains(t, p.heightPrunable, CollectionHeight{Name: attRecCollection, HeightField: dependentHeightField})

	seedHeightBlocks(t, n, 1, 20)
	for i := 1; i <= 3; i++ {
		addHeightDoc(t, n, attRecCollection, map[string]any{"attested_doc": fmt.Sprintf("d%d", i)})
	}

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, 3, countHeightDocs(t, n, attRecCollection))
}

// A dependent whose height field carries no index is skipped, so its documents survive the sweep.
func TestHeightPruneSkipsUnindexedHeightField(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})

	require.Equal(t, []CollectionHeight{
		{Name: logCollection, HeightField: dependentHeightField},
		{Name: txCollection, HeightField: dependentHeightField},
		{Name: snapshotCollection, HeightField: snapshotHeightField},
	}, p.heightPrunable)

	seedHeightBlocks(t, n, 1, 20)
	for i := 1; i <= 20; i++ {
		addHeightDoc(t, n, aleCollection, map[string]any{dependentHeightField: i, "address": fmt.Sprintf("a%d", i)})
	}

	require.NoError(t, p.runPrune(context.Background()))

	require.Len(t, blockNumbers(t, n, aleCollection, dependentHeightField), 20)
}

// A snapshot is retained on the newest block it covers, not on a blockNumber field. The sweep
// orders on whichever field the collection declares.
func TestHeightPruneUsesTheCollectionsOwnHeightField(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})

	seedHeightBlocks(t, n, 1, 20)
	// The cutoff is 15, so the first two snapshots are below the window and the last two are not.
	for _, end := range []int{5, 10, 16, 20} {
		addHeightDoc(t, n, snapshotCollection,
			map[string]any{snapshotHeightField: end, "merkleRoot": fmt.Sprintf("r%d", end)})
	}

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, []int64{16, 20}, blockNumbers(t, n, snapshotCollection, snapshotHeightField))
}

// A node bootstrapped with historical blocks keeps them.
func TestRetainHistorySuppressesHeightPrune(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
	p.SetRetainHistory(true)

	seedHeightBlocks(t, n, 1, 20)

	require.NoError(t, p.runPrune(context.Background()))

	require.Len(t, blockNumbers(t, n, blockCollection, blockHeightField), 20)
	require.Len(t, blockNumbers(t, n, logCollection, dependentHeightField), 20)
}
