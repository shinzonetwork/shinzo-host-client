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

// pruneUntilSettled runs cycles until one removes nothing. A cycle advances the window by at most
// max_blocks_per_cycle, so a store well behind the window takes several.
func pruneUntilSettled(t *testing.T, p *Pruner) {
	t.Helper()
	for range 20 {
		before := p.GetMetrics().TotalDocsSubmitted
		require.NoError(t, p.runPrune(context.Background()))
		if p.GetMetrics().TotalDocsSubmitted == before {
			return
		}
	}
	t.Fatal("prune never settled")
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

	pruneUntilSettled(t, p)

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

	pruneUntilSettled(t, p)

	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, logCollection, dependentHeightField))
}

// Block zero is a real block number, not an empty collection.
func TestPruneHandlesBlockZero(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})

	seedHeightBlocks(t, n, 0, 20)

	pruneUntilSettled(t, p)

	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, logCollection, dependentHeightField))
}

// Dependents vastly outnumber blocks, so a cycle that cannot clear them never reaches the block
// collection and the store's block count never falls.
func TestPruneRemovesBlocksEveryCycle(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, MaxBlocksPerCycle: 2})

	// Several dependents per block, so they outnumber blocks the way they do in practice.
	for i := 1; i <= 20; i++ {
		addHeightDoc(t, n, blockCollection, map[string]any{blockHeightField: i, "hash": fmt.Sprintf("h%d", i)})
		for j := range 10 {
			addHeightDoc(t, n, logCollection,
				map[string]any{dependentHeightField: i, "address": fmt.Sprintf("a%d-%d", i, j)})
		}
	}

	for range 3 {
		before := len(blockNumbers(t, n, blockCollection, blockHeightField))
		require.NoError(t, p.runPrune(context.Background()))
		require.Less(t, len(blockNumbers(t, n, blockCollection, blockHeightField)), before,
			"a cycle removed no blocks")
	}
}

// A block whose height cannot be read sorts ahead of the rest and takes one of the cycle's places,
// but it must not stop the cycle finding a cutoff.
func TestPruneAdvancesWhenTheOldestBlockHeightIsUnreadable(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, MaxBlocksPerCycle: 3})

	seedHeightBlocks(t, n, 1, 20)
	addHeightDoc(t, n, blockCollection, map[string]any{"hash": "no-height"})

	require.NoError(t, p.runPrune(context.Background()))

	// The unreadable block takes one of the three places, so the cutoff is the second block and
	// the cycle stays bounded instead of falling back to the retention window.
	require.Equal(t, []int64{3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		blockNumbers(t, n, logCollection, dependentHeightField))
}

// Block numbering is not always contiguous. A cycle counts blocks so that a gap does not slow the
// window down.
func TestPruneAdvancesAcrossAGapInBlockNumbering(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, MaxBlocksPerCycle: 3})

	addHeightDoc(t, n, blockCollection, map[string]any{blockHeightField: 1, "hash": "h1"})
	seedHeightBlocks(t, n, 100, 120)

	require.NoError(t, p.runPrune(context.Background()))

	// Three blocks go: 1, 100 and 101.
	require.Equal(t, int64(102), blockNumbers(t, n, blockCollection, blockHeightField)[0])
}

// A dependent can hold a document older than any block held. The cutoff is counted off the block
// collection, so one stale document cannot hold the window back.
func TestPruneCutoffIgnoresAStaleDependent(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, MaxBlocksPerCycle: 3})

	seedHeightBlocks(t, n, 100, 120)
	addHeightDoc(t, n, txCollection, map[string]any{dependentHeightField: 1, "hash": "stale"})

	require.NoError(t, p.runPrune(context.Background()))

	// The cutoff is the third oldest block, which sits above the stale document, so both go.
	require.Equal(t, int64(103), blockNumbers(t, n, blockCollection, blockHeightField)[0])
	require.Empty(t, blockNumbers(t, n, txCollection, dependentHeightField))
}

// A cycle advances the window by at most max_blocks_per_cycle, so one cycle cannot turn into an
// unbounded purge however far the store is behind.
func TestPruneAdvancesByAtMostMaxBlocksPerCycle(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, MaxBlocksPerCycle: 3})

	seedHeightBlocks(t, n, 1, 20)

	require.NoError(t, p.runPrune(context.Background()))

	// Blocks 1-3 go, so the oldest left is 4 rather than the retention cutoff of 15.
	require.Equal(t, int64(4), blockNumbers(t, n, blockCollection, blockHeightField)[0])
	require.Equal(t, []int64{4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		blockNumbers(t, n, logCollection, dependentHeightField))
}

// Repeated cycles have to settle on the retention window rather than overshoot it.
func TestPruneStopsAtTheRetentionWindow(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, MaxBlocksPerCycle: 4})

	seedHeightBlocks(t, n, 1, 20)

	for range 10 {
		require.NoError(t, p.runPrune(context.Background()))
	}

	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, logCollection, dependentHeightField))
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

// A document with no height sorts first on ASC. Reading that as an empty collection would leave
// everything behind it unpruned.
func TestHeightPruneIgnoresADocumentWithNoHeight(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})

	seedHeightBlocks(t, n, 1, 20)
	addHeightDoc(t, n, logCollection, map[string]any{"address": "no-height"})

	pruneUntilSettled(t, p)

	// Cutoff 15, so logs 16-20 remain, plus the one with no height.
	require.Equal(t, 6, countHeightDocs(t, n, logCollection))
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
