package pruner

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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
// field, dependents on blockNumber, one on a differently named field, and one unindexed.
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
	blockNumber: Int @index
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
	p := NewPruner(cfg, n, &Cutoff{}, heightTestCollections())
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

// seedLogs writes perHeight Logs at every block number in [from, to].
func seedLogs(t *testing.T, n *node.Node, from, to, perHeight int) {
	t.Helper()
	for i := from; i <= to; i++ {
		for j := range perHeight {
			addHeightDoc(t, n, logCollection, map[string]any{dependentHeightField: i, "address": fmt.Sprintf("a%d-%d", i, j)})
		}
	}
}

// heights returns the integers in [from, to].
func heights(from, to int64) []int64 {
	out := make([]int64, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

// With blocks 1-20 and a window of 5, the cutoff is 15.
func TestPruneTrimsBlocksAndDependentsToTheCutoff(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
	seedHeightBlocks(t, n, 1, 20)

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, int64(15), p.cutoff.Load())
	require.Equal(t, heights(16, 20), blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, heights(16, 20), blockNumbers(t, n, logCollection, dependentHeightField))
}

// A dependent collection can hold blocks the block collection has already dropped.
func TestPruneRemovesDependentTailBelowTheCutoff(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
	for i := 16; i <= 20; i++ {
		addHeightDoc(t, n, blockCollection, map[string]any{blockHeightField: i, "hash": fmt.Sprintf("h%d", i)})
	}
	seedLogs(t, n, 1, 20, 1)

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, heights(16, 20), blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, heights(16, 20), blockNumbers(t, n, logCollection, dependentHeightField))
}

// Block zero is a valid height, so each collection's sweep starts at 0.
func TestPruneHandlesBlockZero(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
	seedHeightBlocks(t, n, 0, 20)

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, heights(16, 20), blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, heights(16, 20), blockNumbers(t, n, logCollection, dependentHeightField))
}

// A cycle deletes the lowest heights first across the collections and stops at its budget, so blocks
// go every cycle however many dependents sit below the cutoff. The next cycle carries on from the
// height the last one stopped in.
func TestPruneDeletesBlocksWithinTheBudgetWhenDependentsExceedIt(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, MaxDocsPerCycle: 25})
	for i := 1; i <= 20; i++ {
		addHeightDoc(t, n, blockCollection, map[string]any{blockHeightField: i, "hash": fmt.Sprintf("h%d", i)})
	}
	seedLogs(t, n, 1, 20, 10)

	// 25 documents: heights 1 and 2 (10 Logs and a block each), then 3 Logs at height 3.
	require.NoError(t, p.runPrune(context.Background()))
	require.Equal(t, heights(3, 20), blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, 200-23, countHeightDocs(t, n, logCollection))
	require.Equal(t, int64(25), p.GetMetrics().TotalDocsSubmitted)
	require.Equal(t, int64(2), p.GetMetrics().TotalBlocksPruned)

	// The other 7 Logs at 3 and block 3, height 4 in full, then 6 Logs at 5.
	require.NoError(t, p.runPrune(context.Background()))
	require.Equal(t, heights(5, 20), blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, 200-23-23, countHeightDocs(t, n, logCollection))
}

// A block is deleted only once the dependents at its height are gone: 12 documents take height 1
// (10 Logs and its block) and one Log at height 2, so block 2 stays.
func TestPruneKeepsABlockUntilTheDependentsAtItsHeightAreGone(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, MaxDocsPerCycle: 12})
	for i := 1; i <= 20; i++ {
		addHeightDoc(t, n, blockCollection, map[string]any{blockHeightField: i, "hash": fmt.Sprintf("h%d", i)})
	}
	seedLogs(t, n, 1, 20, 10)

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, heights(2, 20), blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, 200-11, countHeightDocs(t, n, logCollection))
}

// Documents with no height are never selected, and more of them than a page holds cannot fill a page
// and hold the sweep back.
func TestPruneIsNotHeldBackByDocumentsWithNoHeight(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, MaxDocsPerCycle: 3})
	seedHeightBlocks(t, n, 1, 20)
	for i := range purgeBatchSize + 1 {
		addHeightDoc(t, n, logCollection, map[string]any{"address": fmt.Sprintf("no-height-%d", i)})
	}

	for range 15 {
		require.NoError(t, p.runPrune(context.Background()))
	}

	require.Equal(t, heights(16, 20), blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, 5+purgeBatchSize+1, countHeightDocs(t, n, logCollection))
}

// A collection with more documents due than one page is read again within the same cycle.
func TestPruneReadsMoreThanOnePagePerCycle(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
	seedHeightBlocks(t, n, 1, 20)
	seedLogs(t, n, 1, 11, 100)

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, heights(16, 20), blockNumbers(t, n, blockCollection, blockHeightField))
	require.Equal(t, heights(16, 20), blockNumbers(t, n, logCollection, dependentHeightField))
}

// Without any block held there is no cutoff, and nothing goes.
func TestPruneHasNoCutoffWithoutBlocks(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
	seedLogs(t, n, 50, 50, 1)

	require.NoError(t, p.runPrune(context.Background()))

	require.Zero(t, p.cutoff.Load())
	require.Equal(t, []int64{50}, blockNumbers(t, n, logCollection, dependentHeightField))
}

// Once blocks up to 120 arrive, the next cycle raises the cutoff in one step to 115: the highest
// block less 5.
func TestPruneCutoffFollowsTheChain(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
	seedHeightBlocks(t, n, 1, 20)
	require.NoError(t, p.runPrune(context.Background()))
	require.Equal(t, int64(15), p.cutoff.Load())

	seedHeightBlocks(t, n, 21, 120)
	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, int64(115), p.cutoff.Load())
	require.Equal(t, heights(116, 120), blockNumbers(t, n, blockCollection, blockHeightField))
}

// The cutoff is published before the first delete, so the filter rejects what the sweep removes.
func TestPrunePublishesTheCutoffBeforeDeleting(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
	seedHeightBlocks(t, n, 1, 20)
	var seen []int64
	p.purgeDocs = func(context.Context, []client.DocID) error {
		seen = append(seen, p.cutoff.Load())
		return nil
	}

	require.NoError(t, p.runPrune(context.Background()))

	require.NotEmpty(t, seen)
	for _, cutoff := range seen {
		require.Equal(t, int64(15), cutoff)
	}
}

// The first cycle runs when the pruner starts, so the cutoff is published without waiting an interval.
func TestPrunePublishesTheCutoffWhenItStarts(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, IntervalSeconds: 3600})
	seedHeightBlocks(t, n, 1, 20)

	require.NoError(t, p.Start(context.Background()))
	t.Cleanup(func() { p.Stop(context.Background()) })

	require.Eventually(t, func() bool { return p.cutoff.Load() == 15 }, 5*time.Second, 10*time.Millisecond)
}

// A dependent that cannot be read or deleted does not hold the blocks back: they still go up to the
// cutoff, and the dependent is left for a later cycle.
func TestPruneGoesPastADependentThatFails(t *testing.T) {
	t.Run("read fails", func(t *testing.T) {
		p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
		seedHeightBlocks(t, n, 1, 20)
		p.heightPrunable = []CollectionHeight{{Name: logCollection, HeightField: "noSuchField"}}

		require.NoError(t, p.runPrune(context.Background()))

		require.Equal(t, heights(16, 20), blockNumbers(t, n, blockCollection, blockHeightField))
		require.Len(t, blockNumbers(t, n, logCollection, dependentHeightField), 20)
	})

	t.Run("delete fails", func(t *testing.T) {
		p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
		seedHeightBlocks(t, n, 1, 20)
		var sizes []int
		p.purgeDocs = func(_ context.Context, ids []client.DocID) error {
			sizes = append(sizes, len(ids))
			if len(sizes) == 1 {
				return errors.New("purge failed")
			}
			return nil
		}

		require.NoError(t, p.runPrune(context.Background()))

		// The Log at height 1 fails, then the 15 blocks up to the cutoff go in one batch.
		require.Equal(t, []int{1, 15}, sizes)
	})
}

// The sweep resumes each collection where it got to, so a document that arrives below that point
// waits for the hourly pass from each collection's lowest height.
func TestPruneReachesDocumentsBelowTheSweepOnTheBottomPass(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
	clock := time.Unix(1_700_000_000, 0)
	p.now = func() time.Time { return clock }
	seedHeightBlocks(t, n, 1, 20)
	require.NoError(t, p.runPrune(context.Background()))

	seedLogs(t, n, 3, 3, 1)
	clock = clock.Add(time.Minute)
	require.NoError(t, p.runPrune(context.Background()))
	require.Equal(t, append([]int64{3}, heights(16, 20)...), blockNumbers(t, n, logCollection, dependentHeightField))

	clock = clock.Add(bottomPassInterval)
	require.NoError(t, p.runPrune(context.Background()))
	require.Equal(t, heights(16, 20), blockNumbers(t, n, logCollection, dependentHeightField))
}

// A dependent whose height field carries no index is skipped, so its documents survive the sweep.
func TestPruneSkipsUnindexedHeightField(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})

	require.Equal(t, []CollectionHeight{
		{Name: logCollection, HeightField: dependentHeightField},
		{Name: txCollection, HeightField: dependentHeightField},
		{Name: attRecCollection, HeightField: dependentHeightField},
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
func TestPruneUsesTheCollectionsOwnHeightField(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
	seedHeightBlocks(t, n, 1, 20)
	// The cutoff is 15, so the first two snapshots are at or below it and the last two are not.
	for _, end := range []int{5, 10, 16, 20} {
		addHeightDoc(t, n, snapshotCollection,
			map[string]any{snapshotHeightField: end, "merkleRoot": fmt.Sprintf("r%d", end)})
	}

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, []int64{16, 20}, blockNumbers(t, n, snapshotCollection, snapshotHeightField))
}

// Attestation records go at the same cutoff as the blocks they attest.
func TestPruneSweepsAttestationRecordsAtTheCutoff(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
	seedHeightBlocks(t, n, 1, 20)
	for _, height := range []int{5, 15, 16} {
		addHeightDoc(t, n, attRecCollection,
			map[string]any{dependentHeightField: height, "attested_doc": fmt.Sprintf("d%d", height)})
	}

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, []int64{16}, blockNumbers(t, n, attRecCollection, dependentHeightField))
}

// A node bootstrapped with historical blocks keeps them, and publishes no cutoff.
func TestRetainHistorySuppressesHeightPrune(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5})
	p.SetRetainHistory(true)
	seedHeightBlocks(t, n, 1, 20)

	require.NoError(t, p.runPrune(context.Background()))

	require.Zero(t, p.cutoff.Load())
	require.Len(t, blockNumbers(t, n, blockCollection, blockHeightField), 20)
	require.Len(t, blockNumbers(t, n, logCollection, dependentHeightField), 20)
}
