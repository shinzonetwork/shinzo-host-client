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

// EventQueue.Push only accepts collection names it holds an enum for, so tests use the production
// names.
const (
	blockCollection   = "Ethereum__Mainnet__Block"
	logCollection     = "Ethereum__Mainnet__Log"
	txCollection      = "Ethereum__Mainnet__Transaction"
	attRecCollection  = "Ethereum__Mainnet__AttestationRecord"
	blockNumberColumn = "number"
)

// heightTestSchema mirrors the shape the pruner depends on: a block collection with its own number
// field, dependents carrying blockNumber, and a dependent carrying neither.
const heightTestSchema = `
type Ethereum__Mainnet__Block {
	number: Int
	hash: String
}
type Ethereum__Mainnet__Log {
	blockNumber: Int
	address: String
}
type Ethereum__Mainnet__Transaction {
	blockNumber: Int
	hash: String
}
type Ethereum__Mainnet__AttestationRecord {
	attested_doc: String
}
`

func heightTestCollections() CollectionConfig {
	return CollectionConfig{
		BlockCollection:      blockCollection,
		BlockNumberField:     blockNumberColumn,
		DependentCollections: []string{logCollection, txCollection, attRecCollection},
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
			n, err := parseBlockNumber(d[fieldName])
			require.NoError(t, err)
			out = append(out, n)
		}
	case []any:
		for _, raw := range docs {
			d, ok := raw.(map[string]any)
			require.True(t, ok)
			n, err := parseBlockNumber(d[fieldName])
			require.NoError(t, err)
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
		addHeightDoc(t, n, blockCollection, map[string]any{"number": i, "hash": fmt.Sprintf("h%d", i)})
		addHeightDoc(t, n, logCollection, map[string]any{"blockNumber": i, "address": fmt.Sprintf("a%d", i)})
	}
}

// A restart leaves the store holding documents the queue never recorded.
func TestPruneRemovesDocumentsTheQueueNeverSaw(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, DocsPerBlock: 1000})
	p.SetQueue(NewEventQueue(heightTestCollections()))

	seedHeightBlocks(t, n, 1, 20)

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, blockCollection, blockNumberColumn))
	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, logCollection, dependentBlockNumberField))
}

// A dependent collection can hold blocks the block collection has already dropped.
func TestPruneRemovesDependentTailBelowTheWindow(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, DocsPerBlock: 1000})
	p.SetQueue(NewEventQueue(heightTestCollections()))

	for i := 16; i <= 20; i++ {
		addHeightDoc(t, n, blockCollection, map[string]any{"number": i, "hash": fmt.Sprintf("h%d", i)})
	}
	for i := 1; i <= 20; i++ {
		addHeightDoc(t, n, logCollection, map[string]any{"blockNumber": i, "address": fmt.Sprintf("a%d", i)})
	}

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, blockCollection, blockNumberColumn))
	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, logCollection, dependentBlockNumberField))
}

// Block zero is a real block number, not an empty collection.
func TestPruneHandlesBlockZero(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, DocsPerBlock: 1000})
	p.SetQueue(NewEventQueue(heightTestCollections()))

	seedHeightBlocks(t, n, 0, 20)

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, blockCollection, blockNumberColumn))
	require.Equal(t, []int64{16, 17, 18, 19, 20}, blockNumbers(t, n, logCollection, dependentBlockNumberField))
}

// The drain and the height sweep share one budget, so a cycle that spends it draining does not
// then run an unbounded sweep.
func TestQueueDrainAndHeightSweepShareTheCycleBudget(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{
		Enabled: true, MaxBlocks: 5, DocsPerBlock: 1, MaxDocsPerCycle: 4,
	})
	q := NewEventQueue(heightTestCollections())
	p.SetQueue(q)

	seedHeightBlocks(t, n, 1, 20)
	for i := range 9 {
		q.Push(logCollection, testDocID(i))
	}

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, 5, q.Len())
	require.Len(t, blockNumbers(t, n, logCollection, dependentBlockNumberField), 20)
	require.Len(t, blockNumbers(t, n, blockCollection, blockNumberColumn), 20)
}

// The sweep stops once the cycle's budget is spent, however far below the window the store is.
func TestHeightSweepStopsAtTheCycleBudget(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{
		Enabled: true, MaxBlocks: 5, DocsPerBlock: 1000, MaxDocsPerCycle: 3,
	})
	p.SetQueue(NewEventQueue(heightTestCollections()))

	seedHeightBlocks(t, n, 1, 20)

	require.NoError(t, p.runPrune(context.Background()))

	require.Len(t, blockNumbers(t, n, logCollection, dependentBlockNumberField), 17)
	require.Len(t, blockNumbers(t, n, blockCollection, blockNumberColumn), 20)
}

// The budget is spent across collections in order: a collection that needs less than the remainder
// leaves the rest for the next one.
func TestHeightSweepBudgetIsSharedAcrossCollections(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{
		Enabled: true, MaxBlocks: 5, DocsPerBlock: 1000, MaxDocsPerCycle: 5,
	})
	p.SetQueue(NewEventQueue(heightTestCollections()))

	for i := 1; i <= 20; i++ {
		addHeightDoc(t, n, blockCollection, map[string]any{"number": i, "hash": fmt.Sprintf("h%d", i)})
		addHeightDoc(t, n, txCollection, map[string]any{"blockNumber": i, "hash": fmt.Sprintf("t%d", i)})
	}
	// Only two Log rows sit below the cutoff of 15, so Log cannot use the whole budget.
	for _, i := range []int{14, 15} {
		addHeightDoc(t, n, logCollection, map[string]any{"blockNumber": i, "address": fmt.Sprintf("a%d", i)})
	}

	require.NoError(t, p.runPrune(context.Background()))

	require.Empty(t, blockNumbers(t, n, logCollection, dependentBlockNumberField))
	require.Equal(t, []int64{4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		blockNumbers(t, n, txCollection, dependentBlockNumberField))
	require.Len(t, blockNumbers(t, n, blockCollection, blockNumberColumn), 20)
}

// Zero is unlimited to the query planner, so a spent budget must remove nothing rather than
// everything.
func TestPurgeCollectionBelowRemovesNothingWithoutBudget(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, DocsPerBlock: 1000})
	seedHeightBlocks(t, n, 1, 20)

	purged, err := p.purgeCollectionBelow(context.Background(), logCollection, dependentBlockNumberField, 15, 0)
	require.NoError(t, err)
	require.Zero(t, purged)
	require.Len(t, blockNumbers(t, n, logCollection, dependentBlockNumberField), 20)
}

// A collection with no block-number field cannot be ordered by height, so it is left alone.
func TestHeightPruneSkipsCollectionWithoutBlockNumber(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, DocsPerBlock: 1000})
	p.SetQueue(NewEventQueue(heightTestCollections()))

	require.Equal(t, []string{logCollection, txCollection}, p.heightPrunable)

	seedHeightBlocks(t, n, 1, 20)
	for i := 1; i <= 3; i++ {
		addHeightDoc(t, n, attRecCollection, map[string]any{"attested_doc": fmt.Sprintf("d%d", i)})
	}

	require.NoError(t, p.runPrune(context.Background()))

	require.Equal(t, 3, countHeightDocs(t, n, attRecCollection))
}

// A node bootstrapped with historical blocks keeps them.
func TestRetainHistorySuppressesHeightPrune(t *testing.T) {
	p, n := newHeightTestPruner(t, &Config{Enabled: true, MaxBlocks: 5, DocsPerBlock: 1000})
	p.SetQueue(NewEventQueue(heightTestCollections()))
	p.SetRetainHistory(true)

	seedHeightBlocks(t, n, 1, 20)

	require.NoError(t, p.runPrune(context.Background()))

	require.Len(t, blockNumbers(t, n, blockCollection, blockNumberColumn), 20)
	require.Len(t, blockNumbers(t, n, logCollection, dependentBlockNumberField), 20)
}

// One cycle removes at most MaxDocsPerCycle, however far behind the queue is.
func TestDrainQueueStopsAtThePerCycleLimit(t *testing.T) {
	p, _ := newHeightTestPruner(t, &Config{
		Enabled: true, MaxBlocks: 1, DocsPerBlock: 10, MaxDocsPerCycle: 25,
	})
	q := NewEventQueue(heightTestCollections())
	p.SetQueue(q)

	for i := range 200 {
		q.Push(logCollection, testDocID(i))
	}

	drained, err := p.drainQueue(context.Background(), q)
	require.NoError(t, err)
	require.Equal(t, int64(25), drained)
	require.Equal(t, 175, q.Len())

	drained, err = p.drainQueue(context.Background(), q)
	require.NoError(t, err)
	require.Equal(t, int64(25), drained)
	require.Equal(t, 150, q.Len())
}
