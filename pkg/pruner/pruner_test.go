package pruner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb/client"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

func testPrunerWithBlockedCycle() *Pruner {
	p := &Pruner{
		cfg:      &Config{Enabled: true},
		stopChan: make(chan struct{}),
	}
	p.isRunning = true
	// Stands in for a purge in flight. A cycle can run for over an hour, and nothing in
	// the purge path can be cancelled, so Stop has to cope with it never finishing.
	p.wg.Add(1)
	return p
}

// The queue file is written after the wait, so a Stop that waits without a bound never
// reaches it and the queue is lost on every restart.
func TestStopReturnsWhenShutdownBudgetExpires(t *testing.T) {
	p := testPrunerWithBlockedCycle()
	defer p.wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		p.Stop(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after its context expired")
	}
}

func TestStopSavesQueueWhenBudgetExpires(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prune_queue.gob")

	q := NewEventQueue(CollectionConfig{
		BlockCollection:      testBlockCollection,
		DependentCollections: []string{testLogCollection},
	})
	_, err := q.LoadFromFile(path) // sets the save path; the file does not exist yet
	require.NoError(t, err)
	q.Push(testBlockCollection, testDocID(1))
	q.Push(testLogCollection, testDocID(2))

	p := testPrunerWithBlockedCycle()
	defer p.wg.Done()
	p.queue = q

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	p.Stop(ctx)

	require.FileExists(t, path, "queue was not saved when the shutdown budget expired")

	restored := NewEventQueue(CollectionConfig{
		BlockCollection:      testBlockCollection,
		DependentCollections: []string{testLogCollection},
	})
	n, err := restored.LoadFromFile(path)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, 2, restored.Len())
}

// Close can be reached more than once, and the second call must not close stopChan again.
func TestStopIsIdempotent(t *testing.T) {
	p := &Pruner{cfg: &Config{Enabled: true}, stopChan: make(chan struct{})}
	p.isRunning = true

	ctx := context.Background()
	p.Stop(ctx)

	require.NotPanics(t, func() { p.Stop(ctx) })
}

// A purge that has already been told to stop must not start another batch.
func TestPurgeStopsBeforeFirstBatch(t *testing.T) {
	p := &Pruner{cfg: &Config{Enabled: true}, stopChan: make(chan struct{})}
	close(p.stopChan)

	// defraNode is nil, so reaching the collection lookup would panic. Returning at the
	// stop check before any submission is what keeps this safe.
	submitted, err := p.purgeByDocIDs(context.Background(), testLogCollection, []string{testDocID(1)})
	require.ErrorIs(t, err, errStopped)
	require.Zero(t, submitted)
}

// testDocIDs returns n distinct docIDs.
func testDocIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = testDocID(i + 1)
	}
	return ids
}

// A purge of a real backlog runs for minutes and DefraDB does not check the context inside a
// call, so the batch boundary is the only place a stop can be noticed. One call for the whole
// list would leave shutdown unbounded however short the budget is.
func TestPurgeSubmitsInBatchesAndStopsAtABoundary(t *testing.T) {
	// Pinned by value: this is how often a stop can be noticed, so raising it lengthens
	// shutdown and should take a deliberate test update rather than passing silently.
	require.Equal(t, 1000, purgeBatchSize)
	const docs = 2010

	t.Run("batches the whole list", func(t *testing.T) {
		var sizes []int
		p := &Pruner{cfg: &Config{Enabled: true}, stopChan: make(chan struct{})}
		p.purgeDocs = func(_ context.Context, ids []client.DocID) error {
			sizes = append(sizes, len(ids))
			return nil
		}

		submitted, err := p.purgeByDocIDs(context.Background(), testLogCollection, testDocIDs(docs))
		require.NoError(t, err)
		require.Equal(t, int64(docs), submitted)
		require.Equal(t, []int{1000, 1000, 10}, sizes,
			"the list must be handed over in bounded batches, not in one call")
	})

	t.Run("stops at the next boundary", func(t *testing.T) {
		p := &Pruner{cfg: &Config{Enabled: true}, stopChan: make(chan struct{})}
		calls := 0
		p.purgeDocs = func(_ context.Context, _ []client.DocID) error {
			calls++
			close(p.stopChan) // the pruner is told to stop while this batch is in flight
			return nil
		}

		submitted, err := p.purgeByDocIDs(context.Background(), testLogCollection, testDocIDs(docs))
		require.ErrorIs(t, err, errStopped)
		require.Equal(t, 1, calls, "no batch may start after the stop")
		require.Equal(t, int64(1000), submitted, "the batch that did run is still reported")
	})
}

func TestPurgeStopsOnCancelledContext(t *testing.T) {
	p := &Pruner{cfg: &Config{Enabled: true}, stopChan: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	submitted, err := p.purgeByDocIDs(ctx, testLogCollection, []string{testDocID(1)})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, submitted)
}

// DrainDocs empties every collection up front, so a cycle that stops part-way has to re-queue the
// collections it never reached as well as the one it stopped on. Nothing else re-adds a document
// once it has replicated, so anything left behind is never pruned.
func TestPurgeRequeuesEveryDrainedCollectionOnStop(t *testing.T) {
	cols := DefaultCollectionConfig()
	order := append(append([]string{}, cols.DependentCollections...), cols.BlockCollection)

	q := NewEventQueue(cols)
	for i, name := range order {
		q.Push(name, testDocID(i+1))
	}
	require.Equal(t, len(order), q.Len(), "every collection should be registered and queued")

	result := q.DrainDocs(len(order))
	require.NotNil(t, result)
	require.Zero(t, q.Len())

	p := &Pruner{cfg: &Config{Enabled: true}, collections: cols, stopChan: make(chan struct{})}
	close(p.stopChan)

	// defraNode is nil, so purgeByDocIDs returning at its stop check is what keeps this from
	// panicking, and is also what ends the cycle on the first collection.
	require.NoError(t, p.purgeFromDrainResult(context.Background(), q, result))

	require.Equal(t, len(order), q.Len(), "collections after the one that stopped were dropped")
	require.Equal(t, colBlock, q.entries[len(q.entries)-1].Collection,
		"blocks must stay behind their dependents so a later drain cannot purge them first")
}

// The pruner logs through the package-level sugared logger, which is nil until Init runs.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "pruner-logs")
	if err != nil {
		panic(err)
	}
	logger.Init(false, dir)

	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}
