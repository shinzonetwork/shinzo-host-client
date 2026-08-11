package pruner

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testBlockCollection = "Ethereum__Mainnet__Block"
	testLogCollection   = "Ethereum__Mainnet__Log"
)

func testDocID(n int) string {
	return fmt.Sprintf("bae-%08x-0000-0000-0000-000000000000", n)
}

// TestEventQueueRequeue verifies that re-queued docs return to the front of the queue so the
// next drain retries them, and that the block count is restored only when a block is re-queued.
func TestEventQueueRequeue(t *testing.T) {
	q := NewEventQueue(CollectionConfig{
		BlockCollection:      testBlockCollection,
		DependentCollections: []string{testLogCollection},
	})

	q.Push(testBlockCollection, testDocID(1))
	q.Push(testLogCollection, testDocID(2))
	q.Push(testLogCollection, testDocID(3))
	q.Push(testLogCollection, testDocID(4)) // ingested later; must stay behind the re-queued docs

	drained := q.DrainDocs(3)
	require.NotNil(t, drained)
	require.Equal(t, 1, drained.BlockCount)
	require.Equal(t, 0, q.BlockCount())

	logIDs := drained.DocIDsByCollection[testLogCollection]
	blockIDs := drained.DocIDsByCollection[testBlockCollection]
	require.Len(t, logIDs, 2)
	require.Len(t, blockIDs, 1)

	// A failed dependent purge re-queues its docs at the front, so the next drain returns them
	// before the later doc, and the block count is unchanged.
	q.Requeue(testLogCollection, logIDs)
	require.Equal(t, 3, q.Len())
	require.Equal(t, 0, q.BlockCount())
	require.Equal(t, logIDs, q.DrainDocs(2).DocIDsByCollection[testLogCollection])

	// A failed block purge restores the block count.
	q.Requeue(testBlockCollection, blockIDs)
	require.Equal(t, 1, q.BlockCount())
}

// The queue only prunes once it exceeds MaxDocs, so a restart that loses it restarts
// that count from zero. Save and LoadFromFile are what carry it across.
func TestEventQueueSaveRestoresAcrossRestart(t *testing.T) {
	cfg := CollectionConfig{
		BlockCollection:      testBlockCollection,
		DependentCollections: []string{testLogCollection},
	}
	path := filepath.Join(t.TempDir(), "prune_queue.gob")

	q := NewEventQueue(cfg)
	// The file does not exist yet; this is also what gives Save its path.
	loaded, err := q.LoadFromFile(path)
	require.NoError(t, err)
	require.Equal(t, 0, loaded)

	q.Push(testBlockCollection, testDocID(1))
	q.Push(testLogCollection, testDocID(2))
	q.Push(testLogCollection, testDocID(3))
	require.NoError(t, q.Save())

	restored := NewEventQueue(cfg)
	count, err := restored.LoadFromFile(path)
	require.NoError(t, err)
	require.Equal(t, 3, count)
	require.Equal(t, 3, restored.Len())
	require.Equal(t, 1, restored.BlockCount())

	// The restored entries have to be drainable, not just counted.
	drained := restored.DrainDocs(3)
	require.NotNil(t, drained)
	require.Equal(t, []string{testDocID(1)}, drained.DocIDsByCollection[testBlockCollection])
	require.Equal(t, []string{testDocID(2), testDocID(3)}, drained.DocIDsByCollection[testLogCollection])
}

// A queue whose load failed has no path, and Save must not error on it.
func TestEventQueueSaveWithoutPath(t *testing.T) {
	q := NewEventQueue(CollectionConfig{BlockCollection: testBlockCollection})
	q.Push(testBlockCollection, testDocID(1))
	require.NoError(t, q.Save())
}
