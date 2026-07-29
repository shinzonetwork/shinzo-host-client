package pruner

import (
	"fmt"
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
