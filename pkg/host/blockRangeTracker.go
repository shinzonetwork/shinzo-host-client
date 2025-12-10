package host

import (
	"sort"
	"sync"
)

// BlockRange represents a contiguous range of processed block numbers, inclusive.
// For example, {Start: 1, End: 100} means blocks 1 through 100 have been processed.
type BlockRange struct {
	Start uint64
	End   uint64
}

// BlockRangeTracker manages and tracks processed block ranges to handle
// non-linear block arrival from P2P replication.
// It maintains a sorted, non-overlapping list of BlockRanges.
type BlockRangeTracker struct {
	mu     sync.RWMutex
	ranges []BlockRange
}

// NewBlockRangeTracker creates a new, empty tracker.
func NewBlockRangeTracker() *BlockRangeTracker {
	return &BlockRangeTracker{
		ranges: make([]BlockRange, 0),
	}
}

// Add incorporates a new block number into the tracked ranges.
// This method handles inserting new ranges, extending existing ones, and merging
// adjacent ranges to maintain a compact list.
func (b *BlockRangeTracker) Add(blockNumber uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Find the index of the first range that starts after the new block number.
	i := sort.Search(len(b.ranges), func(i int) bool {
		return b.ranges[i].Start > blockNumber
	})

	// Case 1: The new block is before the first range.
	if i == 0 {
		// If it's adjacent to the start of the first range, extend it.
		if len(b.ranges) > 0 && b.ranges[0].Start == blockNumber+1 {
			b.ranges[0].Start = blockNumber
		} else {
			// Otherwise, insert a new range for this single block.
			b.ranges = append([]BlockRange{{Start: blockNumber, End: blockNumber}}, b.ranges...)
		}
		return
	}

	// At this point, i > 0. Check the previous range (at index i-1).
	prev := &b.ranges[i-1]

	// Case 2: The block is already contained within the previous range.
	if blockNumber <= prev.End {
		return // Already processed, nothing to do.
	}

	// Case 3: The block is adjacent to the end of the previous range.
	if prev.End+1 == blockNumber {
		prev.End = blockNumber

		// Check if this merge connects the previous range with the current one.
		if i < len(b.ranges) && prev.End+1 == b.ranges[i].Start {
			prev.End = b.ranges[i].End
			// Remove the merged range.
			b.ranges = append(b.ranges[:i], b.ranges[i+1:]...)
		}
		return
	}

	// Case 4: The block is adjacent to the start of the current range (at index i).
	if i < len(b.ranges) && b.ranges[i].Start-1 == blockNumber {
		b.ranges[i].Start = blockNumber
		return
	}

	// Case 5: The block is isolated. Insert a new range for this single block.
	newRange := BlockRange{Start: blockNumber, End: blockNumber}
	b.ranges = append(b.ranges[:i], append([]BlockRange{newRange}, b.ranges[i:]...)...)
}

// GetHighest returns the highest block number that has been processed.
// It returns 0 if no blocks have been processed.
func (b *BlockRangeTracker) GetHighest() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.ranges) == 0 {
		return 0
	}
	return b.ranges[len(b.ranges)-1].End
}

// GetUnprocessedRanges identifies the gaps in processed blocks up to the latest known block.
// It returns a slice of BlockRange representing the blocks that still need to be processed.
func (b *BlockRangeTracker) GetUnprocessedRanges(latestKnownBlock uint64) []BlockRange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var gaps []BlockRange
	var lastProcessed uint64 = 0

	if len(b.ranges) == 0 {
		if latestKnownBlock > 0 {
			return []BlockRange{{Start: 1, End: latestKnownBlock}} // Assuming blocks start at 1
		}
		return nil
	}

	for _, r := range b.ranges {
		if r.Start > lastProcessed+1 {
			gaps = append(gaps, BlockRange{Start: lastProcessed + 1, End: r.Start - 1})
		}
		lastProcessed = r.End
	}

	// Check for a gap at the end (from the end of the last range to the latest known block).
	if latestKnownBlock > lastProcessed {
		gaps = append(gaps, BlockRange{Start: lastProcessed + 1, End: latestKnownBlock})
	}

	return gaps
}

// AddRange efficiently adds a contiguous range of block numbers to the tracker.
// This is more efficient than calling Add() for each block individually.
func (b *BlockRangeTracker) AddRange(start, end uint64) {
	for blockNum := start; blockNum <= end; blockNum++ {
		b.Add(blockNum)
	}
}

// Contains checks if a block number has been processed (is within any tracked range).
func (b *BlockRangeTracker) Contains(blockNumber uint64) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, r := range b.ranges {
		if blockNumber >= r.Start && blockNumber <= r.End {
			return true
		}
	}
	return false
}
