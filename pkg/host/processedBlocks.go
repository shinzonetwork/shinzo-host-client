package host

import (
	"sync"
)

// ProcessedBlocks tracks processed block numbers efficiently using a low-water-mark.
type ProcessedBlocks struct {
	mu           sync.RWMutex
	lowWaterMark uint64 // Represents the highest block N where all blocks from startHeight to N are processed.
	highestBlock uint64
	recentBlocks map[uint64]struct{} // Stores processed blocks above the lowWaterMark.
}

// NewProcessedBlocks creates a new tracker with a given start height.
func NewProcessedBlocks(startHeight uint64) *ProcessedBlocks {
	return &ProcessedBlocks{
		lowWaterMark: startHeight,
		highestBlock: startHeight,
		recentBlocks: make(map[uint64]struct{}),
	}
}

// Add marks a block number as processed and attempts to advance the low-water-mark.
func (pb *ProcessedBlocks) Add(blockNumber uint64) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Ignore blocks that are already covered by the low-water-mark.
	if blockNumber <= pb.lowWaterMark {
		return
	}

	// Update the highest block seen so far.
	if blockNumber > pb.highestBlock {
		pb.highestBlock = blockNumber
	}

	// Add the new block to the set of recent/sparse blocks.
	pb.recentBlocks[blockNumber] = struct{}{}

	// Try to advance the low-water-mark by checking for contiguous blocks.
	nextBlock := pb.lowWaterMark + 1
	for {
		if _, exists := pb.recentBlocks[nextBlock]; exists {
			// This block is contiguous, so advance the mark...
			pb.lowWaterMark = nextBlock
			// ...and remove it from the map since it's now covered by the mark.
			delete(pb.recentBlocks, nextBlock)
			// Check the next one.
			nextBlock++
		} else {
			// The sequence is no longer contiguous, so we stop.
			break
		}
	}
}

// GetHighest returns the highest block number processed so far.
func (pb *ProcessedBlocks) GetHighest() uint64 {
	pb.mu.RLock()
	defer pb.mu.RUnlock()
	return pb.highestBlock
}

// HasProcessed checks if a block has been processed.
func (pb *ProcessedBlocks) HasProcessed(blockNumber uint64) bool {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	// A block is considered processed if it's covered by the low-water-mark...
	if blockNumber <= pb.lowWaterMark {
		return true
	}
	// ...or if it's in the set of recently processed blocks above the mark.
	_, exists := pb.recentBlocks[blockNumber]
	return exists
}
