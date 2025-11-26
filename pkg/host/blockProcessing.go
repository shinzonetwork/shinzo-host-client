package host

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/graphql"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

const maxChunkSize = 100

// ProcessingChunk represents a contiguous range of blocks that have been processed or are being processed.
// MissingBlocks contains the block numbers within the range that haven't been processed yet.
type ProcessingChunk struct {
	StartBlock    uint64   // First block in the chunk (inclusive)
	EndBlock      uint64   // Last block in the chunk (inclusive)
	MissingBlocks []uint64 // Sorted list of block numbers that are missing within this range
}

func (c *ProcessingChunk) IsProcessingComplete() bool {
	return len(c.MissingBlocks) == 0
}

// RemoveProcessedBlocks removes the given block numbers from MissingBlocks.
func (c *ProcessingChunk) RemoveProcessedBlocks(blockNumbers []uint64) {
	if len(blockNumbers) == 0 {
		return
	}

	// Create a map for O(1) lookup
	processedMap := make(map[uint64]bool, len(blockNumbers))
	for _, bn := range blockNumbers {
		processedMap[bn] = true
	}

	// Filter out processed blocks
	newMissing := make([]uint64, 0, len(c.MissingBlocks))
	for _, missing := range c.MissingBlocks {
		if !processedMap[missing] {
			newMissing = append(newMissing, missing)
		}
	}

	c.MissingBlocks = newMissing
	sort.Slice(c.MissingBlocks, func(i, j int) bool {
		return c.MissingBlocks[i] < c.MissingBlocks[j]
	})
}

// getNextChunkStart returns the starting block number for the next chunk for a given view.
// If there are no chunks, returns 1. Otherwise, returns the last chunk's EndBlock + 1.
func (h *Host) getNextChunkStart(viewName string) uint64 {
	h.chunksMutex.RLock()
	defer h.chunksMutex.RUnlock()
	chunks := h.ViewProcessedChunks[viewName]
	if len(chunks) == 0 {
		return 1
	}
	lastChunk := chunks[len(chunks)-1]
	return lastChunk.EndBlock + 1
}

// extractBlockNumberFromDoc extracts the block number from a document, handling different field locations.
func extractBlockNumberFromDoc(doc map[string]any) (uint64, error) {
	convertToUint64 := func(v any) (uint64, bool) {
		switch val := v.(type) {
		case uint64:
			return val, true
		case uint32:
			return uint64(val), true
		case int64:
			if val < 0 {
				return 0, false
			}
			return uint64(val), true
		case int32:
			if val < 0 {
				return 0, false
			}
			return uint64(val), true
		case int:
			if val < 0 {
				return 0, false
			}
			return uint64(val), true
		case float64:
			if val < 0 {
				return 0, false
			}
			return uint64(val), true
		case float32:
			if val < 0 {
				return 0, false
			}
			return uint64(val), true
		default:
			return 0, false
		}
	}

	// Try blockNumber first (Log, Transaction)
	if blockNum, ok := doc["blockNumber"]; ok && blockNum != nil {
		if result, ok := convertToUint64(blockNum); ok {
			return result, nil
		}
	}

	// Try number field (Block)
	if num, ok := doc["number"]; ok && num != nil {
		if result, ok := convertToUint64(num); ok {
			return result, nil
		}
	}

	// Try nested transaction.blockNumber (AccessListEntry)
	if transaction, ok := doc["transaction"].(map[string]any); ok && transaction != nil {
		if blockNum, ok := transaction["blockNumber"]; ok && blockNum != nil {
			if result, ok := convertToUint64(blockNum); ok {
				return result, nil
			}
		}
	}

	return 0, fmt.Errorf("unable to extract blockNumber from document")
}

// findMissingBlocks finds block numbers that are in the expected range but not in the actual list.
// Both expected and actual should be sorted slices.
func findMissingBlocks(expected []uint64, actual []uint64) []uint64 {
	actualSet := make(map[uint64]bool, len(actual))
	for _, bn := range actual {
		actualSet[bn] = true
	}

	missing := make([]uint64, 0)
	for _, bn := range expected {
		if !actualSet[bn] {
			missing = append(missing, bn)
		}
	}

	return missing
}

func (h *Host) getCurrentBlockNumber(ctx context.Context) (uint64, error) {
	query := `query GetHighestBlockNumber { Block(order: {number: DESC}, limit: 1) { number } }`
	latestBlock, err := defra.QuerySingle[attestation.Block](ctx, h.DefraNode, query)
	if err != nil {
		return 0, err
	}

	return latestBlock.Number, nil
}

// viewHasNewBlocks checks if there are new blocks to process for a view.
// Returns true if the current highest block number is greater than the end of the most recent chunk.
func (h *Host) viewHasNewBlocks(ctx context.Context, view *view.View) bool {
	latestBlockNumber, err := h.getCurrentBlockNumber(ctx)
	if err != nil {
		logger.Sugar.Errorf("Error fetching latest block number: %w", err)
		return false
	}

	nextBlockToProcess := h.getNextChunkStart(view.Name)
	return latestBlockNumber >= nextBlockToProcess
}

func (h *Host) processView(ctx context.Context, view *view.View) error {
	if !h.viewHasNewBlocks(ctx, view) {
		return nil
	}

	currentBlockNumber, err := h.getCurrentBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("Error getting current block number: %w", err)
	}

	// Get the starting block number for the next chunk
	startBlock := h.getNextChunkStart(view.Name)
	endBlock := currentBlockNumber

	// If startBlock > endBlock, there's nothing to process
	if startBlock > endBlock {
		return nil
	}

	logger.Sugar.Infof("Processing view %s on blocks %d -> %d...", view.Name, startBlock, endBlock)

	// ApplyView now handles querying and returns processed/missing blocks
	processedBlocks, missingBlocks, err := h.ApplyView(ctx, *view, startBlock, endBlock)
	if err != nil {
		return fmt.Errorf("error applying view %s from blocks %d to %d: %w", view.Name, startBlock, endBlock, err)
	}

	logger.Sugar.Debugf("Updating chunks for view %s: range %d-%d, processed: %d blocks, missing: %d blocks", view.Name, startBlock, endBlock, len(processedBlocks), len(missingBlocks))

	// Update chunks based on processed and missing blocks
	h.updateChunksForView(view.Name, startBlock, endBlock, processedBlocks, missingBlocks)

	logger.Sugar.Infof("Successfully processed view %s on blocks %d -> %d (processed: %d, missing: %d blocks)", view.Name, startBlock, endBlock, len(processedBlocks), len(missingBlocks))

	// Merge contiguous chunks after processing
	h.mergeContiguousChunks(view.Name)

	return nil
}

// updateChunksForView updates or creates chunks based on processed and missing blocks
func (h *Host) updateChunksForView(viewName string, startBlock, endBlock uint64, processedBlocks, missingBlocks []uint64) {
	h.chunksMutex.Lock()
	defer h.chunksMutex.Unlock()
	chunks := h.ViewProcessedChunks[viewName]
	var mostRecentChunk *ProcessingChunk

	if len(chunks) > 0 {
		mostRecentChunk = chunks[len(chunks)-1]
		logger.Sugar.Debugf("Updating chunks for view %s: existing chunk %d-%d, new range %d-%d", viewName, mostRecentChunk.StartBlock, mostRecentChunk.EndBlock, startBlock, endBlock)

		// Check if extending would exceed max chunk size
		proposedChunkSize := endBlock - mostRecentChunk.StartBlock + 1
		shouldCreateNewChunk := proposedChunkSize > maxChunkSize

		// Check if we can extend the existing chunk or need a new one
		if mostRecentChunk.EndBlock+1 == startBlock && !shouldCreateNewChunk {
			logger.Sugar.Debugf("Chunks are contiguous, extending chunk %d-%d to %d-%d", mostRecentChunk.StartBlock, mostRecentChunk.EndBlock, mostRecentChunk.StartBlock, endBlock)
			// Contiguous - extend the existing chunk
			mostRecentChunk.EndBlock = endBlock
			// Remove processed blocks from missing list
			mostRecentChunk.RemoveProcessedBlocks(processedBlocks)
			// Merge missing blocks
			missingSet := make(map[uint64]bool)
			for _, mb := range mostRecentChunk.MissingBlocks {
				missingSet[mb] = true
			}
			for _, mb := range missingBlocks {
				if !missingSet[mb] {
					mostRecentChunk.MissingBlocks = append(mostRecentChunk.MissingBlocks, mb)
				}
			}
			sort.Slice(mostRecentChunk.MissingBlocks, func(i, j int) bool {
				return mostRecentChunk.MissingBlocks[i] < mostRecentChunk.MissingBlocks[j]
			})
		} else if startBlock <= mostRecentChunk.EndBlock && !shouldCreateNewChunk {
			// New blocks overlap with existing chunk - extend and update missing blocks
			if endBlock > mostRecentChunk.EndBlock {
				oldEnd := mostRecentChunk.EndBlock
				mostRecentChunk.EndBlock = endBlock

				// Remove processed blocks from missing list
				mostRecentChunk.RemoveProcessedBlocks(processedBlocks)

				// Add any gaps between old end and new blocks as missing
				for bn := oldEnd + 1; bn < startBlock; bn++ {
					missingSet := make(map[uint64]bool)
					for _, mb := range mostRecentChunk.MissingBlocks {
						missingSet[mb] = true
					}
					if !missingSet[bn] {
						mostRecentChunk.MissingBlocks = append(mostRecentChunk.MissingBlocks, bn)
					}
				}

				// Merge new missing blocks
				missingSet := make(map[uint64]bool)
				for _, mb := range mostRecentChunk.MissingBlocks {
					missingSet[mb] = true
				}
				for _, mb := range missingBlocks {
					if !missingSet[mb] {
						mostRecentChunk.MissingBlocks = append(mostRecentChunk.MissingBlocks, mb)
					}
				}
				sort.Slice(mostRecentChunk.MissingBlocks, func(i, j int) bool {
					return mostRecentChunk.MissingBlocks[i] < mostRecentChunk.MissingBlocks[j]
				})
			} else {
				// Range is within existing chunk - just update missing/processed blocks
				mostRecentChunk.RemoveProcessedBlocks(processedBlocks)
				// Merge new missing blocks
				missingSet := make(map[uint64]bool)
				for _, mb := range mostRecentChunk.MissingBlocks {
					missingSet[mb] = true
				}
				for _, mb := range missingBlocks {
					if !missingSet[mb] {
						mostRecentChunk.MissingBlocks = append(mostRecentChunk.MissingBlocks, mb)
					}
				}
				sort.Slice(mostRecentChunk.MissingBlocks, func(i, j int) bool {
					return mostRecentChunk.MissingBlocks[i] < mostRecentChunk.MissingBlocks[j]
				})
			}
		} else {
			// There's a gap, overlap would exceed max size, or we need a new chunk
			// Check if we should extend or create new chunk based on max size
			if !shouldCreateNewChunk && mostRecentChunk.EndBlock+1 <= startBlock {
				// There's a gap between the last chunk and new blocks, but we can extend
				gapStart := mostRecentChunk.EndBlock + 1
				gapEnd := startBlock - 1

				// Add gap blocks to missing list
				missingSet := make(map[uint64]bool)
				for _, mb := range mostRecentChunk.MissingBlocks {
					missingSet[mb] = true
				}
				for bn := gapStart; bn <= gapEnd; bn++ {
					if !missingSet[bn] {
						mostRecentChunk.MissingBlocks = append(mostRecentChunk.MissingBlocks, bn)
					}
				}

				// Extend chunk to include new blocks
				mostRecentChunk.EndBlock = endBlock

				// Remove processed blocks from missing list
				mostRecentChunk.RemoveProcessedBlocks(processedBlocks)

				// Merge new missing blocks
				for _, mb := range missingBlocks {
					if !missingSet[mb] {
						mostRecentChunk.MissingBlocks = append(mostRecentChunk.MissingBlocks, mb)
					}
				}
				sort.Slice(mostRecentChunk.MissingBlocks, func(i, j int) bool {
					return mostRecentChunk.MissingBlocks[i] < mostRecentChunk.MissingBlocks[j]
				})
			} else {
				// Create new chunk (either because of gap or max size exceeded)
				// Calculate the actual range for the new chunk, respecting max size
				newChunkStart := startBlock
				newChunkEnd := endBlock
				if endBlock-startBlock+1 > maxChunkSize {
					// Limit to max chunk size
					newChunkEnd = startBlock + maxChunkSize - 1
				}

				// Filter missing blocks to only those in the new chunk range
				newChunkMissing := make([]uint64, 0)
				for _, mb := range missingBlocks {
					if mb >= newChunkStart && mb <= newChunkEnd {
						newChunkMissing = append(newChunkMissing, mb)
					}
				}

				mostRecentChunk = &ProcessingChunk{
					StartBlock:    newChunkStart,
					EndBlock:      newChunkEnd,
					MissingBlocks: newChunkMissing,
				}
				chunks = append(chunks, mostRecentChunk)
				h.ViewProcessedChunks[viewName] = chunks
			}
		}
	} else {
		// No chunks yet - create chunks, respecting max size
		// Create multiple chunks if the range exceeds maxChunkSize
		currentStart := startBlock
		for currentStart <= endBlock {
			currentEnd := currentStart + maxChunkSize - 1
			if currentEnd > endBlock {
				currentEnd = endBlock
			}

			// Filter missing blocks to only those in this chunk range
			chunkMissing := make([]uint64, 0)
			for _, mb := range missingBlocks {
				if mb >= currentStart && mb <= currentEnd {
					chunkMissing = append(chunkMissing, mb)
				}
			}

			// Filter processed blocks to only those in this chunk range
			chunkProcessed := make([]uint64, 0)
			for _, pb := range processedBlocks {
				if pb >= currentStart && pb <= currentEnd {
					chunkProcessed = append(chunkProcessed, pb)
				}
			}

			newChunk := &ProcessingChunk{
				StartBlock:    currentStart,
				EndBlock:      currentEnd,
				MissingBlocks: chunkMissing,
			}
			// Remove processed blocks from missing list
			newChunk.RemoveProcessedBlocks(chunkProcessed)

			chunks = append(chunks, newChunk)
			currentStart = currentEnd + 1
		}
		h.ViewProcessedChunks[viewName] = chunks
		mostRecentChunk = chunks[len(chunks)-1]
	}

	// Remove processed blocks from the most recent chunk's missing list
	if mostRecentChunk != nil {
		mostRecentChunk.RemoveProcessedBlocks(processedBlocks)
	}
}

// mergeContiguousChunks merges chunks that are contiguous (last block of chunk N + 1 = first block of chunk N+1)
// It also splits chunks that exceed maxChunkSize
func (h *Host) mergeContiguousChunks(viewName string) {
	h.chunksMutex.Lock()
	defer h.chunksMutex.Unlock()
	chunks := h.ViewProcessedChunks[viewName]
	if len(chunks) == 0 {
		return
	}

	// First, split any chunks that exceed maxChunkSize
	var splitChunks []*ProcessingChunk
	for _, chunk := range chunks {
		chunkSize := chunk.EndBlock - chunk.StartBlock + 1
		if chunkSize <= maxChunkSize {
			splitChunks = append(splitChunks, chunk)
			continue
		}

		// Split chunk into multiple chunks of maxChunkSize
		currentStart := chunk.StartBlock
		for currentStart <= chunk.EndBlock {
			currentEnd := currentStart + maxChunkSize - 1
			if currentEnd > chunk.EndBlock {
				currentEnd = chunk.EndBlock
			}

			// Filter missing blocks to only those in this chunk range
			chunkMissing := make([]uint64, 0)
			for _, mb := range chunk.MissingBlocks {
				if mb >= currentStart && mb <= currentEnd {
					chunkMissing = append(chunkMissing, mb)
				}
			}

			newChunk := &ProcessingChunk{
				StartBlock:    currentStart,
				EndBlock:      currentEnd,
				MissingBlocks: chunkMissing,
			}
			splitChunks = append(splitChunks, newChunk)
			currentStart = currentEnd + 1
		}
	}

	if len(splitChunks) <= 1 {
		h.ViewProcessedChunks[viewName] = splitChunks
		return // Nothing to merge
	}

	chunks = splitChunks

	// Sort chunks by StartBlock to ensure we process them in order
	sortedChunks := make([]*ProcessingChunk, len(chunks))
	copy(sortedChunks, chunks)
	sort.Slice(sortedChunks, func(i, j int) bool {
		return sortedChunks[i].StartBlock < sortedChunks[j].StartBlock
	})

	merged := []*ProcessingChunk{sortedChunks[0]}

	for i := 1; i < len(sortedChunks); i++ {
		current := sortedChunks[i]
		lastMerged := merged[len(merged)-1]

		// Check if chunks are contiguous or overlapping
		// Also check if merging would exceed max chunk size
		proposedSize := current.EndBlock - lastMerged.StartBlock + 1
		if current.StartBlock <= lastMerged.EndBlock+1 && proposedSize <= maxChunkSize {
			// Contiguous or overlapping and within max size - merge them
			if current.EndBlock > lastMerged.EndBlock {
				lastMerged.EndBlock = current.EndBlock
			}

			// Merge missing blocks from both chunks
			missingSet := make(map[uint64]bool)
			for _, mb := range lastMerged.MissingBlocks {
				missingSet[mb] = true
			}
			for _, mb := range current.MissingBlocks {
				if !missingSet[mb] {
					lastMerged.MissingBlocks = append(lastMerged.MissingBlocks, mb)
				}
			}

			// If there's a gap between chunks, mark those blocks as missing
			if lastMerged.EndBlock+1 < current.StartBlock {
				for bn := lastMerged.EndBlock + 1; bn < current.StartBlock; bn++ {
					if !missingSet[bn] {
						lastMerged.MissingBlocks = append(lastMerged.MissingBlocks, bn)
					}
				}
			}

			// Sort missing blocks
			sort.Slice(lastMerged.MissingBlocks, func(i, j int) bool {
				return lastMerged.MissingBlocks[i] < lastMerged.MissingBlocks[j]
			})
		} else {
			// Not contiguous - add as new chunk
			merged = append(merged, current)
		}
	}

	// Update the chunks list
	h.ViewProcessedChunks[viewName] = merged
}

func (h *Host) processAllViews(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond) // Check for new blocks every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Sugar.Info("Block processing stopped due to context cancellation")
			return
		case <-ticker.C:
			// Process all hosted views
			for _, view := range h.HostedViews {
				err := h.processView(ctx, &view)
				if err != nil {
					logger.Sugar.Errorf("Error processing view %s: %v", view.Name, err)
				}
			}
		}
	}
}

// retryMissingBlocks periodically checks all chunks for missing blocks and processes them when they arrive.
func (h *Host) retryMissingBlocks(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second) // Check every 2 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Sugar.Info("Chunk retry stopped due to context cancellation")
			return
		case <-ticker.C:
			// Iterate through all views and their chunks
			h.chunksMutex.RLock()
			// Make a copy of the map keys to avoid holding the lock during iteration
			viewNames := make([]string, 0, len(h.ViewProcessedChunks))
			for viewName := range h.ViewProcessedChunks {
				viewNames = append(viewNames, viewName)
			}
			h.chunksMutex.RUnlock()

			for _, viewName := range viewNames {
				h.chunksMutex.Lock()
				chunks := h.ViewProcessedChunks[viewName]
				if len(chunks) == 0 {
					h.chunksMutex.Unlock()
					continue
				}
				// Make a copy of the chunks slice to avoid holding the lock during processing
				chunksCopy := make([]*ProcessingChunk, len(chunks))
				copy(chunksCopy, chunks)
				h.chunksMutex.Unlock()

				// Find the view object
				var viewObj *view.View
				for i := range h.HostedViews {
					if h.HostedViews[i].Name == viewName {
						viewObj = &h.HostedViews[i]
						break
					}
				}
				if viewObj == nil {
					continue
				}

				// Process each chunk that has missing blocks
				// We iterate backwards to safely remove chunks
				for i := len(chunksCopy) - 1; i >= 0; i-- {
					// Re-read chunk from map to get current state (with lock)
					h.chunksMutex.RLock()
					chunks := h.ViewProcessedChunks[viewName]
					if i >= len(chunks) {
						// Chunk was removed by another goroutine
						h.chunksMutex.RUnlock()
						continue
					}
					chunk := chunks[i]
					missingBlocksCopy := make([]uint64, len(chunk.MissingBlocks))
					copy(missingBlocksCopy, chunk.MissingBlocks)
					h.chunksMutex.RUnlock()

					if len(missingBlocksCopy) == 0 {
						// Chunk is complete - remove it if it's not the most recent chunk
						if i < len(chunks)-1 {
							// Not the most recent chunk, remove it
							h.chunksMutex.Lock()
							chunks = h.ViewProcessedChunks[viewName]
							if i < len(chunks)-1 {
								chunks = append(chunks[:i], chunks[i+1:]...)
								h.ViewProcessedChunks[viewName] = chunks
							}
							h.chunksMutex.Unlock()
							continue
						}
						// Most recent chunk is complete - keep it for tracking progress
						continue
					}

					// Query DefraDB to see which missing blocks have arrived
					availableBlocks, err := h.queryMissingBlocks(ctx, viewObj, missingBlocksCopy)
					if err != nil {
						logger.Sugar.Errorf("Error querying missing blocks for view %s chunk %d-%d: %v", viewName, chunk.StartBlock, chunk.EndBlock, err)
						continue
					}

					if len(availableBlocks) == 0 {
						// No missing blocks have arrived yet
						logger.Sugar.Debugf("No missing blocks found for view %s chunk %d-%d (checking %d missing blocks)", viewName, chunk.StartBlock, chunk.EndBlock, len(missingBlocksCopy))
						continue
					}

					logger.Sugar.Infof("Found %d available blocks out of %d missing for view %s chunk %d-%d", len(availableBlocks), len(missingBlocksCopy), viewName, chunk.StartBlock, chunk.EndBlock)

					// Process the newly available blocks
					// We need to process them in contiguous ranges
					err = h.processMissingBlocks(ctx, viewObj, availableBlocks)
					if err != nil {
						// Log error but check if blocks were actually processed (view data might exist even if attestation failed)
						logger.Sugar.Errorf("Error processing missing blocks for view %s: %v", viewName, err)

						// Check if view data exists for these blocks - if so, consider them processed
						// This handles cases where view processing succeeded but attestation record posting failed
						processedBlocks := []uint64{}
						for _, blockNum := range availableBlocks {
							// Query to see if view has data for this block
							viewQuery := fmt.Sprintf("%s(filter: { blockNumber: { _eq: %d } }, limit: 1) { blockNumber }", viewName, blockNum)
							results, queryErr := defra.QueryArray[map[string]any](ctx, h.DefraNode, viewQuery)
							if queryErr == nil && len(results) > 0 {
								// View data exists - block was processed
								processedBlocks = append(processedBlocks, blockNum)
							}
						}

						if len(processedBlocks) > 0 {
							// Some blocks were successfully processed - remove them from missing list
							// Need to hold lock when modifying chunk
							h.chunksMutex.Lock()
							chunks = h.ViewProcessedChunks[viewName]
							if i < len(chunks) {
								chunks[i].RemoveProcessedBlocks(processedBlocks)
							}
							h.chunksMutex.Unlock()
							logger.Sugar.Infof("Removed %d blocks from missing list (view data exists) for view %s chunk %d-%d", len(processedBlocks), viewName, chunk.StartBlock, chunk.EndBlock)
						}
						continue
					}

					// Remove processed blocks from the chunk's missing list
					// Need to hold lock when modifying chunk
					h.chunksMutex.Lock()
					chunks = h.ViewProcessedChunks[viewName]
					if i < len(chunks) {
						chunks[i].RemoveProcessedBlocks(availableBlocks)
						logger.Sugar.Infof("Processed %d missing blocks for view %s chunk %d-%d (%d still missing)", len(availableBlocks), viewName, chunk.StartBlock, chunk.EndBlock, len(chunks[i].MissingBlocks))

						// After processing, check if chunk is now complete and remove if not most recent
						if chunks[i].IsProcessingComplete() && i < len(chunks)-1 {
							// Not the most recent chunk and now complete - remove it
							chunks = append(chunks[:i], chunks[i+1:]...)
							h.ViewProcessedChunks[viewName] = chunks
						}
					}
					h.chunksMutex.Unlock()
				}
			}
		}
	}
}

// queryMissingBlocks queries DefraDB using _in filter to check which of the missing blocks have arrived.
func (h *Host) queryMissingBlocks(ctx context.Context, v *view.View, missingBlocks []uint64) ([]uint64, error) {
	if len(missingBlocks) == 0 {
		return []uint64{}, nil
	}

	// Use _in filter to query for specific block numbers
	query, err := graphql.WithBlockNumberInFilter(*v.Query, missingBlocks)
	if err != nil {
		return nil, fmt.Errorf("error adding block number _in filter: %w", err)
	}

	logger.Sugar.Debugf("Querying for missing blocks with query: %s", query)

	// Query for documents
	sourceDocuments, err := defra.QueryArray[map[string]any](ctx, h.DefraNode, query)
	if err != nil {
		logger.Sugar.Errorf("Error executing query for missing blocks: %v, query was: %s", err, query)
		return nil, fmt.Errorf("error querying source data: %w", err)
	}

	logger.Sugar.Debugf("Query returned %d documents for %d missing blocks", len(sourceDocuments), len(missingBlocks))

	// Extract block numbers from documents
	availableSet := make(map[uint64]bool)
	for _, doc := range sourceDocuments {
		blockNum, err := extractBlockNumberFromDoc(doc)
		if err != nil {
			continue
		}
		// Only include blocks that were in our missing list
		for _, missing := range missingBlocks {
			if missing == blockNum {
				availableSet[blockNum] = true
				break
			}
		}
	}

	// Convert to sorted slice
	availableBlocks := make([]uint64, 0, len(availableSet))
	for bn := range availableSet {
		availableBlocks = append(availableBlocks, bn)
	}
	sort.Slice(availableBlocks, func(i, j int) bool {
		return availableBlocks[i] < availableBlocks[j]
	})

	return availableBlocks, nil
}

// processMissingBlocks processes blocks that were previously missing but have now arrived.
// It processes them in contiguous ranges for efficiency.
func (h *Host) processMissingBlocks(ctx context.Context, v *view.View, blockNumbers []uint64) error {
	if len(blockNumbers) == 0 {
		return nil
	}

	// Sort block numbers to process in order
	sort.Slice(blockNumbers, func(i, j int) bool {
		return blockNumbers[i] < blockNumbers[j]
	})

	// Process blocks in contiguous ranges
	rangeStart := blockNumbers[0]
	rangeEnd := blockNumbers[0]

	for i := 1; i < len(blockNumbers); i++ {
		if blockNumbers[i] == rangeEnd+1 {
			// Contiguous - extend range
			rangeEnd = blockNumbers[i]
		} else {
			// Gap found - process current range
			_, _, err := h.ApplyView(ctx, *v, rangeStart, rangeEnd)
			if err != nil {
				return fmt.Errorf("error applying view for range %d-%d: %w", rangeStart, rangeEnd, err)
			}
			// Start new range
			rangeStart = blockNumbers[i]
			rangeEnd = blockNumbers[i]
		}
	}

	// Process final range
	_, _, err := h.ApplyView(ctx, *v, rangeStart, rangeEnd)
	if err != nil {
		return fmt.Errorf("error applying view for final range %d-%d: %w", rangeStart, rangeEnd, err)
	}

	return nil
}
