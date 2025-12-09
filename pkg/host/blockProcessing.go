package host

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

func (h *Host) getMostRecentBlockNumberProcessed() uint64 {
	return h.attestationRangeTracker.GetHighest()
}

func (h *Host) getMostRecentBlockNumberProcessedForView(view *view.View) uint64 {
	if tracker, exists := h.viewRangeTrackers[view.Name]; exists {
		return tracker.GetHighest()
	}
	return 0
}

// processAllViewsWithSubscription uses DefraDB subscriptions for real-time block processing
// This replaces polling with event-driven processing using the new Subscribe function
func (h *Host) processAllViewsWithSubscription(ctx context.Context) {
	logger.Sugar.Info("Starting subscription-based block processing with BlockRangeTracker integration")

	// Use subscription-only approach - no polling fallback
	for {
		err := h.processWithBlockSubscription(ctx)
		if err != nil {
			logger.Sugar.Errorf("Block subscription failed: %v", err)
			logger.Sugar.Info("Retrying subscription in 5 seconds...")

			select {
			case <-ctx.Done():
				logger.Sugar.Info("Context cancelled, stopping subscription retries")
				return
			case <-time.After(5 * time.Second):
				logger.Sugar.Info("Retrying block subscription...")
				continue
			}
		}

		// If we get here, the subscription ended normally (context cancelled)
		return
	}
}

// processWithBlockSubscription uses the new Subscribe function for real-time block events
func (h *Host) processWithBlockSubscription(ctx context.Context) error {
	// Create subscription for new blocks
	subscription := `subscription { Block { number __typename } }`

	blockChan, err := defra.Subscribe[attestation.Block](ctx, h.DefraNode, subscription)
	if err != nil {
		return fmt.Errorf("failed to create block subscription: %w", err)
	}

	logger.Sugar.Info("✅ Successfully created DefraDB block subscription - now processing blocks in real-time!")

	var lastBlockNumber uint64 = 0

	// Also do an initial check for existing blocks
	go h.processInitialBlocks(ctx)

	for {
		select {
		case <-ctx.Done():
			logger.Sugar.Info("Block subscription stopped due to context cancellation")
			return nil
		case block, ok := <-blockChan:
			if !ok {
				logger.Sugar.Warn("Block subscription channel closed, restarting...")
				return fmt.Errorf("subscription channel closed")
			}

			currentBlock := block.Number
			if currentBlock > lastBlockNumber {
				logger.Sugar.Infof("📦 New block received via subscription: %d (was %d)", currentBlock, lastBlockNumber)
				h.mostRecentBlockReceived = currentBlock
				lastBlockNumber = currentBlock

				// Process all hosted views with BlockRangeTracker
				for _, view := range h.HostedViews {
					err := h.processViewWithRangeTracker(ctx, &view, currentBlock)
					if err != nil {
						logger.Sugar.Errorf("Error processing view %s for block %d: %v", view.Name, currentBlock, err)
					}
				}
			}
		}
	}
}

// processInitialBlocks handles any existing blocks when starting up
func (h *Host) processInitialBlocks(ctx context.Context) {
	// Give DefraDB a moment to be ready
	time.Sleep(2 * time.Second)

	query := `query GetHighestBlockNumber { Block(order: {number: DESC}, limit: 1) { number } }`
	latestBlock, err := defra.QuerySingle[attestation.Block](ctx, h.DefraNode, query)
	if err != nil {
		if !strings.Contains(err.Error(), "no documents found") {
			logger.Sugar.Debugf("Error getting initial block number: %v", err)
		}
		return
	}

	if latestBlock.Number > 0 {
		startHeight := h.config.Shinzo.StartHeight
		if startHeight == 0 {
			startHeight = 1 // Default to block 1 if not configured
		}

		logger.Sugar.Infof("Found existing blocks up to %d, configured StartHeight: %d", latestBlock.Number, startHeight)
		h.mostRecentBlockReceived = latestBlock.Number

		// Mark blocks before StartHeight as processed to avoid processing historical blocks
		if latestBlock.Number >= startHeight {
			// If we have blocks beyond StartHeight, mark everything before StartHeight as processed
			if startHeight > 1 {
				h.attestationRangeTracker.AddRange(1, startHeight-1)
				logger.Sugar.Infof("Marked blocks 1-%d as processed (before StartHeight)", startHeight-1)
			}

			// Initialize view trackers with blocks before StartHeight as processed
			for _, view := range h.HostedViews {
				if tracker, exists := h.viewRangeTrackers[view.Name]; exists {
					if startHeight > 1 {
						tracker.AddRange(1, startHeight-1)
					}
				} else {
					tracker := NewBlockRangeTracker()
					if startHeight > 1 {
						tracker.AddRange(1, startHeight-1)
					}
					h.viewRangeTrackers[view.Name] = tracker
				}
				logger.Sugar.Infof("Initialized view %s tracker - will process blocks from %d onwards", view.Name, startHeight)
			}

			logger.Sugar.Infof("✅ Host will process blocks starting from StartHeight %d (ignoring blocks 1-%d)", startHeight, startHeight-1)
		} else {
			// Current latest block is before StartHeight, mark all existing blocks as processed
			h.attestationRangeTracker.AddRange(1, latestBlock.Number)

			for _, view := range h.HostedViews {
				if tracker, exists := h.viewRangeTrackers[view.Name]; exists {
					tracker.AddRange(1, latestBlock.Number)
				} else {
					tracker := NewBlockRangeTracker()
					tracker.AddRange(1, latestBlock.Number)
					h.viewRangeTrackers[view.Name] = tracker
				}
			}

			logger.Sugar.Infof("✅ Current blocks (%d) are before StartHeight (%d) - waiting for blocks %d+", latestBlock.Number, startHeight, startHeight)
		}
	}
}

// processViewWithRangeTracker processes a view using BlockRangeTracker for efficient gap detection
func (h *Host) processViewWithRangeTracker(ctx context.Context, view *view.View, latestBlock uint64) error {
	// Get the tracker for this view (create if doesn't exist)
	tracker, exists := h.viewRangeTrackers[view.Name]
	if !exists {
		tracker = NewBlockRangeTracker()
		h.viewRangeTrackers[view.Name] = tracker
	}

	// Find unprocessed ranges
	unprocessedRanges := tracker.GetUnprocessedRanges(latestBlock)
	if len(unprocessedRanges) == 0 {
		return nil // No gaps to fill
	}

	logger.Sugar.Infof("Processing view %s: found %d unprocessed ranges up to block %d", view.Name, len(unprocessedRanges), latestBlock)

	// Process each range
	for _, blockRange := range unprocessedRanges {
		err := h.ApplyView(ctx, *view, blockRange.Start, blockRange.End)
		if err != nil {
			if strings.Contains(err.Error(), "No source data found") {
				logger.Sugar.Debugf("No source data for view %s in range %d-%d", view.Name, blockRange.Start, blockRange.End)
			} else {
				logger.Sugar.Errorf("Error applying view %s on range %d-%d: %v", view.Name, blockRange.Start, blockRange.End, err)
				continue // Don't mark as processed if there was an error
			}
		}

		// Mark all blocks in this range as processed
		tracker.AddRange(blockRange.Start, blockRange.End)
		logger.Sugar.Debugf("Marked blocks %d-%d as processed for view %s", blockRange.Start, blockRange.End, view.Name)
	}

	return nil
}

// getCurrentBlockNumber queries DefraDB for the highest block number
// TODO: This will be replaced by subscription events from app-sdk
// func (h *Host) getCurrentBlockNumber(ctx context.Context) (uint64, error) {
// 	query := `query GetHighestBlockNumber { Block(order: {number: DESC}, limit: 1) { number } }`
// 	latestBlock, err := defra.QuerySingle[attestation.Block](ctx, h.DefraNode, query)
// 	if err != nil {
// 		// Handle the case where no blocks exist yet
// 		if strings.Contains(err.Error(), "no documents found") {
// 			return 0, nil // Return 0 when no blocks exist
// 		}
// 		return 0, err
// 	}
// 	return latestBlock.Number, nil
// }

// // TODO: Move this to app-sdk as defra.Subscribe[attestation.Block]() method
// func (h *Host) processView(ctx context.Context, view *view.View) error {
// 	// Get the latest block number available in DefraDB
// 	latestBlock, err := h.getCurrentBlockNumber(ctx)
// 	if err != nil {
// 		return fmt.Errorf("error getting current block number: %w", err)
// 	}

// 	// Get the tracker for this view (create if doesn't exist)
// 	tracker, exists := h.viewRangeTrackers[view.Name]
// 	if !exists {
// 		tracker = NewBlockRangeTracker()
// 		h.viewRangeTrackers[view.Name] = tracker
// 	}

// 	// Find unprocessed ranges
// 	unprocessedRanges := tracker.GetUnprocessedRanges(latestBlock)
// 	if len(unprocessedRanges) == 0 {
// 		return nil // No gaps to fill
// 	}

// 	logger.Sugar.Infof("Processing view %s: found %d unprocessed ranges", view.Name, len(unprocessedRanges))

// 	// Process each range
// 	for _, blockRange := range unprocessedRanges {
// 		err := h.ApplyView(ctx, *view, blockRange.Start, blockRange.End)
// 		if err != nil {
// 			if strings.Contains(err.Error(), "No source data found") {
// 				logger.Sugar.Debugf("No source data for view %s in range %d-%d", view.Name, blockRange.Start, blockRange.End)
// 			} else {
// 				logger.Sugar.Errorf("Error applying view %s on range %d-%d: %w", view.Name, blockRange.Start, blockRange.End, err)
// 			}
// 		}

// 		// Mark all blocks in this range as processed
// 		tracker.AddRange(blockRange.Start, blockRange.End)
// 	}

// 	return nil
// }

// // processAllViewsWithPolling is the fallback polling method
// func (h *Host) processAllViewsWithPolling(ctx context.Context) {
// 	logger.Sugar.Warn("Using fallback polling method for block processing")
// 	ticker := time.NewTicker(2 * time.Second) // Slower polling as fallback
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			logger.Sugar.Info("Fallback block processing stopped due to context cancellation")
// 			return
// 		case <-ticker.C:
// 			// Process all hosted views
// 			for _, view := range h.HostedViews {
// 				err := h.processView(ctx, &view)
// 				if err != nil {
// 					logger.Sugar.Errorf("Error processing view %s: %v", view.Name, err)
// 				}
// 			}
// 		}
// 	}
// }

// func (h *Host) processAllViews(ctx context.Context) {
// 	// Subscribe to new Block documents for real-time processing
// 	blockChan, err := h.subscribeToNewBlocks(ctx)
// 	if err != nil {
// 		logger.Sugar.Errorf("Failed to subscribe to new blocks, falling back to polling: %v", err)
// 		// Fallback to polling if subscription fails
// 		h.processAllViewsWithPolling(ctx)
// 		return
// 	}

// 	logger.Sugar.Info("Started event-driven block processing with DefraDB subscriptions")

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			logger.Sugar.Info("Block processing stopped due to context cancellation")
// 			return
// 		case newBlockNumber := <-blockChan:
// 			// Process all views when new blocks arrive
// 			logger.Sugar.Debugf("New block %d arrived, processing all views", newBlockNumber)
// 			h.mostRecentBlockReceived = newBlockNumber

// 			for _, view := range h.HostedViews {
// 				err := h.processView(ctx, &view)
// 				if err != nil {
// 					logger.Sugar.Errorf("Error processing view %s: %v", view.Name, err)
// 				}
// 			}
// 		}
// 	}
// }
