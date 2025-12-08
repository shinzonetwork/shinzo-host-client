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

func (h *Host) getCurrentBlockNumber(ctx context.Context) (uint64, error) {
	query := `query GetHighestBlockNumber { Block(order: {number: DESC}, limit: 1) { number } }`
	latestBlock, err := defra.QuerySingle[attestation.Block](ctx, h.DefraNode, query)
	if err != nil {
		// Handle the case where no blocks exist yet
		if strings.Contains(err.Error(), "no documents found") {
			return 0, nil // Return 0 when no blocks exist
		}
		return 0, err
	}

	return latestBlock.Number, nil
}

func (h *Host) processView(ctx context.Context, view *view.View) error {
	// Get the latest block number available in DefraDB
	latestBlock, err := h.getCurrentBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("error getting current block number: %w", err)
	}

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

	logger.Sugar.Infof("Processing view %s: found %d unprocessed ranges", view.Name, len(unprocessedRanges))

	// Process each range
	for _, blockRange := range unprocessedRanges {
		err := h.ApplyView(ctx, *view, blockRange.Start, blockRange.End)
		if err != nil {
			if strings.Contains(err.Error(), "No source data found") {
				logger.Sugar.Debugf("No source data for view %s in range %d-%d", view.Name, blockRange.Start, blockRange.End)
			} else {
				logger.Sugar.Errorf("Error applying view %s on range %d-%d: %w", view.Name, blockRange.Start, blockRange.End, err)
			}
		}

		// Mark all blocks in this range as processed
		tracker.AddRange(blockRange.Start, blockRange.End)
	}

	return nil
}

func (h *Host) processAllViews(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second) // Check for new blocks every second
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
