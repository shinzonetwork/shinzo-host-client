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
	return h.attestationProcessedBlocks.GetHighest()
}

func (h *Host) getMostRecentBlockNumberProcessedForView(view *view.View) uint64 {
	if tracker, exists := h.viewProcessedBlocks[view.Name]; exists {
		return tracker.GetHighest()
	}
	return 0
}

// getUnprocessedBlocks fetches all blocks from DefraDB that are newer than the given block number.
func (h *Host) getUnprocessedBlocks(ctx context.Context, sinceBlockNumber uint64) ([]attestation.Block, error) {
	// Query for all blocks with a number greater than sinceBlockNumber.
	query := fmt.Sprintf(`query GetUnprocessedBlocks {
		Block(filter: {number: {_gt: %d}}) {
			number
		}
	}`, sinceBlockNumber)

	blocks, err := defra.QueryArray[attestation.Block](ctx, h.DefraNode, query)
	if err != nil {
		// It's common to have no new blocks, so we check for "no documents found" and return an empty slice.
		if strings.Contains(err.Error(), "no documents found") {
			return []attestation.Block{}, nil
		}
		return nil, err
	}

	return blocks, nil
}

func (h *Host) getCurrentBlockNumber(ctx context.Context) (uint64, error) {
	query := `query GetHighestBlockNumber { Block(order: {number: DESC}, limit: 1) { number } }`
	latestBlock, err := defra.QuerySingle[attestation.Block](ctx, h.DefraNode, query)
	if err != nil {
		return 0, err
	}

	return latestBlock.Number, nil
}

func (h *Host) processView(ctx context.Context, view *view.View) error {
	// Get all blocks since the last highest processed block for this view
	lastProcessed := h.getMostRecentBlockNumberProcessedForView(view)
	blocksToProcess, err := h.getUnprocessedBlocks(ctx, lastProcessed)
	if err != nil {
		return fmt.Errorf("error getting unprocessed blocks: %w", err)
	}

	if len(blocksToProcess) == 0 {
		return nil // No new blocks to process
	}

	logger.Sugar.Infof("Processing view %s on %d new blocks...", view.Name, len(blocksToProcess))

	for _, block := range blocksToProcess {
		// Check if the block has been processed to handle out-of-order arrivals
		if h.viewProcessedBlocks[view.Name].HasProcessed(block.Number) {
			continue
		}

		err := h.ApplyView(ctx, *view, block.Number, block.Number)
		if err != nil {
			// If no source data is found, it's not a critical error, just log and continue
			if strings.Contains(err.Error(), "No source data found") {
				logger.Sugar.Debugf("No source data for view %s on block %d", view.Name, block.Number)
			} else {
				logger.Sugar.Errorf("Error applying view %s on block %d: %w", view.Name, block.Number, err)
			}
			// Add to processed list even if it fails to avoid retrying a failing block indefinitely
			h.viewProcessedBlocks[view.Name].Add(block.Number)
			continue
		}
		h.viewProcessedBlocks[view.Name].Add(block.Number)
	}

	logger.Sugar.Infof("Successfully processed view %s", view.Name)
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
