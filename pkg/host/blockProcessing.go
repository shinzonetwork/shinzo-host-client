package host

import (
	"context"
	"fmt"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

func (h *Host) getMostRecentBlockNumberProcessed() uint64 {
	h.processedBlocksMutex.RLock()
	defer h.processedBlocksMutex.RUnlock()

	blockNumber, err := h.attestationProcessedBlocks.Peek()
	if err != nil {
		return 0
	}
	return blockNumber
}

// getMostRecentBlockNumberProcessedUnlocked is an unlocked version for use when already holding the lock
func (h *Host) getMostRecentBlockNumberProcessedUnlocked() uint64 {
	blockNumber, err := h.attestationProcessedBlocks.Peek()
	if err != nil {
		return 0
	}
	return blockNumber
}

func (h *Host) getMostRecentBlockNumberProcessedForView(view *view.View) uint64 {
	h.processedBlocksMutex.RLock()
	defer h.processedBlocksMutex.RUnlock()

	blockNumber, err := h.viewProcessedBlocks[view.Name].Peek()
	if err != nil {
		return 0
	}
	return blockNumber
}

// getMostRecentBlockNumberProcessedForViewUnlocked is an unlocked version for use when already holding the lock
func (h *Host) getMostRecentBlockNumberProcessedForViewUnlocked(view *view.View) uint64 {
	blockNumber, err := h.viewProcessedBlocks[view.Name].Peek()
	if err != nil {
		return 0
	}
	return blockNumber
}

func (h *Host) hasNewBlocks(ctx context.Context) bool {
	latestBlockNumber, err := h.getCurrentBlockNumber(ctx)
	if err != nil {
		logger.Sugar.Errorf("Error fetching latest block number: %w", err)
		return false
	}

	return latestBlockNumber > h.getMostRecentBlockNumberProcessed()
}

func (h *Host) viewHasNewBlocks(ctx context.Context, view *view.View) bool {
	latestBlockNumber, err := h.getCurrentBlockNumber(ctx)
	if err != nil {
		logger.Sugar.Errorf("Error fetching latest block number: %w", err)
		return false
	}

	return latestBlockNumber > h.getMostRecentBlockNumberProcessedForView(view)
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
	if !h.viewHasNewBlocks(ctx, view) {
		return nil
	}

	currentBlockNumber, err := h.getCurrentBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("Error getting current block number: %w", err)
	}

	// Lock for the entire read-check-write operation to make it atomic
	h.processedBlocksMutex.Lock()
	lastProcessedBlockNumber := h.getMostRecentBlockNumberProcessedForViewUnlocked(view)
	processFromBlockNumber := lastProcessedBlockNumber + 1
	h.processedBlocksMutex.Unlock()

	logger.Sugar.Infof("Processing view %s on blocks %d -> %d...", view.Name, processFromBlockNumber, currentBlockNumber)

	err = h.ApplyView(ctx, *view, processFromBlockNumber, currentBlockNumber)
	if err != nil {
		return fmt.Errorf("Error applying view: %w", err)
	}

	logger.Sugar.Infof("Successfully processed view %s on blocks %d -> %d", view.Name, processFromBlockNumber, currentBlockNumber)

	h.processedBlocksMutex.Lock()
	h.viewProcessedBlocks[view.Name].Push(currentBlockNumber)
	h.processedBlocksMutex.Unlock()
	return nil
}

func (h *Host) processBlocks(ctx context.Context) {
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
					logger.Sugar.Errorf("Error processing view %s: %w", view.Name, err)
				}
			}

			// Process primitive attestation records
			err := h.processPrimitiveAttestationRecords(ctx)
			if err != nil {
				logger.Sugar.Errorf("Error processing attestation records on primitives: %w", err)
			}
		}
	}
}

func (h *Host) processPrimitiveAttestationRecords(ctx context.Context) error {
	if !h.hasNewBlocks(ctx) {
		return nil
	}

	currentBlockNumber, err := h.getCurrentBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("Error getting current block number: %w", err)
	}

	// Lock for the entire read-check-write operation to make it atomic
	h.processedBlocksMutex.Lock()
	lastProcessedBlockNumber := h.getMostRecentBlockNumberProcessedUnlocked()
	processFromBlockNumber := lastProcessedBlockNumber + 1
	h.processedBlocksMutex.Unlock()

	logger.Sugar.Infof("Processing attestation records on blocks %d -> %d...", processFromBlockNumber, currentBlockNumber)

	err = h.PostPrimitiveAttestationRecords(ctx, processFromBlockNumber, currentBlockNumber)
	if err != nil {
		return fmt.Errorf("Error applying view: %w", err)
	}

	logger.Sugar.Infof("Successfully processed attestation records on blocks %d -> %d", processFromBlockNumber, currentBlockNumber)

	h.processedBlocksMutex.Lock()
	h.attestationProcessedBlocks.Push(currentBlockNumber)
	h.processedBlocksMutex.Unlock()
	return nil
}
