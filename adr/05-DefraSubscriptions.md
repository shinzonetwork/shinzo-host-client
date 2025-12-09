## Problem Statement

Currently we are polling DefraDB for new blocks and processing them. This is not efficient. I propose we switch to some form of subscription to DefraDB to be notified of new blocks.

```go
func (h *Host) subscribeToNewBlocks(ctx context.Context) (<-chan uint64, error) {
    // Subscribe to new Block documents
    subscription := `
        subscription {
            Block {
                number
            }
        }
    `
    
    blockChan := make(chan uint64, 100)
    
    // DefraDB subscription - triggers only when new blocks arrive
    err := h.DefraNode.Subscribe(ctx, subscription, func(block attestation.Block) {
        select {
        case blockChan <- block.Number:
        case <-ctx.Done():
            return
        }
    })
    
    return blockChan, err
}
```

Then we can support event driven processing of blocks.

```go
func (h *Host) processAllViewsEventDriven(ctx context.Context) {
    blockChan, err := h.subscribeToNewBlocks(ctx)
    if err != nil {
        logger.Sugar.Errorf("Failed to subscribe to blocks: %v", err)
        return
    }
    
    for {
        select {
        case <-ctx.Done():
            return
        case newBlockNumber := <-blockChan:
            // Only process when NEW blocks actually arrive
            h.mostRecentBlockReceived = newBlockNumber
            
            // Process all views for this new block
            for _, view := range h.HostedViews {
                err := h.processView(ctx, &view)
                if err != nil {
                    logger.Sugar.Errorf("Error processing view %s: %v", view.Name, err)
                }
            }
        }
    }
}
```