package host

import (
	"context"
	"fmt"
	"strings"

	"github.com/sourcenetwork/immutable"

	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/event"
)

// startEventListener subscribes to DefraDB's event bus to track all document updates.
func (h *Host) startEventListener(ctx context.Context) error {
	subscription, err := h.DefraNode.DB.Events().Subscribe(event.UpdateName)
	if err != nil {
		return err
	}

	go func() {
		defer h.DefraNode.DB.Events().Unsubscribe(subscription)

		for {
			select {
			case <-ctx.Done():
				logger.Sugar.Info("🛑 Event listener stopped")
				return

			case msg, ok := <-subscription.Message():
				if !ok {
					logger.Sugar.Warn("⚠️  Event subscription closed")
					return
				}

				if updateEvent, ok := msg.Data.(event.Update); ok {
					if updateEvent.DocID == "" {
						logger.Sugar.Debugf("📡 Collection-level sync: CollectionID=%s, CID=%s (skipping, no specific document)",
							updateEvent.CollectionID, updateEvent.Cid)
						continue
					}

					if updateEvent.IsRelay {
						logger.Sugar.Debugf("📡 P2P document synced: DocID=%s, CollectionID=%s, CID=%s",
							updateEvent.DocID, updateEvent.CollectionID, updateEvent.Cid)
					} else {
						logger.Sugar.Debugf("📝 Local document updated: DocID=%s, CollectionID=%s, CID=%s",
							updateEvent.DocID, updateEvent.CollectionID, updateEvent.Cid)
					}

					doc, err := h.getDocumentByID(context.Background(), updateEvent.DocID, updateEvent.CollectionID)
					if err != nil {
						logger.Sugar.Debugf("⚠️  Could not fetch document %s: %v (skipping attestation)", updateEvent.DocID, err)
						if h.metrics != nil {
							h.metrics.IncrementDocumentsProcessed()
						}
						continue
					}

					// Enqueue the document for attestation processing
					if h.processingPipeline != nil {
						blockNumber := uint64(0)
						if blockNum, ok := doc["blockNumber"].(float64); ok {
							blockNumber = uint64(blockNum)
						}

						docType := h.getDocTypeFromCollectionID(updateEvent.CollectionID)

						h.processingPipeline.processDocumentDirect(
							updateEvent.DocID,
							docType,
							blockNumber,
							doc,
						)
					} else {
						if h.metrics != nil {
							h.metrics.IncrementDocumentsProcessed()
						}
					}
				}
			}
		}
	}()

	logger.Sugar.Info("✅ Event listener started (handling both P2P and local documents)")
	return nil
}

// getDocumentByID fetches a document from DefraDB by its ID and collection ID.
func (h *Host) getDocumentByID(ctx context.Context, docID string, collectionID string) (map[string]interface{}, error) {
	cols, err := h.DefraNode.DB.GetCollections(ctx, client.CollectionFetchOptions{
		CollectionID: immutable.Some(collectionID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get collections for ID %s: %w", collectionID, err)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("no collection found for ID %s", collectionID)
	}
	col := cols[0]

	docIDParsed, err := client.NewDocIDFromString(docID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse document ID %s: %w", docID, err)
	}

	doc, err := col.Get(ctx, docIDParsed, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get document %s: %w", docID, err)
	}

	docMap, err := doc.ToMap()
	if err != nil {
		return nil, fmt.Errorf("failed to convert document to map: %w", err)
	}

	return docMap, nil
}

// getDocTypeFromCollectionID determines the document type from the collection ID.
func (h *Host) getDocTypeFromCollectionID(collectionID string) string {
	cols, err := h.DefraNode.DB.GetCollections(context.Background(), client.CollectionFetchOptions{
		CollectionID: immutable.Some(collectionID),
	})
	if err != nil {
		logger.Sugar.Warnf("⚠️  Failed to get collections for ID %s: %v", collectionID, err)
		return "unknown"
	}
	if len(cols) == 0 {
		logger.Sugar.Warnf("⚠️  No collection found for ID %s", collectionID)
		return "unknown"
	}

	name := cols[0].Name()

	lowerName := strings.ToLower(name)
	if strings.Contains(lowerName, "transaction") {
		return "Transaction"
	}
	if strings.Contains(lowerName, "log") {
		return "Log"
	}
	if strings.Contains(lowerName, "block") {
		return "Block"
	}

	return name
}
