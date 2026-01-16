package host

import (
	"context"
	"fmt"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	attestationService "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
)

// Document represents a document from DefraDB
type Document struct {
	ID          string
	Type        string
	BlockNumber uint64
	Data        map[string]any
}

// processDocumentAttestationBatch handles attestation processing for multiple documents in a single batch.
func (h *Host) processDocumentAttestationBatch(ctx context.Context, docs []Document) error {
	if len(docs) == 0 {
		return nil
	}

	inputs := make([]attestationService.DocumentAttestationInput, 0, len(docs))
	for _, doc := range docs {
		versions, err := attestationService.ExtractVersionsFromDocument(doc.Data)
		if err != nil {
			continue
		}

		if len(versions) > 0 {
			inputs = append(inputs, attestationService.DocumentAttestationInput{
				DocID:    doc.ID,
				DocType:  doc.Type,
				Versions: versions,
			})
		}
	}

	if len(inputs) == 0 {
		return nil
	}

	maxConcurrentVerifications := h.config.Shinzo.MaxConcurrentVerifications
	if maxConcurrentVerifications <= 0 {
		maxConcurrentVerifications = 50
	}
	return attestationService.HandleDocumentAttestationBatch(ctx, h.signatureVerifier, h.DefraNode, inputs, maxConcurrentVerifications)
}

// processAttestationEventsWithSubscription starts simple DefraDB subscriptions
func (h *Host) processAttestationEventsWithSubscription(ctx context.Context) {
	logger.Sugar.Info("🚀 Starting simple DefraDB subscriptions")

	// Start simple subscriptions (direct processing)
	go h.subscribeToDocumentType(ctx, constants.CollectionBlock)
	go h.subscribeToDocumentType(ctx, constants.CollectionTransaction)
	go h.subscribeToDocumentType(ctx, constants.CollectionLog)
	go h.subscribeToDocumentType(ctx, constants.CollectionAccessListEntry)

	logger.Sugar.Info("✅ All subscriptions started")

	// Wait for context cancellation
	<-ctx.Done()
	logger.Sugar.Info("🛑 Subscriptions stopped")
}

// subscribeToDocumentType creates a simple subscription for a document type
func (h *Host) subscribeToDocumentType(ctx context.Context, docType string) {
	logger.Sugar.Infof("📡 Starting %s subscription", docType)

	// Create subscription query
	var subscription string
	switch docType {
	case constants.CollectionBlock:
		subscription = `subscription { ` + constants.CollectionBlock + ` { _docID number hash _version { cid signature { value identity type } collectionVersionId } } }`
	case constants.CollectionTransaction:
		subscription = `subscription { ` + constants.CollectionTransaction + ` { _docID hash blockNumber _version { cid signature { value identity type } collectionVersionId } } }`
	case constants.CollectionLog:
		subscription = `subscription { ` + constants.CollectionLog + ` { _docID address blockNumber _version { cid signature { value identity type } collectionVersionId } } }`
	case constants.CollectionAccessListEntry:
		subscription = `subscription { ` + constants.CollectionAccessListEntry + ` { _docID address _version { cid signature { value identity type } collectionVersionId } } }`
	default:
		subscription = fmt.Sprintf(`subscription { %s { _docID __typename _version { cid signature { value identity type } collectionVersionId } } }`, docType)
	}

	// Create DefraDB subscription
	docChan, err := defra.Subscribe[map[string]any](ctx, h.DefraNode, subscription)
	if err != nil {
		logger.Sugar.Errorf("❌ Failed to create %s subscription: %v", docType, err)
		return
	}

	logger.Sugar.Infof("✅ %s subscription created", docType)

	// Process events
	for {
		select {
		case <-ctx.Done():
			logger.Sugar.Infof("🛑 %s subscription stopped", docType)
			return

		case gqlResponse, ok := <-docChan:
			if !ok {
				logger.Sugar.Warnf("⚠️ %s subscription channel closed", docType)
				return
			}

			// Process documents directly
			h.processSubscriptionResponse(docType, gqlResponse)
		}
	}
}

// processSubscriptionResponse processes documents from subscription
func (h *Host) processSubscriptionResponse(docType string, gqlResponse map[string]any) {
	if docArray, exists := gqlResponse[docType]; exists {
		if docs, ok := docArray.([]any); ok {
			for _, docInterface := range docs {
				if docMap, ok := docInterface.(map[string]any); ok {
					// Extract document info
					docID := ""
					if id, exists := docMap["_docID"]; exists {
						docID = fmt.Sprintf("%v", id)
					}

					blockNumber := uint64(0)
					switch docType {
					case constants.CollectionBlock:
						if num, exists := docMap["number"]; exists {
							if numFloat, ok := num.(float64); ok {
								blockNumber = uint64(numFloat)
							}
						}
					case constants.CollectionTransaction, constants.CollectionLog:
						if num, exists := docMap["blockNumber"]; exists {
							if numFloat, ok := num.(float64); ok {
								blockNumber = uint64(numFloat)
							}
						}
					}

					if docID != "" {
						// Process via pipeline (enqueued to worker pool)
						h.processingPipeline.processDocumentDirect(docID, docType, blockNumber, docMap)
					}
				}
			}
		}
	}
}
