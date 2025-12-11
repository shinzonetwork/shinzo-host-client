package host

import (
	"context"
	"fmt"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	hostAttestation "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
)

// Document represents a document from DefraDB
type Document struct {
	ID          string
	Type        string
	BlockNumber uint64
	Data        map[string]interface{}
}

// processDocumentAttestation handles attestation processing for a single document
func (h *Host) processDocumentAttestation(ctx context.Context, docID string, docType string, blockNumber uint64, docData map[string]interface{}) error {
	logger.Sugar.Debugf("🔍 Processing %s document %s for attestation (block %d)", docType, docID, blockNumber)

	// Create Document struct
	document := Document{
		ID:          docID,
		Type:        docType,
		BlockNumber: blockNumber,
		Data:        docData,
	}

	// Handle attestation
	err := h.handleDocumentAttestation(ctx, document, blockNumber)
	if err != nil {
		return fmt.Errorf("failed to create attestation for %s document %s: %w", docType, docID, err)
	}

	// Mark block as processed
	h.attestationRangeTracker.Add(blockNumber)
	logger.Sugar.Debugf("✅ Attestation created for %s document %s", docType, docID)
	return nil
}

// handleDocumentAttestation manages attestations for a single document
func (h *Host) handleDocumentAttestation(ctx context.Context, doc Document, blockNumber uint64) error {
	// Extract version information (signatures) from the document
	versions, err := hostAttestation.ExtractVersionsFromDocument(doc.Data)
	if err != nil {
		return fmt.Errorf("failed to extract versions from document %s: %w", doc.ID, err)
	}

	// Use the attestation handler
	return hostAttestation.HandleDocumentAttestation(ctx, h.DefraNode, doc.ID, doc.Type, versions)
}

// processAttestationEventsWithSubscription starts simple DefraDB subscriptions
func (h *Host) processAttestationEventsWithSubscription(ctx context.Context) {
	logger.Sugar.Info("🚀 Starting simple DefraDB subscriptions")

	// Start simple subscriptions (direct processing)
	go h.subscribeToDocumentType(ctx, "Block")
	go h.subscribeToDocumentType(ctx, "Transaction")
	go h.subscribeToDocumentType(ctx, "Log")
	go h.subscribeToDocumentType(ctx, "AccessListEntry")

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
	case "Block":
		subscription = `subscription { Block { _docID number hash _version { cid signature { value identity type } schemaVersionId } } }`
	case "Transaction":
		subscription = `subscription { Transaction { _docID hash blockNumber _version { cid signature { value identity type } schemaVersionId } } }`
	case "Log":
		subscription = `subscription { Log { _docID address blockNumber _version { cid signature { value identity type } schemaVersionId } } }`
	case "AccessListEntry":
		subscription = `subscription { AccessListEntry { _docID address _version { cid signature { value identity type } schemaVersionId } } }`
	default:
		subscription = fmt.Sprintf(`subscription { %s { _docID __typename _version { cid signature { value identity type } schemaVersionId } } }`, docType)
	}

	// Create DefraDB subscription
	docChan, err := defra.Subscribe[map[string]interface{}](ctx, h.DefraNode, subscription)
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
func (h *Host) processSubscriptionResponse(docType string, gqlResponse map[string]interface{}) {
	if docArray, exists := gqlResponse[docType]; exists {
		if docs, ok := docArray.([]interface{}); ok {
			for _, docInterface := range docs {
				if docMap, ok := docInterface.(map[string]interface{}); ok {
					// Extract document info
					docID := ""
					if id, exists := docMap["_docID"]; exists {
						docID = fmt.Sprintf("%v", id)
					}

					blockNumber := uint64(0)
					if docType == "Block" {
						if num, exists := docMap["number"]; exists {
							if numFloat, ok := num.(float64); ok {
								blockNumber = uint64(numFloat)
							}
						}
					} else if docType == "Transaction" || docType == "Log" {
						if num, exists := docMap["blockNumber"]; exists {
							if numFloat, ok := num.(float64); ok {
								blockNumber = uint64(numFloat)
							}
						}
					}

					if docID != "" {
						// Process directly via pipeline
						go h.processingPipeline.processDocumentDirect(docID, docType, blockNumber, docMap)
					}
				}
			}
		}
	}
}
