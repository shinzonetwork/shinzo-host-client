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

	// Start batch signature subscription for batch-signed documents
	go h.subscribeToBatchSignatures(ctx)

	logger.Sugar.Info("✅ All subscriptions started (including batch signatures)")

	// Wait for context cancellation
	<-ctx.Done()
	logger.Sugar.Info("🛑 Subscriptions stopped")
}

// subscribeToBatchSignatures creates a subscription for batch signatures
func (h *Host) subscribeToBatchSignatures(ctx context.Context) {
	logger.Sugar.Info("📡 Starting Ethereum__Mainnet__BatchSignature subscription")

	subscription := `subscription { ` + constants.CollectionBatchSignature + ` {
		_docID
		blockNumber
		blockHash
		merkleRoot
		cidCount
		signatureType
		signatureIdentity
		signatureValue
		createdAt
	} }`

	sigChan, err := defra.Subscribe[map[string]any](ctx, h.DefraNode, subscription)
	if err != nil {
		logger.Sugar.Errorf("❌ Failed to create Ethereum__Mainnet__BatchSignature subscription: %v", err)
		return
	}

	logger.Sugar.Info("✅ Ethereum__Mainnet__BatchSignature subscription created")

	for {
		select {
		case <-ctx.Done():
			logger.Sugar.Info("🛑 Ethereum__Mainnet__BatchSignature subscription stopped")
			return

		case gqlResponse, ok := <-sigChan:
			if !ok {
				logger.Sugar.Warn("⚠️ Ethereum__Mainnet__BatchSignature subscription channel closed")
				return
			}

			h.processBatchSignatureResponse(ctx, gqlResponse)
		}
	}
}

// processBatchSignatureResponse processes batch signatures from subscription
func (h *Host) processBatchSignatureResponse(ctx context.Context, gqlResponse map[string]any) {
	if sigArray, exists := gqlResponse[constants.CollectionBatchSignature]; exists {
		if sigs, ok := sigArray.([]any); ok {
			for _, sigInterface := range sigs {
				if sigMap, ok := sigInterface.(map[string]any); ok {
					batchSig := &attestationService.BatchSignature{}

					if blockNum, ok := sigMap["blockNumber"].(float64); ok {
						batchSig.BlockNumber = int64(blockNum)
					}
					if blockHash, ok := sigMap["blockHash"].(string); ok {
						batchSig.BlockHash = blockHash
					}
					if merkleRoot, ok := sigMap["merkleRoot"].(string); ok {
						batchSig.MerkleRoot = merkleRoot
					}
					if cidCount, ok := sigMap["cidCount"].(float64); ok {
						batchSig.CIDCount = int(cidCount)
					}
					if sigType, ok := sigMap["signatureType"].(string); ok {
						batchSig.SignatureType = sigType
					}
					if sigIdentity, ok := sigMap["signatureIdentity"].(string); ok {
						batchSig.SignatureIdentity = sigIdentity
					}
					if sigValue, ok := sigMap["signatureValue"].(string); ok {
						batchSig.SignatureValue = sigValue
					}
					if createdAt, ok := sigMap["createdAt"].(string); ok {
						batchSig.CreatedAt = createdAt
					}

					if h.batchSignatureVerifier != nil {
						if err := h.batchSignatureVerifier.VerifyBatchSignature(ctx, batchSig); err != nil {
							logger.Sugar.Warnf("❌ Invalid batch signature for block %d: %v", batchSig.BlockNumber, err)
							continue
						}

						h.batchSignatureVerifier.AddBatchSignature(batchSig)
						logger.Sugar.Debugf("✅ Verified and cached batch signature for block %d (%d CIDs)",
							batchSig.BlockNumber, batchSig.CIDCount)

						h.processAttestationsFromBatchSignature(ctx, batchSig)
					}
				}
			}
		}
	}
}

// processAttestationsFromBatchSignature creates a single attestation record for the entire block
// that was covered by a verified batch signature. Since the batch signature already proves
// all documents in the block are valid (via merkle root), we only need one attestation per block.
// The attestation record stores all CIDs from the block, allowing individual document verification.
func (h *Host) processAttestationsFromBatchSignature(ctx context.Context, batchSig *attestationService.BatchSignature) {
	if h.DefraNode == nil {
		return
	}

	blockNumber := batchSig.BlockNumber

	var blockCIDs []string
	if h.blockCIDCollector != nil {
		blockCIDs = h.blockCIDCollector.GetBlockCIDs(blockNumber)

		if len(blockCIDs) > 0 && h.batchSignatureVerifier != nil {
			valid, err := h.batchSignatureVerifier.VerifyCIDsAgainstBatchSignature(blockCIDs, batchSig)
			if err != nil {
				logger.Sugar.Warnf("Failed to verify CIDs against batch signature for block %d: %v", blockNumber, err)
			} else if !valid {
				logger.Sugar.Warnf("CIDs do not match merkle root for block %d (collected %d CIDs, expected %d)",
					blockNumber, len(blockCIDs), batchSig.CIDCount)
			} else {
				logger.Sugar.Debugf("Verified %d CIDs match merkle root for block %d", len(blockCIDs), blockNumber)
			}
		}

		defer h.blockCIDCollector.ClearBlock(blockNumber)
	}

	// Create a single attestation record for this block
	// The batch signature proves all documents in this block are valid
	// AttestedDocId uses a block-level identifier format: "block:{blockNumber}"
	blockAttestedID := fmt.Sprintf("block:%d", blockNumber)

	// Store all document CIDs in the attestation record
	// This allows verifying individual documents against the block attestation
	attestedCIDs := blockCIDs
	if len(attestedCIDs) == 0 {
		attestedCIDs = []string{batchSig.MerkleRoot}
	}

	record := &constants.AttestationRecord{
		AttestedDocId: blockAttestedID,
		SourceDocId:   batchSig.MerkleRoot,
		CIDs:          attestedCIDs,
		DocType:       "Block", // Indicates this is a block-level attestation
		VoteCount:     1,
	}

	if err := attestationService.PostAttestationRecord(ctx, h.DefraNode, record); err != nil {
		logger.Sugar.Warnf("Failed to post block attestation for block %d: %v", blockNumber, err)
		if h.metrics != nil {
			h.metrics.IncrementAttestationErrors()
		}
		return
	}

	if h.metrics != nil {
		h.metrics.IncrementAttestationsCreated()
	}

	logger.Sugar.Infof("✅ Created block attestation for block %d (%d CIDs, merkle root: %s)",
		blockNumber, len(attestedCIDs), truncateString(batchSig.MerkleRoot, 16))
}

// truncateString truncates a string to maxLen and adds "..." if truncated
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
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
		subscription = `subscription { ` + constants.CollectionAccessListEntry + ` { _docID address blockNumber _version { cid signature { value identity type } collectionVersionId } } }`
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
					case constants.CollectionTransaction, constants.CollectionLog, constants.CollectionAccessListEntry:
						if num, exists := docMap["blockNumber"]; exists {
							if numFloat, ok := num.(float64); ok {
								blockNumber = uint64(numFloat)
							}
						}
					}

					if h.blockCIDCollector != nil && blockNumber > 0 {
						if versionData, exists := docMap["_version"]; exists {
							if versionArray, ok := versionData.([]any); ok && len(versionArray) > 0 {
								if firstVersion, ok := versionArray[0].(map[string]any); ok {
									if cid, ok := firstVersion["cid"].(string); ok && cid != "" {
										h.blockCIDCollector.AddDocumentCID(int64(blockNumber), docID, docType, cid)
									} else {
										logger.Sugar.Debugf("No CID in _version for %s doc %s (block %d)", docType, docID, blockNumber)
									}
								}
							} else {
								logger.Sugar.Debugf("_version not array or empty for %s doc %s", docType, docID)
							}
						} else {
							logger.Sugar.Debugf("No _version field for %s doc %s (block %d)", docType, docID, blockNumber)
						}
					} else if h.blockCIDCollector != nil && blockNumber == 0 {
						logger.Sugar.Debugf("blockNumber is 0 for %s doc %s, skipping CID collection", docType, docID)
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
