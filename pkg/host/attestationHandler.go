package host

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	attestationService "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/event"
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

// processAttestationEventsWithSubscription starts DefraDB event listeners
func (h *Host) processAttestationEventsWithSubscription(ctx context.Context) {
	logger.Sugar.Info("🚀 Starting DefraDB event listener")

	// Start event bus listener - handles both metrics AND attestation creation for all P2P docs
	go h.startEventBusListener(ctx)

	logger.Sugar.Info("✅ Event bus listener started (metrics + attestations via event bus)")

	// Wait for context cancellation
	<-ctx.Done()
	logger.Sugar.Info("🛑 Event listeners stopped")
}

// initCollectionIDMap builds a lookup map from DefraDB's CID-based CollectionID to collection names.
func (h *Host) initCollectionIDMap(ctx context.Context) error {
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		return fmt.Errorf("DefraNode not available")
	}

	collections, err := h.DefraNode.DB.GetCollections(ctx, client.CollectionFetchOptions{})
	if err != nil {
		return fmt.Errorf("failed to get collections: %w", err)
	}

	h.collectionIDMap = make(map[string]string)
	for _, col := range collections {
		collectionID := col.CollectionID()
		collectionName := col.Name()
		h.collectionIDMap[collectionID] = collectionName
	}

	logger.Sugar.Infof("Initialized collection ID map with %d collections", len(h.collectionIDMap))
	return nil
}

// startEventBusListener subscribes to DefraDB's event bus to track document metrics
// This is more efficient than GraphQL subscriptions and tracks P2P-received docs directly
func (h *Host) startEventBusListener(ctx context.Context) {
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		logger.Sugar.Warn("DefraNode not available, skipping event bus listener")
		return
	}

	// Initialize collection ID mapping at startup
	if err := h.initCollectionIDMap(ctx); err != nil {
		logger.Sugar.Errorf("Failed to initialize collection ID map: %v", err)
		// Continue anyway - metrics will show as "unmapped" but event bus will still work
	}

	// Subscribe to document update events
	updateSub, err := h.DefraNode.DB.Events().Subscribe(event.UpdateName)
	if err != nil {
		logger.Sugar.Errorf("Failed to subscribe to DefraDB events: %v", err)
		return
	}

	logger.Sugar.Info("📡 DefraDB event bus listener started (tracking P2P document metrics)")

	for {
		select {
		case <-ctx.Done():
			logger.Sugar.Info("🛑 Event bus listener stopped")
			return

		case msg, ok := <-updateSub.Message():
			if !ok {
				logger.Sugar.Warn("Event bus channel closed")
				return
			}

			switch msg.Name {
			case event.UpdateName:
				update, ok := msg.Data.(event.Update)
				if !ok {
					continue
				}

				if !update.IsRelay {
					continue
				}

				collectionName := h.mapCollectionID(update.CollectionID)

				if h.metrics != nil {
					h.metrics.IncrementDocumentsReceived()
					if collectionName != "" && collectionName != constants.CollectionBatchSignature && collectionName != constants.CollectionBlock {
						h.metrics.IncrementDocumentByType(collectionName)
					} else if collectionName == "" {
						logger.Sugar.Debugf("Unmapped CollectionID: %s (DocID: %s)", update.CollectionID, update.DocID)
					}
				}

				if collectionName == constants.CollectionBatchSignature {
					go h.processBatchSignatureFromEventBus(ctx, update.DocID)
				}
			}
		}
	}
}

// mapCollectionID maps DefraDB's CID-based CollectionID to our collection name
// using the pre-built lookup map initialized at startup
func (h *Host) mapCollectionID(collectionID string) string {
	if h.collectionIDMap == nil {
		return ""
	}
	return h.collectionIDMap[collectionID]
}

// processedBatchSigDocIDs tracks batch signatures we've already processed.
const maxProcessedBatchSigDocIDs = 100000
const maxConcurrentBatchSigProcessing = 8

var processedBatchSigDocIDs sync.Map
var processedBatchSigCount int64
var batchSigProcessingSem = make(chan struct{}, maxConcurrentBatchSigProcessing)

// processBatchSignatureFromEventBus fetches a BatchSignature document by DocID and processes it
func (h *Host) processBatchSignatureFromEventBus(ctx context.Context, docID string) {
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		return
	}

	if _, alreadyProcessing := processedBatchSigDocIDs.LoadOrStore(docID, struct{}{}); alreadyProcessing {
		return
	}

	count := atomic.AddInt64(&processedBatchSigCount, 1)
	if count >= maxProcessedBatchSigDocIDs {
		processedBatchSigDocIDs = sync.Map{}
		atomic.StoreInt64(&processedBatchSigCount, 0)
		logger.Sugar.Infof("Cleared processedBatchSigDocIDs (reached %d entries)", count)
	}

	select {
	case batchSigProcessingSem <- struct{}{}:
		defer func() { <-batchSigProcessingSem }()
	case <-time.After(5 * time.Second):
		processedBatchSigDocIDs.Delete(docID)
		return
	case <-ctx.Done():
		processedBatchSigDocIDs.Delete(docID)
		return
	}

	// Query the batch signature document by _docID with retry logic
	query := fmt.Sprintf(`query {
		%s(filter: {_docID: {_eq: "%s"}}) {
			_docID
			blockNumber
			blockHash
			merkleRoot
			cidCount
			signatureType
			signatureIdentity
			signatureValue
			createdAt
		}
	}`, constants.CollectionBatchSignature, docID)

	var results []map[string]any
	var err error
	maxRetries := 3

	for attempt := range maxRetries {
		results, err = defra.QueryArray[map[string]any](ctx, h.DefraNode, query)
		if err == nil && len(results) > 0 {
			break
		}
		if attempt < maxRetries-1 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}

	if err != nil {
		logger.Sugar.Debugf("Failed to fetch batch signature %s after %d retries: %v", docID, maxRetries, err)
		return
	}

	if len(results) == 0 {
		logger.Sugar.Debugf("BatchSignature %s not found after %d retries", docID, maxRetries)
		return
	}

	sigDoc := results[0]
	h.processBatchSignatureDocument(ctx, sigDoc)
}

// processBatchSignatureDocument extracts fields from a BatchSignature document
// and processes them.
func (h *Host) processBatchSignatureDocument(ctx context.Context, sigDoc map[string]any) {
	blockNumber := int64(0)
	if num, ok := sigDoc["blockNumber"].(float64); ok {
		blockNumber = int64(num)
	}

	merkleRoot, ok := sigDoc["merkleRoot"].(string)
	if !ok || merkleRoot == "" {
		logger.Sugar.Debugf("BatchSignature for block %d has no merkle root, skipping", blockNumber)
		return
	}

	batchSig := &attestationService.BatchSignature{
		BlockNumber: blockNumber,
		MerkleRoot:  merkleRoot,
	}

	if blockHash, ok := sigDoc["blockHash"].(string); ok {
		batchSig.BlockHash = blockHash
	}
	if cidCount, ok := sigDoc["cidCount"].(float64); ok {
		batchSig.CIDCount = int(cidCount)
	}
	if sigType, ok := sigDoc["signatureType"].(string); ok {
		batchSig.SignatureType = sigType
	}
	if sigIdentity, ok := sigDoc["signatureIdentity"].(string); ok {
		batchSig.SignatureIdentity = sigIdentity
	}
	if sigValue, ok := sigDoc["signatureValue"].(string); ok {
		batchSig.SignatureValue = sigValue
	}
	if createdAt, ok := sigDoc["createdAt"].(string); ok {
		batchSig.CreatedAt = createdAt
	}

	if h.batchSignatureVerifier != nil {
		if err := h.batchSignatureVerifier.VerifyBatchSignature(ctx, batchSig); err != nil {
			logger.Sugar.Warnf("Invalid batch signature for block %d: %v", batchSig.BlockNumber, err)
			if h.metrics != nil {
				h.metrics.IncrementSignatureFailures()
			}
			return
		}

		if h.metrics != nil {
			h.metrics.IncrementSignatureVerifications()
		}

		h.batchSignatureVerifier.AddBatchSignature(batchSig)

		h.processAttestationsFromBatchSignature(ctx, batchSig)
	}
}

// processAttestationsFromBatchSignature creates a single attestation record for the entire block
// that was covered by a verified batch signature.
func (h *Host) processAttestationsFromBatchSignature(ctx context.Context, batchSig *attestationService.BatchSignature) {
	if h.DefraNode == nil {
		return
	}

	blockNumber := batchSig.BlockNumber

	blockAttestedID := fmt.Sprintf("block:%d", blockNumber)

	record := &constants.AttestationRecord{
		AttestedDocId: blockAttestedID,
		SourceDocId:   batchSig.SignatureIdentity,
		CIDs:          []string{batchSig.MerkleRoot},
		DocType:       "Block",
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
		h.metrics.IncrementDocumentByType(constants.CollectionBlock)
		h.metrics.IncrementAttestationsCreated()
		h.metrics.UpdateMostRecentBlock(uint64(blockNumber))
	}

	logger.Sugar.Infof("✅ Created block attestation for block %d (indexer: %s, merkle: %s)",
		blockNumber, truncateString(batchSig.SignatureIdentity, 16), truncateString(batchSig.MerkleRoot, 16))
}

// truncateString truncates a string to maxLen and adds "..." if truncated
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
