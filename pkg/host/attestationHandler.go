package host

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	attestationService "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/event"
)

// attestedBlocks tracks which blocks already have an attestation record (in-memory, for logging only).
var attestedBlocks sync.Map

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
	logger.Sugar.Info("Starting DefraDB event listener")
	// Start event bus listener - handles both metrics AND attestation creation for all P2P docs
	go h.startEventBusListener(ctx)
	logger.Sugar.Info("Event bus listener started")
	// Wait for context cancellation
	<-ctx.Done()
	logger.Sugar.Info("Event listeners stopped")
}

// Known collection IDs - stored at startup for direct comparison
var (
	batchSigCollectionID    string
	blockCollectionID       string
	transactionCollectionID string
	logCollectionID         string
	accessListCollectionID  string
)

// initKnownCollectionIDs fetches the CollectionIDs for collections we care about at startup.
func (h *Host) initKnownCollectionIDs(ctx context.Context) error {
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		return fmt.Errorf("DefraNode not available")
	}

	// Get BatchSignature collection ID
	cols, err := h.DefraNode.DB.GetCollections(ctx)
	if err != nil {
		return fmt.Errorf("failed to get collections: %w", err)
	}

	for _, col := range cols {
		switch col.Name() {
		case constants.CollectionBatchSignature:
			batchSigCollectionID = col.CollectionID()
		case constants.CollectionBlock:
			blockCollectionID = col.CollectionID()
		case constants.CollectionTransaction:
			transactionCollectionID = col.CollectionID()
		case constants.CollectionLog:
			logCollectionID = col.CollectionID()
		case constants.CollectionAccessListEntry:
			accessListCollectionID = col.CollectionID()
		}
	}

	logger.Sugar.Infof("Initialized %d known collection IDs", 5)
	return nil
}

// docEvent represents a document event to be processed
type docEvent struct {
	docID          string
	collectionName string
}

// docQueue is the unified queue for all document processing with drop-oldest backpressure
var docQueue chan docEvent

// initDocQueue initializes the document queue with config values
func (h *Host) initDocQueue() (workerCount, queueSize int) {
	queueSize = h.config.Shinzo.DocQueueSize
	if queueSize <= 0 {
		queueSize = 5000
	}
	workerCount = h.config.Shinzo.DocWorkerCount
	if workerCount <= 0 {
		workerCount = 16
	}
	docQueue = make(chan docEvent, queueSize)
	return workerCount, queueSize
}

// enqueueDoc adds a document to the processing queue with drop-oldest backpressure
func enqueueDoc(evt docEvent) {
	for {
		select {
		case docQueue <- evt:
			return
		default:
			select {
			case <-docQueue:
			default:
			}
		}
	}
}

// docWorker processes documents from the unified queue.
func (h *Host) docWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt := <-docQueue:
			switch evt.collectionName {
			case constants.CollectionBatchSignature:
				h.processBatchSignatureFromEventBus(ctx, evt.docID)
			}
		}
	}
}

// startEventBusListener subscribes to DefraDB's event bus to track document metrics
func (h *Host) startEventBusListener(ctx context.Context) {
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		logger.Sugar.Warn("DefraNode not available, skipping event bus listener")
		return
	}

	if err := h.initKnownCollectionIDs(ctx); err != nil {
		logger.Sugar.Errorf("Failed to initialize known collection IDs: %v", err)
	}

	workerCount, queueSize := h.initDocQueue()
	for range workerCount {
		go h.docWorker(ctx)
	}
	logger.Sugar.Infof("Started %d document workers (queue: %d)", workerCount, queueSize)

	// Subscribe to document update events
	updateSub, err := h.DefraNode.DB.Events().Subscribe(event.UpdateName)
	if err != nil {
		logger.Sugar.Errorf("Failed to subscribe to DefraDB events: %v", err)
		return
	}

	logger.Sugar.Info("DefraDB event bus listener started")

	for {
		select {
		case <-ctx.Done():
			logger.Sugar.Info("Event bus listener stopped")
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

				if h.metrics != nil {
					h.metrics.IncrementDocumentsReceived()
				}

				switch update.CollectionID {
				case batchSigCollectionID:
					if h.metrics != nil {
						h.metrics.IncrementDocumentByType(constants.CollectionBatchSignature)
					}
					enqueueDoc(docEvent{docID: update.DocID, collectionName: constants.CollectionBatchSignature})
					if h.pruneQueue != nil {
						h.pruneQueue.Push(constants.CollectionBatchSignature, update.DocID)
					}
				case blockCollectionID:
					if h.pruneQueue != nil {
						h.pruneQueue.Push(constants.CollectionBlock, update.DocID)
					}
				case transactionCollectionID:
					if h.metrics != nil {
						h.metrics.IncrementDocumentByType(constants.CollectionTransaction)
					}
					if h.pruneQueue != nil {
						h.pruneQueue.Push(constants.CollectionTransaction, update.DocID)
					}
				case logCollectionID:
					if h.metrics != nil {
						h.metrics.IncrementDocumentByType(constants.CollectionLog)
					}
					if h.pruneQueue != nil {
						h.pruneQueue.Push(constants.CollectionLog, update.DocID)
					}
				case accessListCollectionID:
					if h.metrics != nil {
						h.metrics.IncrementDocumentByType(constants.CollectionAccessListEntry)
					}
					if h.pruneQueue != nil {
						h.pruneQueue.Push(constants.CollectionAccessListEntry, update.DocID)
					}
				}
			}
		}
	}
}

// processBatchSignatureFromEventBus fetches a BatchSignature document by DocID and processes it.
func (h *Host) processBatchSignatureFromEventBus(ctx context.Context, docID string) {
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		return
	}

	col, err := h.DefraNode.DB.GetCollectionByName(ctx, constants.CollectionBatchSignature)
	if err != nil {
		logger.Sugar.Warnf("Failed to get BatchSignature collection: %v", err)
		return
	}

	docIDTyped, err := client.NewDocIDFromString(docID)
	if err != nil {
		return
	}

	var doc *client.Document
	maxRetries := 10

	for attempt := range maxRetries {
		doc, err = col.Get(ctx, docIDTyped)
		if err == nil && doc != nil {
			break
		}
		if attempt < maxRetries-1 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	}

	if err != nil || doc == nil {
		logger.Sugar.Warnf("Failed to fetch BatchSignature doc %s after %d retries: %v", docID, maxRetries, err)
		return
	}

	h.processBatchSignatureDocument(ctx, doc)
}

// processBatchSignatureDocument extracts fields from a client.Document and processes them.
func (h *Host) processBatchSignatureDocument(ctx context.Context, doc *client.Document) {
	blockNumberVal, err := doc.Get("blockNumber")
	if err != nil {
		logger.Sugar.Warnf("BatchSignature missing blockNumber: %v", err)
		return
	}
	blockNumber, ok := blockNumberVal.(int64)
	if !ok {
		if f, ok := blockNumberVal.(float64); ok {
			blockNumber = int64(f)
		}
	}

	merkleRootVal, err := doc.Get("merkleRoot")
	if err != nil {
		logger.Sugar.Warnf("BatchSignature for block %d missing merkleRoot: %v", blockNumber, err)
		return
	}
	merkleRoot, ok := merkleRootVal.(string)
	if !ok || merkleRoot == "" {
		logger.Sugar.Warnf("BatchSignature for block %d has empty merkleRoot", blockNumber)
		return
	}

	batchSig := &attestationService.BatchSignature{
		BlockNumber: blockNumber,
		MerkleRoot:  merkleRoot,
	}

	if val, err := doc.Get("blockHash"); err == nil {
		if s, ok := val.(string); ok {
			batchSig.BlockHash = s
		}
	}
	if val, err := doc.Get("cidCount"); err == nil {
		switch v := val.(type) {
		case int64:
			batchSig.CIDCount = int(v)
		case float64:
			batchSig.CIDCount = int(v)
		}
	}
	if val, err := doc.Get("signatureType"); err == nil {
		if s, ok := val.(string); ok {
			batchSig.SignatureType = s
		}
	}
	if val, err := doc.Get("signatureIdentity"); err == nil {
		if s, ok := val.(string); ok {
			batchSig.SignatureIdentity = s
		}
	}
	if val, err := doc.Get("signatureValue"); err == nil {
		if s, ok := val.(string); ok {
			batchSig.SignatureValue = s
		}
	}
	if val, err := doc.Get("createdAt"); err == nil {
		if s, ok := val.(string); ok {
			batchSig.CreatedAt = s
		}
	}
	if val, err := doc.Get("cids"); err == nil && val != nil {
		switch v := val.(type) {
		case []string:
			batchSig.CIDs = v
		case []any:
			for _, item := range v {
				if s, ok := item.(string); ok {
					batchSig.CIDs = append(batchSig.CIDs, s)
				}
			}
		}
	}

	if h.batchSignatureVerifier != nil {
		if err := h.batchSignatureVerifier.VerifyBatchSignature(ctx, batchSig); err != nil {
			logger.Sugar.Warnf("Invalid batch signature for block %d: %v", batchSig.BlockNumber, err)
			if h.metrics != nil {
				h.metrics.IncrementSignatureFailures()
			}
			return
		}

		// Verify CID list against merkle root (if present)
		if len(batchSig.CIDs) > 0 {
			cidMatch, cidErr := h.batchSignatureVerifier.VerifyCIDListAgainstMerkleRoot(batchSig)
			if cidErr != nil {
				logger.Sugar.Warnf("Block %d: CID list verification error: %v", batchSig.BlockNumber, cidErr)
			} else if !cidMatch {
				logger.Sugar.Warnf("Block %d: CID list does NOT match Merkle root", batchSig.BlockNumber)
				if h.metrics != nil {
					h.metrics.IncrementSignatureFailures()
				}
				return
			}
		}

		if h.metrics != nil {
			h.metrics.IncrementSignatureVerifications()
		}

		h.batchSignatureVerifier.AddBatchSignature(batchSig)

		h.processAttestationsFromBatchSignature(ctx, batchSig)
	}
}

// processAttestationsFromBatchSignature creates or updates an attestation record for a block.
func (h *Host) processAttestationsFromBatchSignature(ctx context.Context, batchSig *attestationService.BatchSignature) {
	if h.DefraNode == nil {
		return
	}

	blockNumber := batchSig.BlockNumber
	blockAttestedID := fmt.Sprintf("block:%d:%s", blockNumber, batchSig.MerkleRoot)

	if len(batchSig.CIDs) == 0 {
		logger.Sugar.Warnf("Skipping attestation for block %d: batch signature has no CID list", blockNumber)
		return
	}

	record := &constants.AttestationRecord{
		AttestedDocId: blockAttestedID,
		SourceDocId:   batchSig.SignatureIdentity,
		CIDs:          batchSig.CIDs,
		DocType:       "Block",
		VoteCount:     1,
	}

	var lastErr error
	for attempt := range 5 {
		if err := attestationService.PostAttestationRecord(ctx, h.DefraNode, record); err != nil {
			lastErr = err
			if strings.Contains(err.Error(), "transaction conflict") || strings.Contains(err.Error(), "Please retry") {
				time.Sleep(time.Duration(10*(1<<attempt)) * time.Millisecond)
				continue
			}
			// Non-retryable error
			logger.Sugar.Warnf("Failed to post attestation for block %d: %v", blockNumber, err)
			if h.metrics != nil {
				h.metrics.IncrementAttestationErrors()
			}
			return
		}
		// Success
		if _, existed := attestedBlocks.LoadOrStore(blockNumber, true); existed {
			logger.Sugar.Infof("Updated attestation for block %d (indexer: %s)", blockNumber, truncateString(batchSig.SignatureIdentity, 16))
		} else {
			if h.metrics != nil {
				h.metrics.IncrementAttestationsCreated()
				h.metrics.IncrementDocumentByType(constants.CollectionBlock)
			}
			logger.Sugar.Infof("Created attestation for block %d (indexer: %s)", blockNumber, truncateString(batchSig.SignatureIdentity, 16))
		}
		if h.metrics != nil {
			h.metrics.UpdateMostRecentBlock(uint64(blockNumber))
		}
		return
	}

	logger.Sugar.Warnf("Failed to post attestation for block %d after retries: %v", blockNumber, lastErr)
	if h.metrics != nil {
		h.metrics.IncrementAttestationErrors()
	}
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
