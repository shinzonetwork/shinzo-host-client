package host

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	attestationService "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/immutable"
)

// attestedBlocks tracks which blocks already have an attestation record (in-memory, for logging only).
var (
	attestedBlocks sync.Map  //nolint:gochecknoglobals
	startTime      time.Time //nolint:gochecknoglobals,unused
)

// Document represents a document from DefraDB.
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

// processAttestationEventsWithSubscription starts DefraDB event listeners.
func (h *Host) processAttestationEventsWithSubscription(ctx context.Context) {
	logger.Sugar.Info("Starting DefraDB event listener")
	// Start event bus listener - handles both metrics AND attestation creation for all P2P docs
	go h.startEventBusListener(ctx)
	logger.Sugar.Info("Event bus listener started")
	// Wait for context cancellation
	<-ctx.Done()
	logger.Sugar.Info("Event listeners stopped")
}

// Known collection IDs - stored at startup for direct comparison.
var (
	blockSigCollectionID    string //nolint:gochecknoglobals
	blockCollectionID       string //nolint:gochecknoglobals
	transactionCollectionID string //nolint:gochecknoglobals
	logCollectionID         string //nolint:gochecknoglobals
	accessListCollectionID  string //nolint:gochecknoglobals
)

// collectionIDToName - mapping for ID to Name.
var collectionIDToName = map[string]string{ //nolint:gochecknoglobals
	blockSigCollectionID:    constants.CollectionBlockSignature,
	blockCollectionID:       constants.CollectionBlock,
	transactionCollectionID: constants.CollectionTransaction,
	logCollectionID:         constants.CollectionLog,
	accessListCollectionID:  constants.CollectionAccessListEntry,
}

// collectionsWithMetrics - mapping collection to bool.
var collectionsWithMetrics = map[string]bool{ //nolint:gochecknoglobals
	constants.CollectionBlockSignature:  true,
	constants.CollectionTransaction:     true,
	constants.CollectionLog:             true,
	constants.CollectionAccessListEntry: true,
}

// initKnownCollectionIDs fetches the CollectionIDs for collections we care about at startup.
func (h *Host) initKnownCollectionIDs(ctx context.Context) error {
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		return ErrDefraNodeUnavailable
	}

	// Get BlockSignature collection ID
	cols, err := h.DefraNode.DB.GetCollections(ctx)
	if err != nil {
		return fmt.Errorf("failed to get collections: %w", err)
	}

	for _, col := range cols {
		switch col.Name() {
		case constants.CollectionBlockSignature:
			blockSigCollectionID = col.CollectionID()
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

	logger.Sugar.Infof("Initialized %d known collection IDs", knownCollectionIDs)
	return nil
}

// docEvent represents a document event to be processed.
type docEvent struct {
	docID          string
	collectionName string
}

// docQueue is the unified queue for all document processing with drop-oldest backpressure.
var docQueue chan docEvent //nolint:gochecknoglobals

// initDocQueue initializes the document queue with config values.
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

// enqueueDoc adds a document to the processing queue with drop-oldest backpressure.
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
			if evt.collectionName == constants.CollectionBlockSignature {
				h.processBlockSignatureFromEventBus(ctx, evt.docID)
			}
		}
	}
}

// startEventBusListener subscribes to DefraDB's event bus to track document metrics.
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

			if msg.Name == event.UpdateName {
				update, ok := msg.Data.(event.Update)
				if !ok || !update.IsRelay {
					continue
				}

				if h.metrics != nil {
					h.metrics.IncrementDocumentsReceived()
				}

				collectionName, known := collectionIDToName[update.CollectionID]
				if !known {
					continue
				}

				if collectionsWithMetrics[collectionName] && h.metrics != nil {
					h.metrics.IncrementDocumentByType(collectionName)
				}
				if collectionName == constants.CollectionBlockSignature {
					enqueueDoc(docEvent{docID: update.DocID, collectionName: collectionName})
				}
				if h.pruneQueue != nil {
					h.pruneQueue.Push(collectionName, update.DocID)
				}
			}
		}
	}
}

// processBlockSignatureFromEventBus fetches a BlockSignature document by DocID and processes it.
func (h *Host) processBlockSignatureFromEventBus(ctx context.Context, docID string) {
	startTime = time.Now()
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		return
	}

	col, err := h.DefraNode.DB.GetCollectionByName(ctx, constants.CollectionBlockSignature)
	if err != nil {
		logger.Sugar.Warnf("Failed to get BlockSignature collection: %v", err)
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
			case <-time.After(attestationRetryDelayMs * time.Millisecond):
			}
		}
	}

	if err != nil || doc == nil {
		logger.Sugar.Warnf("Failed to fetch BlockSignature doc %s after %d retries: %v", docID, maxRetries, err)
		return
	}

	h.processBlockSignatureDocument(ctx, doc)
}

// extractBlockSignatureCore extracts blockNumber and merkleRoot, returning a partial BlockSignature or error.
func extractBlockSignatureCore(doc *client.Document) (*attestationService.BlockSignature, int64, error) {
	blockNumberVal, err := doc.Get("blockNumber")
	if err != nil {
		return nil, 0, fmt.Errorf("missing blockNumber: %w", err)
	}
	blockNumber, ok := blockNumberVal.(int64)
	if !ok {
		if f, ok := blockNumberVal.(float64); ok {
			blockNumber = int64(f)
		}
	}

	merkleRootVal, err := doc.Get("merkleRoot")
	if err != nil {
		return nil, blockNumber, fmt.Errorf("block %d missing merkleRoot: %w", blockNumber, err)
	}
	merkleRoot, ok := merkleRootVal.(string)
	if !ok || merkleRoot == "" {
		return nil, blockNumber, fmt.Errorf("block %d: %w", blockNumber, ErrEmptyMerkleRoot)
	}

	return &attestationService.BlockSignature{
		BlockNumber: blockNumber,
		MerkleRoot:  merkleRoot,
	}, blockNumber, nil
}

// populateBlockSignatureFields fills in optional string/int fields on a BlockSignature from a document.
func populateBlockSignatureFields(doc *client.Document, blockSig *attestationService.BlockSignature) {
	if val, err := doc.Get("blockHash"); err == nil {
		if s, ok := val.(string); ok {
			blockSig.BlockHash = s
		}
	}
	if val, err := doc.Get("cidCount"); err == nil {
		switch v := val.(type) {
		case int64:
			blockSig.CIDCount = int(v)
		case float64:
			blockSig.CIDCount = int(v)
		}
	}
	if val, err := doc.Get("signatureType"); err == nil {
		if s, ok := val.(string); ok {
			blockSig.SignatureType = s
		}
	}
	if val, err := doc.Get("signatureIdentity"); err == nil {
		if s, ok := val.(string); ok {
			blockSig.SignatureIdentity = s
		}
	}
	if val, err := doc.Get("signatureValue"); err == nil {
		if s, ok := val.(string); ok {
			blockSig.SignatureValue = s
		}
	}
	if val, err := doc.Get("createdAt"); err == nil {
		if s, ok := val.(string); ok {
			blockSig.CreatedAt = s
		}
	}
}

// populateBlockSignatureCIDs extracts the CID list from a document into a BlockSignature.
func populateBlockSignatureCIDs(doc *client.Document, blockSig *attestationService.BlockSignature) {
	cidVal, err := doc.Get("cids")
	if err != nil || cidVal == nil {
		return
	}
	switch v := cidVal.(type) {
	case []string:
		blockSig.CIDs = v
	case []immutable.Option[string]:
		for _, item := range v {
			if item.HasValue() {
				blockSig.CIDs = append(blockSig.CIDs, item.Value())
			}
		}
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok {
				blockSig.CIDs = append(blockSig.CIDs, s)
			}
		}
	}
}

// verifyBlockSignature verifies the signature and CID list, updating metrics on failure.
// Returns false if verification fails and processing should stop.
func (h *Host) verifyBlockSignature(blockSig *attestationService.BlockSignature) bool {
	if err := h.blockSignatureVerifier.VerifyBlockSignature(blockSig); err != nil {
		logger.Sugar.Warnf("Invalid block signature for block %d: %v", blockSig.BlockNumber, err)
		if h.metrics != nil {
			h.metrics.IncrementSignatureFailures()
		}
		return false
	}

	if len(blockSig.CIDs) > 0 {
		cidMatch, cidErr := h.blockSignatureVerifier.VerifyCIDListAgainstMerkleRoot(blockSig)
		if cidErr != nil {
			logger.Sugar.Warnf("Block %d: CID list verification error: %v", blockSig.BlockNumber, cidErr)
		} else if !cidMatch {
			logger.Sugar.Warnf("Block %d: CID list does NOT match Merkle root", blockSig.BlockNumber)
			if h.metrics != nil {
				h.metrics.IncrementSignatureFailures()
			}
			return false
		}
	}

	return true
}

// processBlockSignatureDocument extracts fields from a client.Document and processes them.
func (h *Host) processBlockSignatureDocument(ctx context.Context, doc *client.Document) {
	startTime := time.Now()

	blockSig, blockNumber, err := extractBlockSignatureCore(doc)
	if err != nil {
		logger.Sugar.Warnf("BlockSignature extraction failed: %v (block %d)", err, blockNumber)
		return
	}

	populateBlockSignatureFields(doc, blockSig)
	populateBlockSignatureCIDs(doc, blockSig)

	if h.blockSignatureVerifier != nil {
		if !h.verifyBlockSignature(blockSig) {
			return
		}

		if h.metrics != nil {
			h.metrics.IncrementSignatureVerifications()
		}

		h.blockSignatureVerifier.AddBlockSignature(blockSig)
		h.processAttestationsFromBlockSignature(ctx, blockSig)
	}

	if h.metrics != nil {
		h.metrics.UpdateLastProcessingTime(float64(time.Since(startTime).Milliseconds()))
	}
}

// processAttestationsFromBlockSignature creates or updates an attestation record for a block.
func (h *Host) processAttestationsFromBlockSignature(ctx context.Context, blockSig *attestationService.BlockSignature) {
	if h.DefraNode == nil {
		return
	}

	blockNumber := blockSig.BlockNumber
	blockAttestedID := fmt.Sprintf("block:%d:%s", blockNumber, blockSig.MerkleRoot)

	if len(blockSig.CIDs) == 0 {
		logger.Sugar.Warnf("Skipping attestation for block %d: block signature has no CID list", blockNumber)
		return
	}

	record := &constants.AttestationRecord{
		AttestedDocID: blockAttestedID,
		SourceDocIDs:  []string{blockSig.SignatureIdentity},
		CIDs:          blockSig.CIDs,
		DocType:       "Block",
		VoteCount:     1,
	}

	var lastErr error
	for attempt := range maxAttestationRetries {
		if err := attestationService.PostAttestationRecord(ctx, h.DefraNode, record); err != nil {
			lastErr = err
			if strings.Contains(err.Error(), "transaction conflict") || strings.Contains(err.Error(), "Please retry") {
				time.Sleep(time.Duration(attestationBackoffBase*(1<<attempt)) * time.Millisecond)
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
			logger.Sugar.Infof("Updated attestation for block %d (indexer: %s)", blockNumber, truncateString(blockSig.SignatureIdentity, identityTruncateLength))
		} else {
			if h.metrics != nil {
				h.metrics.IncrementAttestationsCreated()
				h.metrics.IncrementDocumentByType(constants.CollectionBlock)
			}
			logger.Sugar.Infof("Created attestation for block %d (indexer: %s)", blockNumber, truncateString(blockSig.SignatureIdentity, identityTruncateLength))
		}
		if h.metrics != nil {
			h.metrics.UpdateMostRecentBlock(uint64(blockNumber)) //nolint:gosec // block numbers are always positive
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
