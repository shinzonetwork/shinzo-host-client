package host

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	attestationService "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/immutable"
)

// attestedBlocks tracks which blocks already have an attestation record (in-memory, for logging only).
var attestedBlocks sync.Map //nolint:gochecknoglobals

// processAttestationEventsWithSubscription runs the attestation listener on updates, a
// subscription to DefraDB's update events, and returns when the listener stops.
func (h *Host) processAttestationEventsWithSubscription(ctx context.Context, updates event.Subscription) {
	logger.Sugar.Info("Starting DefraDB event listener")
	h.startEventBusListener(ctx, updates)
	logger.Sugar.Info("Event listeners stopped")
}

// initKnownCollectionIDs maps the IDs of the chain's collections to their names. Update events
// carry only a collection ID.
func (h *Host) initKnownCollectionIDs(ctx context.Context) error {
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		return ErrDefraNodeUnavailable
	}

	cols, err := h.DefraNode.DB.GetCollections(ctx)
	if err != nil {
		return fmt.Errorf("failed to get collections: %w", err)
	}

	c := h.collections
	known := []string{c.Block.Name, c.Transaction.Name, c.Log.Name, c.AccessListEntry.Name, c.BlockSignature.Name, c.AttestationRecord.Name}
	h.collectionNames = make(map[string]string, len(known))
	for _, col := range cols {
		if slices.Contains(known, col.Name()) {
			h.collectionNames[col.CollectionID()] = col.Name()
		}
	}

	logger.Sugar.Infof("Initialized %d known collection IDs", len(h.collectionNames))
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
			if evt.collectionName == h.collections.BlockSignature.Name {
				h.processBlockSignatureFromEventBus(ctx, evt.docID)
			}
		}
	}
}

// startEventBusListener reads update events from updates: it counts the documents peers send and
// queues block signatures for attestation. It closes updates when it stops.
func (h *Host) startEventBusListener(ctx context.Context, updates event.Subscription) {
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		logger.Sugar.Warn("DefraNode not available, skipping event bus listener")
		return
	}
	defer defradb.CloseSubscription(h.DefraNode.DB.Events(), updates)

	if err := h.initKnownCollectionIDs(ctx); err != nil {
		logger.Sugar.Errorf("Failed to initialize known collection IDs: %v", err)
	}

	workerCount, queueSize := h.initDocQueue()
	for range workerCount {
		go h.docWorker(ctx)
	}
	logger.Sugar.Infof("Started %d document workers (queue: %d)", workerCount, queueSize)

	logger.Sugar.Info("DefraDB event bus listener started")

	for {
		select {
		case <-ctx.Done():
			logger.Sugar.Info("Event bus listener stopped")
			return

		case msg, ok := <-updates.Message():
			if !ok {
				logger.Sugar.Warn("Event bus channel closed")
				return
			}

			if msg.Name == event.UpdateName {
				update, ok := msg.Data.(event.Update)
				if !ok {
					continue
				}

				collectionName, known := h.collectionNames[update.CollectionID]
				if !known {
					continue
				}

				// Local writes skip the metric and verification: "documents received"
				// only counts inbound peer traffic, and a BlockSignature only needs
				// verification when a peer sent it.
				if !update.IsRelay {
					continue
				}

				if h.metrics != nil {
					h.metrics.IncrementDocumentsReceived()
					switch collectionName {
					case h.collections.Transaction.Name:
						h.metrics.IncrementTransactionsProcessed()
					case h.collections.Log.Name:
						h.metrics.IncrementLogsProcessed()
					case h.collections.AccessListEntry.Name:
						h.metrics.IncrementAccessListsProcessed()
					case h.collections.BlockSignature.Name:
						h.metrics.IncrementBlockSignaturesProcessed()
					}
				}
				if collectionName == h.collections.BlockSignature.Name {
					enqueueDoc(docEvent{docID: update.DocID, collectionName: collectionName})
				}
			}
		}
	}
}

// processBlockSignatureFromEventBus fetches a BlockSignature document by DocID and processes it.
func (h *Host) processBlockSignatureFromEventBus(ctx context.Context, docID string) {
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		return
	}

	col, err := h.DefraNode.DB.GetCollectionByName(ctx, h.collections.BlockSignature.Name)
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
		doc, err = col.GetDocument(ctx, docIDTyped)
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

		// A verified peer signature is the host observing that indexer attesting.
		if h.attesters != nil {
			h.attesters.Observe(blockSig.SignatureIdentity)
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
		DocType:       docTypeBlock,
		VoteCount:     1,
		BlockNumber:   &blockNumber,
	}

	var lastErr error
	for attempt := range maxAttestationRetries {
		if err := attestationService.PostAttestationRecord(ctx, h.DefraNode, h.collections.AttestationRecord.Name, record); err != nil {
			if errors.Is(err, attestationService.ErrDocumentNotFound) {
				logger.Sugar.Infof("Skipping attestation for block %d: the pruner deleted its record because the block is at or below the retention cutoff", blockNumber)
				return
			}
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
				h.metrics.IncrementBlocksProcessed()
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
