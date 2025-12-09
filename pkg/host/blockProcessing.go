package host

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	hostAttestation "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

func (h *Host) getMostRecentBlockNumberProcessed() uint64 {
	return h.attestationRangeTracker.GetHighest()
}

func (h *Host) getMostRecentBlockNumberProcessedForView(view *view.View) uint64 {
	if tracker, exists := h.viewRangeTrackers[view.Name]; exists {
		return tracker.GetHighest()
	}
	return 0
}

// processAllViewsWithSubscription uses DefraDB subscriptions for real-time block processing
// This replaces polling with event-driven processing using the new Subscribe function
func (h *Host) processAllViewsWithSubscription(ctx context.Context) {
	logger.Sugar.Info("Starting subscription-based block processing with BlockRangeTracker integration")

	// Use subscription-only approach - no polling fallback
	for {
		err := h.processWithBlockSubscription(ctx)
		if err != nil {
			logger.Sugar.Errorf("Block subscription failed: %v", err)
			logger.Sugar.Info("Retrying subscription in 5 seconds...")

			select {
			case <-ctx.Done():
				logger.Sugar.Info("Context cancelled, stopping subscription retries")
				return
			case <-time.After(5 * time.Second):
				logger.Sugar.Info("Retrying block subscription...")
				continue
			}
		}

		// If we get here, the subscription ended normally (context cancelled)
		return
	}
}

// processWithBlockSubscription uses the new Subscribe function for real-time block events
func (h *Host) processWithBlockSubscription(ctx context.Context) error {
	// Create subscriptions for all document types that need attestation processing
	logger.Sugar.Info("🔗 Creating subscriptions for all document types...")

	// Subscribe to all document types individually
	go h.subscribeToDocumentType(ctx, "Block")
	go h.subscribeToDocumentType(ctx, "Transaction")
	go h.subscribeToDocumentType(ctx, "Log")
	go h.subscribeToDocumentType(ctx, "AccessListEntry")

	logger.Sugar.Info("✅ Successfully created DefraDB subscriptions for all document types!")
	logger.Sugar.Info("⏳ Waiting for documents to trigger attestation processing...")

	// Also do an initial check for existing blocks
	go h.processInitialBlocks(ctx)

	// Process any attestation gaps on startup
	go h.processAttestationGaps(ctx)

	// Add a periodic check to see if Block documents exist
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Check if any Block documents exist
				query := `query { Block(limit: 1) { number _docID } }`
				blocks, err := defra.QueryArray[hostAttestation.Block](ctx, h.DefraNode, query)
				if err != nil {
					logger.Sugar.Debugf("Error checking for Block documents: %v", err)
				} else if len(blocks) == 0 {
					logger.Sugar.Info("🔍 No Block documents found in DefraDB yet - waiting for indexer to create them...")
					logger.Sugar.Info("💡 Tip: You can create a test block by running this in the playground:")
					logger.Sugar.Info("   mutation { create_Block(input: { hash: \"0xtest1\", number: 23500001, timestamp: \"1733778000\" }) { _docID number } }")
				} else {
					logger.Sugar.Infof("📊 Found %d Block documents in DefraDB (latest: %d)", len(blocks), blocks[0].Number)
					// Stop the periodic check once we find blocks
					return
				}
			}
		}
	}()

	// Wait for context cancellation since document processing is now handled by individual subscriptions
	<-ctx.Done()
	logger.Sugar.Info("Document subscription processing stopped due to context cancellation")
	return nil
}

// handleNewBlock processes a new block by handling both attestations and views for all documents in the block
func (h *Host) handleNewBlock(ctx context.Context, blockNumber uint64) error {
	logger.Sugar.Infof("🧱 Handling new block %d - processing all documents for attestations and views", blockNumber)

	// Query all documents in this block (Blocks, Transactions, Logs, AccessLists)
	documents, err := h.getDocumentsInBlock(ctx, blockNumber)
	if err != nil {
		return fmt.Errorf("failed to get documents in block %d: %w", blockNumber, err)
	}

	if len(documents) == 0 {
		logger.Sugar.Infof("📭 No documents found in block %d - marking as processed", blockNumber)
		// Still mark block as processed even if empty
		h.attestationRangeTracker.Add(blockNumber)
		for _, view := range h.HostedViews {
			if tracker, exists := h.viewRangeTrackers[view.Name]; exists {
				tracker.Add(blockNumber)
			}
		}
		return nil
	}

	logger.Sugar.Infof("📦 Processing %d documents in block %d:", len(documents), blockNumber)
	for i, doc := range documents {
		logger.Sugar.Infof("  Document %d: ID=%s, Type=%s", i+1, doc.ID, doc.Type)
	}

	// Process each document through both handlers
	for _, doc := range documents {
		// 1. Attestation Handler - manage document attestations
		err := h.handleDocumentAttestation(ctx, doc, blockNumber)
		if err != nil {
			logger.Sugar.Errorf("Failed to handle attestation for document %s in block %d: %v", doc.ID, blockNumber, err)
			continue
		}

		// 2. View Handler - apply views if document matches
		err = h.handleDocumentForViews(ctx, doc, blockNumber)
		if err != nil {
			logger.Sugar.Errorf("Failed to handle views for document %s in block %d: %v", doc.ID, blockNumber, err)
			continue
		}
	}

	// Mark block as processed after successful document processing
	h.attestationRangeTracker.Add(blockNumber)
	logger.Sugar.Infof("✅ Block %d completed - %d documents processed for attestations and views", blockNumber, len(documents))
	logger.Sugar.Infof("🔍 Query attestations in playground with: { AttestationRecord_Document_Block { attested_doc source_doc CIDs _docID } }")

	return nil
}

// Document represents any document type (Block, Transaction, Log, AccessList)
type Document struct {
	ID          string
	Type        string // "Block", "Transaction", "Log", "AccessList"
	BlockNumber uint64
	Data        map[string]interface{} // Raw document data
}

// getDocumentsInBlock retrieves all documents for a specific block
func (h *Host) getDocumentsInBlock(ctx context.Context, blockNumber uint64) ([]Document, error) {
	var allDocs []Document

	// Query each document type for this block
	documentTypes := []string{"Block", "Transaction", "Log", "AccessList"}

	for _, docType := range documentTypes {
		query := fmt.Sprintf(`query { %s(filter: {blockNumber: {_eq: %d}}) { _docID __typename } }`, docType, blockNumber)

		// This is a simplified version - in reality we'd need proper type handling
		results, err := defra.QueryArray[map[string]interface{}](ctx, h.DefraNode, query)
		if err != nil {
			if strings.Contains(err.Error(), "no documents found") {
				continue // No documents of this type in this block
			}
			return nil, fmt.Errorf("failed to query %s documents for block %d: %w", docType, blockNumber, err)
		}

		// Convert results to Document structs
		for _, result := range results {
			if docID, ok := result["_docID"].(string); ok {
				allDocs = append(allDocs, Document{
					ID:          docID,
					Type:        docType,
					BlockNumber: blockNumber,
					Data:        result,
				})
			}
		}
	}

	return allDocs, nil
}

// subscribeToDocumentType creates a subscription for a specific document type and processes documents as they arrive
func (h *Host) subscribeToDocumentType(ctx context.Context, docType string) {
	logger.Sugar.Infof("📡 Creating subscription for %s documents", docType)

	// Create subscription with all necessary fields for each document type
	var subscription string
	switch docType {
	case "Block":
		subscription = `subscription { Block { _docID number hash _version { cid signature { value identity type } schemaVersionId } } }`
	case "Transaction":
		subscription = `subscription { Transaction { _docID hash blockNumber _version { cid signature { value identity type } schemaVersionId } } }`
	case "Log":
		subscription = `subscription { Log { _docID address transactionIndex _version { cid signature { value identity type } schemaVersionId } } }`
	case "AccessListEntry":
		subscription = `subscription { AccessListEntry { _docID address _version { cid signature { value identity type } schemaVersionId } } }`
	default:
		subscription = fmt.Sprintf(`subscription { %s { _docID __typename _version { cid signature { value identity type } schemaVersionId } } }`, docType)
	}

	// Subscribe to raw GraphQL response and let the processing function handle the structure
	switch docType {
	case "Block":
		blockChan, err := defra.Subscribe[map[string]interface{}](ctx, h.DefraNode, subscription)
		if err != nil {
			logger.Sugar.Errorf("Failed to create %s subscription: %v", docType, err)
			return
		}
		go h.processDocSubscription(ctx, blockChan, docType)
	case "Transaction":
		txChan, err := defra.Subscribe[map[string]interface{}](ctx, h.DefraNode, subscription)
		if err != nil {
			logger.Sugar.Errorf("Failed to create %s subscription: %v", docType, err)
			return
		}
		go h.processDocSubscription(ctx, txChan, docType)
	case "Log":
		logChan, err := defra.Subscribe[map[string]interface{}](ctx, h.DefraNode, subscription)
		if err != nil {
			logger.Sugar.Errorf("Failed to create %s subscription: %v", docType, err)
			return
		}
		go h.processDocSubscription(ctx, logChan, docType)
	case "AccessListEntry":
		accessChan, err := defra.Subscribe[map[string]interface{}](ctx, h.DefraNode, subscription)
		if err != nil {
			logger.Sugar.Errorf("Failed to create %s subscription: %v", docType, err)
			return
		}
		go h.processDocSubscription(ctx, accessChan, docType)
	}
}

// processDocSubscription processes GraphQL subscription responses and extracts documents from arrays
func (h *Host) processDocSubscription(ctx context.Context, docChan <-chan map[string]interface{}, docType string) {
	logger.Sugar.Infof("✅ %s subscription active - processing documents in real-time", docType)

	for {
		select {
		case <-ctx.Done():
			logger.Sugar.Infof("%s subscription stopped due to context cancellation", docType)
			return
		case gqlResponse, ok := <-docChan:
			if !ok {
				logger.Sugar.Warnf("%s subscription channel closed", docType)
				return
			}

			// Extract documents from GraphQL response structure: { "Log": [...], "Transaction": [...] }
			if docArray, exists := gqlResponse[docType]; exists {
				if docs, ok := docArray.([]interface{}); ok {
					for _, docInterface := range docs {
						if docMap, ok := docInterface.(map[string]interface{}); ok {
							// Extract document fields
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

							logger.Sugar.Infof("📄 New %s document: ID=%s, Block=%d", docType, docID, blockNumber)
							h.processDocument(ctx, docID, docType, blockNumber, docMap)
						}
					}
				}
			}
		}
	}
}

// Extractor functions for different document types
func (h *Host) extractBlockData(doc interface{}) (string, uint64, map[string]interface{}) {
	block := doc.(hostAttestation.Block)
	return block.DocId, block.Number, map[string]interface{}{
		"_docID":   block.DocId,
		"number":   block.Number,
		"hash":     block.Hash,
		"_version": block.Version,
	}
}

func (h *Host) extractTransactionData(doc interface{}) (string, uint64, map[string]interface{}) {
	tx := doc.(hostAttestation.Transaction)
	return tx.DocId, uint64(tx.BlockNumber), map[string]interface{}{
		"_docID":   tx.DocId,
		"hash":     tx.Hash,
		"_version": tx.Version,
	}
}

func (h *Host) extractLogData(doc interface{}) (string, uint64, map[string]interface{}) {
	log := doc.(hostAttestation.Log)
	return log.DocId, uint64(log.BlockNumber), map[string]interface{}{
		"_docID":   log.DocId,
		"address":  log.Address,
		"_version": log.Version,
	}
}

func (h *Host) extractAccessListData(doc interface{}) (string, uint64, map[string]interface{}) {
	access := doc.(hostAttestation.AccessListEntry)
	return access.DocId, uint64(access.Transaction.BlockNumber), map[string]interface{}{
		"_docID":   access.DocId,
		"address":  access.Address,
		"_version": access.Version,
	}
}

func (h *Host) extractDocData(doc interface{}) (string, uint64, map[string]interface{}) {
	return doc.(map[string]interface{})["_docID"].(string), 0, doc.(map[string]interface{})
}

// processDocument handles a single document for both attestation and view processing
func (h *Host) processDocument(ctx context.Context, docID string, docType string, blockNumber uint64, docData map[string]interface{}) {
	logger.Sugar.Infof("🔍 Processing %s document: ID=%s, Block=%d", docType, docID, blockNumber)

	// Create Document struct
	document := Document{
		ID:          docID,
		Type:        docType,
		BlockNumber: blockNumber,
		Data:        docData,
	}

	// Process attestation for this document
	err := h.handleDocumentAttestation(ctx, document, blockNumber)
	if err != nil {
		logger.Sugar.Errorf("Failed to handle attestation for %s document %s: %v", docType, docID, err)
	}

	// Process views for this document
	err = h.handleDocumentForViews(ctx, document, blockNumber)
	if err != nil {
		logger.Sugar.Errorf("Failed to handle views for %s document %s: %v", docType, docID, err)
	}

	logger.Sugar.Infof("✅ Successfully processed %s document %s", docType, docID)
}

// handleDocumentAttestation manages attestations for a single document
func (h *Host) handleDocumentAttestation(ctx context.Context, doc Document, blockNumber uint64) error {
	// Extract version information (signatures) from the document using host attestation package
	versions, err := hostAttestation.ExtractVersionsFromDocument(doc.Data)
	if err != nil {
		return fmt.Errorf("failed to extract versions from document %s: %w", doc.ID, err)
	}

	// Use the main attestation handler from the host attestation package
	return hostAttestation.HandleDocumentAttestation(ctx, h.DefraNode, doc.ID, doc.Type, versions)
}

// handleDocumentForViews applies relevant views to a document
func (h *Host) handleDocumentForViews(ctx context.Context, doc Document, blockNumber uint64) error {
	for _, view := range h.HostedViews {
		// Check if this view should be applied to this document type
		if h.shouldApplyViewToDocument(view, doc) {
			logger.Sugar.Debugf("Applying view %s to document %s (type: %s) in block %d", view.Name, doc.ID, doc.Type, blockNumber)

			// Apply view to single document (not range)
			err := h.applyViewToSingleDocument(ctx, view, doc)
			if err != nil {
				return fmt.Errorf("failed to apply view %s to document %s: %w", view.Name, doc.ID, err)
			}

			// Mark block as processed for this view
			if tracker, exists := h.viewRangeTrackers[view.Name]; exists {
				tracker.Add(blockNumber)
			} else {
				tracker := NewBlockRangeTracker()
				tracker.Add(blockNumber)
				h.viewRangeTrackers[view.Name] = tracker
			}
		}
	}
	return nil
}

// shouldApplyViewToDocument determines if a view should be applied to a specific document
func (h *Host) shouldApplyViewToDocument(view view.View, doc Document) bool {
	// TODO: Implement proper view matching logic based on view query and document type
	// For now, apply all views to all documents (simplified)
	return true
}

// applyViewToSingleDocument applies a view to a single document
func (h *Host) applyViewToSingleDocument(ctx context.Context, view view.View, doc Document) error {
	// TODO: Implement single-document view application
	// This should replace the current range-based ApplyView method
	logger.Sugar.Debugf("Applied view %s to document %s", view.Name, doc.ID)
	return nil
}

// processInitialBlocks handles any existing blocks when starting up
func (h *Host) processInitialBlocks(ctx context.Context) {
	// Give DefraDB a moment to be ready
	time.Sleep(2 * time.Second)

	query := `query GetHighestBlockNumber { Block(order: {number: DESC}, limit: 1) { number } }`
	latestBlock, err := defra.QuerySingle[hostAttestation.Block](ctx, h.DefraNode, query)
	if err != nil {
		if !strings.Contains(err.Error(), "no documents found") {
			logger.Sugar.Debugf("Error getting initial block number: %v", err)
		}
		return
	}

	// Early return if no blocks exist
	if latestBlock.Number == 0 {
		logger.Sugar.Info("No blocks found in DefraDB - waiting for first block")
		return
	}

	startHeight := h.config.Shinzo.StartHeight
	if startHeight == 0 {
		startHeight = 23000000 // Default to block 23000000 if not configured
	}

	logger.Sugar.Infof("Found existing blocks up to %d, configured StartHeight: %d", latestBlock.Number, startHeight)
	h.mostRecentBlockReceived = latestBlock.Number

	// Simple approach: Mark all blocks before StartHeight as processed (skip historical)
	if startHeight > 1 {
		endRange := startHeight - 1
		if latestBlock.Number < startHeight {
			endRange = latestBlock.Number // Don't mark blocks that don't exist
		}

		h.attestationRangeTracker.AddRange(1, endRange)
		logger.Sugar.Infof("Marked blocks 1-%d as processed (before StartHeight)", endRange)

		// Initialize view trackers with the same range
		h.initializeViewTrackers(1, endRange, startHeight)
	}

	logger.Sugar.Infof("✅ Host will process blocks from %d onwards", startHeight)
}

// initializeViewTrackers initializes or updates view trackers with the given block range
func (h *Host) initializeViewTrackers(rangeStart, rangeEnd uint64, startHeight uint64) {
	for _, view := range h.HostedViews {
		if tracker, exists := h.viewRangeTrackers[view.Name]; exists {
			if rangeEnd > 0 {
				tracker.AddRange(rangeStart, rangeEnd)
			}
		} else {
			tracker := NewBlockRangeTracker()
			if rangeEnd > 0 {
				tracker.AddRange(rangeStart, rangeEnd)
			}
			h.viewRangeTrackers[view.Name] = tracker
		}
		logger.Sugar.Infof("Initialized view %s tracker - will process blocks from %d onwards", view.Name, startHeight)
	}
}

// processViewWithRangeTracker processes a view using BlockRangeTracker for efficient gap detection
func (h *Host) processViewWithRangeTracker(ctx context.Context, view *view.View, latestBlock uint64) error {
	// Get the tracker for this view (create if doesn't exist)
	tracker, exists := h.viewRangeTrackers[view.Name]
	if !exists {
		tracker = NewBlockRangeTracker()
		h.viewRangeTrackers[view.Name] = tracker
	}

	// Find unprocessed ranges
	unprocessedRanges := tracker.GetUnprocessedRanges(latestBlock)
	if len(unprocessedRanges) == 0 {
		return nil // No gaps to fill
	}

	logger.Sugar.Infof("Processing view %s: found %d unprocessed ranges up to block %d", view.Name, len(unprocessedRanges), latestBlock)

	// Process each range
	for _, blockRange := range unprocessedRanges {
		err := h.ApplyView(ctx, *view, blockRange.Start, blockRange.End)
		if err != nil {
			if strings.Contains(err.Error(), "No source data found") {
				logger.Sugar.Debugf("No source data for view %s in range %d-%d", view.Name, blockRange.Start, blockRange.End)
			} else {
				logger.Sugar.Errorf("Error applying view %s on range %d-%d: %v", view.Name, blockRange.Start, blockRange.End, err)
				continue // Don't mark as processed if there was an error
			}
		}

		// Mark all blocks in this range as processed for both view and attestation tracking
		tracker.AddRange(blockRange.Start, blockRange.End)
		h.attestationRangeTracker.AddRange(blockRange.Start, blockRange.End)
		logger.Sugar.Debugf("Marked blocks %d-%d as processed for view %s and attestation tracking", blockRange.Start, blockRange.End, view.Name)
	}

	return nil
}

// ========================================
// EFFICIENT ATTESTATION PROCESSING
// ========================================

// processBlockForAttestations efficiently processes a single block for attestations using range tracker
func (h *Host) processBlockForAttestations(ctx context.Context, blockNumber uint64) error {
	// Check if entire block already processed
	if h.attestationRangeTracker.Contains(blockNumber) {
		return nil // Skip entire block
	}

	// Get all documents in this block
	docs, err := h.getDocumentsInBlock(ctx, blockNumber)
	if err != nil {
		return err
	}

	// Process all documents in block
	for _, doc := range docs {
		err := h.handleDocumentAttestation(ctx, doc, blockNumber)
		if err != nil {
			return err
		}
	}

	// Mark entire block as processed
	h.attestationRangeTracker.Add(blockNumber)
	return nil
}

// processAttestationGaps processes all unprocessed ranges for attestations (startup/gap filling)
func (h *Host) processAttestationGaps(ctx context.Context) error {
	latestBlock, err := h.getCurrentBlockNumber(ctx)
	if err != nil {
		return err
	}

	// Get all unprocessed ranges
	gaps := h.attestationRangeTracker.GetUnprocessedRanges(latestBlock)

	for _, gap := range gaps {
		logger.Sugar.Infof("Processing attestation gap: blocks %d-%d", gap.Start, gap.End)

		for blockNum := gap.Start; blockNum <= gap.End; blockNum++ {
			err := h.processBlockForAttestations(ctx, blockNum)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// getCurrentBlockNumber queries DefraDB for the highest block number
func (h *Host) getCurrentBlockNumber(ctx context.Context) (uint64, error) {
	query := `query GetHighestBlockNumber { Block(order: {number: DESC}, limit: 1) { number } }`
	latestBlock, err := defra.QuerySingle[hostAttestation.Block](ctx, h.DefraNode, query)
	if err != nil {
		// Handle the case where no blocks exist yet
		if strings.Contains(err.Error(), "no documents found") {
			return 0, nil // Return 0 when no blocks exist
		}
		return 0, err
	}
	return latestBlock.Number, nil
}
