package host

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	attestationService "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/immutable"
)

// attestedChain is an enabled chain as the attestation listener serves it: its tables, the queue of
// its block signatures waiting for one of its workers, its counters (nil without host metrics) and
// its OpenTelemetry attributes (nil without instruments).
type attestedChain struct {
	collections chain.Collections
	signatures  chan string
	metrics     *server.ChainMetrics
	instruments *chainInstruments
}

// chainTable is one of an enabled chain's tables.
type chainTable struct {
	chain *attestedChain
	name  string
}

// knownTables maps the collection IDs of the chains' subscribed tables to the chain and table.
// Update events carry only a collection ID.
func (h *Host) knownTables(ctx context.Context, chains []*attestedChain) (map[string]chainTable, error) {
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		return nil, ErrDefraNodeUnavailable
	}

	cols, err := h.DefraNode.DB.GetCollections(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get collections: %w", err)
	}

	byName := make(map[string]chainTable)
	for _, attested := range chains {
		for _, name := range attested.collections.Subscribed() {
			byName[name] = chainTable{chain: attested, name: name}
		}
	}
	tables := make(map[string]chainTable, len(byName))
	for _, col := range cols {
		if t, ok := byName[col.Name()]; ok {
			tables[col.CollectionID()] = t
		}
	}

	logger.Sugar.Infof("Initialized %d known collection IDs", len(tables))
	return tables, nil
}

// docQueueSettings returns how many attestation workers each enabled chain gets and how many block
// signatures its queue holds.
func (h *Host) docQueueSettings() (workerCount, queueSize int) {
	queueSize = h.config.Shinzo.DocQueueSize
	if queueSize <= 0 {
		queueSize = 5000
	}
	workerCount = h.config.Shinzo.DocWorkerCount
	if workerCount <= 0 {
		workerCount = 16
	}
	return workerCount, queueSize
}

// enqueueSignature adds a block signature's document ID to queue, dropping the oldest IDs while the
// queue is full, and returns how many it dropped.
func enqueueSignature(queue chan string, docID string) (dropped int) {
	for {
		select {
		case queue <- docID:
			return dropped
		default:
			select {
			case <-queue:
				dropped++
			default:
			}
		}
	}
}

// signatureWorker attests the block signatures queued for attested until ctx is done.
func (h *Host) signatureWorker(ctx context.Context, attested *attestedChain) {
	for {
		select {
		case <-ctx.Done():
			return
		case docID := <-attested.signatures:
			h.processBlockSignatureFromEventBus(ctx, attested.collections, docID)
		}
	}
}

// startEventBusListener reads update events from updates: it counts the documents peers send and
// queues each enabled chain's block signatures for attestation. It closes updates when it stops, and
// returns once every chain's attestation workers have stopped.
func (h *Host) startEventBusListener(ctx context.Context, updates event.Subscription) {
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		logger.Sugar.Warn("DefraNode not available, skipping event bus listener")
		return
	}

	// The workers stop when the listener returns, and the listener waits for them, so whoever waits
	// for the listener knows no worker is still using DefraDB.
	workerCtx, stopWorkers := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() {
		stopWorkers()
		workers.Wait()
	}()
	workerCount, queueSize := h.docQueueSettings()
	chains := make([]*attestedChain, len(h.chains))
	for i, c := range h.chains {
		chains[i] = &attestedChain{
			collections: c,
			signatures:  make(chan string, queueSize),
			metrics:     h.chainMetrics(c.Prefix),
			instruments: h.instruments.chain(c.Prefix),
		}
		if o := chains[i].instruments; o != nil {
			o.queue.Store(&chains[i].signatures)
		}
		for range workerCount {
			workers.Go(func() { h.signatureWorker(workerCtx, chains[i]) })
		}
		logger.Sugar.Infof("Started %d document workers (queue: %d, chain: %s)", workerCount, queueSize, c.Prefix)
	}
	// Deferred calls run last in, first out, so updates is closed, and read until it is, before the
	// workers are waited on: a stopping worker's write publishes an update, and once updates' buffer
	// is full the bus stalls until someone reads it.
	defer defradb.CloseSubscription(h.DefraNode.DB.Events(), updates)

	tables, err := h.knownTables(ctx, chains)
	if err != nil {
		logger.Sugar.Errorf("Failed to initialize known collection IDs: %v", err)
	}

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

				table, known := tables[update.CollectionID]
				if !known {
					continue
				}

				// Local writes skip the metric and verification: "documents received"
				// only counts inbound peer traffic, and a BlockSignature only needs
				// verification when a peer sent it.
				if !update.IsRelay {
					continue
				}

				c := table.chain.collections
				m := table.chain.metrics
				o := table.chain.instruments
				if o != nil {
					h.instruments.documents.Add(ctx, 1, o.tables[table.name])
				}
				if m != nil {
					// Keyed without the chain prefix, so every chain reports its tables under the same names.
					m.RecordDocument(strings.TrimPrefix(table.name, c.Prefix+"__"))
					if table.name == c.BlockSignature.Name {
						m.BlockSignaturesProcessed.Add(1)
					}
				}
				if table.name == c.BlockSignature.Name {
					dropped := enqueueSignature(table.chain.signatures, update.DocID)
					if m != nil {
						m.QueueDrops.Add(int64(dropped))
						m.ObserveQueueDepth(len(table.chain.signatures))
					}
					if o != nil && dropped > 0 {
						h.instruments.blockSigsDropped.Add(ctx, int64(dropped), o.chain)
					}
				}
			}
		}
	}
}

// processBlockSignatureFromEventBus fetches one of chain c's BlockSignature documents by DocID and
// processes it.
func (h *Host) processBlockSignatureFromEventBus(ctx context.Context, c chain.Collections, docID string) {
	if h.DefraNode == nil || h.DefraNode.DB == nil {
		return
	}

	col, err := h.DefraNode.DB.GetCollectionByName(ctx, c.BlockSignature.Name)
	if err != nil {
		logger.Sugar.Warnf("Failed to get BlockSignature collection: %v (chain: %s)", err, c.Prefix)
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
		logger.Sugar.Warnf("Failed to fetch BlockSignature doc %s after %d retries: %v (chain: %s)", docID, maxRetries, err, c.Prefix)
		return
	}

	h.processBlockSignatureDocument(ctx, c, doc)
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

// verifyBlockSignature verifies the signature and CID list, updating metrics on failure; prefix
// names the block's chain in its log lines. Returns false if verification fails and processing
// should stop.
func (h *Host) verifyBlockSignature(ctx context.Context, prefix string, blockSig *attestationService.BlockSignature) bool {
	if err := h.blockSignatureVerifier.VerifyBlockSignature(blockSig); err != nil {
		logger.Sugar.Warnf("Invalid block signature for block %d: %v (chain: %s)", blockSig.BlockNumber, err, prefix)
		if m := h.chainMetrics(prefix); m != nil {
			m.SignatureFailures.Add(1)
		}
		if o := h.instruments.chain(prefix); o != nil {
			h.instruments.blockSignatures.Add(ctx, 1, o.failure)
		}
		return false
	}

	if len(blockSig.CIDs) > 0 {
		cidMatch, cidErr := h.blockSignatureVerifier.VerifyCIDListAgainstMerkleRoot(blockSig)
		if cidErr != nil {
			logger.Sugar.Warnf("Block %d: CID list verification error: %v (chain: %s)", blockSig.BlockNumber, cidErr, prefix)
		} else if !cidMatch {
			logger.Sugar.Warnf("Block %d: CID list does NOT match Merkle root (chain: %s)", blockSig.BlockNumber, prefix)
			if m := h.chainMetrics(prefix); m != nil {
				m.SignatureFailures.Add(1)
			}
			if o := h.instruments.chain(prefix); o != nil {
				h.instruments.blockSignatures.Add(ctx, 1, o.failure)
			}
			return false
		}
	}

	return true
}

// processBlockSignatureDocument extracts fields from one of chain c's BlockSignature documents and
// processes them.
func (h *Host) processBlockSignatureDocument(ctx context.Context, c chain.Collections, doc *client.Document) {
	startTime := time.Now()

	blockSig, blockNumber, err := extractBlockSignatureCore(doc)
	if err != nil {
		logger.Sugar.Warnf("BlockSignature extraction failed: %v (block %d, chain: %s)", err, blockNumber, c.Prefix)
		return
	}

	populateBlockSignatureFields(doc, blockSig)
	populateBlockSignatureCIDs(doc, blockSig)

	if h.blockSignatureVerifier != nil {
		if !h.verifyBlockSignature(ctx, c.Prefix, blockSig) {
			return
		}

		o := h.instruments.chain(c.Prefix)
		if o != nil {
			// Atomic max: CAS retries until the stored value is at least blockNumber.
			for cur := o.received.Load(); blockNumber > cur; cur = o.received.Load() {
				if o.received.CompareAndSwap(cur, blockNumber) {
					break
				}
			}
		}

		// A verified peer signature is the host observing that indexer attesting on chain c.
		if set := h.attesters[c.Prefix]; set != nil {
			set.Observe(blockSig.SignatureIdentity)
		}

		if m := h.chainMetrics(c.Prefix); m != nil {
			m.SignatureVerifications.Add(1)
		}
		if o != nil {
			h.instruments.blockSignatures.Add(ctx, 1, o.success)
		}

		h.blockSignatureVerifier.AddBlockSignature(blockSig)
		h.processAttestationsFromBlockSignature(ctx, c, blockSig)
	}

	if h.metrics != nil {
		h.metrics.UpdateLastProcessingTime(float64(time.Since(startTime).Milliseconds()))
	}
}

// processAttestationsFromBlockSignature creates or updates the attestation record of one of chain c's
// blocks.
func (h *Host) processAttestationsFromBlockSignature(ctx context.Context, c chain.Collections, blockSig *attestationService.BlockSignature) {
	if h.DefraNode == nil {
		return
	}

	blockNumber := blockSig.BlockNumber
	blockAttestedID := fmt.Sprintf("block:%d:%s", blockNumber, blockSig.MerkleRoot)

	if len(blockSig.CIDs) == 0 {
		logger.Sugar.Warnf("Skipping attestation for block %d: block signature has no CID list (chain: %s)", blockNumber, c.Prefix)
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

	m := h.chainMetrics(c.Prefix)
	o := h.instruments.chain(c.Prefix)
	var lastErr error
	for attempt := range maxAttestationRetries {
		start := time.Now()
		created, err := attestationService.PostAttestationRecord(ctx, h.DefraNode, c.AttestationRecord.Name, record)
		h.recordAttestation(ctx, o, start, err)
		if err != nil {
			if errors.Is(err, attestationService.ErrDocumentNotFound) {
				logger.Sugar.Infof("Skipping attestation for block %d: the pruner deleted its record because the block is at or below the retention cutoff (chain: %s)", blockNumber, c.Prefix)
				return
			}
			lastErr = err
			if strings.Contains(err.Error(), "transaction conflict") || strings.Contains(err.Error(), "Please retry") {
				time.Sleep(time.Duration(attestationBackoffBase*(1<<attempt)) * time.Millisecond)
				continue
			}
			// Non-retryable error
			logger.Sugar.Warnf("Failed to post attestation for block %d: %v (chain: %s)", blockNumber, err, c.Prefix)
			if m != nil {
				m.AttestationErrors.Add(1)
			}
			if o != nil && !errors.Is(err, context.Canceled) {
				h.instruments.attestations.Add(ctx, 1, o.failure)
			}
			return
		}
		if o != nil {
			h.instruments.attestations.Add(ctx, 1, o.success)
		}
		if created {
			if m != nil {
				m.AttestationsCreated.Add(1)
			}
			logger.Sugar.Infof("Created attestation for block %d (indexer: %s, chain: %s)", blockNumber, truncateString(blockSig.SignatureIdentity, identityTruncateLength), c.Prefix)
		} else {
			logger.Sugar.Infof("Updated attestation for block %d (indexer: %s, chain: %s)", blockNumber, truncateString(blockSig.SignatureIdentity, identityTruncateLength), c.Prefix)
		}
		if m != nil {
			m.UpdateCurrentBlock(blockNumber)
		}
		return
	}

	logger.Sugar.Warnf("Failed to post attestation for block %d after retries: %v (chain: %s)", blockNumber, lastErr, c.Prefix)
	if m != nil {
		m.AttestationErrors.Add(1)
	}
	if o != nil {
		h.instruments.attestations.Add(ctx, 1, o.failure)
	}
}

// recordAttestation records an attestation write duration on the chain with instruments o,
// skipping pruned records and cancellations.
func (h *Host) recordAttestation(ctx context.Context, o *chainInstruments, start time.Time, err error) {
	if o == nil || errors.Is(err, attestationService.ErrDocumentNotFound) || errors.Is(err, context.Canceled) {
		return
	}
	outcome := o.success
	if err != nil {
		outcome = o.failure
	}
	h.instruments.attestationTime.Record(ctx, time.Since(start).Seconds(), outcome)
}

// chainMetrics returns the counters of the chain with prefix, or nil when the host keeps no
// metrics or none for that chain.
func (h *Host) chainMetrics(prefix string) *server.ChainMetrics {
	if h.metrics == nil {
		return nil
	}
	return h.metrics.Chain(prefix)
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
