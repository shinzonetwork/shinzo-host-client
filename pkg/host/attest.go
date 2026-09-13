package host

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/defradb/node"
	"github.com/sourcenetwork/immutable"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
)

const (
	defaultBlockSigQueueSize   = 5000
	defaultBlockSigWorkerCount = 16
	blockSigFetchRetries       = 10
)

func (s *defraService) AttestSignatures(ctx context.Context) {
	if s.node == nil {
		return
	}
	go s.attestSignatures(ctx, s.node)
}

func (s *defraService) attestSignatures(ctx context.Context, defraNode *node.Node) {
	log := s.log.Sugar()

	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionBlockSignature)
	if err != nil {
		log.Warnw("signature attestation disabled, collection unavailable", "error", err)
		return
	}
	blockSigCollectionID := col.CollectionID()

	sub, err := defraNode.DB.Events().Subscribe(event.UpdateName)
	if err != nil {
		log.Warnw("signature attestation disabled, event subscription failed", "error", err)
		return
	}

	queueSize := s.cfg.Shinzo.DocQueueSize
	if queueSize <= 0 {
		queueSize = defaultBlockSigQueueSize
	}
	workerCount := s.cfg.Shinzo.DocWorkerCount
	if workerCount <= 0 {
		workerCount = defaultBlockSigWorkerCount
	}

	queue := make(chan string, queueSize)
	verifier := attestation.NewBlockSignatureVerifier(blockSignatureCacheSize)

	for range workerCount {
		go blockSignatureWorker(ctx, defraNode, verifier, queue, s.metrics, log)
	}
	log.Infow("attesting block signatures", "workers", workerCount, "queue_size", queueSize)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sub.Message():
			if !ok {
				return
			}
			update, ok := msg.Data.(event.Update)
			if !ok || update.CollectionID != blockSigCollectionID || !update.IsRelay {
				continue
			}
			s.metrics.IncrementDocumentsReceived()
			s.metrics.IncrementDocumentByType(constants.CollectionBlockSignature)
			enqueueDropOldest(queue, update.DocID)
		}
	}
}

var trackedMetricCollections = []string{
	constants.CollectionBlock,
	constants.CollectionTransaction,
	constants.CollectionLog,
	constants.CollectionAccessListEntry,
}

func enqueueDropOldest(queue chan string, docID string) {
	for {
		select {
		case queue <- docID:
			return
		default:
			select {
			case <-queue:
			default:
			}
		}
	}
}

func blockSignatureWorker(
	ctx context.Context,
	defraNode *node.Node,
	verifier *attestation.BlockSignatureVerifier,
	queue <-chan string,
	metrics *server.HostMetrics,
	log *zap.SugaredLogger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case docID := <-queue:
			verifyAndAttestBlockSignature(ctx, defraNode, verifier, docID, metrics, log)
		}
	}
}

func verifyAndAttestBlockSignature(
	ctx context.Context,
	defraNode *node.Node,
	verifier *attestation.BlockSignatureVerifier,
	docID string,
	metrics *server.HostMetrics,
	log *zap.SugaredLogger,
) {
	doc, err := fetchBlockSignatureDoc(ctx, defraNode, docID)
	if err != nil {
		log.Warnw("failed to fetch block signature document", "doc_id", docID, "error", err)
		return
	}

	blockSig, blockNumber, err := blockSignatureCore(doc)
	if err != nil {
		log.Warnw("block signature extraction failed", "block", blockNumber, "error", err)
		return
	}
	fillBlockSignatureFields(doc, blockSig)
	fillBlockSignatureCIDs(doc, blockSig)

	if err := verifier.VerifyBlockSignature(blockSig); err != nil {
		log.Warnw("invalid block signature", "block", blockSig.BlockNumber, "error", err)
		metrics.IncrementSignatureFailures()
		return
	}
	if len(blockSig.CIDs) > 0 {
		match, err := verifier.VerifyCIDListAgainstMerkleRoot(blockSig)
		if err != nil {
			log.Warnw("cid list verification error", "block", blockSig.BlockNumber, "error", err)
		} else if !match {
			log.Warnw("cid list does not match merkle root", "block", blockSig.BlockNumber)
			metrics.IncrementSignatureFailures()
			return
		}
	}
	metrics.IncrementSignatureVerifications()

	verifier.AddBlockSignature(blockSig)
	postBlockAttestation(ctx, defraNode, blockSig, metrics, log)
}

func fetchBlockSignatureDoc(ctx context.Context, defraNode *node.Node, docID string) (*client.Document, error) {
	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionBlockSignature)
	if err != nil {
		return nil, fmt.Errorf("get collection: %w", err)
	}
	docIDTyped, err := client.NewDocIDFromString(docID)
	if err != nil {
		return nil, fmt.Errorf("parse doc id: %w", err)
	}

	var doc *client.Document
	for attempt := 0; attempt < blockSigFetchRetries; attempt++ {
		doc, err = col.GetDocument(ctx, docIDTyped)
		if err == nil && doc != nil {
			return doc, nil
		}
		if attempt < blockSigFetchRetries-1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(attestationRetryDelayMs * time.Millisecond):
			}
		}
	}
	return nil, fmt.Errorf("after %d attempts: %w", blockSigFetchRetries, err)
}

func blockSignatureCore(doc *client.Document) (*attestation.BlockSignature, int64, error) {
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

	return &attestation.BlockSignature{BlockNumber: blockNumber, MerkleRoot: merkleRoot}, blockNumber, nil
}

func fillBlockSignatureFields(doc *client.Document, blockSig *attestation.BlockSignature) {
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

func fillBlockSignatureCIDs(doc *client.Document, blockSig *attestation.BlockSignature) {
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

func postBlockAttestation(
	ctx context.Context,
	defraNode *node.Node,
	blockSig *attestation.BlockSignature,
	metrics *server.HostMetrics,
	log *zap.SugaredLogger,
) {
	if len(blockSig.CIDs) == 0 {
		log.Warnw("skipping attestation, block signature has no cid list", "block", blockSig.BlockNumber)
		return
	}

	record := &attestation.Record{
		AttestedDocID: fmt.Sprintf("block:%d:%s", blockSig.BlockNumber, blockSig.MerkleRoot),
		SourceDocIDs:  []string{blockSig.SignatureIdentity},
		CIDs:          blockSig.CIDs,
		DocType:       docTypeBlock,
		VoteCount:     1,
	}

	var lastErr error
	for attempt := range maxAttestationRetries {
		err := attestation.PostAttestationRecord(ctx, defraNode, record)
		if err == nil {
			log.Infow("created attestation for block", "block", blockSig.BlockNumber, "signer", blockSig.SignatureIdentity)
			metrics.IncrementAttestationsCreated()
			metrics.UpdateMostRecentBlock(uint64(blockSig.BlockNumber))
			return
		}
		lastErr = err
		if strings.Contains(err.Error(), "transaction conflict") || strings.Contains(err.Error(), "Please retry") {
			time.Sleep(time.Duration(attestationBackoffBase*(1<<attempt)) * time.Millisecond)
			continue
		}
		log.Warnw("failed to post block attestation", "block", blockSig.BlockNumber, "error", err)
		metrics.IncrementAttestationErrors()
		return
	}
	log.Warnw("failed to post block attestation after retries", "block", blockSig.BlockNumber, "error", lastErr)
	metrics.IncrementAttestationErrors()
}
