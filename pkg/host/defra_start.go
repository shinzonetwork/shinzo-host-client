package host

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/sec"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sourcenetwork/corelog"
	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/defradb/node"
	"github.com/sourcenetwork/immutable"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/shinzonetwork/shinzo-host-client/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/shinzonetwork/shinzo-host-client/pkg/snapshot"
)

const bytesPerMB = 1024 * 1024

const (
	schemaReadyPollInterval = time.Second
	schemaReadyMaxAttempts  = 30
)

const (
	defaultBlockSigQueueSize   = 5000
	defaultBlockSigWorkerCount = 16
	blockSigFetchRetries       = 10
)

const (
	defaultPeerDiscoveryTimeout = 10 * time.Second
	defaultP2PPort              = "9171"
	defaultConnectRetries       = 5
	defaultConnectBaseDelay     = time.Second
	maxConnectBackoff           = 30 * time.Second
	defaultReconnectInterval    = 60 * time.Second
)

type DefraService interface {
	Start(ctx context.Context) error
	Bootstrap(ctx context.Context)
	MaintainPeerConnections(ctx context.Context)
	AttestSignatures(ctx context.Context)
	TrackDocumentMetrics(ctx context.Context)
	Stop(ctx context.Context) error
	DB() node.DB
	Options() *options.NodeOptions
	Metrics() *server.HostMetrics
}

func NewDefraService(
	cfg *hostconfig.Config,
	log *zap.Logger,
	identityKey identity.FullIdentity,
	peerKeySeed []byte,
) DefraService {
	return &defraService{
		cfg:         cfg,
		log:         log,
		identityKey: identityKey,
		peerKeySeed: peerKeySeed,
		metrics:     server.NewHostMetrics(),
	}
}

type defraService struct {
	cfg         *hostconfig.Config
	log         *zap.Logger
	identityKey identity.FullIdentity
	peerKeySeed []byte
	metrics     *server.HostMetrics

	node *node.Node // set once Start succeeds
}

func (s *defraService) Start(ctx context.Context) error {
	configureCorelog(s.cfg)

	nodeOpts, err := buildNodeOptions(s.cfg, s.identityKey, s.peerKeySeed)
	if err != nil {
		return err
	}

	var filter *EventFilter
	if s.cfg.EventFilter.Enabled {
		rules, err := hostconfig.LoadFilters(s.cfg.Node.DataDir, s.cfg.EventFilter)
		if err != nil {
			return fmt.Errorf("loading event filters: %w", err)
		}
		filter = NewEventFilter(s.cfg.EventFilter, rules)
	}

	defraNode, err := node.New(ctx, nodeOpts)
	if err != nil {
		return fmt.Errorf("configuring defra node: %w", err)
	}
	if filter != nil {
		defraNode.ReplicationFilter = filter
	}

	if err := defraNode.Start(ctx); err != nil {
		return fmt.Errorf("starting defra node: %w", err)
	}

	schemaStr := resolveDefraSchema(ctx, s.cfg, s.log.Sugar())
	if err := applyDefraSchema(ctx, defraNode, schemaStr); err != nil {
		_ = defraNode.Close(ctx)
		return fmt.Errorf("applying schema: %w", err)
	}

	if err := waitSchemaQueryable(ctx, defraNode); err != nil {
		_ = defraNode.Close(ctx)
		return err
	}

	if s.cfg.P2P.Enabled {
		if err := defraNode.DB.AddP2PCollections(ctx, constants.AllCollections); err != nil {
			_ = defraNode.Close(ctx)
			return fmt.Errorf("registering p2p collections: %w", err)
		}
	}

	s.node = defraNode
	return nil
}

func (s *defraService) Bootstrap(ctx context.Context) {
	if s.node == nil {
		return
	}
	snapCfg := s.cfg.Snapshot
	if !snapCfg.Enabled || snapCfg.IndexerURL == "" || len(snapCfg.HistoricalRanges) == 0 {
		return
	}

	log := s.log.Sugar()
	log.Infow("bootstrapping from snapshots", "indexer", snapCfg.IndexerURL, "ranges", len(snapCfg.HistoricalRanges))

	needed, snapClient, err := pendingSnapshots(ctx, s.node, snapCfg, log)
	if err != nil || len(needed) == 0 {
		return
	}

	imported := importPendingSnapshots(ctx, s.node, snapClient, needed, log)
	if imported > 0 {
		rebuildSnapshotIndexes(ctx, s.node, log)
	}

	log.Infow("snapshot bootstrap complete", "imported", imported, "needed", len(needed))
}

func (s *defraService) MaintainPeerConnections(ctx context.Context) {
	if s.node == nil || !s.cfg.P2P.Enabled || len(s.cfg.P2P.BootstrapPeers) == 0 {
		return
	}
	go s.maintainPeerConnections(ctx, s.node)
}

func (s *defraService) maintainPeerConnections(ctx context.Context, defraNode *node.Node) {
	log := s.log.Sugar()
	p2pCfg := s.cfg.P2P

	discoveryTimeout := time.Duration(p2pCfg.PeerDiscoveryTimeoutMs) * time.Millisecond
	if discoveryTimeout <= 0 {
		discoveryTimeout = defaultPeerDiscoveryTimeout
	}
	peers := resolvePeerAddrs(ctx, p2pCfg.BootstrapPeers, discoveryTimeout, log)
	if len(peers) == 0 {
		log.Warn("no bootstrap peers resolved, nothing to connect to")
		return
	}

	for _, addr := range peers {
		connectPeerWithRetry(ctx, defraNode, addr, p2pCfg, log)
	}

	if !p2pCfg.EnableAutoReconnect {
		return
	}

	interval := time.Duration(p2pCfg.ReconnectIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = defaultReconnectInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var noPeersCh <-chan event.Message
	if sub, err := defraNode.DB.Events().Subscribe(event.P2PNoPeersName); err != nil {
		log.Warnw("mesh-loss listener disabled, event subscription failed", "error", err)
	} else {
		noPeersCh = sub.Message()
	}

	log.Infow("maintaining peer connections", "peers", len(peers), "reconnect_interval", interval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconnectMissingPeers(ctx, defraNode, peers, p2pCfg, log)
		case msg, ok := <-noPeersCh:
			if !ok {
				noPeersCh = nil
				continue
			}
			if _, ok := msg.Data.(event.P2PNoPeers); !ok {
				continue
			}
			// P2PNoPeers fires on any publish with no subscribers, normal
			// for routine activity, only reconnect on a genuine mesh loss.
			if active, err := defraNode.DB.ActivePeers(ctx); err == nil && len(active) == 0 {
				reconnectMissingPeers(ctx, defraNode, peers, p2pCfg, log)
			}
		}
	}
}

func connectPeerWithRetry(ctx context.Context, defraNode *node.Node, peerAddr string, cfg hostconfig.P2PConfig, log *zap.SugaredLogger) {
	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultConnectRetries
	}
	baseDelay := time.Duration(cfg.RetryBaseDelayMs) * time.Millisecond
	if baseDelay <= 0 {
		baseDelay = defaultConnectBaseDelay
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := defraNode.DB.Connect(ctx, []string{peerAddr}); err == nil {
			log.Infow("connected to bootstrap peer", "peer", peerAddr, "attempt", attempt+1)
			return
		} else { //nolint:revive // clearer as an explicit else here
			lastErr = err
		}

		if attempt == maxRetries-1 {
			break
		}
		delay := min(baseDelay*time.Duration(int64(1)<<attempt), maxConnectBackoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
	log.Warnw("failed to connect to bootstrap peer", "peer", peerAddr, "attempts", maxRetries, "error", lastErr)
}

func reconnectMissingPeers(ctx context.Context, defraNode *node.Node, bootstrapPeers []string, cfg hostconfig.P2PConfig, log *zap.SugaredLogger) {
	active, err := defraNode.DB.ActivePeers(ctx)
	if err != nil {
		log.Warnw("failed to check active peers", "error", err)
		return
	}
	activeSet := make(map[string]bool, len(active)*2) //nolint:mnd // room for both raw addr and extracted peer id
	for _, addr := range active {
		activeSet[addr] = true
		if id := peerIDFromMultiaddr(addr); id != "" {
			activeSet[id] = true
		}
	}

	for _, addr := range bootstrapPeers {
		if activeSet[addr] {
			continue
		}
		if id := peerIDFromMultiaddr(addr); id != "" && activeSet[id] {
			continue
		}
		go connectPeerWithRetry(ctx, defraNode, addr, cfg, log)
	}
}

func peerIDFromMultiaddr(multiaddr string) string {
	const p2pSuffix = "/p2p/"
	i := strings.LastIndex(multiaddr, p2pSuffix)
	if i == -1 {
		return ""
	}
	return multiaddr[i+len(p2pSuffix):]
}

type resolvedPeerAddr struct {
	index int
	addr  string
}

func resolvePeerAddrs(ctx context.Context, peers []string, timeout time.Duration, log *zap.SugaredLogger) []string {
	results := make([]resolvedPeerAddr, 0, len(peers))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i, raw := range peers {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		maddr, err := normalizePeerAddr(raw)
		if err != nil {
			log.Warnw("invalid bootstrap peer address", "addr", raw, "error", err)
			continue
		}

		if multiaddrHasPeerID(maddr) {
			mu.Lock()
			results = append(results, resolvedPeerAddr{index: i, addr: maddr.String()})
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func(idx int, addr ma.Multiaddr) {
			defer wg.Done()
			full, err := probePeerID(ctx, addr, timeout)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				log.Warnw("failed to discover peer id", "addr", addr.String(), "error", err)
				return
			}
			results = append(results, resolvedPeerAddr{index: idx, addr: full})
		}(i, maddr)
	}
	wg.Wait()

	sort.Slice(results, func(i, j int) bool { return results[i].index < results[j].index })
	resolved := make([]string, len(results))
	for i, r := range results {
		resolved[i] = r.addr
	}
	return resolved
}

func normalizePeerAddr(addr string) (ma.Multiaddr, error) {
	if strings.HasPrefix(addr, "/") {
		return ma.NewMultiaddr(addr)
	}

	host, port, err := net.SplitHostPort(addr)
	if err == nil {
		return buildPeerMultiaddr(host, port)
	}

	host = strings.TrimPrefix(addr, "[")
	host = strings.TrimSuffix(host, "]")
	return buildPeerMultiaddr(host, defaultP2PPort)
}

func buildPeerMultiaddr(host, port string) (ma.Multiaddr, error) {
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, fmt.Errorf("%q: %w", host, ErrInvalidIPAddress)
	}
	proto := "ip4"
	if ip.To4() == nil {
		proto = "ip6"
	}
	return ma.NewMultiaddr(fmt.Sprintf("/%s/%s/tcp/%s", proto, host, port))
}

func multiaddrHasPeerID(maddr ma.Multiaddr) bool {
	for _, p := range maddr.Protocols() {
		if p.Code == ma.P_P2P {
			return true
		}
	}
	return false
}

func probePeerID(ctx context.Context, targetAddr ma.Multiaddr, timeout time.Duration) (string, error) {
	tmpHost, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		return "", fmt.Errorf("creating discovery host: %w", err)
	}
	defer func() { _ = tmpHost.Close() }()

	bogusID, err := peer.Decode("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN")
	if err != nil {
		return "", fmt.Errorf("decoding placeholder peer id: %w", err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err = tmpHost.Connect(dialCtx, peer.AddrInfo{ID: bogusID, Addrs: []ma.Multiaddr{targetAddr}})
	if err == nil {
		// the placeholder id matched, astronomically unlikely but valid
		return fmt.Sprintf("%s/p2p/%s", targetAddr, bogusID), nil
	}

	actual, extractErr := peerIDFromDialError(err)
	if extractErr != nil {
		return "", fmt.Errorf("%w (dial error: %w)", extractErr, err)
	}
	return fmt.Sprintf("%s/p2p/%s", targetAddr, actual), nil
}

func peerIDFromDialError(err error) (peer.ID, error) {
	var mismatch sec.ErrPeerIDMismatch
	if errors.As(err, &mismatch) {
		return mismatch.Actual, nil
	}

	const marker = "but remote key matches "
	_, after, ok := strings.Cut(err.Error(), marker)
	if !ok {
		return "", ErrNoPeerIDMismatchInfo
	}
	idStr := after
	for _, delim := range []string{".", ",", " ", "\n", ")"} {
		if i := strings.Index(idStr, delim); i != -1 {
			idStr = idStr[:i]
		}
	}
	idStr = strings.TrimSpace(idStr)

	id, err := peer.Decode(idStr)
	if err != nil {
		return "", fmt.Errorf("extracted invalid peer id %q: %w", idStr, err)
	}
	return id, nil
}

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

// trackedMetricCollections are the collections /metrics reports per-type counts for.
// BlockSignature is deliberately excluded: AttestSignatures already counts it, and
// watching it here too would double the total.
var trackedMetricCollections = []string{ //nolint:gochecknoglobals
	constants.CollectionBlock,
	constants.CollectionTransaction,
	constants.CollectionLog,
	constants.CollectionAccessListEntry,
}

func (s *defraService) TrackDocumentMetrics(ctx context.Context) {
	if s.node == nil {
		return
	}
	go s.trackDocumentMetrics(ctx, s.node)
}

func (s *defraService) trackDocumentMetrics(ctx context.Context, defraNode *node.Node) {
	log := s.log.Sugar()

	collectionIDToName := make(map[string]string, len(trackedMetricCollections))
	for _, name := range trackedMetricCollections {
		col, err := defraNode.DB.GetCollectionByName(ctx, name)
		if err != nil {
			log.Warnw("document metrics: collection unavailable, skipping", "collection", name, "error", err)
			continue
		}
		collectionIDToName[col.CollectionID()] = name
	}
	if len(collectionIDToName) == 0 {
		log.Warn("document metrics disabled, no tracked collections available")
		return
	}

	sub, err := defraNode.DB.Events().Subscribe(event.UpdateName)
	if err != nil {
		log.Warnw("document metrics disabled, event subscription failed", "error", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sub.Message():
			if !ok {
				return
			}
			update, ok := msg.Data.(event.Update)
			if !ok || !update.IsRelay {
				continue
			}
			name, tracked := collectionIDToName[update.CollectionID]
			if !tracked {
				continue
			}
			s.metrics.IncrementDocumentsReceived()
			s.metrics.IncrementDocumentByType(name)
		}
	}
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
			metrics.UpdateMostRecentBlock(uint64(blockSig.BlockNumber)) //nolint:gosec // block numbers are always positive
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

func (s *defraService) Stop(ctx context.Context) error {
	if s.node == nil {
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- s.node.Close(ctx) }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		s.log.Sugar().Warn("defra did not close within the shutdown budget, abandoning it so the process can exit")
		return ctx.Err()
	}
}

func (s *defraService) DB() node.DB {
	if s.node == nil {
		return nil
	}
	return s.node.DB
}

func (s *defraService) Options() *options.NodeOptions {
	if s.node == nil {
		return nil
	}
	return s.node.Options()
}

func (s *defraService) Metrics() *server.HostMetrics {
	return s.metrics
}

func configureCorelog(cfg *hostconfig.Config) {
	format := corelog.FormatJSON
	if cfg.Logger.Development {
		format = ""
	}

	general := corelog.Config{
		Level:  corelogLevel(cfg.Logger.Level),
		Format: format,
		Output: corelog.OutputStdout,
	}
	corelog.SetConfig(general)

	corelog.SetConfigOverride("http", corelog.Config{
		Level:  corelog.LevelError,
		Format: format,
		Output: corelog.OutputStdout,
	})
}

func corelogLevel(zapLevel string) string {
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(zapLevel)); err == nil && lvl >= zapcore.ErrorLevel {
		return corelog.LevelError
	}
	return corelog.LevelInfo
}

func buildNodeOptions(
	cfg *hostconfig.Config,
	identityKey identity.FullIdentity,
	peerKeySeed []byte,
) (*options.NodeOptionsBuilder, error) {
	nodeOpts := options.Node()
	nodeOpts.DB().SetLensRuntime("wazero").SetNodeIdentity(identityKey)
	nodeOpts.Store().SetPath(cfg.Store.Path)
	if cfg.Store.ValueLogFileSizeMB > 0 {
		nodeOpts.Store().SetBadgerFileSize(cfg.Store.ValueLogFileSizeMB * bytesPerMB)
	}

	nodeOpts.SetDisableAPI(true)
	nodeOpts.SetDisableP2P(!cfg.P2P.Enabled)

	if !cfg.P2P.Enabled {
		return nodeOpts, nil
	}

	p2pKey, err := p2pPrivateKeyBytes(peerKeySeed)
	if err != nil {
		return nil, fmt.Errorf("p2p key: %w", err)
	}
	// Bootstrap peers are deliberately not set here. The node comes up able
	// to accept connections, but dialing out to bootstrap peers is
	// MaintainPeerConnections' job alone, run later, after schema/snapshot
	// are ready, not something go-p2p should attempt on its own during
	// Start.
	nodeOpts.P2P().
		SetEnablePubSub(true).
		SetListenAddresses(cfg.P2P.ListenAddr).
		SetPrivateKey(p2pKey)

	return nodeOpts, nil
}

func p2pPrivateKeyBytes(seed []byte) ([]byte, error) {
	priv, _, err := libp2pcrypto.GenerateEd25519Key(bytes.NewReader(seed))
	if err != nil {
		return nil, fmt.Errorf("generating p2p key: %w", err)
	}
	raw, err := priv.Raw()
	if err != nil {
		return nil, fmt.Errorf("marshaling p2p key: %w", err)
	}
	return raw, nil
}

func waitSchemaQueryable(ctx context.Context, defraNode *node.Node) error {
	query := `{ ` + constants.CollectionBlock + ` { __typename } }`

	var lastErr error
	for attempt := 1; attempt <= schemaReadyMaxAttempts; attempt++ {
		_, err := defradb.QuerySingle[map[string]any](ctx, defraNode, query)
		if err == nil {
			return nil
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for schema to become queryable: %w", ctx.Err())
		case <-time.After(schemaReadyPollInterval):
		}
	}

	return fmt.Errorf("after %d attempts: %w: %w", schemaReadyMaxAttempts, ErrDefraDBNotReady, lastErr)
}

func resolveDefraSchema(ctx context.Context, cfg *hostconfig.Config, log *zap.SugaredLogger) string {
	parsedURL, err := url.Parse(cfg.Snapshot.IndexerURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		log.Warnf("no usable indexer URL (%q), using the embedded schema", cfg.Snapshot.IndexerURL)
		return schema.GetSchema()
	}
	endpoint := parsedURL.JoinPath(cfg.Schema.IndexerSchemaEndpoint).String()

	fetched, err := schema.GetSchemaDynamic(ctx, schemaHTTPClient(cfg.Schema), endpoint)
	if err != nil {
		switch {
		case schema.IsDataLevelError(err):
			log.Warnw("indexer returned an invalid schema, using the embedded schema", "endpoint", endpoint, "error", err)
		case schema.IsNetworkLevelError(err):
			log.Warnw("could not reach the indexer for schema, using the embedded schema", "endpoint", endpoint, "error", err)
		default:
			log.Warnw("schema fetch failed, using the embedded schema", "endpoint", endpoint, "error", err)
		}
		return schema.GetSchema()
	}
	return fetched
}

func schemaHTTPClient(cfg hostconfig.SchemaConfig) *http.Client {
	client := &http.Client{Timeout: time.Duration(cfg.HTTPClientTimeoutSecs) * time.Second}
	if cfg.AuthToken != "" {
		client.Transport = schemaAuthTransport{token: cfg.AuthToken}
	}
	return client
}

type schemaAuthTransport struct {
	token string
}

func (t schemaAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+t.token)
	return http.DefaultTransport.RoundTrip(req)
}

func applyDefraSchema(ctx context.Context, defraNode *node.Node, schemaStr string) error {
	_, err := defraNode.DB.AddCollection(ctx, schemaStr)
	if err != nil && strings.Contains(err.Error(), "collection already exists") {
		return nil
	}
	return err
}

func pendingSnapshots(
	ctx context.Context,
	defraNode *node.Node,
	snapCfg hostconfig.SnapshotConfig,
	log *zap.SugaredLogger,
) ([]snapshot.Info, *snapshot.Client, error) {
	snapClient := snapshot.NewClient(snapCfg.IndexerURL)

	available, err := snapClient.ListSnapshots()
	if err != nil {
		log.Warnw("failed to list snapshots from indexer", "error", err)
		return nil, nil, err
	}
	if len(available) == 0 {
		log.Info("no snapshots available from indexer")
		return nil, nil, nil
	}

	existingMin, existingMax := existingBlockRange(ctx, defraNode)
	if existingMax > 0 {
		log.Infow("existing blocks in db", "min", existingMin, "max", existingMax)
	}

	needed := coveringSnapshots(available, snapCfg.HistoricalRanges, existingMin, existingMax)
	if len(needed) == 0 {
		log.Info("no new snapshots needed, ranges already covered")
		return nil, nil, nil
	}

	log.Infow("found snapshots to import", "count", len(needed))
	return needed, snapClient, nil
}

func coveringSnapshots(available []snapshot.Info, ranges []hostconfig.BlockRange, existingMin, existingMax int64) []snapshot.Info {
	var needed []snapshot.Info
	seen := make(map[string]bool)

	for _, r := range ranges {
		for _, snap := range available {
			if snap.EndBlock < r.Start || snap.StartBlock > r.End {
				continue
			}
			if !snap.Signed {
				continue
			}
			if existingMax > 0 && snap.StartBlock >= existingMin && snap.EndBlock <= existingMax {
				continue
			}
			if !seen[snap.Filename] {
				seen[snap.Filename] = true
				needed = append(needed, snap)
			}
		}
	}

	sort.Slice(needed, func(i, j int) bool {
		return needed[i].StartBlock < needed[j].StartBlock
	})

	return needed
}

func existingBlockRange(ctx context.Context, defraNode *node.Node) (int64, int64) {
	type blockResult struct {
		Number int64 `json:"number"`
	}
	type queryResult struct {
		Block []blockResult `json:"Ethereum__Mainnet__Block"`
	}

	maxQuery := fmt.Sprintf(`query { %s(order: {number: DESC}, limit: 1) { number } }`, constants.CollectionBlock)
	maxResult := defraNode.DB.ExecRequest(ctx, maxQuery)
	if maxResult.GQL.Errors != nil {
		return 0, 0
	}
	maxJSON, err := json.Marshal(maxResult.GQL.Data)
	if err != nil {
		return 0, 0
	}
	var maxQR queryResult
	if err := json.Unmarshal(maxJSON, &maxQR); err != nil || len(maxQR.Block) == 0 {
		return 0, 0
	}
	maxBlock := maxQR.Block[0].Number

	minQuery := fmt.Sprintf(`query { %s(order: {number: ASC}, limit: 1) { number } }`, constants.CollectionBlock)
	minResult := defraNode.DB.ExecRequest(ctx, minQuery)
	if minResult.GQL.Errors != nil {
		return 0, maxBlock
	}
	minJSON, err := json.Marshal(minResult.GQL.Data)
	if err != nil {
		return 0, maxBlock
	}
	var minQR queryResult
	if err := json.Unmarshal(minJSON, &minQR); err != nil || len(minQR.Block) == 0 {
		return 0, maxBlock
	}

	return minQR.Block[0].Number, maxBlock
}

func importPendingSnapshots(
	ctx context.Context,
	defraNode *node.Node,
	snapClient *snapshot.Client,
	needed []snapshot.Info,
	log *zap.SugaredLogger,
) int {
	tmpDir := os.TempDir()
	var imported int

	for _, snap := range needed {
		if err := importSnapshot(ctx, defraNode, snapClient, snap, tmpDir, log); err != nil {
			log.Warnw("failed to import snapshot", "file", snap.Filename, "error", err)
			continue
		}
		imported++
	}

	return imported
}

func importSnapshot(
	ctx context.Context,
	defraNode *node.Node,
	snapClient *snapshot.Client,
	snap snapshot.Info,
	tmpDir string,
	log *zap.SugaredLogger,
) error {
	if snap.Signature == nil {
		return fmt.Errorf("snapshot %s marked signed but has no signature data", snap.Filename)
	}

	tmpPath := filepath.Join(tmpDir, snap.Filename)
	log.Infow("downloading snapshot", "file", snap.Filename, "bytes", snap.SizeBytes)
	if err := snapClient.DownloadSnapshot(snap.Filename, tmpPath); err != nil {
		return fmt.Errorf("download: %w", err)
	}

	result, err := snapshot.ImportWithVerification(ctx, defraNode, tmpPath, snap.Signature)
	_ = os.Remove(tmpPath)
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}

	log.Infow("imported snapshot", "file", snap.Filename, "start", result.StartBlock, "end", result.EndBlock)
	attestSnapshotImport(ctx, defraNode, snap.Signature, log)

	return nil
}

func rebuildSnapshotIndexes(ctx context.Context, defraNode *node.Node, log *zap.SugaredLogger) {
	if err := snapshot.RebuildAllIndexes(ctx, defraNode, constants.AllCollections); err != nil {
		log.Warnw("failed to rebuild indexes after snapshot import", "error", err)
	}
}

func attestSnapshotImport(ctx context.Context, defraNode *node.Node, sig *snapshot.SignatureData, log *zap.SugaredLogger) {
	if len(sig.BlockSigMerkleRoots) == 0 {
		log.Warnw("skipping attestation for snapshot, no block sig merkle roots", "start", sig.StartBlock, "end", sig.EndBlock)
		return
	}

	record := &attestation.Record{
		AttestedDocID: fmt.Sprintf("snapshot:%d-%d", sig.StartBlock, sig.EndBlock),
		SourceDocIDs:  []string{sig.SignatureIdentity},
		CIDs:          sig.BlockSigMerkleRoots,
		DocType:       "Snapshot",
		VoteCount:     1,
	}

	if err := attestation.PostAttestationRecord(ctx, defraNode, record); err != nil {
		log.Warnw("failed to create attestation for snapshot", "start", sig.StartBlock, "end", sig.EndBlock, "error", err)
		return
	}
	log.Infow("created attestation for snapshot", "start", sig.StartBlock, "end", sig.EndBlock, "signer", sig.SignatureIdentity)
}
