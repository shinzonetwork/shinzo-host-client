package host

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
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

type DefraService interface {
	Start(ctx context.Context) error
	Bootstrap(ctx context.Context)
	AttestSignatures(ctx context.Context)
	Stop(ctx context.Context) error
	DB() node.DB
	Options() *options.NodeOptions
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
	}
}

type defraService struct {
	cfg         *hostconfig.Config
	log         *zap.Logger
	identityKey identity.FullIdentity
	peerKeySeed []byte

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
		go blockSignatureWorker(ctx, defraNode, verifier, queue, log)
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
			enqueueDropOldest(queue, update.DocID)
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
	log *zap.SugaredLogger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case docID := <-queue:
			verifyAndAttestBlockSignature(ctx, defraNode, verifier, docID, log)
		}
	}
}

func verifyAndAttestBlockSignature(
	ctx context.Context,
	defraNode *node.Node,
	verifier *attestation.BlockSignatureVerifier,
	docID string,
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
		return
	}
	if len(blockSig.CIDs) > 0 {
		match, err := verifier.VerifyCIDListAgainstMerkleRoot(blockSig)
		if err != nil {
			log.Warnw("cid list verification error", "block", blockSig.BlockNumber, "error", err)
		} else if !match {
			log.Warnw("cid list does not match merkle root", "block", blockSig.BlockNumber)
			return
		}
	}

	verifier.AddBlockSignature(blockSig)
	postBlockAttestation(ctx, defraNode, blockSig, log)
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

func postBlockAttestation(ctx context.Context, defraNode *node.Node, blockSig *attestation.BlockSignature, log *zap.SugaredLogger) {
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
			return
		}
		lastErr = err
		if strings.Contains(err.Error(), "transaction conflict") || strings.Contains(err.Error(), "Please retry") {
			time.Sleep(time.Duration(attestationBackoffBase*(1<<attempt)) * time.Millisecond)
			continue
		}
		log.Warnw("failed to post block attestation", "block", blockSig.BlockNumber, "error", err)
		return
	}
	log.Warnw("failed to post block attestation after retries", "block", blockSig.BlockNumber, "error", lastErr)
}

func (s *defraService) Stop(ctx context.Context) error {
	if s.node == nil {
		return nil
	}
	return s.node.Close(ctx)
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
	nodeOpts.P2P().
		SetEnablePubSub(true).
		SetListenAddresses(cfg.P2P.ListenAddr).
		SetBootstrapPeers(cfg.P2P.BootstrapPeers...).
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
