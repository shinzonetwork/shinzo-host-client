package pruner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
)

const (
	// purgeBatchSize is how many documents are handed to DefraDB per call. It sets how
	// often a purge can notice a stop, not the transaction size: DefraDB commits in its
	// own smaller chunks regardless.
	purgeBatchSize = 1000
	// purgeProgressInterval bounds how often a long purge reports progress, so the line
	// stays useful on a purge that runs for hours without flooding a short one.
	purgeProgressInterval = 30 * time.Second
)

// errStopped ends a purge early because the pruner is shutting down. Its documents are
// re-queued, so the work resumes rather than being lost.
var errStopped = errors.New("pruner stopped")

// stopping reports why further work should be abandoned, or nil to carry on.
func (p *Pruner) stopping(ctx context.Context) error {
	select {
	case <-p.stopChan:
		return errStopped
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Pruner handles periodic removal of old blockchain documents from DefraDB.
// With an EventQueue set, it drains docIDs tracked from P2P replication
// events. With no queue set or an underfilled queue, falls back to
// filter-based pruning by block range.
type Pruner struct {
	cfg         *Config
	collections CollectionConfig
	defraNode   *node.Node
	queue       PrunerQueue // EventQueue (the only implementation in host)
	stopChan    chan struct{}
	wg          sync.WaitGroup
	mu          sync.RWMutex

	// purgeDocs deletes one batch of documents. Nil outside tests, where the collection's own
	// PurgeByDocIDs is used; a purge is otherwise only reachable through a running node.
	purgeDocs func(ctx context.Context, docIDs []client.DocID) error

	// Metrics
	lastPruneTime     time.Time
	totalBlocksPruned int64
	totalDocsPruned   int64
	isRunning         bool
}

// Metrics holds pruning statistics.
type Metrics struct {
	Enabled           bool      `json:"enabled"`
	IsRunning         bool      `json:"is_running"`
	LastPruneTime     time.Time `json:"last_prune_time"`
	TotalBlocksPruned int64     `json:"total_blocks_pruned"`
	TotalDocsPruned   int64     `json:"total_docs_pruned"`
}

// NewPruner creates a new Pruner instance.
func NewPruner(cfg *Config, defraNode *node.Node, collections ...CollectionConfig) *Pruner {
	cols := DefaultCollectionConfig()
	if len(collections) > 0 {
		cols = collections[0]
	}
	return &Pruner{
		cfg:         cfg,
		collections: cols,
		defraNode:   defraNode,
		stopChan:    make(chan struct{}),
	}
}

// SetQueue sets the queue implementation for queue-based pruning.
func (p *Pruner) SetQueue(queue PrunerQueue) {
	p.queue = queue
}

// Start begins the pruning loop in a background goroutine.
func (p *Pruner) Start(ctx context.Context) error {
	if !p.cfg.Enabled {
		logger.Sugar.Info("Pruner is disabled")
		return nil
	}

	if p.defraNode == nil {
		logger.Sugar.Warn("Pruner requires embedded DefraDB node, skipping")
		return nil
	}

	p.mu.Lock()
	if p.isRunning {
		p.mu.Unlock()
		return nil
	}
	p.isRunning = true
	p.mu.Unlock()

	logger.Sugar.Debugf("Starting pruner (max_blocks=%d, docs_per_block=%d, max_docs=%d, interval=%ds)",
		p.cfg.MaxBlocks, p.cfg.DocsPerBlock, p.cfg.MaxDocs(), p.cfg.IntervalSeconds)

	p.wg.Add(1)
	go p.pruneLoop(ctx)

	return nil
}

// Stop signals the pruner to stop, waits for the current cycle to unwind, and saves the
// queue. It gives up waiting when ctx expires and saves anyway: the queue file is the
// only record of what still needs pruning, and a cycle can outlast any shutdown budget.
func (p *Pruner) Stop(ctx context.Context) {
	p.mu.Lock()
	if !p.isRunning {
		p.mu.Unlock()
		return
	}
	p.isRunning = false
	p.mu.Unlock()

	logger.Sugar.Info("Pruner stopping, waiting for the current cycle to finish")
	close(p.stopChan)

	stopped := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-ctx.Done():
		// Save is safe here: it snapshots under the queue's own lock, so a purge still
		// unwinding cannot tear the file.
		logger.Sugar.Warn("Pruner did not stop within the shutdown budget, saving the queue anyway")
	}

	if p.queue != nil {
		queueLen := p.queue.Len()
		if err := p.queue.Save(); err != nil {
			logger.Sugar.Errorf("Failed to save prune queue (%d entries): %v", queueLen, err)
		} else {
			logger.Sugar.Infof("Saved prune queue (%d entries)", queueLen)
		}
	}

	logger.Sugar.Info("Pruner stopped")
}

// GetMetrics returns current pruning statistics.
func (p *Pruner) GetMetrics() Metrics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return Metrics{
		Enabled:           p.cfg.Enabled,
		IsRunning:         p.isRunning,
		LastPruneTime:     p.lastPruneTime,
		TotalBlocksPruned: p.totalBlocksPruned,
		TotalDocsPruned:   p.totalDocsPruned,
	}
}

// pruneLoop runs the periodic pruning check.
func (p *Pruner) pruneLoop(ctx context.Context) {
	defer p.wg.Done()

	// Run startup cleanup only for indexer queues (no P2P) or when no queue is set.
	// For event queues (hosts), skip startup cleanup — the DB may contain snapshot-
	// imported data that should not be pruned. Only queue-tracked data gets pruned.
	_, isEventQueue := p.queue.(*EventQueue)
	if !isEventQueue {
		logger.Sugar.Debugf("Running startup cleanup for pre-existing blocks...")
		if err := p.startupCleanup(ctx); err != nil {
			logger.Sugar.Errorf("Startup cleanup failed: %v", err)
		}
	} else {
		logger.Sugar.Debugf("Skipping startup cleanup (event queue mode — only queue-tracked data is pruned)")
	}

	ticker := time.NewTicker(time.Duration(p.cfg.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopChan:
			return
		case <-ticker.C:
			if err := p.runPrune(ctx); err != nil {
				logger.Sugar.Errorf("Prune failed: %v", err)
			}
		}
	}
}

// runPrune executes the appropriate pruning strategy based on queue type and state.
func (p *Pruner) runPrune(ctx context.Context) error {
	if p.queue == nil {
		return p.filterBasedPrune(ctx)
	}

	switch q := p.queue.(type) {
	case *EventQueue:
		return p.runEventQueuePrune(ctx, q)
	default:
		return p.filterBasedPrune(ctx)
	}
}

// runEventQueuePrune drains the EventQueue and purges by docIDs.
// Uses doc-count threshold (max_blocks * docs_per_block) because P2P events
// arrive in non-deterministic order — block docs may arrive before their
// dependent docs (transactions, logs, etc.).
func (p *Pruner) runEventQueuePrune(ctx context.Context, q *EventQueue) error {
	totalDocs := int64(q.Len())
	maxDocs := p.cfg.MaxDocs()

	if totalDocs <= maxDocs {
		// Queue is underfilled (e.g., after a crash restart where the queue was lost).
		// Do NOT fall back to filter-based pruning — the DB may contain snapshot-
		// imported data that should not be pruned. Only prune what the queue tracks.
		//
		// Logged so a queue that never reaches the threshold is distinguishable from a
		// pruner that is not running.
		logger.Sugar.Infof("Prune skipped: queue has %d docs, threshold %d (max_blocks=%d × docs_per_block=%d)",
			totalDocs, maxDocs, p.cfg.MaxBlocks, p.cfg.DocsPerBlock)
		return nil
	}

	excess := int(totalDocs - maxDocs)
	result := q.DrainDocs(excess)
	if result == nil {
		return nil
	}

	logger.Sugar.Infof("Pruning %d docs (%d blocks), queue had %d docs, keeping %d (max_blocks=%d × docs_per_block=%d, prune_history=%v)",
		excess, result.BlockCount, totalDocs, maxDocs, p.cfg.MaxBlocks, p.cfg.DocsPerBlock, p.cfg.PruneHistory)

	return p.purgeFromDrainResult(ctx, q, result)
}

// purgeFromDrainResult deletes documents from a DrainResult, dependent collections first and
// the block collection last. Anything left unpurged goes back on the queue rather than being
// dropped, whether a single collection failed or a stop ended the cycle early, so it is retried
// on a later cycle instead of leaking from the store.
func (p *Pruner) purgeFromDrainResult(ctx context.Context, q *EventQueue, result *DrainResult) error {
	startTime := time.Now()
	totalSubmitted := int64(0)

	// Dependents before blocks, so a block is never removed ahead of the documents that
	// reference it.
	order := make([]string, 0, len(p.collections.DependentCollections)+1)
	order = append(order, p.collections.DependentCollections...)
	order = append(order, p.collections.BlockCollection)

	for i, colName := range order {
		docIDs, ok := result.DocIDsByCollection[colName]
		if !ok || len(docIDs) == 0 {
			continue
		}

		submitted, err := p.purgeByDocIDs(ctx, colName, docIDs)
		if err == nil {
			// A failed collection is re-queued and purged again later, so counting its
			// partial progress here would count those documents twice.
			totalSubmitted += submitted
			continue
		}

		// Either path puts back whatever was drained, including anything already purged.
		// Re-purging a document that is gone is a single lookup, whereas dropping it leaks
		// the document.
		if errors.Is(err, errStopped) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// This collection and every one after it are still drained, so all of them go back.
			docs, cols := q.RequeueDrained(result, order[i:])
			logger.Sugar.Infof("Prune stopped during %s, re-queued %d docs across %d collections",
				colName, docs, cols)
			return nil
		}

		q.Requeue(colName, docIDs)
		logger.Sugar.Warnf("Failed to purge %s, re-queued %d docs: %v", colName, len(docIDs), err)
	}

	logger.Sugar.Infof("Prune cycle done: submitted %d docs for %d blocks in %v",
		totalSubmitted, result.BlockCount, time.Since(startTime))

	p.mu.Lock()
	p.totalBlocksPruned += int64(result.BlockCount)
	p.totalDocsPruned += totalSubmitted
	p.lastPruneTime = time.Now()
	p.mu.Unlock()

	return nil
}

// startupCleanup removes blocks left over from previous runs that aren't in the queue.
func (p *Pruner) startupCleanup(ctx context.Context) error {
	lowest, err := p.getLowestBlockNumber(ctx)
	if err != nil {
		return err
	}

	highest, err := p.getHighestBlockNumber(ctx)
	if err != nil {
		return err
	}

	if lowest == 0 && highest == 0 {
		logger.Sugar.Debugf("No existing blocks in database")
		return nil
	}

	currentCount := highest - lowest + 1
	if currentCount <= p.cfg.MaxBlocks {
		logger.Sugar.Debugf("Existing blocks %d-%d (count=%d) within limit (max_blocks=%d), no cleanup needed",
			lowest, highest, currentCount, p.cfg.MaxBlocks)
		return nil
	}

	toPrune := currentCount - p.cfg.MaxBlocks
	cutoffBlock := lowest + toPrune - 1

	logger.Sugar.Infof("Startup cleanup: pruning blocks %d-%d (%d blocks, keeping %d-%d)",
		lowest, cutoffBlock, toPrune, cutoffBlock+1, highest)

	totalPurged, err := p.pruneBlockRange(ctx, lowest, cutoffBlock)
	if err != nil {
		logger.Sugar.Errorf("Startup: failed to prune blocks %d-%d: %v", lowest, cutoffBlock, err)
		return err
	}

	logger.Sugar.Infof("Startup cleanup complete: purged %d documents", totalPurged)

	p.mu.Lock()
	p.totalBlocksPruned += toPrune
	p.totalDocsPruned += totalPurged
	p.lastPruneTime = time.Now()
	p.mu.Unlock()

	return nil
}

// filterBasedPrune checks the actual DB block count and prunes excess blocks.
// Used by the indexer queue (no P2P) and as a fallback when the queue is underfilled.
func (p *Pruner) filterBasedPrune(ctx context.Context) error {
	highest, err := p.getHighestBlockNumber(ctx)
	if err != nil {
		return err
	}
	if highest == 0 {
		return nil
	}

	lowest, err := p.getLowestBlockNumber(ctx)
	if err != nil {
		return err
	}
	if lowest == 0 {
		return nil
	}

	dbBlockCount := highest - lowest + 1
	if dbBlockCount <= p.cfg.MaxBlocks {
		return nil
	}

	excess := dbBlockCount - p.cfg.MaxBlocks
	cutoff := lowest + excess - 1

	logger.Sugar.Infof("Filter-based prune: %d excess blocks (%d-%d), pruning %d-%d",
		excess, lowest, highest, lowest, cutoff)

	purged, err := p.pruneBlockRange(ctx, lowest, cutoff)
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.totalBlocksPruned += excess
	p.totalDocsPruned += purged
	p.lastPruneTime = time.Now()
	p.mu.Unlock()

	return nil
}

// pruneBlockRange removes all documents for blocks in [startBlock, endBlock].
// Uses order+limit queries to get docIDs, then purges them.
// Safe to call with concurrent P2P replication — merge handles missing blocks gracefully.
func (p *Pruner) pruneBlockRange(ctx context.Context, startBlock, endBlock int64) (int64, error) {
	totalPurged := int64(0)

	logger.Sugar.Infof("pruneBlockRange: deleting blocks %d-%d (%d blocks)",
		startBlock, endBlock, endBlock-startBlock+1)

	// Dependent collections first, block collection last
	for _, colName := range p.collections.DependentCollections {
		docIDs, err := p.queryOldestDocIDs(ctx, colName, "blockNumber", endBlock)
		if err != nil {
			logger.Sugar.Warnf("pruneBlockRange: query failed for %s (skipping): %v", colName, err)
			continue
		}
		if len(docIDs) > 0 {
			purged, err := p.purgeByDocIDs(ctx, colName, docIDs)
			if err != nil {
				logger.Sugar.Warnf("pruneBlockRange: failed to purge %s: %v", colName, err)
			} else {
				totalPurged += purged
			}
		}
	}

	blockDocIDs, err := p.queryOldestDocIDs(ctx, p.collections.BlockCollection, p.collections.BlockNumberField, endBlock)
	if err != nil {
		return totalPurged, fmt.Errorf("query failed for blocks: %w", err)
	}
	if len(blockDocIDs) > 0 {
		purged, err := p.purgeByDocIDs(ctx, p.collections.BlockCollection, blockDocIDs)
		if err != nil {
			return totalPurged, fmt.Errorf("failed to purge blocks: %w", err)
		}
		totalPurged += purged
	}

	logger.Sugar.Infof("pruneBlockRange: purged %d docs for blocks %d-%d", totalPurged, startBlock, endBlock)
	return totalPurged, nil
}

// ─── Document operations ─────────────────────────────────────────────────────

// queryOldestDocIDs queries for docIDs where fieldName <= maxBlockNumber using order+limit.
// Works on P2P-replicated data where filter queries return empty results.
func (p *Pruner) queryOldestDocIDs(ctx context.Context, collectionName, fieldName string, maxBlockNumber int64) ([]string, error) {
	limit := 50000
	query := fmt.Sprintf(`query {
		%s(order: { %s: ASC }, limit: %d) {
			_docID
			%s
		}
	}`, collectionName, fieldName, limit, fieldName)

	result := p.defraNode.DB.ExecRequest(ctx, query)
	if len(result.GQL.Errors) > 0 {
		return nil, fmt.Errorf("query failed for %s: %w", collectionName, result.GQL.Errors[0])
	}

	data, ok := result.GQL.Data.(map[string]any)
	if !ok {
		return nil, nil
	}

	// DefraDB may return []map[string]interface{} or []interface{} depending on context.
	// In Go these are distinct types, so we must handle both.
	raw := data[collectionName]

	var docIDs []string

	switch docs := raw.(type) {
	case []map[string]any:
		for _, docMap := range docs {
			bn, err := parseBlockNumber(docMap[fieldName])
			if err != nil {
				return nil, err
			}
			if bn > maxBlockNumber {
				break
			}
			if docID, ok := docMap["_docID"].(string); ok {
				docIDs = append(docIDs, docID)
			}
		}
	case []any:
		for _, doc := range docs {
			docMap, ok := doc.(map[string]any)
			if !ok {
				continue
			}
			bn, err := parseBlockNumber(docMap[fieldName])
			if err != nil {
				return nil, err
			}
			if bn > maxBlockNumber {
				break
			}
			if docID, ok := docMap["_docID"].(string); ok {
				docIDs = append(docIDs, docID)
			}
		}
	default:
		return nil, nil
	}

	return docIDs, nil
}

// purgeByDocIDs deletes documents by their docIDs.
func (p *Pruner) purgeByDocIDs(ctx context.Context, collectionName string, docIDs []string) (int64, error) {
	if len(docIDs) == 0 {
		return 0, nil
	}
	if err := p.stopping(ctx); err != nil {
		return 0, err
	}

	startTime := time.Now()
	logger.Sugar.Infof("Purging %d documents from %s", len(docIDs), collectionName)

	purge := p.purgeDocs
	if purge == nil {
		col, err := p.defraNode.DB.GetCollectionByName(ctx, collectionName)
		if err != nil {
			return 0, fmt.Errorf("failed to get collection %s: %w", collectionName, err)
		}
		purge = func(ctx context.Context, ids []client.DocID) error {
			return col.PurgeByDocIDs(ctx, ids, p.cfg.PruneHistory)
		}
	}

	clientDocIDs := make([]client.DocID, 0, len(docIDs))
	for _, id := range docIDs {
		docID, err := client.NewDocIDFromString(id)
		if err != nil {
			logger.Sugar.Warnf("Skipping invalid docID %s: %v", id, err)
			continue
		}
		clientDocIDs = append(clientDocIDs, docID)
	}

	// Submitted in batches so a stop is honoured part-way through. DefraDB commits its
	// own transactions inside each call and does not check the context, so without this
	// the whole list runs to completion however long it takes.
	var submitted int64
	lastProgress := startTime
	for i := 0; i < len(clientDocIDs); i += purgeBatchSize {
		if err := p.stopping(ctx); err != nil {
			return submitted, err
		}

		end := min(i+purgeBatchSize, len(clientDocIDs))
		if err := purge(ctx, clientDocIDs[i:end]); err != nil {
			return submitted, err
		}
		submitted += int64(end - i)

		if time.Since(lastProgress) >= purgeProgressInterval {
			logger.Sugar.Infof("Purging %s: %d/%d submitted in %v",
				collectionName, submitted, len(clientDocIDs), time.Since(startTime))
			lastProgress = time.Now()
		}
	}

	// Submitted, not deleted: PurgeByDocIDs reports only an error, and a document that
	// was already gone purges silently, so this cannot distinguish the two.
	logger.Sugar.Infof("Submitted %d/%d documents from %s in %v",
		submitted, len(docIDs), collectionName, time.Since(startTime))
	return submitted, nil
}

// ─── Block number queries ────────────────────────────────────────────────────

func (p *Pruner) getLowestBlockNumber(ctx context.Context) (int64, error) {
	query := `query {
		` + p.collections.BlockCollection + ` (order: {` + p.collections.BlockNumberField + `: ASC}, limit: 1) {
			` + p.collections.BlockNumberField + `
		}
	}`

	result := p.defraNode.DB.ExecRequest(ctx, query)
	if len(result.GQL.Errors) > 0 {
		return 0, result.GQL.Errors[0]
	}

	return p.extractBlockNumber(result.GQL.Data)
}

func (p *Pruner) getHighestBlockNumber(ctx context.Context) (int64, error) {
	query := `query {
		` + p.collections.BlockCollection + ` (order: {` + p.collections.BlockNumberField + `: DESC}, limit: 1) {
			` + p.collections.BlockNumberField + `
		}
	}`

	result := p.defraNode.DB.ExecRequest(ctx, query)
	if len(result.GQL.Errors) > 0 {
		return 0, result.GQL.Errors[0]
	}

	return p.extractBlockNumber(result.GQL.Data)
}

func (p *Pruner) extractBlockNumber(gqlData any) (int64, error) {
	data, ok := gqlData.(map[string]any)
	if !ok {
		return 0, nil
	}

	blocksRaw := data[p.collections.BlockCollection]

	if blocksTyped, ok := blocksRaw.([]map[string]any); ok {
		if len(blocksTyped) == 0 {
			return 0, nil
		}
		if number, ok := blocksTyped[0][p.collections.BlockNumberField]; ok {
			return parseBlockNumber(number)
		}
		return 0, nil
	}

	blocks, ok := blocksRaw.([]any)
	if !ok || len(blocks) == 0 {
		return 0, nil
	}

	block, ok := blocks[0].(map[string]any)
	if !ok {
		return 0, nil
	}

	if number, ok := block[p.collections.BlockNumberField]; ok {
		return parseBlockNumber(number)
	}
	return 0, nil
}

func parseBlockNumber(number any) (int64, error) {
	switch v := number.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	}
	return 0, nil
}
