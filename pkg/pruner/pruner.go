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
	// purgeProgressInterval bounds how often a long purge reports progress, so it stays
	// visible without flooding a short one.
	purgeProgressInterval = 30 * time.Second
)

// dependentBlockNumberField names the block a dependent document belongs to. The block collection
// names its own field through CollectionConfig.
const dependentBlockNumberField = "blockNumber"

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
	// retainHistory disables the height sweep, for a node bootstrapped with history it should keep.
	retainHistory bool
	// heightPrunable is the subset of DependentCollections carrying dependentBlockNumberField.
	heightPrunable []string
	stopChan       chan struct{}
	wg             sync.WaitGroup
	mu             sync.RWMutex

	// purgeDocs deletes one batch of documents. Set only by tests; in production it is nil
	// and the collection's own PurgeByDocIDs is used, since a purge is otherwise only
	// reachable through a running node.
	purgeDocs func(ctx context.Context, docIDs []client.DocID) error

	// Metrics
	lastPruneTime      time.Time
	totalBlocksPruned  int64
	totalDocsSubmitted int64
	isRunning          bool
}

// Metrics holds pruning statistics.
type Metrics struct {
	Enabled            bool      `json:"enabled"`
	IsRunning          bool      `json:"is_running"`
	LastPruneTime      time.Time `json:"last_prune_time"`
	TotalBlocksPruned  int64     `json:"total_blocks_pruned"`
	TotalDocsSubmitted int64     `json:"total_docs_submitted"`
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

// SetRetainHistory keeps blocks below the retention window instead of pruning them by height.
func (p *Pruner) SetRetainHistory(retain bool) {
	p.retainHistory = retain
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

// Stop signals the pruner to stop and waits for the current cycle, giving up when ctx
// expires. Nothing in the purge path can be cancelled, so a cycle can outlast any budget;
// the queue is saved either way so the work resumes after a restart.
func (p *Pruner) Stop(ctx context.Context) {
	p.mu.Lock()
	if !p.isRunning {
		p.mu.Unlock()
		return
	}
	p.isRunning = false
	p.mu.Unlock()

	logger.Sugar.Infof("Pruner stopping, waiting for current operation to finish...")
	close(p.stopChan)

	stopped := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-ctx.Done():
		// Saving is still safe: the queue snapshots under its own lock, so a cycle that
		// is still unwinding cannot tear the file.
		logger.Sugar.Warn("Pruner did not stop within the shutdown budget, saving the queue anyway")
	}

	// Save queue to disk for fast restart
	if p.queue != nil {
		queueLen := p.queue.Len()
		logger.Sugar.Infof("Saving prune queue to disk (%d entries)...", queueLen)
		if err := p.queue.Save(); err != nil {
			logger.Sugar.Errorf("Failed to save prune queue: %v", err)
		} else {
			logger.Sugar.Infof("Prune queue saved successfully")
		}
	}

	logger.Sugar.Info("Pruner stopped")
}

// GetMetrics returns current pruning statistics.
func (p *Pruner) GetMetrics() Metrics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return Metrics{
		Enabled:            p.cfg.Enabled,
		IsRunning:          p.isRunning,
		LastPruneTime:      p.lastPruneTime,
		TotalBlocksPruned:  p.totalBlocksPruned,
		TotalDocsSubmitted: p.totalDocsSubmitted,
	}
}

// pruneLoop runs the periodic pruning check.
func (p *Pruner) pruneLoop(ctx context.Context) {
	defer p.wg.Done()

	p.heightPrunable = p.resolveHeightPrunable(ctx)

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

// resolveHeightPrunable returns the dependent collections the height sweep can order on. One
// without the field is bounded only by the queue.
func (p *Pruner) resolveHeightPrunable(ctx context.Context) []string {
	prunable := make([]string, 0, len(p.collections.DependentCollections))
	var skipped []string

	for _, name := range p.collections.DependentCollections {
		col, err := p.defraNode.DB.GetCollectionByName(ctx, name)
		if err != nil {
			skipped = append(skipped, name)
			continue
		}
		if _, ok := col.Version().GetFieldByName(dependentBlockNumberField); !ok {
			skipped = append(skipped, name)
			continue
		}
		prunable = append(prunable, name)
	}

	if len(skipped) > 0 {
		logger.Sugar.Warnf("Height prune skips %v: no %s field, so these are bounded only by the queue",
			skipped, dependentBlockNumberField)
	}
	return prunable
}

// runPrune executes the appropriate pruning strategy based on queue type and state.
func (p *Pruner) runPrune(ctx context.Context) error {
	if p.queue == nil {
		return p.pruneBeyondRetention(ctx, p.cfg.MaxDocsPerCycle)
	}

	switch q := p.queue.(type) {
	case *EventQueue:
		return p.runEventQueuePrune(ctx, q)
	default:
		return p.pruneBeyondRetention(ctx, p.cfg.MaxDocsPerCycle)
	}
}

// runEventQueuePrune drains the EventQueue and purges by docIDs.
// Uses doc-count threshold (max_blocks * docs_per_block) because P2P events
// arrive in non-deterministic order — block docs may arrive before their
// dependent docs (transactions, logs, etc.).
func (p *Pruner) runEventQueuePrune(ctx context.Context, q *EventQueue) error {
	drained, err := p.drainQueue(ctx, q)
	if err != nil {
		return err
	}
	if p.retainHistory {
		return nil
	}
	return p.pruneBeyondRetention(ctx, p.cfg.MaxDocsPerCycle-drained)
}

// drainQueue removes the queue's excess over max_docs, within the cycle's budget, and returns how
// much of the budget it used.
func (p *Pruner) drainQueue(ctx context.Context, q *EventQueue) (int64, error) {
	totalDocs := int64(q.Len())
	maxDocs := p.cfg.MaxDocs()

	if totalDocs <= maxDocs {
		// Logged so a queue that never reaches the threshold is distinguishable from a
		// pruner that is not running.
		logger.Sugar.Infof("Prune skipped: queue has %d docs, threshold %d (max_blocks=%d × docs_per_block=%d)",
			totalDocs, maxDocs, p.cfg.MaxBlocks, p.cfg.DocsPerBlock)
		return 0, nil
	}

	excess := min(totalDocs-maxDocs, p.cfg.MaxDocsPerCycle)
	result := q.DrainDocs(int(excess))
	if result == nil {
		return 0, nil
	}

	logger.Sugar.Infof("Pruning %d docs (%d blocks), queue had %d docs, keeping %d (max_blocks=%d × docs_per_block=%d, prune_history=%v)",
		excess, result.BlockCount, totalDocs, maxDocs, p.cfg.MaxBlocks, p.cfg.DocsPerBlock, p.cfg.PruneHistory)

	return excess, p.purgeFromDrainResult(ctx, q, result)
}

// purgeFromDrainResult deletes documents from a DrainResult, dependent collections first and
// the block collection last. Anything left unpurged goes back on the queue rather than being
// dropped, whether a single collection failed or a stop ended the cycle early, so it is retried
// on a later cycle instead of leaking from the store.
func (p *Pruner) purgeFromDrainResult(ctx context.Context, q *EventQueue, result *DrainResult) error {
	startTime := time.Now()
	totalSubmitted := int64(0)
	blocksPruned := int64(0)

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
			if colName == p.collections.BlockCollection {
				blocksPruned = int64(result.BlockCount)
			}
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
	// Only blocks whose own purge succeeded. A re-queued block collection is drained and
	// counted again on a later cycle, so counting it here counts those blocks twice.
	p.totalBlocksPruned += blocksPruned
	p.totalDocsSubmitted += totalSubmitted
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

	totalSubmitted, blocksPruned, err := p.pruneBelow(ctx, cutoffBlock, p.cfg.MaxDocsPerCycle)
	if err != nil {
		logger.Sugar.Errorf("Startup: failed to prune blocks %d-%d: %v", lowest, cutoffBlock, err)
		return err
	}

	logger.Sugar.Infof("Startup cleanup complete: submitted %d documents", totalSubmitted)

	p.mu.Lock()
	p.totalBlocksPruned += blocksPruned
	p.totalDocsSubmitted += totalSubmitted
	p.lastPruneTime = time.Now()
	p.mu.Unlock()

	return nil
}

// pruneBeyondRetention removes documents for blocks below the retention window, whether or not the
// queue knows about them, within the budget left for this cycle. The cutoff is measured from the
// highest block the node holds.
func (p *Pruner) pruneBeyondRetention(ctx context.Context, budget int64) error {
	if budget <= 0 {
		return nil
	}

	highest, err := p.getHighestBlockNumber(ctx)
	if err != nil {
		return err
	}

	cutoff := highest - p.cfg.MaxBlocks
	if cutoff <= 0 {
		// The node holds no more than the retention window, including when the store is empty.
		return nil
	}

	submitted, blocks, err := p.pruneBelow(ctx, cutoff, budget)
	if err != nil {
		return err
	}
	if submitted == 0 {
		return nil
	}

	p.mu.Lock()
	p.totalBlocksPruned += blocks
	p.totalDocsSubmitted += submitted
	p.lastPruneTime = time.Now()
	p.mu.Unlock()

	return nil
}

// pruneBelow removes documents at or below cutoff, dependent collections before the block
// collection, so a block is not removed ahead of the documents that reference it. A stop ends the
// cycle where it is; what is left is found again by the next one.
//
// Safe to run alongside P2P replication: a merge for a removed block is handled as a new document.
func (p *Pruner) pruneBelow(ctx context.Context, cutoff, budget int64) (submitted, blocks int64, err error) {
	for _, colName := range p.heightPrunable {
		purged, err := p.purgeCollectionBelow(ctx, colName, dependentBlockNumberField, cutoff, budget-submitted)
		if err != nil {
			if abandoned(err) {
				return submitted, blocks, nil
			}
			logger.Sugar.Warnf("Prune below %d: %s skipped: %v", cutoff, colName, err)
			continue
		}
		submitted += purged
	}

	blocks, err = p.purgeCollectionBelow(ctx, p.collections.BlockCollection, p.collections.BlockNumberField, cutoff, budget-submitted)
	if err != nil {
		if abandoned(err) {
			return submitted, 0, nil
		}
		return submitted, 0, fmt.Errorf("prune below %d: %s: %w", cutoff, p.collections.BlockCollection, err)
	}
	submitted += blocks

	if submitted > 0 {
		logger.Sugar.Infof("Prune below %d: submitted %d documents across %d blocks", cutoff, submitted, blocks)
	}
	return submitted, blocks, nil
}

// abandoned reports whether an error ended the work rather than failed it, so the caller stops
// instead of moving on to the next collection.
func abandoned(err error) bool {
	return errors.Is(err, errStopped) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// purgeCollectionBelow removes one collection's documents at or below cutoff, up to the query
// limit. Each collection is checked on its own, because a dependent can hold older blocks than the
// block collection does.
func (p *Pruner) purgeCollectionBelow(ctx context.Context, collectionName, fieldName string, cutoff, limit int64) (int64, error) {
	oldest, found, err := p.edgeBlockNumber(ctx, collectionName, fieldName, "ASC")
	if err != nil {
		return 0, err
	}
	if !found || oldest > cutoff {
		return 0, nil
	}

	docIDs, err := p.queryOldestDocIDs(ctx, collectionName, fieldName, cutoff, limit)
	if err != nil {
		return 0, err
	}
	if len(docIDs) == 0 {
		return 0, nil
	}

	return p.purgeByDocIDs(ctx, collectionName, docIDs)
}

// ─── Document operations ─────────────────────────────────────────────────────

// queryOldestDocIDs queries for docIDs where fieldName <= maxBlockNumber using order+limit.
// Works on P2P-replicated data where filter queries return empty results.
func (p *Pruner) queryOldestDocIDs(ctx context.Context, collectionName, fieldName string, maxBlockNumber, limit int64) ([]string, error) {
	// A limit of zero is unlimited to the query planner, so a spent budget stops here.
	if limit <= 0 {
		return nil, nil
	}

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
	lowest, _, err := p.edgeBlockNumber(ctx, p.collections.BlockCollection, p.collections.BlockNumberField, "ASC")
	return lowest, err
}

func (p *Pruner) getHighestBlockNumber(ctx context.Context) (int64, error) {
	highest, _, err := p.edgeBlockNumber(ctx, p.collections.BlockCollection, p.collections.BlockNumberField, "DESC")
	return highest, err
}

// edgeBlockNumber reads the block number at one end of a collection's ordering. The bool is false
// when the collection is empty, which a zero block number cannot be distinguished from otherwise.
func (p *Pruner) edgeBlockNumber(ctx context.Context, collectionName, fieldName, direction string) (int64, bool, error) {
	query := fmt.Sprintf(`query {
		%s(order: { %s: %s }, limit: 1) {
			%s
		}
	}`, collectionName, fieldName, direction, fieldName)

	result := p.defraNode.DB.ExecRequest(ctx, query)
	if len(result.GQL.Errors) > 0 {
		return 0, false, result.GQL.Errors[0]
	}

	return extractBlockNumber(result.GQL.Data, collectionName, fieldName)
}

func extractBlockNumber(gqlData any, collectionName, fieldName string) (int64, bool, error) {
	data, ok := gqlData.(map[string]any)
	if !ok {
		return 0, false, nil
	}

	// DefraDB returns []map[string]any or []any depending on context; both reach here.
	var first map[string]any
	switch docs := data[collectionName].(type) {
	case []map[string]any:
		if len(docs) == 0 {
			return 0, false, nil
		}
		first = docs[0]
	case []any:
		if len(docs) == 0 {
			return 0, false, nil
		}
		if first, ok = docs[0].(map[string]any); !ok {
			return 0, false, nil
		}
	default:
		return 0, false, nil
	}

	number, err := parseBlockNumber(first[fieldName])
	return number, err == nil, err
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
