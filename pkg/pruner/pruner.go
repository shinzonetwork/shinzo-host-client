// Package pruner deletes documents for old blocks, so the store does not keep growing.
//
// Each cycle asks the store for the documents below the retention window: those whose block
// height is at or below (highest block held - max_blocks). Each collection names the field
// holding its height and the sweep orders on that field, so the field has to be indexed;
// a dependent whose height field is unindexed is skipped rather than swept.
//
// A cycle deletes at most max_docs_per_cycle documents. Whatever is left is found again by the
// next cycle.
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

// errStopped ends a purge early because the pruner is shutting down. The documents it did not
// reach are still below the window, so the next cycle finds them again.
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

// Pruner periodically removes documents for blocks below the retention window.
type Pruner struct {
	cfg         *Config
	collections CollectionConfig
	defraNode   *node.Node
	// retainHistory turns pruning off, for a node bootstrapped with history it should keep.
	retainHistory bool
	// heightPrunable is the subset of Dependents the sweep can order on.
	heightPrunable []CollectionHeight
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

// SetRetainHistory keeps blocks below the retention window instead of deleting them.
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

	logger.Sugar.Debugf("Starting pruner (max_blocks=%d, max_docs_per_cycle=%d, interval=%ds)",
		p.cfg.MaxBlocks, p.cfg.MaxDocsPerCycle, p.cfg.IntervalSeconds)

	p.wg.Add(1)
	go p.pruneLoop(ctx)

	return nil
}

// Stop signals the pruner to stop and waits for the current cycle, giving up when ctx
// expires. Nothing in the purge path can be cancelled, so a cycle can outlast any budget.
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
		logger.Sugar.Warn("Pruner did not stop within the shutdown budget")
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

// resolveHeightPrunable returns the dependents whose height field carries an index. A field that
// is absent has no index either, so both cases are skipped.
func (p *Pruner) resolveHeightPrunable(ctx context.Context) []CollectionHeight {
	prunable := make([]CollectionHeight, 0, len(p.collections.Dependents))
	var skipped []string

	for _, dep := range p.collections.Dependents {
		col, err := p.defraNode.DB.GetCollectionByName(ctx, dep.Name)
		if err != nil {
			skipped = append(skipped, dep.Name)
			continue
		}
		// Ordering on an unindexed field materialises the whole collection before the limit applies.
		if len(col.Version().GetIndexesOnField(dep.HeightField)) == 0 {
			skipped = append(skipped, dep.Name)
			continue
		}
		prunable = append(prunable, dep)
	}

	if len(skipped) > 0 {
		logger.Sugar.Errorf("Prune cannot delete old documents from %v: their block-number field is not indexed, so these collections keep growing",
			skipped)
	}
	return prunable
}

// runPrune removes up to max_docs_per_cycle documents below the retention window. The cutoff is
// measured from the highest block the node holds.
func (p *Pruner) runPrune(ctx context.Context) error {
	if p.retainHistory {
		return nil
	}

	highest, err := p.getHighestBlockNumber(ctx)
	if err != nil {
		return err
	}

	cutoff := highest - p.cfg.MaxBlocks
	if cutoff <= 0 {
		// The node holds no more than the retention window, including when the store is empty.
		logger.Sugar.Infof("Prune: nothing to delete, the newest block (%d) is inside the %d blocks being kept",
			highest, p.cfg.MaxBlocks)
		return nil
	}

	submitted, blocks, err := p.pruneBelow(ctx, cutoff, p.cfg.MaxDocsPerCycle)
	if err != nil {
		return err
	}

	// Logged every cycle so a run that deleted nothing is distinguishable from a pruner that is
	// not running.
	logger.Sugar.Infof("Prune: deleted %d documents at or below block %d, including %d blocks, keeping the newest %d blocks",
		submitted, cutoff, blocks, p.cfg.MaxBlocks)

	// A budget spent in full means more documents sit below the window than one cycle removes.
	if submitted >= p.cfg.MaxDocsPerCycle {
		logger.Sugar.Warnf("Prune: stopped at the max_docs_per_cycle limit of %d, anything still below the window goes next cycle",
			p.cfg.MaxDocsPerCycle)
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
	for _, dep := range p.heightPrunable {
		purged, err := p.purgeCollectionBelow(ctx, dep.Name, dep.HeightField, cutoff, budget-submitted)
		if err != nil {
			if abandoned(err) {
				return submitted, blocks, nil
			}
			logger.Sugar.Warnf("Prune: could not delete old documents from %s: %v", dep.Name, err)
			continue
		}
		submitted += purged
	}

	blocks, err = p.purgeCollectionBelow(ctx, p.collections.Block.Name, p.collections.Block.HeightField, cutoff, budget-submitted)
	if err != nil {
		if abandoned(err) {
			return submitted, 0, nil
		}
		return submitted, 0, fmt.Errorf("delete documents at or below block %d from %s: %w", cutoff, p.collections.Block.Name, err)
	}
	submitted += blocks
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
			bn, parsed := parseBlockNumber(docMap[fieldName])
			if !parsed {
				continue
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
			bn, parsed := parseBlockNumber(docMap[fieldName])
			if !parsed {
				continue
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
	logger.Sugar.Infof("Prune: deleting %d documents from %s", len(docIDs), collectionName)

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
			logger.Sugar.Infof("Prune: %s, %d of %d documents deleted so far (%v)",
				collectionName, submitted, len(clientDocIDs), time.Since(startTime))
			lastProgress = time.Now()
		}
	}

	// The count is what was handed to PurgeByDocIDs. It reports only an error, and a document
	// that was already gone purges silently, so the log cannot separate the two.
	logger.Sugar.Infof("Prune: deleted %d of %d documents from %s in %v",
		submitted, len(docIDs), collectionName, time.Since(startTime))
	return submitted, nil
}

// ─── Block number queries ────────────────────────────────────────────────────

func (p *Pruner) getHighestBlockNumber(ctx context.Context) (int64, error) {
	highest, _, err := p.edgeBlockNumber(ctx, p.collections.Block.Name, p.collections.Block.HeightField, "DESC")
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

	number, parsed := parseBlockNumber(first[fieldName])
	return number, parsed, nil
}

// parseBlockNumber reads a block number from a GraphQL value. The bool is false when the
// value is absent or not numeric. Callers must not substitute zero: it sits below every
// cutoff, so the document would be selected for deletion.
func parseBlockNumber(number any) (int64, bool) {
	switch v := number.(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	}
	return 0, false
}
