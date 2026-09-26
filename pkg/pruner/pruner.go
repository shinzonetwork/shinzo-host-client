// Package pruner deletes documents for old blocks, so the store does not keep growing.
//
// The cutoff is the highest block held less max_blocks, and it never falls. It is shared with the
// replication filter, which rejects documents at or below it. Each cycle deletes at most
// max_docs_per_cycle documents at or below the cutoff, lowest height first across the collections.
// Each collection names the field holding its height and the sweep reads that field through its
// index, so a dependent whose height field is unindexed is skipped.
package pruner

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
)

const (
	// purgeBatchSize is how many documents a sweep page reads and one purge call hands to
	// DefraDB. It sets how often a purge can notice a stop, not the transaction size: DefraDB
	// commits in its own smaller chunks regardless.
	purgeBatchSize = 1000
	// bottomPassInterval is how often every collection is swept again from its lowest height,
	// to reach documents that arrived below where the sweep had got to.
	bottomPassInterval = time.Hour
)

var (
	// errStopped ends a cycle early because the pruner is shutting down. What it did not reach is
	// still at or below the cutoff, so a later cycle finds it again.
	errStopped = errors.New("pruner stopped")
	// errUnreadableRow reports a row whose height or docID does not read as expected.
	errUnreadableRow = errors.New("row without a readable height or docID")
)

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
	// cutoff is shared with the replication filter. The pruner is its only writer.
	cutoff *Cutoff
	// from holds, per collection, the height its next sweep page starts at.
	from map[string]int64
	// bottomPassAt is when every collection was last swept from its lowest height.
	bottomPassAt time.Time
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
	// now is time.Now, replaced by tests that step the clock for the bottom pass.
	now func() time.Time

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

// NewPruner creates a new Pruner instance that publishes its cutoff to cutoff.
func NewPruner(cfg *Config, defraNode *node.Node, cutoff *Cutoff, collections ...CollectionConfig) *Pruner {
	cols := DefaultCollectionConfig()
	if len(collections) > 0 {
		cols = collections[0]
	}
	return &Pruner{
		cfg:         cfg,
		collections: cols,
		defraNode:   defraNode,
		cutoff:      cutoff,
		from:        make(map[string]int64),
		stopChan:    make(chan struct{}),
		now:         time.Now,
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

	if p.retainHistory {
		logger.Sugar.Warn("Pruner is not deleting anything because host.snapshot.enabled is true, which keeps imported snapshot history")
	} else {
		logger.Sugar.Infof("Pruner started: keeps the newest %d block heights, deletes at most %d documents per cycle, runs every %ds, prune_history=%t",
			p.cfg.MaxBlocks, p.cfg.MaxDocsPerCycle, p.cfg.IntervalSeconds, p.cfg.PruneHistory)
	}

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

// pruneLoop runs a cycle at once, so the cutoff is published as soon as the pruner starts, and
// then one per interval.
func (p *Pruner) pruneLoop(ctx context.Context) {
	defer p.wg.Done()

	p.heightPrunable = p.resolveHeightPrunable(ctx)

	ticker := time.NewTicker(time.Duration(p.cfg.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		if err := p.runPrune(ctx); err != nil && !abandoned(err) {
			logger.Sugar.Errorf("Prune cycle failed before deleting anything, retention cutoff unchanged; retrying next interval: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-p.stopChan:
			return
		case <-ticker.C:
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

// runPrune publishes this cycle's cutoff and then deletes up to max_docs_per_cycle documents at or
// below it.
func (p *Pruner) runPrune(ctx context.Context) error {
	if p.retainHistory {
		return nil
	}
	if err := p.stopping(ctx); err != nil {
		return err
	}

	now := p.now()
	highest, found, err := p.highestBlockHeight(ctx)
	if err != nil {
		return err
	}
	if !found {
		logger.Sugar.Info("Prune cycle: skipped, this node holds no blocks yet so there is no retention cutoff; nothing is deleted and the retention filter rejects nothing")
		return nil
	}
	target := highest - p.cfg.MaxBlocks
	if target <= 0 {
		logger.Sugar.Infof("Prune cycle: nothing to delete yet, the highest block held (%d) is not above max_blocks (%d)",
			highest, p.cfg.MaxBlocks)
		return nil
	}
	p.cutoff.Raise(target)
	cutoff := p.cutoff.Load()

	if now.Sub(p.bottomPassAt) >= bottomPassInterval {
		clear(p.from)
		p.bottomPassAt = now
		logger.Sugar.Info("Prune cycle: sweeping every collection from its lowest height, as on the first cycle and once an hour, to reach documents that arrived below where the sweep had got to")
	}

	start := time.Now()
	fronts, reached := p.sweep(ctx, cutoff)
	p.recordCycle(highest, cutoff, fronts, reached, time.Since(start))
	return nil
}

// recordCycle logs what a cycle deleted, per collection, and adds it to the metrics. It logs every
// cycle, so a cycle that deleted nothing is distinguishable from a pruner that is not running.
func (p *Pruner) recordCycle(highest, cutoff int64, fronts []*front, reached int64, took time.Duration) {
	var submitted, blocks int64
	var perCollection []string
	for _, f := range fronts {
		if f.submitted == 0 {
			continue
		}
		submitted += f.submitted
		if f.isBlock {
			blocks = f.submitted
		}
		perCollection = append(perCollection, fmt.Sprintf("%s=%d", f.col.Name, f.submitted))
	}
	deleted := fmt.Sprintf("%d documents", submitted)
	if len(perCollection) > 0 {
		deleted += " (" + strings.Join(perCollection, ", ") + ")"
	}

	logger.Sugar.Infof("Prune cycle: keeping blocks %d-%d (the newest %d heights); asked DefraDB to delete %s at or below block %d; took %v",
		cutoff+1, highest, highest-cutoff, deleted, cutoff, took.Round(time.Millisecond))

	if submitted >= p.cfg.MaxDocsPerCycle {
		logger.Sugar.Warnf("Prune cycle reached the max_docs_per_cycle limit (%d) at block %d; the documents from block %d to %d are deleted in the next cycles",
			p.cfg.MaxDocsPerCycle, reached, reached, cutoff)
	}

	if submitted == 0 {
		return
	}

	p.mu.Lock()
	p.totalBlocksPruned += blocks
	p.totalDocsSubmitted += submitted
	p.lastPruneTime = time.Now()
	p.mu.Unlock()
}

// highestBlockHeight returns the height of the highest block held, or false when none is held.
func (p *Pruner) highestBlockHeight(ctx context.Context) (int64, bool, error) {
	block := p.collections.Block
	// _geq: 0 leaves out blocks with no height.
	query := fmt.Sprintf(`query {
		%s(filter: {%s: {_geq: 0}}, order: {%s: DESC}, limit: 1) {
			%s
		}
	}`, block.Name, block.HeightField, block.HeightField, block.HeightField)

	result := p.defraNode.DB.ExecRequest(ctx, query)
	if len(result.GQL.Errors) > 0 {
		return 0, false, fmt.Errorf("query %s: %w", block.Name, result.GQL.Errors[0])
	}

	rows := documentRows(result.GQL.Data, block.Name)
	if len(rows) == 0 {
		return 0, false, nil
	}
	height, ok := parseBlockNumber(rows[0][block.HeightField])
	if !ok {
		return 0, false, fmt.Errorf("%s.%s: %w", block.Name, block.HeightField, errUnreadableRow)
	}
	return height, true, nil
}

// heightDoc is a document selected for deletion and its height.
type heightDoc struct {
	docID  string
	height int64
}

// front is one collection's place in a sweep: the page read from its position, whether anything
// due lies beyond that page, and how many documents the sweep has handed to DefraDB from it.
type front struct {
	col       CollectionHeight
	isBlock   bool
	from      int64
	page      []heightDoc
	exhausted bool
	submitted int64
}

// sweep deletes up to max_docs_per_cycle documents at or below cutoff, lowest height first across
// the collections. At a height the dependents go before the block, so a block outlives the
// documents at its height, unless reading or deleting a dependent failed this cycle. A purge call
// that fails may already have deleted part of its batch, which is not counted. It returns every
// collection's front and the height of the last batch it deleted.
func (p *Pruner) sweep(ctx context.Context, cutoff int64) (fronts []*front, reached int64) {
	var submitted int64
	fronts = make([]*front, 0, len(p.heightPrunable)+1)
	for _, dep := range p.heightPrunable {
		fronts = append(fronts, &front{col: dep, from: p.from[dep.Name]})
	}
	// The block goes last, so on a tie nextBatch picks a dependent.
	fronts = append(fronts, &front{col: p.collections.Block, isBlock: true, from: p.from[p.collections.Block.Name]})
	defer func() {
		for _, f := range fronts {
			p.from[f.col.Name] = f.from
		}
	}()

	for submitted < p.cfg.MaxDocsPerCycle {
		if p.stopping(ctx) != nil {
			return fronts, reached
		}

		p.fillPages(ctx, fronts, cutoff)

		next, limit := nextBatch(fronts)
		if next == nil {
			return fronts, reached
		}

		// The front's first document is always within limit, so the batch is never empty.
		size := min(len(next.page), int(p.cfg.MaxDocsPerCycle-submitted))
		n := 0
		for n < size && next.page[n].height <= limit {
			n++
		}
		batch := next.page[:n]

		docIDs := make([]string, len(batch))
		for i, doc := range batch {
			docIDs[i] = doc.docID
		}
		purged, err := p.purgeByDocIDs(ctx, next.col.Name, docIDs)
		submitted += purged
		next.submitted += purged
		if err != nil {
			if abandoned(err) {
				return fronts, reached
			}
			logger.Sugar.Warnf("Prune: could not delete %d documents from %s at blocks %d to %d, skipping that collection this cycle: %v",
				len(batch), next.col.Name, batch[0].height, batch[n-1].height, err)
			next.page, next.exhausted = nil, true
			continue
		}

		next.page = next.page[n:]
		// The next page starts at this height again, since the batch can end partway through it.
		next.from = batch[n-1].height
		reached = next.from
	}
	return fronts, reached
}

// fillPages reads the next page for every front that has used up its page and may have more due. A
// collection that cannot be read is left alone for the rest of the cycle.
func (p *Pruner) fillPages(ctx context.Context, fronts []*front, cutoff int64) {
	for _, f := range fronts {
		if len(f.page) > 0 || f.exhausted {
			continue
		}
		if err := p.readPage(ctx, f, cutoff); err != nil {
			logger.Sugar.Warnf("Prune: could not read old documents from %s, skipping that collection this cycle: %v", f.col.Name, err)
			f.page, f.exhausted = nil, true
		}
	}
}

// nextBatch picks the front whose page starts at the lowest height, the first in fronts on a tie,
// and returns the highest height it may delete now: no higher than where any other front's page
// starts, and for the block, below every dependent's. Pages hold only heights at or below the
// cutoff, so the limit needs no other bound.
func nextBatch(fronts []*front) (*front, int64) {
	var next *front
	for _, f := range fronts {
		if len(f.page) > 0 && (next == nil || f.page[0].height < next.page[0].height) {
			next = f
		}
	}
	if next == nil {
		return nil, 0
	}

	limit := int64(math.MaxInt64)
	for _, f := range fronts {
		if f == next || len(f.page) == 0 {
			continue
		}
		height := f.page[0].height
		if next.isBlock {
			height--
		}
		limit = min(limit, height)
	}
	return next, limit
}

// readPage reads the next page of a collection's documents from its position, in height order. The
// page ends at the first height above the cutoff; reaching it, or a page shorter than asked for,
// means nothing due lies beyond the page.
func (p *Pruner) readPage(ctx context.Context, f *front, cutoff int64) error {
	field := f.col.HeightField
	// Reading from the position skips the index entries already deleted below it, and those
	// with no height, which the index sorts below every number.
	query := fmt.Sprintf(`query {
		%s(filter: {%s: {_geq: %d}}, order: {%s: ASC}, limit: %d) {
			_docID
			%s
		}
	}`, f.col.Name, field, f.from, field, purgeBatchSize, field)

	result := p.defraNode.DB.ExecRequest(ctx, query)
	if len(result.GQL.Errors) > 0 {
		return fmt.Errorf("query %s: %w", f.col.Name, result.GQL.Errors[0])
	}

	rows := documentRows(result.GQL.Data, f.col.Name)
	f.exhausted = len(rows) < purgeBatchSize
	f.page = f.page[:0]
	for _, row := range rows {
		height, ok := parseBlockNumber(row[field])
		docID, isString := row["_docID"].(string)
		if !ok || !isString {
			return fmt.Errorf("%s.%s: %w", f.col.Name, field, errUnreadableRow)
		}
		if height > cutoff {
			f.exhausted = true
			break
		}
		f.page = append(f.page, heightDoc{docID: docID, height: height})
	}
	return nil
}

// abandoned reports whether an error ended the work rather than failed it, so the caller stops
// instead of moving on to the next collection.
func abandoned(err error) bool {
	return errors.Is(err, errStopped) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// ─── Document operations ─────────────────────────────────────────────────────

// purgeByDocIDs deletes documents by their docIDs.
func (p *Pruner) purgeByDocIDs(ctx context.Context, collectionName string, docIDs []string) (int64, error) {
	if len(docIDs) == 0 {
		return 0, nil
	}
	if err := p.stopping(ctx); err != nil {
		return 0, err
	}

	startTime := time.Now()
	logger.Sugar.Debugf("Prune: deleting %d documents from %s", len(docIDs), collectionName)

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
	for i := 0; i < len(clientDocIDs); i += purgeBatchSize {
		if err := p.stopping(ctx); err != nil {
			return submitted, err
		}

		end := min(i+purgeBatchSize, len(clientDocIDs))
		if err := purge(ctx, clientDocIDs[i:end]); err != nil {
			return submitted, err
		}
		submitted += int64(end - i)
	}

	// The count is what was handed to PurgeByDocIDs. It reports only an error, and a document
	// that was already gone purges silently, so the log cannot separate the two.
	logger.Sugar.Debugf("Prune: asked DefraDB to delete %d of %d documents from %s in %v",
		submitted, len(docIDs), collectionName, time.Since(startTime))
	return submitted, nil
}

// ─── Result parsing ──────────────────────────────────────────────────────────

// documentRows returns a collection's rows from a GraphQL result. DefraDB returns
// []map[string]any or []any depending on context, and in Go those are distinct types.
func documentRows(gqlData any, collectionName string) []map[string]any {
	data, ok := gqlData.(map[string]any)
	if !ok {
		return nil
	}

	switch docs := data[collectionName].(type) {
	case []map[string]any:
		return docs
	case []any:
		rows := make([]map[string]any, 0, len(docs))
		for _, doc := range docs {
			if row, ok := doc.(map[string]any); ok {
				rows = append(rows, row)
			}
		}
		return rows
	}
	return nil
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
