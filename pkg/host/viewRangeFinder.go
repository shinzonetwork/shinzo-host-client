package host

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
	"go.uber.org/zap"
)

// FieldConfig defines how a view tracks a specific collection field
type FieldConfig struct {
	Collection     string                 // "Log", "Transaction", "Block"
	BlockField     string                 // "blockNumber", "number"
	IndexFields    []string               // Fields that trigger reprocessing
	FilterCriteria map[string]interface{} // Additional filters for this collection
}

// ViewConfig defines what data a view tracks and how it processes it
type ViewConfig struct {
	Name           string                 // View name
	TrackedFields  map[string]FieldConfig // Collection -> FieldConfig mapping
	ProcessingMode string                 // "incremental", "full_scan", "event_driven"
}

// ViewTracker maintains processing state for a single view
type ViewTracker struct {
	ViewName        string
	Config          ViewConfig
	LastProcessed   uint64
	ProcessingRange struct {
		Start uint64
		End   uint64 // Current highest block to process
	}
	LastUpdated time.Time
}

// ViewRangeFinder manages processing ranges for multiple views
type ViewRangeFinder struct {
	views  map[string]*ViewTracker
	db     *node.Node
	logger *zap.SugaredLogger
}

// NewViewRangeFinder creates a new view range finder
func NewViewRangeFinder(db *node.Node, logger *zap.SugaredLogger) *ViewRangeFinder {
	return &ViewRangeFinder{
		views:  make(map[string]*ViewTracker),
		db:     db,
		logger: logger,
	}
}

// RegisterView adds a view to be tracked by the range finder
func (rf *ViewRangeFinder) RegisterView(config ViewConfig, startHeight uint64) {
	tracker := &ViewTracker{
		ViewName:      config.Name,
		Config:        config,
		LastProcessed: startHeight - 1, // Start from one block before
		LastUpdated:   time.Now(),
	}

	// Initialize processing range
	tracker.ProcessingRange.Start = startHeight
	tracker.ProcessingRange.End = startHeight

	rf.views[config.Name] = tracker
	rf.logger.Infof("Registered view %s with start height %d", config.Name, startHeight)
}

// UpdateProcessingRange detects new data and updates the processing range for a view
func (vt *ViewTracker) UpdateProcessingRange(ctx context.Context, db *node.Node, logger *zap.SugaredLogger) error {
	maxBlock := uint64(0)

	// Find the highest block number across all tracked collections
	for collection, fieldConfig := range vt.Config.TrackedFields {
		query := fmt.Sprintf(`query { %s(order: {%s: DESC}, limit: 1) { %s } }`,
			collection, fieldConfig.BlockField, fieldConfig.BlockField)

		result, err := defra.QueryArray[map[string]interface{}](ctx, db, query)
		if err != nil {
			logger.Warnf("Failed to query max block for collection %s: %v", collection, err)
			continue
		}

		if len(result) > 0 {
			if blockNum, ok := result[0][fieldConfig.BlockField]; ok {
				var blockNumber uint64
				switch v := blockNum.(type) {
				case float64:
					blockNumber = uint64(v)
				case int64:
					blockNumber = uint64(v)
				case int:
					blockNumber = uint64(v)
				}

				if blockNumber > maxBlock {
					maxBlock = blockNumber
				}
			}
		}
	}

	// Update range if new data is available
	if maxBlock > vt.ProcessingRange.End {
		oldEnd := vt.ProcessingRange.End
		vt.ProcessingRange.End = maxBlock
		vt.LastUpdated = time.Now()

		logger.Infof("View %s: Updated range from %d to %d (found new data up to block %d)",
			vt.ViewName, oldEnd, maxBlock, maxBlock)
		return nil
	}

	return nil
}

// GetUnprocessedBlocks returns the range of blocks that need processing for a view
func (vt *ViewTracker) GetUnprocessedBlocks() (start, end uint64) {
	start = vt.LastProcessed + 1
	end = vt.ProcessingRange.End
	return start, end
}

// MarkBlockProcessed updates the last processed block for a view
func (vt *ViewTracker) MarkBlockProcessed(blockNumber uint64) {
	if blockNumber > vt.LastProcessed {
		vt.LastProcessed = blockNumber
		vt.LastUpdated = time.Now()
	}
}

// BuildRangeQuery creates a query to fetch data in the processing range for a view
func (vt *ViewTracker) BuildRangeQuery(startBlock, endBlock uint64) map[string]string {
	queries := make(map[string]string)

	for collection, fieldConfig := range vt.Config.TrackedFields {
		// Build filter criteria
		filterParts := []string{
			fmt.Sprintf("%s: {_gte: %d, _lte: %d}", fieldConfig.BlockField, startBlock, endBlock),
		}

		// Add additional filter criteria
		for field, criteria := range fieldConfig.FilterCriteria {
			if criteriaMap, ok := criteria.(map[string]interface{}); ok {
				for op, value := range criteriaMap {
					filterParts = append(filterParts, fmt.Sprintf("%s: {%s: \"%v\"}", field, op, value))
				}
			}
		}

		// Build field selection
		fields := append([]string{"_docID", fieldConfig.BlockField}, fieldConfig.IndexFields...)

		query := fmt.Sprintf(`query { 
			%s(
				filter: {%s}
				order: {%s: ASC}
			) { 
				%s 
			} 
		}`, collection, strings.Join(filterParts, ", "), fieldConfig.BlockField, strings.Join(fields, " "))

		queries[collection] = query
	}

	return queries
}

// ProcessViews updates all view ranges and returns views that need processing
func (rf *ViewRangeFinder) ProcessViews(ctx context.Context) (map[string]*ViewTracker, error) {
	viewsNeedingUpdate := make(map[string]*ViewTracker)

	for viewName, tracker := range rf.views {
		// Update the processing range for this view
		err := tracker.UpdateProcessingRange(ctx, rf.db, rf.logger)
		if err != nil {
			rf.logger.Errorf("Failed to update range for view %s: %v", viewName, err)
			continue
		}

		// Check if there are unprocessed blocks
		start, end := tracker.GetUnprocessedBlocks()
		if start <= end {
			rf.logger.Infof("View %s needs processing: blocks %d-%d", viewName, start, end)
			viewsNeedingUpdate[viewName] = tracker
		}
	}

	return viewsNeedingUpdate, nil
}

// GetViewTracker returns the tracker for a specific view
func (rf *ViewRangeFinder) GetViewTracker(viewName string) (*ViewTracker, bool) {
	tracker, exists := rf.views[viewName]
	return tracker, exists
}

// GetAllViews returns all registered view trackers
func (rf *ViewRangeFinder) GetAllViews() map[string]*ViewTracker {
	return rf.views
}
