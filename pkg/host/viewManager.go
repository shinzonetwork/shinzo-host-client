package host

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/sourcenetwork/defradb/node"
)

// ViewState represents the lifecycle state of a view
type ViewState int

const (
	ViewStateNew ViewState = iota
	ViewStateInitializing
	ViewStateActive
	ViewStateInactive
	ViewStateStopped
)

// ManagedView tracks a view's state and processing history
type ManagedView struct {
	View          view.View
	State         ViewState
	LastAccessed  time.Time
	LastProcessed uint64
	RangeTracker  *ViewRangeFinder
	IsActive      bool
	ProcessedDocs int64
	CreatedAt     time.Time

	TotalDocuments     int64
	ProcessingStage    string // "historical", "realtime", "completed"
	LastBatchProcessed int
	mutex              sync.RWMutex
}

// ViewProcessingJob represents work to be done for a view
type ViewProcessingJob struct {
	View     *ManagedView
	Document Document
	Priority int
}

// ViewManager manages the lifecycle of views and their processing
type ViewManager struct {
	activeViews     map[string]*ManagedView
	defraNode       *node.Node
	processingQueue chan ViewProcessingJob
	matcher         *ViewMatcher
	mutex           sync.RWMutex

	// Configuration
	inactivityTimeout time.Duration
	cleanupInterval   time.Duration
	workerCount       int
	queueSize         int

	// Control
	ctx           context.Context
	cancel        context.CancelFunc
	workers       sync.WaitGroup
	cleanupTicker *time.Ticker

	// Metrics callback
	metricsCallback func() *server.HostMetrics

	// Endpoint management for view HTTP endpoints
	endpointManager *ViewEndpointManager
}

func (vm *ViewManager) GetViewProcessingState(viewName string) (*ViewProcessingState, error) {
	vm.mutex.RLock()
	defer vm.mutex.RUnlock()

	view, exists := vm.activeViews[viewName]
	if !exists {
		return nil, fmt.Errorf("view %s not found", viewName)
	}

	view.mutex.RLock()
	defer view.mutex.RUnlock()

	return &ViewProcessingState{
		ViewName:         viewName,
		TotalDocuments:   view.TotalDocuments,
		ProcessedDocs:    view.ProcessedDocs,
		LastProcessedDoc: fmt.Sprintf("block_%d", view.LastProcessed),
		ProcessingStage:  view.ProcessingStage,
	}, nil
}

type ViewProcessingState struct {
	ViewName         string
	TotalDocuments   int64
	ProcessedDocs    int64
	LastProcessedDoc string
	ProcessingStage  string
}

// NewViewManager creates a new view manager
func NewViewManager(defraNode *node.Node, config ViewManagerConfig) *ViewManager {
	ctx, cancel := context.WithCancel(context.Background())

	vm := &ViewManager{
		activeViews:       make(map[string]*ManagedView),
		defraNode:         defraNode,
		processingQueue:   make(chan ViewProcessingJob, config.QueueSize),
		matcher:           NewViewMatcher(),
		inactivityTimeout: config.InactivityTimeout,
		cleanupInterval:   config.CleanupInterval,
		workerCount:       config.WorkerCount,
		queueSize:         config.QueueSize,
		ctx:               ctx,
		cancel:            cancel,
	}

	// Initialize endpoint manager
	vm.endpointManager = NewViewEndpointManager(defraNode, vm)

	// Start background processes
	vm.startWorkers()
	vm.startCleanupProcess()

	return vm
}

// ViewManagerConfig holds configuration for the view manager
type ViewManagerConfig struct {
	InactivityTimeout time.Duration
	CleanupInterval   time.Duration
	WorkerCount       int
	QueueSize         int
}

// RegisterView adds a new view to be managed
func (vm *ViewManager) RegisterView(ctx context.Context, viewDef view.View) error {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	// Check if view already exists
	if _, exists := vm.activeViews[viewDef.Name]; exists {
		return fmt.Errorf("view %s already registered", viewDef.Name)
	}

	logger.Sugar.Infof("🔄 Registering new view: %s", viewDef.Name)

	// Create managed view
	managedView := &ManagedView{
		View:         viewDef,
		State:        ViewStateNew,
		LastAccessed: time.Now(),
		RangeTracker: NewViewRangeFinder(vm.defraNode, logger.Sugar),
		IsActive:     true,
		CreatedAt:    time.Now(),
	}

	// Initialize view in DefraDB
	err := vm.initializeView(ctx, managedView)
	if err != nil {
		return fmt.Errorf("failed to initialize view %s: %w", viewDef.Name, err)
	}

	// Register with matcher
	vm.matcher.RegisterView(managedView)

	// Store managed view
	vm.activeViews[viewDef.Name] = managedView

	// LIFECYCLE STEP 3: Create HTTP endpoint for the view
	endpoint, err := vm.endpointManager.CreateEndpointForView(viewDef.Name)
	if err != nil {
		return fmt.Errorf("failed to create endpoint for view %s: %w", viewDef.Name, err)
	}

	logger.Sugar.Infof("✅ View %s registered successfully with endpoint %s", viewDef.Name, endpoint.Path)

	go vm.processHistoricalDocumentsForView(ctx, managedView)

	return nil
}

func (vm *ViewManager) processHistoricalDocumentsForView(ctx context.Context, view *ManagedView) error {
	logger.Sugar.Infof("🔄 Processing historical documents for view %s", view.View.Name)

	view.mutex.Lock()
	view.ProcessingStage = "historical"
	view.mutex.Unlock()

	var allDocs []Document

	// Extract collections from the view's query
	collections := vm.extractCollectionsFromQuery(view.View.Query)
	if len(collections) == 0 {
		logger.Sugar.Warnf("No collections found in view %s query", view.View.Name)
		return nil
	}

	// Get all collections this view cares about
	for _, collection := range collections {
		// Query ALL documents in collection
		query := fmt.Sprintf(`query { %s { _docID blockNumber number hash ... } }`, collection)

		docs, err := defra.QueryArray[map[string]interface{}](ctx, vm.defraNode, query)
		if err != nil {
			logger.Sugar.Errorf("Failed to query historical docs for %s: %v", collection, err)
			continue
		}

		// Convert to Document structs
		for _, docData := range docs {
			doc := vm.convertToDocument(docData, collection)
			allDocs = append(allDocs, doc)
		}
	}

	// Update total count
	view.mutex.Lock()
	view.TotalDocuments = int64(len(allDocs))
	view.mutex.Unlock()

	// Process using batch method
	vm.processHistoricalBatch(ctx, view, allDocs)

	view.mutex.Lock()
	view.ProcessingStage = "realtime"
	view.mutex.Unlock()

	logger.Sugar.Infof("✅ Historical processing complete for view %s", view.View.Name)
	return nil
}

func (vm *ViewManager) convertToDocument(docData map[string]interface{}, collection string) Document {
	doc := Document{
		Type: collection,
		Data: docData,
	}

	// Extract document ID
	if id, exists := docData["_docID"]; exists {
		doc.ID = fmt.Sprintf("%v", id)
	}

	// Extract block number based on collection type
	if collection == "Block" {
		if num, exists := docData["number"]; exists {
			if numFloat, ok := num.(float64); ok {
				doc.BlockNumber = uint64(numFloat)
			}
		}
	} else if collection == "Transaction" || collection == "Log" || collection == "AccessListEntry" {
		if num, exists := docData["blockNumber"]; exists {
			if numFloat, ok := num.(float64); ok {
				doc.BlockNumber = uint64(numFloat)
			}
		}
	}

	return doc
}

// Add helper function to extract collections from query
func (vm *ViewManager) extractCollectionsFromQuery(query *string) []string {
	if query == nil || *query == "" {
		return nil
	}

	// Simple regex to find collection names in GraphQL query
	// Pattern: collectionName(filter: ...) or just collectionName
	re := regexp.MustCompile(`^(\w+)\s*\(?\s*`)
	matches := re.FindStringSubmatch(*query)
	if len(matches) > 1 {
		return []string{matches[1]}
	}

	return nil
}

func (vm *ViewManager) processHistoricalBatch(ctx context.Context, view *ManagedView, docs []Document) {
	logger.Sugar.Infof("🔄 Processing batch of %d documents for view %s", len(docs), view.View.Name)

	// Process 1000 documents at a time
	batchSize := 1000
	for i := 0; i < len(docs); i += batchSize {
		end := min(i+batchSize, len(docs))
		batch := docs[i:end]

		logger.Sugar.Debugf("Processing batch %d-%d for view %s", i, end, view.View.Name)

		// Process batch in parallel
		vm.processBatchInParallel(ctx, view, batch)

		// Small delay to prevent overwhelming DefraDB
		time.Sleep(10 * time.Millisecond)
	}

	logger.Sugar.Infof("✅ Batch processing complete for view %s", view.View.Name)
}

func (vm *ViewManager) processBatchInParallel(ctx context.Context, view *ManagedView, batch []Document) {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10) // Max 10 parallel workers

	for _, doc := range batch {
		wg.Add(1)
		go func(d Document) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			vm.queueDocumentForProcessing(view, d)
		}(doc)
	}

	wg.Wait()
}

// initializeView sets up the view in DefraDB
func (vm *ViewManager) initializeView(ctx context.Context, managedView *ManagedView) error {
	managedView.State = ViewStateInitializing

	// 1. Create view collection in DefraDB
	err := managedView.View.SubscribeTo(ctx, vm.defraNode)
	if err != nil {
		return fmt.Errorf("failed to subscribe view to DefraDB: %w", err)
	}

	// 2. Configure lens transformations
	err = managedView.View.ConfigureLens(ctx, vm.defraNode)
	if err != nil {
		// Log warning but don't fail - some views might not have lenses
		logger.Sugar.Warnf("Failed to configure lens for view %s: %v", managedView.View.Name, err)
	}

	managedView.State = ViewStateActive
	return nil
}

// ProcessDocument queues a document for processing by applicable views
func (vm *ViewManager) ProcessDocument(ctx context.Context, doc Document) {
	// Find views that should process this document
	applicableViews := vm.matcher.FindApplicableViews(doc)

	for _, managedView := range applicableViews {
		// Skip inactive views
		if !managedView.IsActive {
			continue
		}

		// Check if already processed using range tracker
		if managedView.RangeTracker != nil {
			// For now, always process - range tracking will be added later
			// TODO: Implement range checking once ViewRangeFinder is integrated
		}

		// NEW: Intelligent document filtering
		if vm.shouldProcessDocumentForView(managedView, doc) {
			vm.queueDocumentForProcessing(managedView, doc)
		}

		//track metrics if callback available
		if vm.metricsCallback != nil {
			if metrics := vm.metricsCallback(); metrics != nil {
				metrics.IncrementViewProcessingJobs()
			}
		}

	}
}

func (vm *ViewManager) queueDocumentForProcessing(view *ManagedView, doc Document) {
	// Queue for processing
	job := ViewProcessingJob{
		View:     view,
		Document: doc,
		Priority: 1,
	}

	select {
	case vm.processingQueue <- job:
		// Queued successfully - track metrics if callback available
		if vm.metricsCallback != nil {
			if metrics := vm.metricsCallback(); metrics != nil {
				metrics.IncrementViewProcessingJobs()
			}
		}
	default:
		logger.Sugar.Warnf("View processing queue full, dropping document for view %s", view.View.Name)
	}
}

func (vm *ViewManager) shouldProcessDocumentForView(view *ManagedView, doc Document) bool {
	// 1. Check if view handles this document type (based on query)
	collections := vm.extractCollectionsFromQuery(view.View.Query)
	handlesType := false
	for _, collection := range collections {
		if collection == doc.Type {
			handlesType = true
			break
		}
	}
	if !handlesType {
		return false
	}

	// 2. Check if view has lenses configured (only process views with transformations)
	if !view.View.HasLenses() {
		return false
	}

	// 3. For now, always process documents that pass the above checks
	// TODO: Add deduplication later if needed
	return true
}

// OnViewAccessed updates the last accessed time for a view
func (vm *ViewManager) OnViewAccessed(viewName string) {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	if managedView, exists := vm.activeViews[viewName]; exists {
		managedView.mutex.Lock()
		managedView.LastAccessed = time.Now()

		// Reactivate if inactive
		if !managedView.IsActive {
			logger.Sugar.Infof("🔄 Reactivating view %s due to access", viewName)
			managedView.IsActive = true
			managedView.State = ViewStateActive
		}
		managedView.mutex.Unlock()
	}
}

// GetViewStats returns statistics for a view
func (vm *ViewManager) GetViewStats(viewName string) (*ViewStats, error) {
	vm.mutex.RLock()
	defer vm.mutex.RUnlock()

	managedView, exists := vm.activeViews[viewName]
	if !exists {
		return nil, fmt.Errorf("view %s not found", viewName)
	}

	managedView.mutex.RLock()
	defer managedView.mutex.RUnlock()

	return &ViewStats{
		Name:          managedView.View.Name,
		State:         managedView.State,
		LastAccessed:  managedView.LastAccessed,
		LastProcessed: managedView.LastProcessed,
		ProcessedDocs: managedView.ProcessedDocs,
		CreatedAt:     managedView.CreatedAt,
		IsActive:      managedView.IsActive,
	}, nil
}

// ViewStats contains statistics about a view
type ViewStats struct {
	Name          string
	State         ViewState
	LastAccessed  time.Time
	LastProcessed uint64
	ProcessedDocs int64
	CreatedAt     time.Time
	IsActive      bool
}

// startWorkers launches the worker goroutines for processing view jobs
func (vm *ViewManager) startWorkers() {
	for i := 0; i < vm.workerCount; i++ {
		vm.workers.Add(1)
		go vm.worker(i)
	}
	logger.Sugar.Infof("🚀 Started %d view processing workers", vm.workerCount)
}

// worker processes view jobs from the queue
func (vm *ViewManager) worker(id int) {
	defer vm.workers.Done()

	logger.Sugar.Debugf("👷 View worker %d started", id)

	for {
		select {
		case job, ok := <-vm.processingQueue:
			if !ok {
				logger.Sugar.Debugf("👷 View worker %d shutting down", id)
				return
			}

			err := vm.processViewJob(vm.ctx, job)
			if err != nil {
				logger.Sugar.Errorf("❌ Worker %d failed to process job for view %s: %v", id, job.View.View.Name, err)
			}

		case <-vm.ctx.Done():
			logger.Sugar.Debugf("👷 View worker %d cancelled", id)
			return
		}
	}
}

// processViewJob handles lens transformation for a single document
func (vm *ViewManager) processViewJob(ctx context.Context, job ViewProcessingJob) error {
	view := job.View
	doc := job.Document

	logger.Sugar.Debugf("🔄 Processing document %s for view %s", doc.ID, view.View.Name)

	// Update view access time
	view.mutex.Lock()
	view.LastAccessed = time.Now()
	view.mutex.Unlock()

	// Build query for this specific document
	query := vm.buildDocumentQuery(doc)

	// Check if view has lenses configured
	if len(view.View.Transform.Lenses) > 0 {
		// Apply lens transformation
		logger.Sugar.Debugf("🔍 Applying lens transformation with query: %s", query)
		transformedData, err := view.View.ApplyLensTransform(ctx, vm.defraNode, query)
		if err != nil {
			return fmt.Errorf("lens transformation failed: %w", err)
		}

		logger.Sugar.Debugf("🔄 Lens transformation returned %d documents", len(transformedData))
		if len(transformedData) > 0 {
			logger.Sugar.Debugf("📋 First transformed document: %+v", transformedData[0])
		}

		// Write transformed data to view collection
		if len(transformedData) > 0 {
			docIds, err := view.View.WriteTransformedToCollection(ctx, vm.defraNode, transformedData)
			if err != nil {
				return fmt.Errorf("failed to write to view collection: %w", err)
			}

			logger.Sugar.Debugf("✅ Wrote %d documents to view %s (IDs: %v)", len(transformedData), view.View.Name, docIds)

			// LIFECYCLE STEP 4: Update HTTP endpoint with new data
			err = vm.endpointManager.UpdateViewData(view.View.Name, transformedData)
			if err != nil {
				logger.Sugar.Warnf("Failed to update endpoint data for view %s: %v", view.View.Name, err)
			} else {
				logger.Sugar.Debugf("🌐 Updated endpoint data for view %s", view.View.Name)
			}
		}
	} else {
		// Views without lenses are not processed - they should define proper lens transformations
		logger.Sugar.Warnf("⚠️ View %s has no lens transformations configured - skipping processing", view.View.Name)
		return fmt.Errorf("view %s has no lens transformations configured", view.View.Name)
	}

	// Update processing statistics
	view.mutex.Lock()
	view.LastProcessed = doc.BlockNumber
	view.ProcessedDocs++
	view.mutex.Unlock()

	return nil
}

// buildDocumentQuery creates a query to fetch the specific document for lens processing
func (vm *ViewManager) buildDocumentQuery(doc Document) string {
	// Build a query to fetch this specific document by ID with all relevant fields
	// The ApplyLensTransform method expects just the collection part, not a full GraphQL query
	// Include all fields that the filter_transaction lens might need
	fields := "_docID _version hash blockNumber from to value gasPrice gasUsed transactionHash address topics data"

	// Return just the collection query part that ApplyLensTransform expects
	// Format: CollectionName(filter: {...}) { fields }
	query := fmt.Sprintf(`%s(filter: {_docID: {_eq: "%s"}}) { %s }`, doc.Type, doc.ID, fields)

	return query
}

// startCleanupProcess begins the background cleanup routine
func (vm *ViewManager) startCleanupProcess() {
	vm.cleanupTicker = time.NewTicker(vm.cleanupInterval)

	go func() {
		for {
			select {
			case <-vm.cleanupTicker.C:
				vm.cleanupInactiveViews()
			case <-vm.ctx.Done():
				return
			}
		}
	}()

	logger.Sugar.Infof("🧹 Started view cleanup process (interval: %v)", vm.cleanupInterval)
}

// cleanupInactiveViews deactivates views that haven't been accessed recently
func (vm *ViewManager) cleanupInactiveViews() {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	cutoff := time.Now().Add(-vm.inactivityTimeout)
	deactivatedCount := 0

	for viewName, managedView := range vm.activeViews {
		managedView.mutex.Lock()

		if managedView.LastAccessed.Before(cutoff) && managedView.IsActive {
			logger.Sugar.Infof("😴 Deactivating view %s due to inactivity (last accessed: %v)", viewName, managedView.LastAccessed)
			managedView.IsActive = false
			managedView.State = ViewStateInactive
			deactivatedCount++
		}

		managedView.mutex.Unlock()
	}

	if deactivatedCount > 0 {
		logger.Sugar.Infof("🧹 Deactivated %d inactive views", deactivatedCount)
	}
}

// GetEndpointManager returns the endpoint manager for HTTP route registration
func (vm *ViewManager) GetEndpointManager() *ViewEndpointManager {
	return vm.endpointManager
}

// Close shuts down the view manager
func (vm *ViewManager) Close() error {
	logger.Sugar.Info("🛑 Shutting down ViewManager")

	// Cancel context to stop workers
	vm.cancel()

	// Stop cleanup ticker
	if vm.cleanupTicker != nil {
		vm.cleanupTicker.Stop()
	}

	// Wait for workers to finish
	vm.workers.Wait()

	// Close processing queue
	close(vm.processingQueue)

	logger.Sugar.Info("✅ ViewManager shutdown complete")
	return nil
}

func (vm *ViewManager) UpdateView(ctx context.Context, viewName string, newDefinition view.View) error {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	managedView, exists := vm.activeViews[viewName]
	if !exists {
		return fmt.Errorf("view %s not found", viewName)
	}

	logger.Sugar.Infof("🔄 Updating view %s with new definition", viewName)

	// Pause processing
	vm.pauseViewProcessing(managedView)

	// Update view definition
	managedView.View = newDefinition

	// Reset processing state
	managedView.mutex.Lock()
	managedView.ProcessedDocs = 0
	managedView.LastProcessed = 0
	managedView.ProcessingStage = "updating"
	managedView.mutex.Unlock()

	// Reprocess all documents with new view definition
	go func() {
		defer vm.resumeViewProcessing(managedView)
		// Collect documents for batch processing
		var allDocs []Document
		collections := vm.extractCollectionsFromQuery(managedView.View.Query)

		for _, collection := range collections {
			query := fmt.Sprintf(`query { %s { _docID blockNumber number hash ... } }`, collection)
			docs, err := defra.QueryArray[map[string]interface{}](ctx, vm.defraNode, query)
			if err != nil {
				logger.Sugar.Errorf("Failed to query docs for %s: %v", collection, err)
				continue
			}

			for _, docData := range docs {
				doc := vm.convertToDocument(docData, collection)
				allDocs = append(allDocs, doc)
			}
		}

		vm.processHistoricalBatch(ctx, managedView, allDocs)
	}()

	return nil
}

func (vm *ViewManager) pauseViewProcessing(view *ManagedView) {
	view.mutex.Lock()
	view.IsActive = false
	view.ProcessingStage = "paused"
	view.mutex.Unlock()

	logger.Sugar.Infof("⏸️ Paused processing for view %s", view.View.Name)
}

func (vm *ViewManager) resumeViewProcessing(view *ManagedView) {
	view.mutex.Lock()
	view.IsActive = true
	view.ProcessingStage = "realtime"
	view.mutex.Unlock()

	logger.Sugar.Infof("▶️ Resumed processing for view %s", view.View.Name)
}
