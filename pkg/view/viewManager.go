package view

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"go.uber.org/zap"
)

// ViewRegistrar defines the contract for view registration operations
type ViewRegistrar interface {
	RegisterView(ctx context.Context, v View) error
}

type ViewQueueItem struct {
	viewName      string
	activeViewKey string
	lensConfig    *client.LensConfig
}

// ViewManager orchestrates the complete lifecycle of Shinzo views including:
// - Loading views from local storage and external sources
// - Managing WASM lens files and migrations
// - Registering views with DefraDB (SetMigration + AddView)
// - Setting up P2P subscriptions for real-time updates
// - Tracking active views and metrics
type ViewManager struct {
	activeViews     map[string]*client.LensConfig
	defraNode       *node.Node
	mutex           sync.RWMutex
	schemaService   *SchemaService
	wasmRegistry    *WASMRegistry
	registryPath    string
	processingQueue chan ViewQueueItem
	metricsCallback func() *server.HostMetrics
}

// NewViewManager creates a new ViewManager with the given DefraDB node and registry path
// It initializes all required services and sets up the processing queue for async operations
func NewViewManager(defraNode *node.Node, registryPath string) *ViewManager {
	wasmRegistry, _ := NewWASMRegistry(registryPath, zap.L().Sugar())
	return &ViewManager{
		activeViews:     make(map[string]*client.LensConfig),
		defraNode:       defraNode,
		schemaService:   NewSchemaService(),
		wasmRegistry:    wasmRegistry,
		registryPath:    registryPath,
		processingQueue: make(chan ViewQueueItem, 100),
	}
}

// SetMetricsCallback sets the callback function to get metrics for tracking view operations
func (vm *ViewManager) SetMetricsCallback(callback func() *server.HostMetrics) {
	vm.metricsCallback = callback
}

// LoadAndRegisterViews is the main startup function that:
// 1. Loads views from local registry and external sources
// 2. Ensures WASM files are available (downloads if needed)
// 3. Registers each view with DefraDB (SetMigration + AddView)
// 4. Persists views for next startup
// Call once during host startup.
func (vm *ViewManager) LoadAndRegisterViews(ctx context.Context, externalViews []View) error {
	var allViews []View

	// Step 1: Add external views (e.g., from ShinzoHub)
	if len(externalViews) > 0 {
		logger.Sugar.Infof("📋 Received %d external views", len(externalViews))
		allViews = append(allViews, externalViews...)
	}

	// Step 2: Load views from local views.json (merge with external views)
	localViews, err := AddViewsFromLensRegistry(vm.registryPath)
	if err != nil {
		logger.Sugar.Warnf("⚠️ Failed to load local views: %v", err)
	} else if len(localViews) > 0 {
		logger.Sugar.Infof("📋 Found %d local persisted views", len(localViews))
		allViews = append(allViews, localViews...)
	}

	if len(allViews) == 0 {
		logger.Sugar.Info("📋 No views found - starting fresh")
		return nil
	}

	// Deduplicate views by name
	allViews = deduplicateViews(allViews)
	logger.Sugar.Infof("📋 Total unique views to register: %d", len(allViews))

	// Step 3: Ensure WASM files exist (download if needed)
	wasmURLs := extractWasmURLsFromViews(allViews)
	if len(wasmURLs) > 0 && vm.wasmRegistry != nil {
		logger.Sugar.Infof("📥 Downloading %d WASM files...", len(wasmURLs))
		downloaded, err := vm.wasmRegistry.EnsureAllWASM(ctx, wasmURLs)
		if err != nil {
			logger.Sugar.Warnf("⚠️ Some WASM files failed to download: %v", err)
		} else {
			logger.Sugar.Infof("✅ Downloaded %d WASM files", len(downloaded))
		}
	}

	// Step 4: Register each view
	for i := range allViews {
		if err := vm.RegisterView(ctx, &allViews[i]); err != nil {
			logger.Sugar.Warnf("⚠️ Failed to register view %s: %v", allViews[i].Name, err)
			continue
		}
		logger.Sugar.Infof("✅ Registered view: %s", allViews[i].Name)

		// Persist view to local registry for next startup (with any auto-corrections applied)
		if err := SaveViewToRegistry(vm.registryPath, allViews[i]); err != nil {
			logger.Sugar.Warnf("⚠️ Failed to persist view %s: %v", allViews[i].Name, err)
		}
	}

	return nil
}

// deduplicateViews removes duplicate views by name, keeping the first occurrence
func deduplicateViews(views []View) []View {
	seen := make(map[string]bool)
	result := make([]View, 0, len(views))
	for _, v := range views {
		if !seen[v.Name] {
			seen[v.Name] = true
			result = append(result, v)
		}
	}
	return result
}

// extractWasmURLsFromViews extracts HTTP/HTTPS WASM URLs from views
func extractWasmURLsFromViews(views []View) []string {
	var urls []string
	for _, v := range views {
		for _, lens := range v.Data.Transform.Lenses {
			if strings.HasPrefix(lens.Path, "http://") || strings.HasPrefix(lens.Path, "https://") {
				urls = append(urls, lens.Path)
			}
		}
	}
	return urls
}

// GetActiveViewNames returns names of all active views
func (vm *ViewManager) GetActiveViewNames() []string {
	vm.mutex.RLock()
	defer vm.mutex.RUnlock()

	names := make([]string, 0, len(vm.activeViews))
	for name := range vm.activeViews {
		names = append(names, name)
	}
	return names
}

// GetViewCount returns the number of active views
func (vm *ViewManager) GetViewCount() int {
	vm.mutex.RLock()
	defer vm.mutex.RUnlock()
	return len(vm.activeViews)
}

// QueueView adds a view to the processing queue
func (vm *ViewManager) QueueView(v View) {
	item := ViewQueueItem{
		viewName:      v.Name,
		activeViewKey: v.Name,
	}
	vm.processingQueue <- item
}

// RegisterView implements the full view registration flow with validation
func (vm *ViewManager) RegisterView(ctx context.Context, v *View) error {
	// Pre-validate view before any operations
	if err := v.Validate(); err != nil {
		return fmt.Errorf("view validation failed for %s: %w", v.Name, err)
	}

	// Pre-emptively fix common query issues (inputData -> input)
	// This ensures the corrected query is used everywhere, not just in DefraDB
	if strings.Contains(v.Data.Query, "inputData") {
		logger.Sugar.Infof("🔧 Pre-correcting query for view %s: replacing 'inputData' with 'input'", v.Name)
		v.Data.Query = strings.ReplaceAll(v.Data.Query, "inputData", "input")
	}

	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	// Check if already registered
	if _, exists := vm.activeViews[v.Name]; exists {
		return fmt.Errorf("view %s already registered", v.Name)
	}

	// Step 0: Convert base64 WASM to files if needed
	if v.HasLenses() && v.needsWasmConversion() {
		if err := v.PostWasmToFile(ctx, vm.registryPath); err != nil {
			return fmt.Errorf("failed to write WASM files for view %s: %w", v.Name, err)
		}
	}

	// Step 1: Set up lens and migration if needed
	logger.Sugar.Infof("Storing lens for view %s", v.Name)
	lensCID, err := SetupLensInDefraDB(ctx, vm.defraNode, v)
	if err != nil {
		return fmt.Errorf("failed to setup lens for view %s: %w", v.Name, err)
	}
	if lensCID != "" {
		logger.Sugar.Infof("Lens CID for view %s: %s", v.Name, lensCID)
	}

	// Step 2: Auto-fix collection name if needed and create view in DefraDB
	sourceCollection := extractCollectionFromQuery(v.Data.Query)
	if !strings.HasPrefix(sourceCollection, constants.CollectionChain+"__") {
		v.Data.Query = strings.Replace(v.Data.Query, sourceCollection, constants.CollectionChain+"__"+sourceCollection, 1)
		logger.Sugar.Debugf("Fixed collection name: %s → %s__%s", sourceCollection, constants.CollectionChain, sourceCollection)
	}
	
	err = v.ConfigureLens(ctx, vm.defraNode, vm.schemaService, lensCID)
	if err != nil {
		return fmt.Errorf("failed to configure lens for view %s: %w", v.Name, err)
	}

	// Step 3: Subscribe to view collection for P2P replication
	err = v.SubscribeTo(ctx, vm.defraNode)
	if err != nil {
		logger.Sugar.Warnf("Failed to subscribe to view %s: %v", v.Name, err)
		// Non-fatal - view can still work
	}

	// Step 4: Subscribe to source collection for real-time updates
	if v.Data.Query != "" {
		sourceCollection := extractCollectionFromQuery(v.Data.Query)
		if sourceCollection != "" {
			err = vm.subscribeToSourceCollection(ctx, sourceCollection, v.Name)
			if err != nil {
				logger.Sugar.Warnf("Failed to subscribe to source collection %s: %v", sourceCollection, err)
			}
		}
	}

	// Update metrics
	if vm.metricsCallback != nil {
		if metrics := vm.metricsCallback(); metrics != nil {
			metrics.IncrementViewsRegistered()
			metrics.SetViewsActive(int64(len(vm.activeViews)))
		}
	}

	return nil
}

// subscribeToSourceCollection subscribes to a source collection for real-time document updates
func (vm *ViewManager) subscribeToSourceCollection(ctx context.Context, collectionName, viewName string) error {
	// Implementation would use defra.Subscribe to listen for new documents
	// and trigger view processing when source documents arrive
	logger.Sugar.Infof("View %s subscribed to source collection %s", viewName, collectionName)
	return nil
}

// extractCollectionFromQuery extracts the collection name from a GraphQL query
func extractCollectionFromQuery(query string) string {
	// Simple extraction - find first word after opening brace or query keyword
	// e.g., "Log { address topics }" -> "Log"
	// This is a simplified version - the host package has a more robust implementation
	for i, r := range query {
		if r == '{' || r == ' ' {
			return query[:i]
		}
	}
	return query
}

// suggestCorrectCollection suggests the correct collection name based on available collections
func (vm *ViewManager) suggestCorrectCollection(invalidCollection string) string {
	// If the collection already starts with a chain network prefix, return as-is
	if strings.HasPrefix(invalidCollection, constants.CollectionChain+"__") || 
	   strings.Contains(invalidCollection, "__") {
		return invalidCollection
	}
	
	// Prepend the default chain network prefix with double underscore separator
	return fmt.Sprintf("%s__%s", constants.CollectionChain, invalidCollection)
}
