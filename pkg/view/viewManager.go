package view

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"github.com/sourcenetwork/lens/host-go/config/model"
	"go.uber.org/zap"
)

type ViewQueueItem struct {
	viewName      string
	activeViewKey string
	lensConfig    *client.LensConfig
}

type ViewManager struct {
	activeViews     map[string]*client.LensConfig
	defraNode       *node.Node
	mutex           sync.RWMutex
	lensService     LensService
	schemaService   *SchemaService
	wasmRegistry    *WASMRegistry
	registryPath    string
	processingQueue chan ViewQueueItem
	metricsCallback func() *server.HostMetrics
}

func NewViewManager(defraNode *node.Node, registryPath string) *ViewManager {
	wasmRegistry, _ := NewWASMRegistry(registryPath, zap.L().Sugar())
	return &ViewManager{
		activeViews:     make(map[string]*client.LensConfig),
		defraNode:       defraNode,
		lensService:     NewLensService(defraNode),
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

// LoadAndRegisterViews loads views from local registry and optional external views, ensures WASM files exist, and registers them.
// externalViews can be views fetched from ShinzoHub or other sources.
// This is the main startup function - call once on host startup.
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
	for _, v := range allViews {
		if err := vm.RegisterView(ctx, v); err != nil {
			logger.Sugar.Warnf("⚠️ Failed to register view %s: %v", v.Name, err)
			continue
		}
		logger.Sugar.Infof("✅ Registered view: %s", v.Name)

		// Persist view to local registry for next startup
		if err := SaveViewToRegistry(vm.registryPath, v); err != nil {
			logger.Sugar.Warnf("⚠️ Failed to persist view %s: %v", v.Name, err)
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
		for _, lens := range v.Transform.Lenses {
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

// RegisterView implements the full view registration flow:
// 0. Ensure WASM files exist (convert base64 to file paths if needed)
// 1. lensService.SetMigration - Configure lens transformation
// 2. AddView via ConfigureLens - Create the view in DefraDB
// 3. SubscribeTo - Subscribe to the view collection for P2P replication
// 4. SubscribeToSourceCollection - Subscribe to source collection for real-time updates
func (vm *ViewManager) RegisterView(ctx context.Context, v View) error {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	// Check if already registered
	if _, exists := vm.activeViews[v.Name]; exists {
		return fmt.Errorf("view %s already registered", v.Name)
	}

	// Step 0: Ensure WASM files are written to disk (converts base64 to file paths)
	if v.HasLenses() && v.needsWasmConversion() {
		if err := v.PostWasmToFile(ctx, vm.registryPath); err != nil {
			return fmt.Errorf("failed to write WASM files for view %s: %w", v.Name, err)
		}
	}

	// Step 1: Set migration if view has lenses
	// if v.HasLenses() {
	lensConfig := v.BuildLensConfig()
	logger.Sugar.Infof("Lens config: %v", lensConfig)
	_, err := vm.lensService.SetMigration(ctx, vm.defraNode, lensConfig)
	logger.Sugar.Infof("Lens migrated")
	if err != nil {
		return fmt.Errorf("failed to set migration for view %s: %w", v.Name, err)
	}
	vm.activeViews[v.Name] = &lensConfig
	// }

	// // Step 2: Configure lens and create view (AddView is called inside ConfigureLens)
	// if v.HasLenses() {
	err = v.ConfigureLens(ctx, vm.defraNode, vm.schemaService)
	if err != nil {
		return fmt.Errorf("failed to configure lens for view %s: %w", v.Name, err)
	}
	// }

	// Step 3: Subscribe to the view collection (P2P replication)
	err = v.SubscribeTo(ctx, vm.defraNode)
	if err != nil {
		logger.Sugar.Warnf("Failed to subscribe to view %s: %v", v.Name, err)
		// Non-fatal - view can still work
	}

	// Step 4: Subscribe to source collection for real-time updates
	if v.Query != nil {
		sourceCollection := extractCollectionFromQuery(*v.Query)
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

func (v *View) BuildLensConfig() client.LensConfig {

	// Build all lens modules from transform
	lensModules := make([]model.LensModule, 0, len(v.Transform.Lenses))
	if len(v.Transform.Lenses) == 0 {
		return client.LensConfig{
			SourceSchemaVersionID:      *v.Query,
			DestinationSchemaVersionID: *v.Sdl,
			Lens: model.Lens{
				Lenses: nil,
			},
		}
	} else {
		for _, lens := range v.Transform.Lenses {
			lensModules = append(lensModules, model.LensModule{
				Path:      lens.Path,
				Arguments: lens.Arguments,
			})
		}

		return client.LensConfig{
			SourceSchemaVersionID:      *v.Query,
			DestinationSchemaVersionID: *v.Sdl,
			Lens: model.Lens{
				Lenses: lensModules,
			},
		}
	}
}
