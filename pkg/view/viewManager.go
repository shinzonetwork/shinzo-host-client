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

// Registrar defines the contract for view registration operations.
type Registrar interface {
	RegisterView(ctx context.Context, v View) error
}

// Manager orchestrates the complete lifecycle of Shinzo views including:
// - Loading views from local storage and external sources
// - Managing WASM lens files and migrations
// - Registering views with DefraDB (SetMigration + AddView)
// - Setting up P2P subscriptions for real-time updates
// - Tracking active views and metrics.
type Manager struct {
	activeViews     map[string]*client.LensConfig
	defraNode       *node.Node
	mutex           sync.RWMutex
	schemaService   *SchemaService
	wasmRegistry    *WASMRegistry
	registryPath    string
	metricsCallback func() *server.HostMetrics
}

// NewManager creates a new Manager with the given DefraDB node and registry path
// It initializes all required services and sets up the processing queue for async operations.
func NewManager(defraNode *node.Node, registryPath string) *Manager {
	wasmRegistry, _ := NewWASMRegistry(registryPath, zap.L().Sugar())
	return &Manager{
		activeViews:   make(map[string]*client.LensConfig),
		defraNode:     defraNode,
		schemaService: NewSchemaService(),
		wasmRegistry:  wasmRegistry,
		registryPath:  registryPath,
	}
}

// SetMetricsCallback sets the callback function to get metrics for tracking view operations.
func (m *Manager) SetMetricsCallback(callback func() *server.HostMetrics) {
	m.metricsCallback = callback
}

// LoadAndRegisterViews loads views from local registry and external sources, ensures WASM files exist, and registers them.
func (m *Manager) LoadAndRegisterViews(ctx context.Context, externalViews []View) error {
	var allViews []View

	if len(externalViews) > 0 {
		logger.Sugar.Infof("📋 Received %d external views", len(externalViews))
		allViews = append(allViews, externalViews...)
	}

	localViews, err := AddViewsFromLensRegistry(m.registryPath)
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

	wasmURLs := extractWasmURLsFromViews(allViews)
	if len(wasmURLs) > 0 && m.wasmRegistry != nil {
		logger.Sugar.Infof("📥 Downloading %d WASM files...", len(wasmURLs))
		downloaded, err := m.wasmRegistry.EnsureAllWASM(ctx, wasmURLs)
		if err != nil {
			logger.Sugar.Warnf("⚠️ Some WASM files failed to download: %v", err)
		} else {
			logger.Sugar.Infof("✅ Downloaded %d WASM files", len(downloaded))
		}
	}

	for i := range allViews {
		if err := m.RegisterView(ctx, &allViews[i]); err != nil {
			logger.Sugar.Warnf("⚠️ Failed to register view %s: %v", allViews[i].Name, err)
			continue
		}
		logger.Sugar.Infof("✅ Registered view: %s", allViews[i].Name)

		// Persist view to local registry for next startup (with any auto-corrections applied)
		if err := SaveViewToRegistry(m.registryPath, allViews[i]); err != nil {
			logger.Sugar.Warnf("⚠️ Failed to persist view %s: %v", allViews[i].Name, err)
		}
	}

	return nil
}

// deduplicateViews removes duplicate views by name, keeping the first occurrence.
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

// extractWasmURLsFromViews extracts HTTP/HTTPS WASM URLs from views.
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

// GetActiveViewNames returns names of all active views.
func (m *Manager) GetActiveViewNames() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	names := make([]string, 0, len(m.activeViews))
	for name := range m.activeViews {
		names = append(names, name)
	}
	return names
}

// GetActiveViewDetails returns names + sdl of all active views as [name, sdl] pairs.
func (m *Manager) GetActiveViewDetails() [][]string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	details := make([][]string, 0, len(m.activeViews))
	for name, lensConfig := range m.activeViews {
		details = append(details, []string{name, lensConfig.DestinationCollectionVersionID})
	}
	return details
}

// GetViewCount returns the number of active views.
func (m *Manager) GetViewCount() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.activeViews)
}

// RegisterView implements the full view registration flow with validation.
// RegisterView registers a view with DefraDB and sets up all required infrastructure.
func (m *Manager) RegisterView(ctx context.Context, v *View) error {
	logger.Sugar.Debugf("🔍 Registering view: %s", v.Name)
	logger.Sugar.Debugf("📄 SDL for %s:\n%s", v.Name, v.Data.Sdl)
	logger.Sugar.Debugf("🎯 Query for %s:\n%s", v.Name, v.Data.Query)

	if err := v.Validate(); err != nil {
		return fmt.Errorf("view validation failed for %s: %w", v.Name, err)
	}

	fixQueryInputData(v)

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, exists := m.activeViews[v.Name]; exists {
		return fmt.Errorf("view %s: %w", v.Name, ErrViewAlreadyRegistered)
	}

	if err := m.prepareWasm(v); err != nil {
		return err
	}

	lensCID, err := m.setupLens(ctx, v)
	if err != nil {
		return err
	}

	fixCollectionName(v)

	if err := m.configureLensAndSubscribe(ctx, v, lensCID); err != nil {
		return err
	}

	m.updateMetrics()

	m.activeViews[v.Name] = &client.LensConfig{
		SourceCollectionVersionID:      v.Data.Query,
		DestinationCollectionVersionID: v.Data.Sdl,
	}

	return nil
}

// fixQueryInputData replaces legacy 'inputData' with 'input' in the query.
func fixQueryInputData(v *View) {
	if strings.Contains(v.Data.Query, "inputData") {
		logger.Sugar.Infof("🔧 Pre-correcting query for view %s: replacing 'inputData' with 'input'", v.Name)
		v.Data.Query = strings.ReplaceAll(v.Data.Query, "inputData", "input")
	}
}

// prepareWasm converts base64 WASM to files if needed.
func (m *Manager) prepareWasm(v *View) error {
	if v.HasLenses() && v.needsWasmConversion() {
		if err := v.PostWasmToFile(m.registryPath); err != nil {
			return fmt.Errorf("failed to write WASM files for view %s: %w", v.Name, err)
		}
	}
	return nil
}

// setupLens sets up the lens and migration in DefraDB.
func (m *Manager) setupLens(ctx context.Context, v *View) (string, error) {
	logger.Sugar.Infof("Storing lens for view %s", v.Name)
	lensCID, err := SetupLensInDefraDB(ctx, m.defraNode, v)
	if err != nil {
		return "", fmt.Errorf("failed to setup lens for view %s: %w", v.Name, err)
	}
	if lensCID != "" {
		logger.Sugar.Infof("Lens CID for view %s: %s", v.Name, lensCID)
	}
	return lensCID, nil
}

// fixCollectionName auto-fixes the collection name in the query if needed.
func fixCollectionName(v *View) {
	sourceCollection := extractCollectionFromQuery(v.Data.Query)
	if sourceCollection != "" && !strings.HasPrefix(sourceCollection, constants.CollectionChain+"__") {
		v.Data.Query = strings.Replace(v.Data.Query, sourceCollection, constants.CollectionChain+"__"+sourceCollection, 1)
		logger.Sugar.Debugf("Fixed collection name: %s → %s__%s", sourceCollection, constants.CollectionChain, sourceCollection)
	}
}

// configureLensAndSubscribe configures the lens and subscribes to relevant collections.
func (m *Manager) configureLensAndSubscribe(ctx context.Context, v *View, lensCID string) error {
	if err := v.ConfigureLens(ctx, m.defraNode, lensCID); err != nil {
		return fmt.Errorf("failed to configure lens for view %s: %w", v.Name, err)
	}

	if err := v.SubscribeTo(ctx, m.defraNode); err != nil {
		logger.Sugar.Warnf("Failed to subscribe to view %s: %v", v.Name, err)
	}

	if v.Data.Query != "" {
		sourceCollection := extractCollectionFromQuery(v.Data.Query)
		if sourceCollection != "" {
			if err := m.subscribeToSourceCollection(ctx, sourceCollection, v.Name); err != nil {
				logger.Sugar.Warnf("Failed to subscribe to source collection %s: %v", sourceCollection, err)
			}
		}
	}

	return nil
}

// updateMetrics increments view registration metrics.
func (m *Manager) updateMetrics() {
	if m.metricsCallback != nil {
		if metrics := m.metricsCallback(); metrics != nil {
			metrics.IncrementViewsRegistered()
			metrics.SetViewsActive(int64(len(m.activeViews)))
		}
	}
}

// subscribeToSourceCollection subscribes to a source collection for real-time document updates.
// ctx was replaced with _ because the current implementation is a stub and does not use the context, but it may be needed for future implementations that involve long-running subscriptions or need to handle cancellation.
func (m *Manager) subscribeToSourceCollection(_ context.Context, collectionName, viewName string) error {
	// Implementation would use defra.Subscribe to listen for new documents
	// and trigger view processing when source documents arrive
	logger.Sugar.Infof("View %s subscribed to source collection %s", viewName, collectionName)
	return nil
}

// extractCollectionFromQuery returns the first identifier in a GraphQL
// query, after trimming leading whitespace.
func extractCollectionFromQuery(query string) string {
	query = strings.TrimLeft(query, " \n\t\r")
	for i, r := range query {
		if r == '{' || r == ' ' || r == '\n' || r == '\t' {
			return query[:i]
		}
	}
	return query
}

// suggestCorrectCollection suggests the correct collection name based on available collections.
func (m *Manager) suggestCorrectCollection(invalidCollection string) string {
	// If the collection already starts with a chain network prefix, return as-is
	if strings.HasPrefix(invalidCollection, constants.CollectionChain+"__") ||
		strings.Contains(invalidCollection, "__") {
		return invalidCollection
	}

	// Prepend the default chain network prefix with double underscore separator
	return fmt.Sprintf("%s__%s", constants.CollectionChain, invalidCollection)
}
