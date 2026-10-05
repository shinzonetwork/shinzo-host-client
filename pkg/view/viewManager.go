package view

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/lexer"
	"github.com/vektah/gqlparser/v2/parser"
	"go.uber.org/zap"
)

// Registrar defines the contract for view registration operations.
type Registrar interface {
	RegisterView(ctx context.Context, v View) error
}

// ActiveView is the per-registered-view state the Manager retains in memory.
// Lens is the DefraDB view configuration. ContractAddress is the view's
// on-chain identity, used to resolve a collection name to the SourceHub
// relationship-tuple object_id at request time.
type ActiveView struct {
	Lens            *client.LensConfig
	ContractAddress string
}

// Manager orchestrates the complete lifecycle of Shinzo views including:
// - Loading views from local storage and external sources
// - Managing WASM lens files and migrations
// - Registering views with DefraDB (SetMigration + AddView)
// - Setting up P2P subscriptions for real-time updates
// - Tracking active views and metrics.
type Manager struct {
	activeViews     map[string]*ActiveView
	defraNode       *node.Node
	mutex           sync.RWMutex
	schemaService   *SchemaService
	wasmRegistry    *WASMRegistry
	registryPath    string
	metricsCallback func() *server.HostMetrics
	// chainPrefix is the prefix of the chain the host serves. Views reading another chain are
	// refused.
	chainPrefix string
}

// NewManager creates a new Manager for the chain with chainPrefix, with the given DefraDB node and
// registry path. It initializes all required services.
func NewManager(defraNode *node.Node, registryPath, chainPrefix string) *Manager {
	wasmRegistry, _ := NewWASMRegistry(registryPath, zap.L().Sugar())
	return &Manager{
		activeViews:   make(map[string]*ActiveView),
		defraNode:     defraNode,
		schemaService: NewSchemaService(),
		wasmRegistry:  wasmRegistry,
		registryPath:  registryPath,
		chainPrefix:   chainPrefix,
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
	for name, av := range m.activeViews {
		details = append(details, []string{name, av.Lens.DestinationCollectionVersionID})
	}
	return details
}

// GetViewCount returns the number of active views.
func (m *Manager) GetViewCount() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.activeViews)
}

// IsActive reports whether a view with the given name is currently
// registered, regardless of whether it carries an on-chain contract
// address. Distinguished from ContractAddress so callers can tell
// "not a view" (skip) apart from "view without address" (handle
// explicitly).
func (m *Manager) IsActive(viewName string) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	_, ok := m.activeViews[viewName]
	return ok
}

// ContractAddress returns the on-chain contract address for an active view by
// name. Returns ("", false) when no view by that name is registered or when
// the registered view has no address set; the boolean lets callers
// distinguish "no address available" from a legitimate empty string without
// relying on the string itself.
func (m *Manager) ContractAddress(viewName string) (string, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	av, ok := m.activeViews[viewName]
	if !ok || av.ContractAddress == "" {
		return "", false
	}
	return av.ContractAddress, true
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

	table, err := qualifySourceCollection(v, m.chainPrefix)
	if err != nil {
		return err
	}

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

	if err := m.configureLensAndSubscribe(ctx, v, lensCID, table); err != nil {
		return err
	}

	m.updateMetrics()

	m.activeViews[v.Name] = &ActiveView{
		Lens: &client.LensConfig{
			SourceCollectionVersionID:      v.Data.Query,
			DestinationCollectionVersionID: v.Data.Sdl,
		},
		ContractAddress: v.ContractAddress,
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

// qualifySourceCollection writes the chain into the view's source table and returns the table. It
// returns ErrViewQueryInvalid when the query does not read one table, and ErrViewChainNotServed
// when the table belongs to a chain the host does not serve. A table named without a chain, such
// as Log, is read as Ethereum mainnet's, so such views run only on an Ethereum mainnet host.
func qualifySourceCollection(v *View, chainPrefix string) (string, error) {
	source, at, err := sourceTable(v.Data.Query)
	if err != nil {
		return "", fmt.Errorf("view %s: %w", v.Name, err)
	}
	table := source
	if !strings.Contains(source, "__") {
		table = chain.EthereumMainnet + "__" + source
	}
	if !strings.HasPrefix(table, chainPrefix+"__") {
		return "", fmt.Errorf("view %s reads %s: %w", v.Name, table, ErrViewChainNotServed)
	}
	if table != source {
		v.Data.Query = v.Data.Query[:at] + table + v.Data.Query[at+len(source):]
		logger.Sugar.Debugf("Fixed collection name: %s → %s", source, table)
	}
	return table, nil
}

// sourceTable returns the table a view's query reads and the byte offset of the table's name in the
// query. DefraDB parses a view's query as "query { <query> }" and reads only the first root, so the
// query must hold exactly one root, a field naming the table.
func sourceTable(query string) (string, int, error) {
	const prefix = "query { "
	src := &ast.Source{Input: prefix + query + " }"}
	doc, err := parser.ParseQuery(src)
	if err != nil {
		return "", 0, fmt.Errorf("%w: %w", ErrViewQueryInvalid, err)
	}
	if len(doc.Operations) != 1 || len(doc.Operations[0].SelectionSet) != 1 {
		return "", 0, fmt.Errorf("query must read exactly one table: %w", ErrViewQueryInvalid)
	}
	field, ok := doc.Operations[0].SelectionSet[0].(*ast.Field)
	if !ok {
		return "", 0, fmt.Errorf("query must start with a table: %w", ErrViewQueryInvalid)
	}

	// The tokens are "query", "{", then the root field: its alias and a colon if it has an alias,
	// then its name. Comments are tokens too and are skipped.
	lex := lexer.New(src)
	var tokens []lexer.Token
	for len(tokens) < 5 {
		tok, err := lex.ReadToken()
		if err != nil {
			return "", 0, fmt.Errorf("%w: %w", ErrViewQueryInvalid, err)
		}
		if tok.Kind != lexer.Comment {
			tokens = append(tokens, tok)
		}
	}
	name := tokens[2]
	if tokens[3].Kind == lexer.Colon {
		name = tokens[4]
	}
	// Token positions count runes, not bytes.
	at := len(string([]rune(src.Input)[:name.Pos.Start])) - len(prefix)
	return field.Name, at, nil
}

// configureLensAndSubscribe configures the lens and subscribes to the view and to table, its source.
func (m *Manager) configureLensAndSubscribe(ctx context.Context, v *View, lensCID, table string) error {
	if err := v.ConfigureLens(ctx, m.defraNode, lensCID); err != nil {
		return fmt.Errorf("failed to configure lens for view %s: %w", v.Name, err)
	}

	if err := v.SubscribeTo(ctx, m.defraNode); err != nil {
		logger.Sugar.Warnf("Failed to subscribe to view %s: %v", v.Name, err)
	}

	if err := m.subscribeToSourceCollection(ctx, table, v.Name); err != nil {
		logger.Sugar.Warnf("Failed to subscribe to source collection %s: %v", table, err)
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
	// Implementation would use defradb.Subscribe to listen for new documents
	// and trigger view processing when source documents arrive
	logger.Sugar.Infof("View %s subscribed to source collection %s", viewName, collectionName)
	return nil
}
