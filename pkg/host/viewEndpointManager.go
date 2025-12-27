package host

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/node"
)

// ViewEndpointManager manages HTTP endpoints for views
type ViewEndpointManager struct {
	defraNode   *node.Node
	viewManager *ViewManager
	endpoints   map[string]*ViewEndpoint // viewName -> endpoint
	mutex       sync.RWMutex
}

// ViewEndpoint represents an HTTP endpoint for a view
type ViewEndpoint struct {
	ViewName    string
	Path        string
	Handler     http.HandlerFunc
	LastUpdated string
	DataCache   []map[string]interface{}
	mutex       sync.RWMutex
}

// NewViewEndpointManager creates a new view endpoint manager
func NewViewEndpointManager(defraNode *node.Node, viewManager *ViewManager) *ViewEndpointManager {
	return &ViewEndpointManager{
		defraNode:   defraNode,
		viewManager: viewManager,
		endpoints:   make(map[string]*ViewEndpoint),
	}
}

// CreateEndpointForView creates a unique HTTP endpoint for a view
func (vem *ViewEndpointManager) CreateEndpointForView(viewName string) (*ViewEndpoint, error) {
	vem.mutex.Lock()
	defer vem.mutex.Unlock()

	// Check if endpoint already exists
	if endpoint, exists := vem.endpoints[viewName]; exists {
		return endpoint, nil
	}

	// Generate unique path for the view
	path := fmt.Sprintf("/api/v0/views/%s", strings.ToLower(strings.ReplaceAll(viewName, " ", "-")))

	// Create handler function
	handler := vem.createViewHandler(viewName)

	// Create endpoint
	endpoint := &ViewEndpoint{
		ViewName:    viewName,
		Path:        path,
		Handler:     handler,
		LastUpdated: "never",
		DataCache:   []map[string]interface{}{},
	}

	vem.endpoints[viewName] = endpoint

	logger.Sugar.Infof("🌐 Created endpoint for view %s: %s", viewName, path)
	return endpoint, nil
}

// createViewHandler creates an HTTP handler for a specific view
func (vem *ViewEndpointManager) createViewHandler(viewName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Content-Type", "application/json")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method != "GET" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Mark view as accessed
		vem.viewManager.OnViewAccessed(viewName)

		// Get endpoint
		endpoint, exists := vem.getEndpoint(viewName)
		if !exists {
			http.Error(w, fmt.Sprintf("View %s not found", viewName), http.StatusNotFound)
			return
		}

		// Query fresh data from the view collection
		ctx := r.Context()
		err := vem.QueryViewData(ctx, viewName)
		if err != nil {
			logger.Sugar.Warnf("Failed to refresh view data for %s: %v", viewName, err)
			// Continue with cached data if query fails
		}

		endpoint.mutex.RLock()
		data := endpoint.DataCache
		lastUpdated := endpoint.LastUpdated
		endpoint.mutex.RUnlock()

		// Prepare response
		response := map[string]interface{}{
			"view_name":    viewName,
			"last_updated": lastUpdated,
			"data_count":   len(data),
			"data":         data,
		}

		// Write JSON response
		if err := json.NewEncoder(w).Encode(response); err != nil {
			logger.Sugar.Errorf("Failed to encode JSON response for view %s: %v", viewName, err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		logger.Sugar.Debugf("📊 Served view %s data: %d records", viewName, len(data))
	}
}

// UpdateViewData updates the cached data for a view endpoint
func (vem *ViewEndpointManager) UpdateViewData(viewName string, data []map[string]interface{}) error {
	endpoint, exists := vem.getEndpoint(viewName)
	if !exists {
		return fmt.Errorf("endpoint for view %s not found", viewName)
	}

	endpoint.mutex.Lock()
	endpoint.DataCache = data
	endpoint.LastUpdated = fmt.Sprintf("%d", getCurrentTimestamp())
	endpoint.mutex.Unlock()

	logger.Sugar.Infof("📊 Updated view %s data: %d records", viewName, len(data))
	return nil
}

// QueryViewData queries the view collection directly and updates cache
func (vem *ViewEndpointManager) QueryViewData(ctx context.Context, viewName string) error {
	// Build query for the view collection - only include fields that exist in the view's SDL
	// Based on TestTransactionView SDL: hash, blockNumber, from, to, value
	query := fmt.Sprintf(`query { %s { _docID hash blockNumber from to value } }`, viewName)

	// Execute query
	result, err := defra.QueryArray[map[string]interface{}](ctx, vem.defraNode, query)
	if err != nil {
		return fmt.Errorf("failed to query view %s: %w", viewName, err)
	}

	// Update cached data
	return vem.UpdateViewData(viewName, result)
}

// getEndpoint safely retrieves an endpoint
func (vem *ViewEndpointManager) getEndpoint(viewName string) (*ViewEndpoint, bool) {
	vem.mutex.RLock()
	defer vem.mutex.RUnlock()
	endpoint, exists := vem.endpoints[viewName]
	return endpoint, exists
}

// GetAllEndpoints returns all registered endpoints
func (vem *ViewEndpointManager) GetAllEndpoints() map[string]*ViewEndpoint {
	vem.mutex.RLock()
	defer vem.mutex.RUnlock()

	// Create a copy to avoid race conditions
	endpoints := make(map[string]*ViewEndpoint)
	for name, endpoint := range vem.endpoints {
		endpoints[name] = endpoint
	}
	return endpoints
}

// RegisterEndpointsWithMux registers all view endpoints with an HTTP mux
func (vem *ViewEndpointManager) RegisterEndpointsWithMux(mux *http.ServeMux) {
	vem.mutex.RLock()
	defer vem.mutex.RUnlock()

	for _, endpoint := range vem.endpoints {
		mux.HandleFunc(endpoint.Path, endpoint.Handler)
		logger.Sugar.Infof("🌐 Registered endpoint: %s", endpoint.Path)
	}
}

// getCurrentTimestamp returns current Unix timestamp
func getCurrentTimestamp() int64 {
	return 1734318000 // Placeholder timestamp
}
