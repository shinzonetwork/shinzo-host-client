package host

import (
	"context"
	"fmt"
	"net/http"

	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

// RegisterView registers a new view with the host for processing
func (h *Host) RegisterView(ctx context.Context, viewDef view.View) error {
	if h.viewManager == nil {
		return fmt.Errorf("ViewManager not initialized")
	}

	return h.viewManager.RegisterView(ctx, viewDef)
}

// OnViewAccessed should be called when a view is accessed/queried
func (h *Host) OnViewAccessed(viewName string) {
	if h.viewManager != nil {
		h.viewManager.OnViewAccessed(viewName)
	}
}

// GetViewStats returns statistics for a specific view
func (h *Host) GetViewStats(viewName string) (*ViewStats, error) {
	if h.viewManager == nil {
		return nil, fmt.Errorf("ViewManager not initialized")
	}

	return h.viewManager.GetViewStats(viewName)
}

// GetAllViewStats returns statistics for all registered views
func (h *Host) GetAllViewStats() (map[string]*ViewStats, error) {
	if h.viewManager == nil {
		return nil, fmt.Errorf("ViewManager not initialized")
	}

	stats := make(map[string]*ViewStats)

	// Get stats for each view (this would need to be implemented in ViewManager)
	// For now, return empty map as placeholder
	return stats, nil
}

// GetViewManagerStatus returns overall status of the view management system
func (h *Host) GetViewManagerStatus() *ViewManagerStatus {
	if h.viewManager == nil {
		return &ViewManagerStatus{
			IsActive:    false,
			ViewCount:   0,
			ActiveViews: 0,
		}
	}

	return &ViewManagerStatus{
		IsActive:    true,
		ViewCount:   h.viewManager.matcher.GetViewCount(),
		ActiveViews: h.viewManager.matcher.GetActiveViewCount(),
	}
}

// ViewManagerStatus contains overall status information
type ViewManagerStatus struct {
	IsActive    bool
	ViewCount   int
	ActiveViews int
}

// RegisterViewEndpoints registers all view HTTP endpoints with the provided mux
func (h *Host) RegisterViewEndpoints(mux *http.ServeMux) {
	if h.viewManager != nil && h.viewManager.GetEndpointManager() != nil {
		h.viewManager.GetEndpointManager().RegisterEndpointsWithMux(mux)

		// Register the views listing endpoint
		mux.HandleFunc("/api/v0/views", h.handleViewList)

		logger.Sugar.Info("🌐 Registered all view endpoints with HTTP server")
	}
}

// GetViewEndpoints returns information about all registered view endpoints
func (h *Host) GetViewEndpoints() map[string]*ViewEndpoint {
	if h.viewManager != nil && h.viewManager.GetEndpointManager() != nil {
		return h.viewManager.GetEndpointManager().GetAllEndpoints()
	}
	return make(map[string]*ViewEndpoint)
}

// RefreshViewData manually refreshes the data for a specific view endpoint
func (h *Host) RefreshViewData(ctx context.Context, viewName string) error {
	if h.viewManager != nil && h.viewManager.GetEndpointManager() != nil {
		return h.viewManager.GetEndpointManager().QueryViewData(ctx, viewName)
	}
	return fmt.Errorf("ViewManager or EndpointManager not initialized")
}
