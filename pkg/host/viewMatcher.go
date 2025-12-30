package host

import (
	"strings"
	"sync"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
)

// ViewMatcher maps documents to applicable views based on collection and field criteria
type ViewMatcher struct {
	collectionMatchers map[string][]*ManagedView // "Block" -> [view1, view2]
	fieldMatchers      map[string][]*ManagedView // "address" -> [view1, view3]
	allViews           []*ManagedView            // All registered views
	mutex              sync.RWMutex
}

// NewViewMatcher creates a new view matcher
func NewViewMatcher() *ViewMatcher {
	return &ViewMatcher{
		collectionMatchers: make(map[string][]*ManagedView),
		fieldMatchers:      make(map[string][]*ManagedView),
		allViews:           make([]*ManagedView, 0),
	}
}

// RegisterView adds a view to the matcher's indexes
func (vm *ViewMatcher) RegisterView(managedView *ManagedView) {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	// Add to all views
	vm.allViews = append(vm.allViews, managedView)

	// Analyze view query to determine which collections it tracks
	if managedView.View.Query != nil {
		queryStr := *managedView.View.Query
		vm.indexViewByQuery(managedView, queryStr)
	}

	logger.Sugar.Infof("📋 Registered view %s with matcher", managedView.View.Name)
}

// indexViewByQuery analyzes a GraphQL query to determine collection dependencies
func (vm *ViewMatcher) indexViewByQuery(managedView *ManagedView, queryStr string) {
	// Check for collection references in the query
	collections := constants.AllCollections

	for _, collection := range collections {
		if strings.Contains(queryStr, collection) {
			vm.collectionMatchers[collection] = append(vm.collectionMatchers[collection], managedView)
			logger.Sugar.Debugf("View %s tracks collection: %s", managedView.View.Name, collection)
		}
	}

	// Index common field patterns
	fieldPatterns := map[string][]string{
		"address":     {"address", "from", "to", "miner"},
		"hash":        {"hash", "transactionHash", "blockHash", "parentHash"},
		"blockNumber": {"blockNumber", "number", "transaction.blockNumber"},
		"timestamp":   {"timestamp"},
		"value":       {"value", "gasPrice", "gasUsed"},
	}

	for fieldKey, patterns := range fieldPatterns {
		for _, pattern := range patterns {
			if strings.Contains(queryStr, pattern) {
				vm.fieldMatchers[fieldKey] = append(vm.fieldMatchers[fieldKey], managedView)
				break
			}
		}
	}
}

// FindApplicableViews returns views that should process the given document
func (vm *ViewMatcher) FindApplicableViews(doc Document) []*ManagedView {
	vm.mutex.RLock()
	defer vm.mutex.RUnlock()

	applicableViews := make(map[string]*ManagedView) // Use map to avoid duplicates

	// 1. Match by document type/collection
	if views, exists := vm.collectionMatchers[doc.Type]; exists {
		for _, view := range views {
			if view.IsActive {
				applicableViews[view.View.Name] = view
			}
		}
	}

	// 2. Match by document fields (additional matching logic)
	for fieldKey, views := range vm.fieldMatchers {
		if vm.documentHasField(doc, fieldKey) {
			for _, view := range views {
				if view.IsActive {
					applicableViews[view.View.Name] = view
				}
			}
		}
	}

	// Convert map to slice
	result := make([]*ManagedView, 0, len(applicableViews))
	for _, view := range applicableViews {
		result = append(result, view)
	}

	if len(result) > 0 {
		logger.Sugar.Debugf("📄 Document %s (%s) matches %d views", doc.ID, doc.Type, len(result))
	}

	return result
}

// documentHasField checks if a document contains data relevant to a field pattern
func (vm *ViewMatcher) documentHasField(doc Document, fieldKey string) bool {
	// Check document data for relevant fields
	switch fieldKey {
	case "address":
		return vm.hasAnyField(doc.Data, []string{"address", "from", "to", "miner"})
	case "hash":
		return vm.hasAnyField(doc.Data, []string{"hash", "transactionHash", "blockHash", "parentHash"})
	case "blockNumber":
		return vm.hasAnyField(doc.Data, []string{"blockNumber", "number"}) || doc.BlockNumber > 0
	case "timestamp":
		return vm.hasAnyField(doc.Data, []string{"timestamp"})
	case "value":
		return vm.hasAnyField(doc.Data, []string{"value", "gasPrice", "gasUsed"})
	}
	return false
}

// hasAnyField checks if document data contains any of the specified fields
func (vm *ViewMatcher) hasAnyField(data map[string]interface{}, fields []string) bool {
	for _, field := range fields {
		if _, exists := data[field]; exists {
			return true
		}
	}
	return false
}

// GetViewCount returns the number of registered views
func (vm *ViewMatcher) GetViewCount() int {
	vm.mutex.RLock()
	defer vm.mutex.RUnlock()
	return len(vm.allViews)
}

// GetActiveViewCount returns the number of active views
func (vm *ViewMatcher) GetActiveViewCount() int {
	vm.mutex.RLock()
	defer vm.mutex.RUnlock()

	count := 0
	for _, view := range vm.allViews {
		if view.IsActive {
			count++
		}
	}
	return count
}
