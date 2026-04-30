package shinzohub

import (
	"context"
	"fmt"
)

// IndexerManager defines the interface for managing indexers.
// NOTE: This is not currently wired into the event handler in host.go.
// The event handler calls AddPeer() directly. This interface exists for
// future use when proper indexer tracking is needed.
type IndexerManager interface {
	AddIndexer(ctx context.Context, owner string, did string, connectionString string, sourceChain string, sourceChainID string) error
	GetIndexer(owner string) (Indexer, bool)
	RemoveIndexer(owner string) error
}

// Indexer represents a blockchain indexer.
type Indexer struct {
	Owner            string
	DID              string
	ConnectionString string
	SourceChain      string
	SourceChainID    string
	Active           bool
}

// RegistrationHandler manages indexer registration from IndexerRegistered events.
type RegistrationHandler struct {
	indexerManager IndexerManager
}

// NewRegistrationHandler creates a new registration handler.
func NewRegistrationHandler(indexerManager IndexerManager) *RegistrationHandler {
	return &RegistrationHandler{
		indexerManager: indexerManager,
	}
}

// ProcessIndexerRegisteredEvent handles an IndexerRegistered event from ShinzoHub.
func (rh *RegistrationHandler) ProcessIndexerRegisteredEvent(ctx context.Context, event IndexerRegisteredEvent) error {
	fmt.Printf("Processing IndexerRegistered event for indexer: %s\n", event.Owner)

	// Check if indexer already exists
	if _, exists := rh.indexerManager.GetIndexer(event.Owner); exists {
		fmt.Printf("Indexer %s already exists, updating...\n", event.Owner)
	}

	// Add or update the indexer
	err := rh.indexerManager.AddIndexer(ctx, event.Owner, event.DID, event.ConnectionString, event.SourceChain, event.SourceChainID)
	if err != nil {
		return fmt.Errorf("failed to add indexer %s: %w", event.Owner, err)
	}

	fmt.Printf("Successfully added indexer: %s (did: %s, chain: %s/%s)\n",
		event.Owner, event.DID, event.SourceChain, event.SourceChainID)

	return nil
}

// MockIndexerManager is a mock implementation of IndexerManager for testing.
type MockIndexerManager struct {
	indexers map[string]Indexer
}

// NewMockIndexerManager creates a new mock indexer manager.
func NewMockIndexerManager() *MockIndexerManager {
	return &MockIndexerManager{
		indexers: make(map[string]Indexer),
	}
}

// AddIndexer adds a new indexer.
// replaced ctx with _ since it's not used in this mock implementation.
func (mim *MockIndexerManager) AddIndexer(_ context.Context, owner string, did string, connectionString string, sourceChain string, sourceChainID string) error {
	mim.indexers[owner] = Indexer{
		Owner:            owner,
		DID:              did,
		ConnectionString: connectionString,
		SourceChain:      sourceChain,
		SourceChainID:    sourceChainID,
		Active:           true,
	}
	return nil
}

// GetIndexer retrieves an indexer by owner address.
func (mim *MockIndexerManager) GetIndexer(owner string) (Indexer, bool) {
	indexer, exists := mim.indexers[owner]
	return indexer, exists
}

// RemoveIndexer removes an indexer by owner address.
func (mim *MockIndexerManager) RemoveIndexer(owner string) error {
	delete(mim.indexers, owner)
	return nil
}

// GetIndexerCount returns the number of indexers.
func (mim *MockIndexerManager) GetIndexerCount() int {
	return len(mim.indexers)
}

// ListIndexers returns all indexers.
func (mim *MockIndexerManager) ListIndexers() []Indexer {
	indexers := make([]Indexer, 0, len(mim.indexers))
	for _, indexer := range mim.indexers {
		indexers = append(indexers, indexer)
	}
	return indexers
}
