package shinzohub

import (
	"context"
	"fmt"
)

// IndexerManager defines the interface for managing indexers
type IndexerManager interface {
	AddIndexer(ctx context.Context, key string, owner string, did string, pid string) error
	GetIndexer(key string) (Indexer, bool)
	RemoveIndexer(key string) error
}

// Indexer represents a blockchain indexer
type Indexer struct {
	Key    string
	Owner  string
	DID    string
	Pid    string
	Entity string
	Active bool
}

// EntityRegistrationHandler manages indexer registration from EntityRegistered events
type EntityRegistrationHandler struct {
	indexerManager IndexerManager
}

// NewEntityRegistrationHandler creates a new entity registration handler
func NewEntityRegistrationHandler(indexerManager IndexerManager) *EntityRegistrationHandler {
	return &EntityRegistrationHandler{
		indexerManager: indexerManager,
	}
}

// ProcessEntityRegisteredEvent handles an EntityRegistered event from Shinzo Hub
func (erh *EntityRegistrationHandler) ProcessEntityRegisteredEvent(ctx context.Context, event EntityRegisteredEvent) error {
	fmt.Printf("🚀 Processing EntityRegistered event for indexer: %s\n", event.Key)
	
	// Create a new indexer from the event
	indexer := Indexer{
		Key:    event.Key,
		Owner:  event.Owner,
		DID:    event.DID,
		Pid:    event.Pid,
		Entity: event.Entity,
		Active: true,
	}

	// Check if indexer already exists
	if _, exists := erh.indexerManager.GetIndexer(event.Key); exists {
		fmt.Printf("⚠️  Indexer %s already exists, updating...\n", event.Key)
	}

	// Add or update the indexer
	err := erh.indexerManager.AddIndexer(ctx, indexer.Key, indexer.Owner, indexer.DID, indexer.Pid)
	if err != nil {
		return fmt.Errorf("failed to add indexer %s: %w", event.Key, err)
	}

	fmt.Printf("✅ Successfully added indexer: %s (owner: %s, did: %s, pid: %s)\n", 
		indexer.Key, indexer.Owner, indexer.DID, indexer.Pid)
	
	return nil
}

// MockIndexerManager is a mock implementation of IndexerManager for testing
type MockIndexerManager struct {
	indexers map[string]Indexer
}

// NewMockIndexerManager creates a new mock indexer manager
func NewMockIndexerManager() *MockIndexerManager {
	return &MockIndexerManager{
		indexers: make(map[string]Indexer),
	}
}

// AddIndexer adds a new indexer
func (mim *MockIndexerManager) AddIndexer(ctx context.Context, key string, owner string, did string, pid string) error {
	mim.indexers[key] = Indexer{
		Key:    key,
		Owner:  owner,
		DID:    did,
		Pid:    pid,
		Active: true,
	}
	return nil
}

// GetIndexer retrieves an indexer by key
func (mim *MockIndexerManager) GetIndexer(key string) (Indexer, bool) {
	indexer, exists := mim.indexers[key]
	return indexer, exists
}

// RemoveIndexer removes an indexer by key
func (mim *MockIndexerManager) RemoveIndexer(key string) error {
	delete(mim.indexers, key)
	return nil
}

// GetIndexerCount returns the number of indexers
func (mim *MockIndexerManager) GetIndexerCount() int {
	return len(mim.indexers)
}

// ListIndexers returns all indexers
func (mim *MockIndexerManager) ListIndexers() []Indexer {
	indexers := make([]Indexer, 0, len(mim.indexers))
	for _, indexer := range mim.indexers {
		indexers = append(indexers, indexer)
	}
	return indexers
}
