package shinzohub

import (
	"context"
	"fmt"
)

// EntityType represents the type of entity
type EntityType string

const (
	EntityTypeIndexer EntityType = "Indexer"
	EntityTypeHost    EntityType = "Host"
	EntityTypeUnknown EntityType = "Unknown"
)

// GetEntityType determines the entity type from the entity field value
func GetEntityType(entityValue string) EntityType {
	switch entityValue {
	case "\u0001":
		return EntityTypeIndexer
	case "\u0002":
		return EntityTypeHost
	default:
		return EntityTypeUnknown
	}
}

// UnifiedEventHandler handles both EntityRegistered and Registered events
type UnifiedEventHandler struct {
	indexerManager    IndexerManager
	viewManager       ViewManagerInterface
	peerManager       PeerManager
	blockProcessor    BlockProcessor
}

// PeerManager defines the interface for managing P2P peers
type PeerManager interface {
	AddPeer(ctx context.Context, pid string, did string, owner string) error
	RemovePeer(pid string) error
	GetPeer(pid string) (Peer, bool)
	ListPeers() []Peer
}

// BlockProcessor defines the interface for processing blocks with views
type BlockProcessor interface {
	ApplyViewToBlocks(ctx context.Context, view ViewRegisteredEvent) error
	RemoveViewFromBlocks(viewKey string) error
	GetActiveViews() []string
}

// Peer represents a P2P peer
type Peer struct {
	PID    string
	DID    string
	Owner  string
	Active bool
}

// NewUnifiedEventHandler creates a new unified event handler
func NewUnifiedEventHandler(
	indexerManager IndexerManager,
	viewManager ViewManagerInterface,
	peerManager PeerManager,
	blockProcessor BlockProcessor,
) *UnifiedEventHandler {
	return &UnifiedEventHandler{
		indexerManager: indexerManager,
		viewManager:    viewManager,
		peerManager:    peerManager,
		blockProcessor: blockProcessor,
	}
}

// ProcessEvent processes any ShinzoEvent and routes it appropriately
func (ueh *UnifiedEventHandler) ProcessEvent(ctx context.Context, event ShinzoEvent) error {
	switch e := event.(type) {
	case *EntityRegisteredEvent:
		return ueh.processEntityRegisteredEvent(ctx, *e)
	case *ViewRegisteredEvent:
		return ueh.processViewRegisteredEvent(ctx, *e)
	default:
		return fmt.Errorf("unknown event type: %T", event)
	}
}

// processEntityRegisteredEvent handles EntityRegistered events
func (ueh *UnifiedEventHandler) processEntityRegisteredEvent(ctx context.Context, event EntityRegisteredEvent) error {
	entityType := GetEntityType(event.Entity)
	
	switch entityType {
	case EntityTypeIndexer:
		return ueh.handleIndexerRegistration(ctx, event)
	case EntityTypeHost:
		return ueh.handleHostRegistration(ctx, event)
	case EntityTypeUnknown:
		return fmt.Errorf("unknown entity type: %s", event.Entity)
	}
	
	return nil
}

// handleIndexerRegistration handles indexer registration events
func (ueh *UnifiedEventHandler) handleIndexerRegistration(ctx context.Context, event EntityRegisteredEvent) error {
	fmt.Printf("🚀 Processing Indexer registration: key=%s, owner=%s, pid=%s\n", 
		event.Key, event.Owner, event.Pid)
	
	// Add indexer to the indexer manager
	err := ueh.indexerManager.AddIndexer(ctx, event.Key, event.Owner, event.DID, event.Pid)
	if err != nil {
		return fmt.Errorf("failed to add indexer %s: %w", event.Key, err)
	}
	
	// Add indexer as a P2P peer and start syncing
	err = ueh.peerManager.AddPeer(ctx, event.Pid, event.DID, event.Owner)
	if err != nil {
		return fmt.Errorf("failed to add indexer peer %s: %w", event.Pid, err)
	}
	
	fmt.Printf("✅ Successfully registered indexer and started syncing: %s (pid: %s)\n", 
		event.Key, event.Pid)
	
	return nil
}

// handleHostRegistration handles host registration events
func (ueh *UnifiedEventHandler) handleHostRegistration(ctx context.Context, event EntityRegisteredEvent) error {
	fmt.Printf("🏠 Processing Host registration: key=%s, owner=%s, pid=%s\n", 
		event.Key, event.Owner, event.Pid)
	
	// Add host as a P2P peer (hosts are also peers)
	err := ueh.peerManager.AddPeer(ctx, event.Pid, event.DID, event.Owner)
	if err != nil {
		return fmt.Errorf("failed to add host peer %s: %w", event.Pid, err)
	}
	
	fmt.Printf("✅ Successfully registered host peer: %s (pid: %s)\n", 
		event.Key, event.Pid)
	
	return nil
}

// processViewRegisteredEvent handles ViewRegistered events
func (ueh *UnifiedEventHandler) processViewRegisteredEvent(ctx context.Context, event ViewRegisteredEvent) error {
	fmt.Printf("👁️  Processing View registration: key=%s, creator=%s, view=%s\n", 
		event.Key, event.Creator, event.View.Name)
	
	// Register view with the view manager
	err := ueh.viewManager.RegisterView(ctx, event.View)
	if err != nil {
		return fmt.Errorf("failed to register view %s: %w", event.View.Name, err)
	}
	
	// Start applying view to blocks
	err = ueh.blockProcessor.ApplyViewToBlocks(ctx, event)
	if err != nil {
		return fmt.Errorf("failed to apply view %s to blocks: %w", event.View.Name, err)
	}
	
	fmt.Printf("✅ Successfully registered and applied view: %s\n", event.View.Name)
	
	return nil
}

// MockPeerManager is a mock implementation of PeerManager for testing
type MockPeerManager struct {
	peers map[string]Peer
}

// NewMockPeerManager creates a new mock peer manager
func NewMockPeerManager() *MockPeerManager {
	return &MockPeerManager{
		peers: make(map[string]Peer),
	}
}

// AddPeer adds a new peer
func (mpm *MockPeerManager) AddPeer(ctx context.Context, pid string, did string, owner string) error {
	mpm.peers[pid] = Peer{
		PID:    pid,
		DID:    did,
		Owner:  owner,
		Active: true,
	}
	return nil
}

// RemovePeer removes a peer
func (mpm *MockPeerManager) RemovePeer(pid string) error {
	delete(mpm.peers, pid)
	return nil
}

// GetPeer retrieves a peer by PID
func (mpm *MockPeerManager) GetPeer(pid string) (Peer, bool) {
	peer, exists := mpm.peers[pid]
	return peer, exists
}

// ListPeers returns all peers
func (mpm *MockPeerManager) ListPeers() []Peer {
	peers := make([]Peer, 0, len(mpm.peers))
	for _, peer := range mpm.peers {
		peers = append(peers, peer)
	}
	return peers
}

// GetPeerCount returns the number of peers
func (mpm *MockPeerManager) GetPeerCount() int {
	return len(mpm.peers)
}

// MockBlockProcessor is a mock implementation of BlockProcessor for testing
type MockBlockProcessor struct {
	activeViews map[string]ViewRegisteredEvent
}

// NewMockBlockProcessor creates a new mock block processor
func NewMockBlockProcessor() *MockBlockProcessor {
	return &MockBlockProcessor{
		activeViews: make(map[string]ViewRegisteredEvent),
	}
}

// ApplyViewToBlocks applies a view to blocks
func (mbp *MockBlockProcessor) ApplyViewToBlocks(ctx context.Context, view ViewRegisteredEvent) error {
	mbp.activeViews[view.Key] = view
	return nil
}

// RemoveViewFromBlocks removes a view from blocks
func (mbp *MockBlockProcessor) RemoveViewFromBlocks(viewKey string) error {
	delete(mbp.activeViews, viewKey)
	return nil
}

// GetActiveViews returns all active views
func (mbp *MockBlockProcessor) GetActiveViews() []string {
	views := make([]string, 0, len(mbp.activeViews))
	for key := range mbp.activeViews {
		views = append(views, key)
	}
	return views
}

// GetActiveViewCount returns the number of active views
func (mbp *MockBlockProcessor) GetActiveViewCount() int {
	return len(mbp.activeViews)
}
