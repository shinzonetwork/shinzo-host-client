package shinzohub

import (
	"fmt"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

// ViewRegisteredEvent represents a view registration event from ShinzoHub.
// Emitted by ViewRegistry precompile (0x210) as Cosmos SDK event "ViewRegistered".
type ViewRegisteredEvent struct {
	ViewAddress string
	ViewName    string
	Creator     string
	View        view.View
}

// HostRegisteredEvent represents a host registration event from ShinzoHub.
// Emitted by HostRegistry precompile (0x211) as Cosmos SDK event "HostRegistered".
type HostRegisteredEvent struct {
	Owner            string
	DID              string
	ConnectionString string
}

// IndexerRegisteredEvent represents an indexer registration event from ShinzoHub.
// Emitted by IndexerRegistry precompile (0x212) as Cosmos SDK event "IndexerRegistered".
type IndexerRegisteredEvent struct {
	Owner            string
	DID              string
	ConnectionString string
	SourceChain      string
	SourceChainID    string
}

// ToString returns a human-readable string representation of the ViewRegisteredEvent.
func (vre *ViewRegisteredEvent) ToString() string {
	return fmt.Sprintf("ViewRegistered: address=%s, name=%s, creator=%s",
		vre.ViewAddress, vre.ViewName, vre.Creator)
}

// ToString returns a human-readable string representation of the HostRegisteredEvent.
func (hre *HostRegisteredEvent) ToString() string {
	return fmt.Sprintf("HostRegistered: owner=%s, did=%s, conn=%s",
		hre.Owner, hre.DID, hre.ConnectionString)
}

// ToString returns a human-readable string representation of the IndexerRegisteredEvent.
func (ire *IndexerRegisteredEvent) ToString() string {
	return fmt.Sprintf("IndexerRegistered: owner=%s, did=%s, conn=%s, chain=%s/%s",
		ire.Owner, ire.DID, ire.ConnectionString, ire.SourceChain, ire.SourceChainID)
}
