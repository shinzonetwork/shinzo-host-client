package shinzohub

import (
	"fmt"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

// ViewRegisteredEvent represents a view registration event from ShinzoHub.
// Emitted by ViewRegistry precompile (0x210) as Cosmos SDK event "ViewRegistered".
type ViewRegisteredEvent struct {
	ViewAddress string    // attr "view_address": hex EVM contract address of deployed View.sol
	ViewName    string    // attr "view_name": SDL resource name extracted from viewbundle
	Creator     string    // attr "creator": bech32 address of the view creator
	View        view.View // attr "data": decoded from base64 viewbundle wire format
}

// HostRegisteredEvent represents a host registration event from ShinzoHub.
// Emitted by HostRegistry precompile (0x211) as Cosmos SDK event "HostRegistered".
type HostRegisteredEvent struct {
	Owner            string // attr "owner": bech32 address of host operator
	DID              string // attr "did": decentralized identifier derived from registration pubkey
	ConnectionString string // attr "connection_string": network address (multiaddr or ip:port)
}

// IndexerRegisteredEvent represents an indexer registration event from ShinzoHub.
// Emitted by IndexerRegistry precompile (0x212) as Cosmos SDK event "IndexerRegistered".
type IndexerRegisteredEvent struct {
	Owner            string // attr "owner": bech32 address of indexer operator
	DID              string // attr "did": decentralized identifier derived from registration pubkey
	ConnectionString string // attr "connection_string": network address (multiaddr or ip:port)
	SourceChain      string // attr "source_chain": name of indexed chain (e.g., "ethereum")
	SourceChainID    string // attr "source_chain_id": chain ID as string (e.g., "1")
}

func (vre *ViewRegisteredEvent) ToString() string {
	return fmt.Sprintf("ViewRegistered: address=%s, name=%s, creator=%s",
		vre.ViewAddress, vre.ViewName, vre.Creator)
}

func (hre *HostRegisteredEvent) ToString() string {
	return fmt.Sprintf("HostRegistered: owner=%s, did=%s, conn=%s",
		hre.Owner, hre.DID, hre.ConnectionString)
}

func (ire *IndexerRegisteredEvent) ToString() string {
	return fmt.Sprintf("IndexerRegistered: owner=%s, did=%s, conn=%s, chain=%s/%s",
		ire.Owner, ire.DID, ire.ConnectionString, ire.SourceChain, ire.SourceChainID)
}
