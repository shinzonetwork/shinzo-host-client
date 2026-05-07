package defradb

import (
	"context"
	"fmt"
	"strings"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
)

// BootstrapIntoPeers parses a list of bootstrap peer multiaddr strings into PeerInfo structs.
// Each peer string is expected to be in the format "<multiaddr>/p2p/<peerID>".
// Invalid entries are skipped and their errors are returned alongside the valid peers.
func BootstrapIntoPeers(configuredBootstrapPeers []string) ([]client.PeerInfo, []error) {
	peers := []client.PeerInfo{}
	errors := []error{}

	for _, peer := range configuredBootstrapPeers {
		parts := strings.Split(peer, "/p2p/")
		if len(parts) != 2 { //nolint:mnd
			errors = append(errors, ErrInvalidBootstrapPeer)
			continue
		}
		address := parts[0]
		peerID := parts[1]

		peerInfo := client.PeerInfo{
			Addresses: []string{address},
			ID:        peerID,
		}
		peers = append(peers, peerInfo)
	}

	return peers, errors
}

// PeersIntoBootstrap converts a list of PeerInfo structs into bootstrap peer multiaddr strings.
// Each resulting string is in the format "<multiaddr>/p2p/<peerID>".
// Peers missing an ID or addresses are skipped and their errors are returned alongside the valid peers.
func PeersIntoBootstrap(peers []client.PeerInfo) ([]string, []error) {
	bootstrapPeers := []string{}
	errors := []error{}

	for _, peer := range peers {
		if peer.ID == "" {
			errors = append(errors, ErrPeerEmptyID)
			continue
		}

		if len(peer.Addresses) == 0 {
			errors = append(errors, ErrPeerNoAddresses)
			continue
		}

		// Use the first address if multiple addresses are provided
		address := peer.Addresses[0]
		bootstrapPeer := fmt.Sprintf("%s/p2p/%s", address, peer.ID)
		bootstrapPeers = append(bootstrapPeers, bootstrapPeer)
	}

	return bootstrapPeers, errors
}

// connectToPeers connects the given DefraDB node to the provided list of peer multiaddr strings.
// Returns nil if the peers list is empty. Returns an error if the connection attempt fails.
func connectToPeers(ctx context.Context, defraNode *node.Node, peers []string) error {
	if len(peers) == 0 {
		return nil
	}

	err := defraNode.DB.Connect(ctx, peers)
	if err != nil {
		return fmt.Errorf("error connecting to peer: %w", err)
	}

	return nil
}
