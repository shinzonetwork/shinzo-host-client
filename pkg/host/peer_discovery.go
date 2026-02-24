package host

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
)

// resolveBootstrapPeers takes a list of peer addresses (which may or may not include
// a /p2p/<peerID> component) and returns fully-qualified multiaddrs with peer IDs.
//
// Supported input formats:
//   - Full multiaddr:    /ip4/35.239.160.177/tcp/9171/p2p/12D3KooW...  (passed through as-is)
//   - Without peer ID:   /ip4/35.239.160.177/tcp/9171                   (peer ID discovered automatically)
//   - IP only:           35.239.160.177                                  (default port 9171, peer ID discovered)
//   - IP:port:           35.239.160.177:9171                             (peer ID discovered)
func resolveBootstrapPeers(ctx context.Context, peers []string) []string {
	var resolved []string

	for _, peerAddr := range peers {
		peerAddr = strings.TrimSpace(peerAddr)
		if peerAddr == "" {
			continue
		}

		// Normalize the address to a multiaddr
		maddr, err := normalizeToMultiaddr(peerAddr)
		if err != nil {
			logger.Sugar.Warnf("⚠️ Invalid peer address '%s': %v", peerAddr, err)
			continue
		}

		// Check if the multiaddr already contains a /p2p/ component
		if hasPeerID(maddr) {
			// Already has peer ID — use as-is
			resolved = append(resolved, maddr.String())
			logger.Sugar.Infof("📡 Bootstrap peer (configured): %s", maddr.String())
			continue
		}

		// No peer ID — discover it by connecting
		logger.Sugar.Infof("🔍 Discovering peer ID for %s ...", maddr.String())
		fullAddr, err := discoverPeerID(ctx, maddr)
		if err != nil {
			logger.Sugar.Warnf("⚠️ Failed to discover peer ID for %s: %v", maddr.String(), err)
			// Still add the address without peer ID — AddPeer may handle it or fail gracefully
			resolved = append(resolved, maddr.String())
			continue
		}

		resolved = append(resolved, fullAddr)
		logger.Sugar.Infof("📡 Bootstrap peer (discovered): %s", fullAddr)
	}

	return resolved
}

// normalizeToMultiaddr converts various address formats to a proper multiaddr.
func normalizeToMultiaddr(addr string) (ma.Multiaddr, error) {
	// Already a multiaddr
	if strings.HasPrefix(addr, "/") {
		return ma.NewMultiaddr(addr)
	}

	// IP:port format (e.g., "35.239.160.177:9171")
	if strings.Contains(addr, ":") {
		parts := strings.SplitN(addr, ":", 2)
		return ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%s", parts[0], parts[1]))
	}

	// IP only — use default port 9171
	return ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/9171", addr))
}

// hasPeerID checks if a multiaddr contains a /p2p/ protocol component.
func hasPeerID(maddr ma.Multiaddr) bool {
	for _, p := range maddr.Protocols() {
		if p.Code == ma.P_P2P {
			return true
		}
	}
	return false
}

// discoverPeerID connects to a peer address to discover its peer ID via the
// libp2p security handshake. Returns the full multiaddr string including /p2p/<peerID>.
func discoverPeerID(ctx context.Context, targetAddr ma.Multiaddr) (string, error) {
	// Create a temporary lightweight libp2p host just for discovery
	tmpHost, err := libp2p.New(
		libp2p.NoListenAddrs,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create discovery host: %w", err)
	}
	defer tmpHost.Close()

	// Use the "wrong peer ID" approach: try to connect with a well-known bogus ID,
	// and the error will reveal the actual peer ID from the security handshake.
	bogusPeerID, _ := peer.Decode("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN")

	addrInfo := peer.AddrInfo{
		ID:    bogusPeerID,
		Addrs: []ma.Multiaddr{targetAddr},
	}

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = tmpHost.Connect(dialCtx, addrInfo)
	if err == nil {
		// Unlikely but possible if the bogus ID happened to match
		return fmt.Sprintf("%s/p2p/%s", targetAddr.String(), bogusPeerID.String()), nil
	}

	// Parse the error to extract the actual peer ID
	// Error format: "...peer id mismatch: expected <X>, but remote key matches <Y>"
	actualPeerID, parseErr := extractPeerIDFromMismatchError(err.Error())
	if parseErr != nil {
		return "", fmt.Errorf("connection failed and could not extract peer ID: %w (original error: %v)", parseErr, err)
	}

	return fmt.Sprintf("%s/p2p/%s", targetAddr.String(), actualPeerID), nil
}

// extractPeerIDFromMismatchError parses the actual peer ID from a libp2p mismatch error.
// Expected error format: "...but remote key matches 12D3KooW..."
func extractPeerIDFromMismatchError(errMsg string) (string, error) {
	marker := "but remote key matches "
	idx := strings.Index(errMsg, marker)
	if idx == -1 {
		return "", fmt.Errorf("error does not contain peer ID mismatch info")
	}

	// Extract the peer ID after the marker
	remainder := errMsg[idx+len(marker):]

	// The peer ID ends at the next delimiter or end of string
	peerID := remainder
	for _, delim := range []string{".", ",", " ", "\n", ")"} {
		if i := strings.Index(peerID, delim); i != -1 {
			peerID = peerID[:i]
		}
	}

	peerID = strings.TrimSpace(peerID)

	// Validate it's a real peer ID
	_, err := peer.Decode(peerID)
	if err != nil {
		return "", fmt.Errorf("extracted invalid peer ID '%s': %w", peerID, err)
	}

	return peerID, nil
}
