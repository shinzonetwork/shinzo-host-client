package host

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/sec"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
)

// DefaultPeerDiscoveryTimeout is the default timeout for discovering a peer's ID.
const DefaultPeerDiscoveryTimeout = 10 * time.Second

// DefaultP2PPort is the default libp2p port used when only an IP is provided.
const DefaultP2PPort = "9171"

// resolveBootstrapPeers takes a list of peer addresses (which may or may not include
// a /p2p/<peerID> component) and returns fully-qualified multiaddrs with peer IDs.
// Discovery requests are run in parallel to avoid sequential timeout delays.
//
// Supported input formats:
//   - Full multiaddr:    /ip4/35.239.160.177/tcp/9171/p2p/12D3KooW...  (passed through as-is)
//   - Without peer ID:   /ip4/35.239.160.177/tcp/9171                   (peer ID discovered automatically)
//   - IPv6 multiaddr:    /ip6/::1/tcp/9171                              (peer ID discovered automatically)
//   - IP only:           35.239.160.177                                  (default port 9171, peer ID discovered)
//   - IPv6 only:         ::1                                             (default port 9171, peer ID discovered)
//   - IP:port:           35.239.160.177:9171                             (peer ID discovered)
//   - [IPv6]:port:       [::1]:9171                                      (peer ID discovered)
func resolveBootstrapPeers(ctx context.Context, peers []string, timeout time.Duration) []string {
	if timeout <= 0 {
		timeout = DefaultPeerDiscoveryTimeout
	}

	type result struct {
		index int
		addr  string
	}

	results := make([]result, 0, len(peers))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i, peerAddr := range peers {
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

		// If the multiaddr already contains a /p2p/ component, use as-is
		if hasPeerID(maddr) {
			mu.Lock()
			results = append(results, result{index: i, addr: maddr.String()})
			mu.Unlock()
			logger.Sugar.Infof("📡 Bootstrap peer (configured): %s", maddr.String())
			continue
		}

		// No peer ID — discover it in parallel
		wg.Add(1)
		go func(idx int, addr ma.Multiaddr) {
			defer wg.Done()

			logger.Sugar.Infof("🔍 Discovering peer ID for %s ...", addr.String())
			fullAddr, err := discoverPeerID(ctx, addr, timeout)
			if err != nil {
				logger.Sugar.Warnf("⚠️ Failed to discover peer ID for %s: %v", addr.String(), err)
				// Still add without peer ID — AddPeer may handle it or fail gracefully
				mu.Lock()
				results = append(results, result{index: idx, addr: addr.String()})
				mu.Unlock()
				return
			}

			mu.Lock()
			results = append(results, result{index: idx, addr: fullAddr})
			mu.Unlock()
			logger.Sugar.Infof("📡 Bootstrap peer (discovered): %s", fullAddr)
		}(i, maddr)
	}

	wg.Wait()

	// Sort by original index to maintain config ordering
	sorted := make([]string, 0, len(results))
	indexMap := make(map[int]string, len(results))
	for _, r := range results {
		indexMap[r.index] = r.addr
	}
	for i := 0; i < len(peers); i++ {
		if addr, ok := indexMap[i]; ok {
			sorted = append(sorted, addr)
		}
	}

	return sorted
}

// normalizeToMultiaddr converts various address formats to a proper multiaddr.
// Supports IPv4, IPv6, and multiaddr formats.
func normalizeToMultiaddr(addr string) (ma.Multiaddr, error) {
	// Already a multiaddr
	if strings.HasPrefix(addr, "/") {
		return ma.NewMultiaddr(addr)
	}

	// Try parsing as host:port using net.SplitHostPort, which handles
	// both "1.2.3.4:9171" and "[::1]:9171" correctly.
	host, port, err := net.SplitHostPort(addr)
	if err == nil {
		return buildMultiaddr(host, port)
	}

	// No port — could be bare IPv4 ("1.2.3.4"), bare IPv6 ("::1"), or bracketed IPv6 ("[::1]")
	host = strings.TrimPrefix(addr, "[")
	host = strings.TrimSuffix(host, "]")

	return buildMultiaddr(host, DefaultP2PPort)
}

// buildMultiaddr constructs a multiaddr from a host and port,
// auto-detecting whether the host is IPv4 or IPv6.
func buildMultiaddr(host, port string) (ma.Multiaddr, error) {
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, fmt.Errorf("invalid IP address: %s", host)
	}

	proto := "ip4"
	if ip.To4() == nil {
		proto = "ip6"
	}

	return ma.NewMultiaddr(fmt.Sprintf("/%s/%s/tcp/%s", proto, host, port))
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
//
// It works by dialing with a bogus peer ID. The remote peer's actual ID is revealed
// in the resulting ErrPeerIDMismatch from the security handshake.
func discoverPeerID(ctx context.Context, targetAddr ma.Multiaddr, timeout time.Duration) (string, error) {
	// Create a temporary lightweight libp2p host just for discovery
	tmpHost, err := libp2p.New(
		libp2p.NoListenAddrs,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create discovery host: %w", err)
	}
	defer tmpHost.Close()

	// Use a well-known bogus peer ID; the security handshake will reject it
	// and return ErrPeerIDMismatch containing the actual peer ID.
	bogusPeerID, _ := peer.Decode("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN")

	addrInfo := peer.AddrInfo{
		ID:    bogusPeerID,
		Addrs: []ma.Multiaddr{targetAddr},
	}

	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err = tmpHost.Connect(dialCtx, addrInfo)
	if err == nil {
		// Extremely unlikely: the bogus ID matched the remote peer
		return fmt.Sprintf("%s/p2p/%s", targetAddr.String(), bogusPeerID.String()), nil
	}

	// Extract the actual peer ID from the typed error
	actualPeerID, extractErr := extractPeerIDFromError(err)
	if extractErr != nil {
		return "", fmt.Errorf("connection failed and could not extract peer ID: %w (original error: %v)", extractErr, err)
	}

	return fmt.Sprintf("%s/p2p/%s", targetAddr.String(), actualPeerID.String()), nil
}

// extractPeerIDFromError extracts the actual peer ID from a connection error.
// It unwraps the error chain looking for sec.ErrPeerIDMismatch.
func extractPeerIDFromError(err error) (peer.ID, error) {
	// Walk the error chain looking for ErrPeerIDMismatch
	var mismatchErr sec.ErrPeerIDMismatch
	if errors.As(err, &mismatchErr) {
		return mismatchErr.Actual, nil
	}

	// errors.As may not unwrap through all libp2p error wrappers (e.g., swarm dial errors).
	// Fall back to string matching as a last resort.
	return extractPeerIDFromMismatchString(err.Error())
}

// extractPeerIDFromMismatchString is a fallback that parses the peer ID from
// the error string when errors.As cannot unwrap the typed error.
// Expected format: "...but remote key matches 12D3KooW..."
func extractPeerIDFromMismatchString(errMsg string) (peer.ID, error) {
	const marker = "but remote key matches "
	idx := strings.Index(errMsg, marker)
	if idx == -1 {
		return "", fmt.Errorf("error does not contain peer ID mismatch info")
	}

	remainder := errMsg[idx+len(marker):]

	// The peer ID ends at the next delimiter or end of string
	peerIDStr := remainder
	for _, delim := range []string{".", ",", " ", "\n", ")"} {
		if i := strings.Index(peerIDStr, delim); i != -1 {
			peerIDStr = peerIDStr[:i]
		}
	}
	peerIDStr = strings.TrimSpace(peerIDStr)

	pid, err := peer.Decode(peerIDStr)
	if err != nil {
		return "", fmt.Errorf("extracted invalid peer ID '%s': %w", peerIDStr, err)
	}

	return pid, nil
}
