package host

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/sec"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestHasPeerID_WithPeerID(t *testing.T) {
	maddr, err := ma.NewMultiaddr("/ip4/35.239.160.177/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r")
	require.NoError(t, err)
	require.True(t, hasPeerID(maddr))
}

func TestHasPeerID_WithoutPeerID(t *testing.T) {
	maddr, err := ma.NewMultiaddr("/ip4/35.239.160.177/tcp/9171")
	require.NoError(t, err)
	require.False(t, hasPeerID(maddr))
}

func TestExtractPeerIDFromError_TypedError(t *testing.T) {
	expectedID, err := peer.Decode("12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF")
	require.NoError(t, err)

	bogusID, err := peer.Decode("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN")
	require.NoError(t, err)

	mismatchErr := sec.ErrPeerIDMismatch{
		Expected: bogusID,
		Actual:   expectedID,
	}

	actualID, err := extractPeerIDFromError(mismatchErr)
	require.NoError(t, err)
	require.Equal(t, expectedID, actualID)
}

func TestExtractPeerIDFromError_WrappedTypedError(t *testing.T) {
	expectedID, err := peer.Decode("12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF")
	require.NoError(t, err)

	bogusID, err := peer.Decode("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN")
	require.NoError(t, err)

	mismatchErr := sec.ErrPeerIDMismatch{
		Expected: bogusID,
		Actual:   expectedID,
	}

	// Wrap in another error, as libp2p often does
	wrappedErr := fmt.Errorf("failed to negotiate security protocol: %w", mismatchErr)

	actualID, err := extractPeerIDFromError(wrappedErr)
	require.NoError(t, err)
	require.Equal(t, expectedID, actualID)
}

func TestExtractPeerIDFromError_StringFallback(t *testing.T) {
	// Simulate an error that doesn't unwrap to ErrPeerIDMismatch
	// but contains the expected string format
	errMsg := errPeerIDMismatchDial

	actualID, err := extractPeerIDFromError(errMsg)
	require.NoError(t, err)

	expectedID, _ := peer.Decode("12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF")
	require.Equal(t, expectedID, actualID)
}

func TestExtractPeerIDFromError_NoMismatchInfo(t *testing.T) {
	_, err := extractPeerIDFromError(errConnectionRefused)
	require.Error(t, err)
}

func TestExtractPeerIDFromMismatchString_ValidError(t *testing.T) {
	errMsg := "peer id mismatch: expected 12D3KooWCS9HHoiqfu1YRBiLM5spAbDGiHb2NtXEGUoHYTcCtEfy, " +
		"but remote key matches 12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF."

	pid, err := extractPeerIDFromMismatchString(errMsg)
	require.NoError(t, err)

	expectedID, _ := peer.Decode("12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF")
	require.Equal(t, expectedID, pid)
}

func TestExtractPeerIDFromMismatchString_NoMarker(t *testing.T) {
	_, err := extractPeerIDFromMismatchString("some unrelated error")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not contain peer ID mismatch info")
}

func TestExtractPeerIDFromMismatchString_InvalidPeerID(t *testing.T) {
	errMsg := "but remote key matches INVALID_PEER_ID."
	_, err := extractPeerIDFromMismatchString(errMsg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "extracted invalid peer ID")
}

func TestResolveBootstrapPeers_FullMultiaddrsPassThrough(t *testing.T) {
	peers := []string{
		"/ip4/35.239.160.177/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r",
	}

	resolved := resolveBootstrapPeers(context.Background(), peers, DefaultPeerDiscoveryTimeout)
	require.Len(t, resolved, 1)
	require.Equal(t, peers[0], resolved[0])
}

func TestResolveBootstrapPeers_EmptyAndWhitespace(t *testing.T) {
	peers := []string{"", "  ", "   "}

	resolved := resolveBootstrapPeers(context.Background(), peers, DefaultPeerDiscoveryTimeout)
	require.Empty(t, resolved)
}

func TestResolveBootstrapPeers_InvalidAddresses(t *testing.T) {
	peers := []string{"not-a-valid-address"}

	resolved := resolveBootstrapPeers(context.Background(), peers, DefaultPeerDiscoveryTimeout)
	require.Empty(t, resolved)
}

func TestResolveBootstrapPeers_PreservesOrder(t *testing.T) {
	peers := []string{
		testPeerMultiaddr,
		"/ip4/10.0.0.2/tcp/9171/p2p/12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF",
	}

	resolved := resolveBootstrapPeers(context.Background(), peers, DefaultPeerDiscoveryTimeout)
	require.Len(t, resolved, 2)
	require.Contains(t, resolved[0], "10.0.0.1")
	require.Contains(t, resolved[1], "10.0.0.2")
}

func TestResolveBootstrapPeers_DefaultTimeout(t *testing.T) {
	// Passing 0 should use DefaultPeerDiscoveryTimeout (not panic)
	peers := []string{
		testPeerMultiaddr,
	}

	resolved := resolveBootstrapPeers(context.Background(), peers, 0)
	require.Len(t, resolved, 1)
}

func TestResolveBootstrapPeers_UnreachablePeerFallsBack(t *testing.T) {
	// Use an unreachable address with a very short timeout
	// The address should be returned as-is (without peer ID) after failing discovery
	peers := []string{"192.0.2.1:9171"} // RFC 5737 TEST-NET, guaranteed unreachable

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resolved := resolveBootstrapPeers(ctx, peers, 1*time.Second)
	require.Len(t, resolved, 1)
	require.Equal(t, "/ip4/192.0.2.1/tcp/9171", resolved[0]) // Falls back to address without peer ID
}

// ---------------------------------------------------------------------------
// resolveBootstrapPeers - mixed peers (some with IDs, some without)
// ---------------------------------------------------------------------------

func TestResolveBootstrapPeers_MixedPeers(t *testing.T) {
	peers := []string{
		testPeerMultiaddr, // Full multiaddr
		"",                // Empty (skip)
		"  ",              // Whitespace (skip)
		"not-valid!",      // Invalid (skip)
	}

	resolved := resolveBootstrapPeers(context.Background(), peers, DefaultPeerDiscoveryTimeout)
	require.Len(t, resolved, 1)
	require.Contains(t, resolved[0], "10.0.0.1")
}

func TestResolveBootstrapPeers_NilPeers(t *testing.T) {
	resolved := resolveBootstrapPeers(context.Background(), nil, DefaultPeerDiscoveryTimeout)
	require.Empty(t, resolved)
}

func TestResolveBootstrapPeers_NegativeTimeout(t *testing.T) {
	// Negative timeout should use default
	peers := []string{
		testPeerMultiaddr,
	}
	resolved := resolveBootstrapPeers(context.Background(), peers, -1*time.Second)
	require.Len(t, resolved, 1)
}

// ---------------------------------------------------------------------------
// discoverPeerID - various error conditions
// ---------------------------------------------------------------------------

func TestDiscoverPeerID_UnreachableAddress(t *testing.T) {
	maddr, err := ma.NewMultiaddr("/ip4/192.0.2.1/tcp/9171")
	require.NoError(t, err)

	// Very short timeout to fail quickly
	_, err = discoverPeerID(context.Background(), maddr, 500*time.Millisecond)
	require.Error(t, err)
}

func TestDiscoverPeerID_CancelledContext(t *testing.T) {
	maddr, err := ma.NewMultiaddr("/ip4/192.0.2.1/tcp/9171")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = discoverPeerID(ctx, maddr, 1*time.Second)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// extractPeerIDFromMismatchString - various delimiters
// ---------------------------------------------------------------------------

func TestExtractPeerIDFromMismatchString_CommaDelimiter(t *testing.T) {
	errMsg := "but remote key matches 12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF, something else"
	pid, err := extractPeerIDFromMismatchString(errMsg)
	require.NoError(t, err)
	expectedID, _ := peer.Decode("12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF")
	require.Equal(t, expectedID, pid)
}

func TestExtractPeerIDFromMismatchString_SpaceDelimiter(t *testing.T) {
	errMsg := "but remote key matches 12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF more text"
	pid, err := extractPeerIDFromMismatchString(errMsg)
	require.NoError(t, err)
	expectedID, _ := peer.Decode("12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF")
	require.Equal(t, expectedID, pid)
}

func TestExtractPeerIDFromMismatchString_ParenDelimiter(t *testing.T) {
	errMsg := "but remote key matches 12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF)"
	pid, err := extractPeerIDFromMismatchString(errMsg)
	require.NoError(t, err)
	expectedID, _ := peer.Decode("12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF")
	require.Equal(t, expectedID, pid)
}

func TestExtractPeerIDFromMismatchString_EndOfString(t *testing.T) {
	errMsg := "but remote key matches 12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF"
	pid, err := extractPeerIDFromMismatchString(errMsg)
	require.NoError(t, err)
	expectedID, _ := peer.Decode("12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF")
	require.Equal(t, expectedID, pid)
}
