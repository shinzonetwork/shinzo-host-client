package host

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/sec"
	"github.com/stretchr/testify/require"
)

func TestNormalizeToMultiaddr_FullMultiaddr(t *testing.T) {
	maddr, err := normalizeToMultiaddr("/ip4/35.239.160.177/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r")
	require.NoError(t, err)
	require.Equal(t, "/ip4/35.239.160.177/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r", maddr.String())
}

func TestNormalizeToMultiaddr_MultiaddrWithoutPeerID(t *testing.T) {
	maddr, err := normalizeToMultiaddr("/ip4/35.239.160.177/tcp/9171")
	require.NoError(t, err)
	require.Equal(t, "/ip4/35.239.160.177/tcp/9171", maddr.String())
}

func TestNormalizeToMultiaddr_IPv4WithPort(t *testing.T) {
	maddr, err := normalizeToMultiaddr("35.239.160.177:9171")
	require.NoError(t, err)
	require.Equal(t, "/ip4/35.239.160.177/tcp/9171", maddr.String())
}

func TestNormalizeToMultiaddr_IPv4Only(t *testing.T) {
	maddr, err := normalizeToMultiaddr("35.239.160.177")
	require.NoError(t, err)
	require.Equal(t, "/ip4/35.239.160.177/tcp/9171", maddr.String())
}

func TestNormalizeToMultiaddr_IPv6WithPort(t *testing.T) {
	maddr, err := normalizeToMultiaddr("[::1]:9171")
	require.NoError(t, err)
	require.Equal(t, "/ip6/::1/tcp/9171", maddr.String())
}

func TestNormalizeToMultiaddr_IPv6Only(t *testing.T) {
	maddr, err := normalizeToMultiaddr("::1")
	require.NoError(t, err)
	require.Equal(t, "/ip6/::1/tcp/9171", maddr.String())
}

func TestNormalizeToMultiaddr_BracketedIPv6(t *testing.T) {
	maddr, err := normalizeToMultiaddr("[::1]")
	require.NoError(t, err)
	require.Equal(t, "/ip6/::1/tcp/9171", maddr.String())
}

func TestNormalizeToMultiaddr_IPv6Multiaddr(t *testing.T) {
	maddr, err := normalizeToMultiaddr("/ip6/::1/tcp/9171")
	require.NoError(t, err)
	require.Equal(t, "/ip6/::1/tcp/9171", maddr.String())
}

func TestNormalizeToMultiaddr_IPv6FullAddress(t *testing.T) {
	maddr, err := normalizeToMultiaddr("[2001:db8::1]:4001")
	require.NoError(t, err)
	require.Equal(t, "/ip6/2001:db8::1/tcp/4001", maddr.String())
}

func TestNormalizeToMultiaddr_InvalidAddress(t *testing.T) {
	_, err := normalizeToMultiaddr("not-a-valid-address")
	require.Error(t, err)
}

func TestHasPeerID_WithPeerID(t *testing.T) {
	maddr, err := normalizeToMultiaddr("/ip4/35.239.160.177/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r")
	require.NoError(t, err)
	require.True(t, hasPeerID(maddr))
}

func TestHasPeerID_WithoutPeerID(t *testing.T) {
	maddr, err := normalizeToMultiaddr("/ip4/35.239.160.177/tcp/9171")
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
	errMsg := fmt.Errorf(
		"all dials failed\n  * [/ip4/34.9.109.135/tcp/9171] failed to negotiate security protocol: "+
			"peer id mismatch: expected 12D3KooWCS9HHoiqfu1YRBiLM5spAbDGiHb2NtXEGUoHYTcCtEfy, "+
			"but remote key matches 12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF.",
	)

	actualID, err := extractPeerIDFromError(errMsg)
	require.NoError(t, err)

	expectedID, _ := peer.Decode("12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF")
	require.Equal(t, expectedID, actualID)
}

func TestExtractPeerIDFromError_NoMismatchInfo(t *testing.T) {
	_, err := extractPeerIDFromError(fmt.Errorf("connection refused"))
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

func TestBuildMultiaddr_IPv4(t *testing.T) {
	maddr, err := buildMultiaddr("192.168.1.1", "9171")
	require.NoError(t, err)
	require.Equal(t, "/ip4/192.168.1.1/tcp/9171", maddr.String())
}

func TestBuildMultiaddr_IPv6(t *testing.T) {
	maddr, err := buildMultiaddr("::1", "9171")
	require.NoError(t, err)
	require.Equal(t, "/ip6/::1/tcp/9171", maddr.String())
}

func TestBuildMultiaddr_InvalidIP(t *testing.T) {
	_, err := buildMultiaddr("not-an-ip", "9171")
	require.Error(t, err)
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
		"/ip4/10.0.0.1/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r",
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
		"/ip4/10.0.0.1/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r",
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
