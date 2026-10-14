package chain

import (
	"fmt"
	"net"
	"strings"

	ma "github.com/multiformats/go-multiaddr"
)

// DefaultP2PPort is the libp2p port ParsePeer uses for an IP address given without one.
const DefaultP2PPort = "9171"

// ParsePeer reads a P2P address in any form the host accepts for a bootstrap peer or a
// generator's peer: a multiaddr, with or without /p2p/<peerID>, or an IPv4 or IPv6 address with
// or without a port.
func ParsePeer(addr string) (ma.Multiaddr, error) {
	if strings.HasPrefix(addr, "/") {
		return ma.NewMultiaddr(addr)
	}

	// net.SplitHostPort handles both "1.2.3.4:9171" and "[::1]:9171".
	host, port, err := net.SplitHostPort(addr)
	if err == nil {
		return buildMultiaddr(host, port)
	}

	// No port: a bare IPv4 ("1.2.3.4"), bare IPv6 ("::1") or bracketed IPv6 ("[::1]").
	host = strings.TrimPrefix(addr, "[")
	host = strings.TrimSuffix(host, "]")

	return buildMultiaddr(host, DefaultP2PPort)
}

// buildMultiaddr builds a TCP multiaddr from an IP address and a port, choosing ip4 or ip6 by
// the address.
func buildMultiaddr(host, port string) (ma.Multiaddr, error) {
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, fmt.Errorf("IP address %s: %w", host, ErrInvalidIPAddress)
	}

	proto := "ip4"
	if ip.To4() == nil {
		proto = "ip6"
	}

	return ma.NewMultiaddr(fmt.Sprintf("/%s/%s/tcp/%s", proto, host, port))
}
