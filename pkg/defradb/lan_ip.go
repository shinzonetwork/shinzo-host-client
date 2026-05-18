package defradb

import (
	"fmt"
	"net"
	"strings"
)

// dialFunc abstracts net.Dial for testability.
var dialFunc = net.Dial // nolint:gochecknoglobals

// getLANIP returns the local LAN IP by opening a UDP "connection" to a public
// address. The dial does not actually transmit; the kernel selects the egress
// interface and we read the local address back. Used by StartDefraInstance to
// rewrite loopback addresses to the routable interface so peers can reach us.
func getLANIP() (string, error) {
	conn, err := dialFunc("udp", "8.8.8.8:80")
	if err != nil {
		return "", fmt.Errorf("error retrieving ip address: %w", err)
	}
	defer func() { _ = conn.Close() }()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String(), nil
}

// ResolveListenAddress rewrites loopback hosts (localhost, 127.0.0.1) in the
// given address to the local LAN IP so external peers and reverse proxies can
// reach the bound port. Non-loopback hosts are returned unchanged. If the LAN
// IP cannot be determined, the original address is returned with the error so
// the caller can fall back to its own behaviour.
func ResolveListenAddress(rawURL string) (string, error) {
	ipAddress, err := getLANIP()
	if err != nil {
		return rawURL, err
	}
	resolved := rawURL
	resolved = strings.Replace(resolved, "http://localhost", ipAddress, 1)
	resolved = strings.Replace(resolved, "http://127.0.0.1", ipAddress, 1)
	resolved = strings.Replace(resolved, "localhost", ipAddress, 1)
	resolved = strings.Replace(resolved, "127.0.0.1", ipAddress, 1)
	return resolved, nil
}
