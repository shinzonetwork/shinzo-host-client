package defradb

import (
	"fmt"
	"net"
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
