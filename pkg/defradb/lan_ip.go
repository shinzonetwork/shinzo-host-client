package defradb

import (
	"fmt"
	"net"
)

// dialFunc abstracts net.Dial for testability.
var dialFunc = net.Dial

// getLANIP returns the local LAN IP by opening a UDP "connection" to a public
// address. The dial does not actually transmit; the kernel selects the egress
// interface and we read the local address back. Used by StartDefraInstance to
// rewrite loopback addresses to the routable interface so peers can reach us.
func getLANIP() (string, error) {
	conn, err := dialFunc("udp", "8.8.8.8:80")
	if err != nil {
		return "", fmt.Errorf("Error retrieving ip address: %v", err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String(), nil
}
