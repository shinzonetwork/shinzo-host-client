package defra

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
)

func GetBoostrapPeer(peerInfo client.PeerInfo) (string, error) {
	if len(peerInfo.Addresses) == 0 {
		return "", fmt.Errorf("Node must have at least one address")
	}
	peerAddressString := peerInfo.Addresses[0]
	peerAddressString = peerAddressString[:len(peerAddressString)-1]
	return fmt.Sprintf("%s/p2p/%s", peerAddressString, peerInfo.ID), nil
}

func GetPort(targetNode *node.Node) int {
	url := targetNode.APIURL

	// Check if it's localhost format (http://127.0.0.1:port or http://localhost:port)
	if !strings.Contains(url, "127.0.0.1:") && !strings.Contains(url, "localhost:") {
		return -1
	}

	// Extract port by splitting on colon and taking the last part
	parts := strings.Split(url, ":")
	if len(parts) < 2 {
		return -1
	}

	// Get the last part (port) and remove any trailing path
	portStr := parts[len(parts)-1]
	if strings.Contains(portStr, "/") {
		portStr = strings.Split(portStr, "/")[0]
	}

	// Convert port string to int
	portInt, err := strconv.Atoi(portStr)
	if err != nil {
		return -1
	}

	return portInt
}
