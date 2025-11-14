package host

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/file"
	indexerConfig "github.com/shinzonetwork/indexer/config"
	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/networking"
	"github.com/shinzonetwork/shinzo-host-client/pkg/shinzohub"
	"github.com/stretchr/testify/require"
)

func createHostWithMockViewEventReceiver(t *testing.T, boostrapPeers ...string) (*Host, *shinzohub.MockEventSubscription) {
	mockEventSub := shinzohub.NewMockEventSubscription()

	testHostConfig := &config.Config{
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
			WebSocketUrl:        "ws://dummy-url",
		},
		ShinzoAppConfig: DefaultConfig.ShinzoAppConfig,
		HostConfig: config.HostConfig{
			LensRegistryPath: t.TempDir(),
		},
	}
	testHostConfig.ShinzoAppConfig.DefraDB.Store.Path = t.TempDir()
	ipAddress, err := networking.GetLANIP()
	require.NoError(t, err)
	listenAddress := fmt.Sprintf("/ip4/%s/tcp/0", ipAddress)
	defraUrl := fmt.Sprintf("%s:0", ipAddress)
	testHostConfig.ShinzoAppConfig.DefraDB.Url = defraUrl
	testHostConfig.ShinzoAppConfig.DefraDB.P2P.BootstrapPeers = append(DefaultConfig.ShinzoAppConfig.DefraDB.P2P.BootstrapPeers, boostrapPeers...)
	testHostConfig.ShinzoAppConfig.DefraDB.P2P.ListenAddr = listenAddress

	testHost, err := StartHostingWithEventSubscription(testHostConfig, mockEventSub)
	require.NoError(t, err)

	return testHost, mockEventSub
}

func CreateHostWithLensEventReceived(t *testing.T, boostrapPeers ...string) *Host {
	testHost, mockEventSub := createHostWithMockViewEventReceiver(t, boostrapPeers...)

	sendMockNewViewEvent(t, "viewWithLensEvent", mockEventSub)

	// Verify the host processed the event
	require.Len(t, testHost.HostedViews, 1, "Host should have one hosted view after receiving event")
	return testHost
}

func sendMockNewViewEvent(t *testing.T, eventSourceTextFileName string, mockEventSub *shinzohub.MockEventSubscription) {
	var rawJSON []byte
	path, err := file.FindFile(fmt.Sprintf("../tests/%s.txt", eventSourceTextFileName))
	require.NoError(t, err)
	rawJSON, err = os.ReadFile(path)
	require.NoError(t, err, "Failed to read %s.txt from any of the attempted paths", eventSourceTextFileName)

	// Send the raw JSON message as if it came from the WebSocket
	err = mockEventSub.SendRawJSONMessage(string(rawJSON))
	require.NoError(t, err, "Failed to process raw JSON message")

	// Wait a bit for the event to be processed
	time.Sleep(1 * time.Second)
}

func CreateHostWithTwoViews(t *testing.T, boostrapPeers ...string) *Host {
	testHost, mockEventSub := createHostWithMockViewEventReceiver(t, boostrapPeers...)

	sendMockNewViewEvent(t, "viewWithNoLensEvent", mockEventSub)
	sendMockNewViewEvent(t, "viewWithLensEvent", mockEventSub)
	time.Sleep(1 * time.Second) // Allow a moment for events to process

	// Verify the host processed the event
	require.Len(t, testHost.HostedViews, 2, "Host should have two hosted view after receiving events")
	return testHost
}

func GetGethConfig() indexerConfig.GethConfig {
	return indexerConfig.GethConfig{
		NodeURL: getGethNodeURL(),
		WsURL:   os.Getenv("GCP_GETH_WS_URL"),
		APIKey:  os.Getenv("GCP_GETH_API_KEY"),
	}
}

func getGethNodeURL() string {
	if gcpURL := os.Getenv("GCP_GETH_RPC_URL"); gcpURL != "" {
		return gcpURL
	}
	// Fallback to public node for tests without GCP setup
	return "https://ethereum-rpc.publicnode.com"
}
