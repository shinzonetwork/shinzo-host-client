package host

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/file"
	"github.com/shinzonetwork/host/config"
	"github.com/shinzonetwork/host/pkg/networking"
	"github.com/shinzonetwork/host/pkg/shinzohub"
	"github.com/stretchr/testify/require"
)

var getTenBlocksQuery string = `query GetAll{
  Block(limit:10){
    number
	hash
    _version{
      cid
      signature{
        type
        identity
        value
        __typename
      }
    }
  }
}
`

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

	fmt.Println("Sending mock event")
	sendMockNewViewEvent(t, "viewWithLensEvent", mockEventSub)
	fmt.Println("Event sent")

	// Verify the host processed the event
	require.Len(t, testHost.HostedViews, 1, "Host should have one hosted view after receiving event")
	fmt.Println("Host is now hosting one view")
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
	time.Sleep(200 * time.Millisecond)
}

func CreateHostWithTwoViews(t *testing.T, boostrapPeers ...string) *Host {
	testHost, mockEventSub := createHostWithMockViewEventReceiver(t, boostrapPeers...)

	sendMockNewViewEvent(t, "viewWithNoLensEvent", mockEventSub)
	sendMockNewViewEvent(t, "viewWithLensEvent", mockEventSub)

	// Verify the host processed the event
	require.Len(t, testHost.HostedViews, 2, "Host should have two hosted view after receiving events")
	return testHost
}
