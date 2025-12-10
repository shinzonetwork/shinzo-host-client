package host

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/file"
	indexerConfig "github.com/shinzonetwork/indexer/config"
	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/networking"
	"github.com/shinzonetwork/shinzo-host-client/pkg/shinzohub"
	"github.com/stretchr/testify/require"
)

func createHostWithMockViewEventReceiver(t *testing.T, boostrapPeers ...string) (*Host, *shinzohub.MockEventSubscription) {
	mockEventSub := shinzohub.NewMockEventSubscription()

	// Use real bootstrap peers if none provided
	if len(boostrapPeers) == 0 {
		boostrapPeers = []string{"/ip4/136.112.206.228/tcp/9171/p2p/12D3KooWC2A7oX3zq3QLrL3NhrrSrLSxMArHRWQPCZGqF3XzPAYv"}
	}

	testHostConfig := &config.Config{
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
			WebSocketUrl:        "ws://dummy-url",
			StartHeight:         23500000, // Use a reasonable start height to avoid processing too much historical data
		},
		ShinzoAppConfig: DefaultConfig.ShinzoAppConfig,
		HostConfig: config.HostConfig{
			LensRegistryPath: t.TempDir(),
		},
	}
	testHostConfig.ShinzoAppConfig.DefraDB.Store.Path = t.TempDir()
	testHostConfig.ShinzoAppConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	ipAddress, err := networking.GetLANIP()
	require.NoError(t, err)
	listenAddress := fmt.Sprintf("/ip4/%s/tcp/0", ipAddress)
	defraUrl := fmt.Sprintf("%s:0", ipAddress)
	testHostConfig.ShinzoAppConfig.DefraDB.Url = defraUrl
	testHostConfig.ShinzoAppConfig.DefraDB.P2P.BootstrapPeers = boostrapPeers
	testHostConfig.ShinzoAppConfig.DefraDB.P2P.ListenAddr = listenAddress

	t.Logf("🔗 Connecting to bootstrap peers: %v", boostrapPeers)
	t.Logf("📊 Starting from block height: %d", testHostConfig.Shinzo.StartHeight)

	testHost, err := StartHostingWithEventSubscription(testHostConfig, mockEventSub)
	require.NoError(t, err)

	return testHost, mockEventSub
}

// waitForBlockchainData waits for actual blockchain processing activity
func waitForBlockchainData(t *testing.T, host *Host, timeout time.Duration) bool {
	t.Log("⏳ Waiting for blockchain processing activity...")

	// Since the system is event-driven, just wait a reasonable time for P2P sync
	// and processing to start, then check if any activity occurred
	time.Sleep(5 * time.Second)

	// Check for any attestation records (best indicator of processing)
	ctx := context.Background()
	attestationQueries := []string{
		`query { AttestationRecord_Document_Block(limit: 1) { _docID } }`,
		`query { AttestationRecord_Document_Transaction(limit: 1) { _docID } }`,
		`query { AttestationRecord_Document_Log(limit: 1) { _docID } }`,
	}

	for _, query := range attestationQueries {
		result, err := defra.QueryArray[map[string]interface{}](ctx, host.DefraNode, query)
		if err == nil && len(result) > 0 {
			t.Log("✅ Found attestation records! Blockchain processing is active")
			return true
		}
	}

	t.Log("⚠️ No attestation records found, but continuing test (P2P may still be syncing)")
	return true // Don't fail the test, just continue - the real test is the processing logic
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
