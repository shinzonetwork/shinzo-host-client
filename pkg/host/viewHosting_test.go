package host

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/file"
	"github.com/shinzonetwork/host/config"
	"github.com/shinzonetwork/host/pkg/attestation"
	"github.com/shinzonetwork/host/pkg/shinzohub"
	"github.com/shinzonetwork/host/pkg/view"
	"github.com/shinzonetwork/view-creator/core/models"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func TestPrepareViewWithoutLenses(t *testing.T) {
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type FilteredAndDecodedLogs {transactionHash: String}"
	testView := view.View{
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
		Name:      "FilteredAndDecodedLogs",
	}

	host, err := StartHostingWithTestConfig(t)
	require.NoError(t, err)

	err = host.PrepareView(t.Context(), testView)
	require.NoError(t, err)
}

func TestApplyViewWithoutLenses(t *testing.T) {
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type FilteredAndDecodedLogs {transactionHash: String}"
	testView := view.View{
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
		Name:      "FilteredAndDecodedLogs",
	}

	host, err := StartHostingWithTestConfig(t)
	require.NoError(t, err)

	err = host.PrepareView(t.Context(), testView)
	require.NoError(t, err)

	postDummyLog(t, host.DefraNode, "0x1e3aA9fE4Ef01D3cB3189c129a49E3C03126C636")

	dataBefore, err := defra.QueryArray[attestation.Log](t.Context(), host.DefraNode, "Log {transactionHash}")
	require.NoError(t, err)
	require.Greater(t, len(dataBefore), 0)

	err = host.ApplyView(t.Context(), testView, 1)
	require.NoError(t, err)

	dataWritten, err := defra.QueryArray[attestation.Log](t.Context(), host.DefraNode, fmt.Sprintf("%s {transactionHash}", testView.Name))
	require.NoError(t, err)
	require.ElementsMatch(t, dataBefore, dataWritten) // There is no lens so data should not have changed, only the SDL may have changed (meaning we have less data in the collection)
}

func postDummyLog(t *testing.T, hostDefra *node.Node, address string) {
	dummyLog := map[string]any{
		"address":         address,
		"blockNumber":     12345,
		"data":            "0x0000000000000000000000000000000000000000000000000000000000000001",
		"topics":          []string{"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"},
		"transactionHash": "0xdummy123456789abcdef123456789abcdef123456789abcdef123456789abcdef12",
	}

	ctx := t.Context()

	// Write the dummy log to the Log collection
	logCollection, err := hostDefra.DB.GetCollectionByName(ctx, "Log")
	require.NoError(t, err)

	dummyDoc, err := client.NewDocFromMap(dummyLog, logCollection.Version())
	require.NoError(t, err)

	err = logCollection.Save(ctx, dummyDoc)
	require.NoError(t, err)
}

func TestApplyViewWithoutLenses_StartingBlockPastWhatWeHaveDataFor(t *testing.T) {
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type FilteredAndDecodedLogs {transactionHash: String}"
	testView := view.View{
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
		Name:      "FilteredAndDecodedLogs",
	}

	host, err := StartHostingWithTestConfig(t)
	require.NoError(t, err)

	err = host.PrepareView(t.Context(), testView)
	require.NoError(t, err)

	postDummyLog(t, host.DefraNode, "0x1e3aA9fE4Ef01D3cB3189c129a49E3C03126C636")

	dataBefore, err := defra.QueryArray[attestation.Log](t.Context(), host.DefraNode, "Log {transactionHash}")
	require.NoError(t, err)
	require.Greater(t, len(dataBefore), 0)

	err = host.ApplyView(t.Context(), testView, 999999999)
	require.NoError(t, err)

	dataWritten, err := defra.QueryArray[attestation.Log](t.Context(), host.DefraNode, fmt.Sprintf("%s {transactionHash}", testView.Name))
	require.NoError(t, err)
	require.Len(t, dataWritten, 0)
}

func TestApplyViewWithLens(t *testing.T) {
	testHost := createHostWithLensEventReceived(t)
	defer testHost.Close(t.Context())

	postDummyLog(t, testHost.DefraNode, "0x1e3aA9fE4Ef01D3cB3189c129a49E3C03126C636")
	postDummyLog(t, testHost.DefraNode, "0xabcaA9fE4Ef3333cB3189c129a49E3C03126Cabc")

	dataBefore, err := defra.QueryArray[attestation.Log](t.Context(), testHost.DefraNode, "Log {transactionHash}")
	require.NoError(t, err)
	require.Greater(t, len(dataBefore), 0)

	err = testHost.ApplyView(t.Context(), testHost.HostedViews[0], 1)
	require.NoError(t, err)

	dataWritten, err := defra.QueryArray[attestation.Log](t.Context(), testHost.DefraNode, fmt.Sprintf("%s {transactionHash}", testHost.HostedViews[0].Name))
	require.NoError(t, err)
	require.Less(t, len(dataWritten), len(dataBefore)) // One log should've been filtered out
	require.Len(t, dataWritten, 1)
}

func createHostWithLensEventReceived(t *testing.T) *Host {
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
	testHostConfig.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"

	testHost, err := StartHostingWithEventSubscription(testHostConfig, mockEventSub)
	require.NoError(t, err)

	var rawJSON []byte
	path, err := file.FindFile("../host/viewWithLensEvent.txt")
	require.NoError(t, err)
	rawJSON, err = os.ReadFile(path)
	require.NoError(t, err, "Failed to read viewWithLensEvent.txt from any of the attempted paths")

	// Send the raw JSON message as if it came from the WebSocket
	err = mockEventSub.SendRawJSONMessage(string(rawJSON))
	require.NoError(t, err, "Failed to process raw JSON message")

	// Wait a bit for the event to be processed
	time.Sleep(200 * time.Millisecond)

	// Verify the host processed the event
	require.Len(t, testHost.HostedViews, 1, "Host should have one hosted view after receiving event")
	return testHost
}
