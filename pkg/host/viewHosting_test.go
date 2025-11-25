package host

import (
	"fmt"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
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

	_, _, err = host.ApplyView(t.Context(), testView, 1, 99999999)
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

	processedBlocks, missingBlocks, err := host.ApplyView(t.Context(), testView, 9999999, 99999999)
	require.NoError(t, err)
	require.Len(t, processedBlocks, 0, "Should have no processed blocks when querying range with no data")
	require.Greater(t, len(missingBlocks), 0, "Should have missing blocks when querying range with no data")

	dataWritten, err := defra.QueryArray[attestation.Log](t.Context(), host.DefraNode, fmt.Sprintf("%s {transactionHash}", testView.Name))
	require.NoError(t, err)
	require.Len(t, dataWritten, 0)
}

func TestApplyViewWithLens(t *testing.T) {
	testHost := CreateHostWithLensEventReceived(t)
	defer testHost.Close(t.Context())

	postDummyLog(t, testHost.DefraNode, "0x1e3aA9fE4Ef01D3cB3189c129a49E3C03126C636")
	postDummyLog(t, testHost.DefraNode, "0xabcaA9fE4Ef3333cB3189c129a49E3C03126Cabc")

	dataBefore, err := defra.QueryArray[attestation.Log](t.Context(), testHost.DefraNode, "Log {transactionHash}")
	require.NoError(t, err)
	require.Greater(t, len(dataBefore), 0)

	_, _, err = testHost.ApplyView(t.Context(), testHost.HostedViews[0], 1, 9999999)
	require.NoError(t, err)

	dataWritten, err := defra.QueryArray[attestation.Log](t.Context(), testHost.DefraNode, fmt.Sprintf("%s {transactionHash}", testHost.HostedViews[0].Name))
	require.NoError(t, err)
	require.Less(t, len(dataWritten), len(dataBefore)) // One log should've been filtered out
	require.Len(t, dataWritten, 1)
}
