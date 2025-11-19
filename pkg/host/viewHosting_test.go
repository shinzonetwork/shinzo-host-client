package host

import (
	"fmt"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/host/pkg/attestation"
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

	err = host.ApplyView(t.Context(), testView, 1, 99999999)
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

	err = host.ApplyView(t.Context(), testView, 9999999, 99999999)
	require.Error(t, err)

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

	err = testHost.ApplyView(t.Context(), testHost.HostedViews[0], 1, 9999999)
	require.NoError(t, err)

	dataWritten, err := defra.QueryArray[attestation.Log](t.Context(), testHost.DefraNode, fmt.Sprintf("%s {transactionHash}", testHost.HostedViews[0].Name))
	require.NoError(t, err)
	require.Less(t, len(dataWritten), len(dataBefore)) // One log should've been filtered out
	require.Len(t, dataWritten, 1)
}

func TestPrepareViewStoresViewToDefra(t *testing.T) {
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type TestStoredView {transactionHash: String}"
	testView := view.View{
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
		Name:      "TestStoredView",
	}

	host, err := StartHostingWithTestConfig(t)
	require.NoError(t, err)
	defer host.Close(t.Context())

	// Prepare the view - this should store it to defra
	err = host.PrepareView(t.Context(), testView)
	require.NoError(t, err)

	// Query the View collection to confirm the view was stored
	type ViewDoc struct {
		Name               string `json:"name"`
		Query              string `json:"query"`
		Sdl                string `json:"sdl"`
		LastProcessedBlock *int64 `json:"lastProcessedBlock"`
	}
	
	storedViews, err := defra.QueryArray[ViewDoc](t.Context(), host.DefraNode, `query {
		View(filter: {name: {_eq: "TestStoredView"}}) {
			name
			query
			sdl
			lastProcessedBlock
		}
	}`)
	require.NoError(t, err)
	require.Len(t, storedViews, 1, "View should be stored in defra")
	require.Equal(t, testView.Name, storedViews[0].Name)
	require.Equal(t, *testView.Query, storedViews[0].Query)
	require.Equal(t, *testView.Sdl, storedViews[0].Sdl)
	// lastProcessedBlock should be 0 initially
	require.NotNil(t, storedViews[0].LastProcessedBlock)
	require.Equal(t, int64(0), *storedViews[0].LastProcessedBlock)
	
	// Test updating lastProcessedBlock
	err = host.updateViewLastProcessedBlock(t.Context(), testView.Name, 12345)
	require.NoError(t, err)
	
	// Query again to verify the update
	storedViews, err = defra.QueryArray[ViewDoc](t.Context(), host.DefraNode, `query {
		View(filter: {name: {_eq: "TestStoredView"}}) {
			name
			lastProcessedBlock
		}
	}`)
	require.NoError(t, err)
	require.Len(t, storedViews, 1)
	require.NotNil(t, storedViews[0].LastProcessedBlock)
	require.Equal(t, int64(12345), *storedViews[0].LastProcessedBlock)
}
