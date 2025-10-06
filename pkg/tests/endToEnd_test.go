package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/host/pkg/host"
	"github.com/shinzonetwork/host/pkg/networking"
	"github.com/shinzonetwork/indexer/pkg/indexer"
	"github.com/sourcenetwork/defradb/client"
	defraHTTP "github.com/sourcenetwork/defradb/http"
	netConfig "github.com/sourcenetwork/defradb/net/config"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func TestReadViewsInAppAfterProcessingIndexerPrimitivesWithHost(t *testing.T) {
	// Create a bigPeer to serve as the entrypoint to the network
	bigPeer, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	indexerDefra, testIndexer := startIndexer(t, bigPeer.DB.PeerInfo())
	defer indexerDefra.Close(t.Context())
	defer testIndexer.StopIndexing()

	logs, err := defra.QueryArray[viewResult](t.Context(), indexerDefra, "Log { transactionHash }")
	require.NoError(t, err)
	require.Greater(t, len(logs), 0)

	boostrapPeers, errs := defra.PeersIntoBootstrap([]client.PeerInfo{bigPeer.DB.PeerInfo()})
	require.Len(t, errs, 0)
	testHost := host.CreateHostWithTwoViews(t, boostrapPeers...)
	defer testHost.Close(t.Context())
	require.Len(t, testHost.HostedViews, 2)

	appConfig := defra.DefaultConfig
	appConfig.DefraDB.P2P.BootstrapPeers = append(appConfig.DefraDB.P2P.BootstrapPeers, boostrapPeers...)
	appDefra, err := defra.StartDefraInstanceWithTestConfig(t, appConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer appDefra.Close(t.Context())

	for _, view := range testHost.HostedViews {
		err = view.SubscribeTo(t.Context(), appDefra)

		require.NoError(t, err)
	}

	// Wait until host has received logs
	logs, err = defra.QueryArray[viewResult](t.Context(), testHost.DefraNode, "Log { transactionHash }")
	require.NoError(t, err)
	for len(logs) == 0 {
		time.Sleep(1 * time.Second)
		logs, err = defra.QueryArray[viewResult](t.Context(), testHost.DefraNode, "Log { transactionHash }")
		require.NoError(t, err)
	}
	require.Greater(t, len(logs), 0)

	// Check host has lens results
	unfilteredQuery := fmt.Sprintf("%s { transactionHash }", testHost.HostedViews[0].Name)
	unfilteredResults, err := defra.QueryArray[viewResult](t.Context(), testHost.DefraNode, unfilteredQuery)
	require.NoError(t, err)
	require.Greater(t, len(unfilteredResults), 0)

	filteredQuery := fmt.Sprintf("%s { transactionHash }", testHost.HostedViews[1].Name)
	filteredResults, err := defra.QueryArray[viewResult](t.Context(), testHost.DefraNode, filteredQuery)
	require.NoError(t, err)
	require.NotNil(t, filteredResults)
	require.Greater(t, len(unfilteredResults), len(filteredResults))

	// Wait for app defra to receive lens results
	unfilteredResults, err = defra.QueryArray[viewResult](t.Context(), appDefra, unfilteredQuery)
	require.NoError(t, err)
	for len(unfilteredResults) == 0 {
		time.Sleep(1 * time.Second)
		unfilteredResults, err = defra.QueryArray[viewResult](t.Context(), appDefra, unfilteredQuery)
		require.NoError(t, err)
	}
	require.Greater(t, len(unfilteredResults), 0)

	filteredResults, err = defra.QueryArray[viewResult](t.Context(), appDefra, filteredQuery)
	require.NoError(t, err)
	require.NotNil(t, filteredResults)
	require.Greater(t, len(unfilteredResults), len(filteredResults))

	time.Sleep(60 * time.Second) // Allow more blocks to process

	newUnfilteredResults, err := defra.QueryArray[viewResult](t.Context(), appDefra, unfilteredQuery)
	require.NoError(t, err)
	require.Greater(t, len(newUnfilteredResults), len(unfilteredResults))
}

type viewResult struct {
	TransactionHash string `json:"transactionHash"`
}

func startIndexer(t *testing.T, bigPeer client.PeerInfo) (*node.Node, *indexer.ChainIndexer) {
	ipAddress, err := networking.GetLANIP() // Must use external address like IP address instead of loop back address for this to work - otherwise we will not hop past our big peer
	require.NoError(t, err)
	listenAddress := fmt.Sprintf("/ip4/%s/tcp/0", ipAddress)
	defraUrl := fmt.Sprintf("%s:0", ipAddress)
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		defraHTTP.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	ctx := context.Background()
	indexerDefra := startDefraInstanceForTest(t, ctx, options)
	testConfig := indexer.DefaultConfig
	testConfig.DefraDB.Url = indexerDefra.APIURL

	indexerSchemaApplier := defra.SchemaApplierFromFile{DefaultPath: "schema/schema.graphql"}
	err = indexerSchemaApplier.ApplySchema(ctx, indexerDefra)
	require.NoError(t, err)

	err = indexerDefra.DB.Connect(ctx, bigPeer)
	require.NoError(t, err)

	err = indexerDefra.DB.AddP2PCollections(ctx, "Block", "Transaction", "AccessListEntry", "Log")
	require.NoError(t, err)

	i := indexer.CreateIndexer(testConfig)
	go func() {
		err := i.StartIndexing(true)
		if err != nil {
			panic(fmt.Sprintf("Encountered unexpected error starting defra dependency: %v", err))
		}
	}()

	for !i.IsStarted() || !i.HasIndexedAtLeastOneBlock() {
		time.Sleep(100 * time.Millisecond)
	}

	return indexerDefra, i
}
