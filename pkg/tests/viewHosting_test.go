package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	appDefra "github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/file"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/shinzonetwork/host/config"
	"github.com/shinzonetwork/host/pkg/attestation"
	"github.com/shinzonetwork/host/pkg/host"
	"github.com/shinzonetwork/host/pkg/shinzohub"
	"github.com/shinzonetwork/indexer/pkg/indexer"
	defraHTTP "github.com/sourcenetwork/defradb/http"
	netConfig "github.com/sourcenetwork/defradb/net/config"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func testSetup_HostReceivingDataFromIndexerAndHasReceivedAView(t *testing.T) (*node.Node, *indexer.ChainIndexer, *host.Host) {
	logger.Init(true)
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
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

	indexerSchemaApplier := appDefra.SchemaApplierFromFile{DefaultPath: "schema/schema.graphql"}
	err := indexerSchemaApplier.ApplySchema(ctx, indexerDefra)
	require.NoError(t, err)

	err = indexerDefra.DB.AddP2PCollections(ctx, "Block", "Transaction", "AccessListEntry", "Log")
	require.NoError(t, err)

	testIndexer := indexer.CreateIndexer(testConfig)
	go func() {
		err := testIndexer.StartIndexing(true)
		if err != nil {
			panic(fmt.Sprintf("Encountered unexpected error starting defra dependency: %v", err))
		}
	}()

	for !testIndexer.IsStarted() || !testIndexer.HasIndexedAtLeastOneBlock() {
		time.Sleep(100 * time.Millisecond)
	}

	mockEventSub := shinzohub.NewMockEventSubscription()

	testHostConfig := &config.Config{
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
			WebSocketUrl:        "ws://dummy-url",
		},
		ShinzoAppConfig: host.DefaultConfig.ShinzoAppConfig,
		HostConfig: config.HostConfig{
			LensRegistryPath: t.TempDir(),
		},
	}
	testHostConfig.ShinzoAppConfig.DefraDB.Store.Path = t.TempDir()
	testHostConfig.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"

	// Start hosting with the mock
	testHost, err := host.StartHostingWithEventSubscription(testHostConfig, mockEventSub)
	require.NoError(t, err)
	hostDefra := testHost.DefraNode

	err = hostDefra.DB.Connect(ctx, indexerDefra.DB.PeerInfo())
	require.NoError(t, err)

	// Schema is applied automatically by the app-sdk

	blockNumber, err := queryBlockNumber(ctx, indexerDefra)
	require.NoError(t, err)
	require.Greater(t, blockNumber, 100)

	_, err = queryBlockNumber(ctx, hostDefra)
	require.Error(t, err) // Host shouldn't know the latest block number yet as it hasn't synced with the Indexer

	err = hostDefra.DB.AddP2PCollections(ctx, "Block", "Transaction", "AccessListEntry", "Log")
	require.NoError(t, err)

	blockNumber, err = queryBlockNumber(ctx, hostDefra)
	for attempts := 1; attempts < 60; attempts++ { // It may take some time to sync now that we are connected
		if err == nil {
			break
		}
		t.Logf("Attempt %d to query block number from host failed. Trying again...", attempts)
		time.Sleep(1 * time.Second)
		blockNumber, err = queryBlockNumber(ctx, hostDefra)
	}
	require.NoError(t, err) // We should now have the block number on the Host
	require.Greater(t, blockNumber, 100)

	// Read event json from file
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

	return indexerDefra, testIndexer, testHost
}

func queryBlockNumber(ctx context.Context, defraNode *node.Node) (int, error) {
	query := `query GetHighestBlockNumber { Block(order: {number: DESC}, limit: 1) { number } }`

	block, err := defra.QuerySingle[attestation.Block](ctx, defraNode, query)

	if err != nil {
		return 0, fmt.Errorf("Error querying block number: %v", err)
	}

	if block.Number == 0 {
		return 0, fmt.Errorf("No blocks found")
	}

	return block.Number, nil
}

func TestHostCanApplyLenses(t *testing.T) {
	indexerDefra, testIndexer, testHost := testSetup_HostReceivingDataFromIndexerAndHasReceivedAView(t)
	ctx := context.Background()
	defer indexerDefra.Close(ctx)
	defer testIndexer.StopIndexing()
	defer testHost.Close(ctx)

	require.Len(t, testHost.HostedViews, 1, "Host should have one hosted view after receiving event")
	hostedView := testHost.HostedViews[0]
	require.NotEmpty(t, hostedView.Name, "View should have a name extracted from SDL")
	require.NotNil(t, hostedView.Transform)
	require.NotNil(t, hostedView.Transform.Lenses)
	require.Len(t, hostedView.Transform.Lenses, 1)
	require.Equal(t, "filter", hostedView.Transform.Lenses[0].Label)

	hostDefra := testHost.DefraNode
	err := hostedView.SubscribeTo(ctx, hostDefra)
	require.Error(t, err) // Host should already be subscribed to view

	err = hostedView.PostWasmToFile(ctx, testHost.LensRegistryPath)
	require.NoError(t, err)

	err = hostedView.ConfigureLens(ctx, hostDefra, "Log")
	require.NoError(t, err, "Received unexpected error: %w", err)

	receivedLogDocuments, err := defra.QueryArray[map[string]any](ctx, hostDefra, *hostedView.Query)
	require.NoError(t, err)
	require.Greater(t, len(receivedLogDocuments), 0)

	transformedLogDocuments, err := hostedView.ApplyLensTransform(ctx, hostDefra, receivedLogDocuments)
	require.NoError(t, err)
	require.Greater(t, len(transformedLogDocuments), 0)
	require.Equal(t, len(receivedLogDocuments), len(transformedLogDocuments))

	err = hostedView.WriteTransformedToCollection(ctx, hostDefra, transformedLogDocuments)
	require.NoError(t, err)

	query := fmt.Sprintf("%s {transactionHash}", hostedView.Name)
	transformedLogs, err := defra.QueryArray[attestation.Log](ctx, hostDefra, query)
	require.NoError(t, err)
	require.Greater(t, len(transformedLogs), 0)
}
