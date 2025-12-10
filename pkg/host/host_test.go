package host

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/file"
	"github.com/shinzonetwork/indexer/config"
	"github.com/shinzonetwork/indexer/pkg/indexer"
	"github.com/shinzonetwork/indexer/pkg/logger"
	"github.com/shinzonetwork/indexer/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startDefraInstanceForTest creates a DefraDB node for testing purposes
func startDefraInstanceForTest(t *testing.T, ctx context.Context, options []node.Option) *node.Node {
	myNode, err := node.New(ctx, options...)
	require.NoError(t, err)
	require.NotNil(t, myNode)

	err = myNode.Start(ctx)
	require.NoError(t, err)

	return myNode
}

func TestMain(m *testing.M) {
	logger.Init(true)
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestStartHosting(t *testing.T) {
	myHost, err := StartHosting(nil)

	assert.NoError(t, err)
	if myHost != nil {
		myHost.Close(context.Background())
	}
}

func queryBlockNumber(ctx context.Context, defraNode *node.Node) (uint64, error) {
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

func TestHostCanReplicateFromIndexerViaRegularConnection(t *testing.T) {
	logger.Init(true)
	ctx := t.Context()
	indexerDefra, err := defra.StartDefraInstanceWithTestConfig(t,
		defra.DefaultConfig,
		defra.NewSchemaApplierFromProvidedSchema(schema.GetSchema()),
		"Block")
	require.NoError(t, err)

	filepath, err := file.FindFile("config.yaml")
	require.NoError(t, err)
	testConfig, err := config.LoadConfig(filepath)
	require.NoError(t, err)

	testConfig.DefraDB.Url = indexerDefra.APIURL
	testConfig.Geth = GetGethConfig()
	i, err := indexer.CreateIndexer(testConfig)
	require.NoError(t, err)
	go func() {
		err := i.StartIndexing(true)
		if err != nil {
			panic(fmt.Sprintf("Encountered unexpected error starting defra dependency: %v", err))
		}
	}()
	defer i.StopIndexing()

	time.Sleep(10 * time.Second) // Allow a moment for everything to get started
	for !i.IsStarted() || !i.HasIndexedAtLeastOneBlock() {
		time.Sleep(100 * time.Millisecond)
	}

	testHost, err := StartHostingWithTestConfig(t)
	require.NoError(t, err)
	defer testHost.Close(ctx)
	hostDefra := testHost.DefraNode

	peerInfo, err := indexerDefra.DB.PeerInfo()
	require.NoError(t, err)
	err = hostDefra.DB.Connect(ctx, peerInfo)
	require.NoError(t, err)

	// Schema is applied automatically by the app-sdk

	blockNumber, err := queryBlockNumber(ctx, indexerDefra)
	require.NoError(t, err)
	require.Greater(t, blockNumber, uint64(1))

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
	require.Greater(t, blockNumber, uint64(1))
}

func TestMonitorHighestBlockNumber(t *testing.T) {
	ctx := t.Context()
	testHost, err := StartHostingWithTestConfig(t)
	require.NoError(t, err)
	defer testHost.Close(ctx)

	// Initially, mostRecentBlockReceived should be 0
	require.Equal(t, uint64(0), testHost.mostRecentBlockReceived)

	// Wait a bit for the monitoring goroutine to start
	time.Sleep(100 * time.Millisecond)

	// Add a block to defraNode
	blockNumber1 := uint64(1000)
	postDummyBlock(t, testHost.DefraNode, blockNumber1)

	// Wait for the monitoring goroutine to detect it (checks every second)
	// Give it a bit more than 1 second to account for timing
	maxWait := 3 * time.Second
	startTime := time.Now()
	for time.Since(startTime) < maxWait {
		if testHost.mostRecentBlockReceived >= blockNumber1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	require.Equal(t, blockNumber1, testHost.mostRecentBlockReceived)

	// Add another block with a higher number
	blockNumber2 := uint64(2000)
	postDummyBlock(t, testHost.DefraNode, blockNumber2)

	// Wait for the monitoring goroutine to detect the new block
	startTime = time.Now()
	for time.Since(startTime) < maxWait {
		if testHost.mostRecentBlockReceived >= blockNumber2 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	require.Equal(t, blockNumber2, testHost.mostRecentBlockReceived)

	// Add a block with a lower number - should not update mostRecentBlockReceived
	blockNumber3 := uint64(500)
	postDummyBlock(t, testHost.DefraNode, blockNumber3)

	time.Sleep(1500 * time.Millisecond)

	require.Equal(t, blockNumber2, testHost.mostRecentBlockReceived)
}
