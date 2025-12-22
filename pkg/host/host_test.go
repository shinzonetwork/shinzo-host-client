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
	"github.com/shinzonetwork/indexer/pkg/indexer"
	"github.com/shinzonetwork/indexer/pkg/logger"
	"github.com/shinzonetwork/indexer/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/stack"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/shinzonetwork/view-creator/core/models"
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
	myHost.Close(context.Background())
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
	testConfig, err := indexerConfig.LoadConfig(filepath)
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

func TestHostedViewsPersistAcrossRestarts(t *testing.T) {
	ctx := t.Context()

	// Create a temporary directory for the defra store that will persist
	storePath := t.TempDir()

	// Create first host instance
	testConfig1 := &config.Config{
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
		},
		ShinzoAppConfig: DefaultConfig.ShinzoAppConfig,
		HostConfig: config.HostConfig{
			LensRegistryPath: t.TempDir(),
		},
	}
	testConfig1.ShinzoAppConfig.DefraDB.Store.Path = storePath
	testConfig1.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"

	host1, err := StartHosting(testConfig1)
	require.NoError(t, err)
	require.Len(t, host1.HostedViews, 0, "Initially should have no hosted views")

	// Prepare a view - this should store it to defra
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type RecoveredView {transactionHash: String}"
	testView := view.View{
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
		Name:      "RecoveredView",
	}

	err = host1.PrepareView(ctx, testView)
	require.NoError(t, err)

	// Add the view to HostedViews manually (simulating what happens in handleIncomingEvents)
	host1.HostedViews = append(host1.HostedViews, testView)
	require.Len(t, host1.HostedViews, 1, "First host should have one hosted view")

	// Close the first host
	err = host1.Close(ctx)
	require.NoError(t, err)

	// Wait a bit to ensure defra is fully closed
	time.Sleep(200 * time.Millisecond)

	// Create a second host instance with the same store path
	testConfig2 := &config.Config{
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
		},
		ShinzoAppConfig: DefaultConfig.ShinzoAppConfig,
		HostConfig: config.HostConfig{
			LensRegistryPath: t.TempDir(),
		},
	}
	testConfig2.ShinzoAppConfig.DefraDB.Store.Path = storePath
	testConfig2.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"

	host2, err := StartHosting(testConfig2)
	require.NoError(t, err)
	defer host2.Close(ctx)

	// The view should be recovered from defra
	require.Len(t, host2.HostedViews, 1, "Second host should have recovered the view")
	require.Equal(t, testView.Name, host2.HostedViews[0].Name, "Recovered view should have the same name")
	require.NotNil(t, host2.HostedViews[0].Query)
	require.Equal(t, *testView.Query, *host2.HostedViews[0].Query, "Recovered view should have the same query")
	require.NotNil(t, host2.HostedViews[0].Sdl)
	require.Equal(t, *testView.Sdl, *host2.HostedViews[0].Sdl, "Recovered view should have the same SDL")

	// Verify that viewProcessedBlocks was initialized (but should be empty since we didn't process any blocks)
	require.Contains(t, host2.viewProcessedBlocks, testView.Name, "Recovered view should have a processed blocks stack")
	stack := host2.viewProcessedBlocks[testView.Name]
	require.NotNil(t, stack)
	require.True(t, stack.IsEmpty(), "Processed blocks stack should be empty initially")
}

func TestRecoveredViewsRestoreLastProcessedBlock(t *testing.T) {
	ctx := t.Context()

	// Create a temporary directory for the defra store that will persist
	storePath := t.TempDir()

	// Create first host instance
	testConfig1 := &config.Config{
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
		},
		ShinzoAppConfig: DefaultConfig.ShinzoAppConfig,
		HostConfig: config.HostConfig{
			LensRegistryPath: t.TempDir(),
		},
	}
	testConfig1.ShinzoAppConfig.DefraDB.Store.Path = storePath
	testConfig1.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"

	host1, err := StartHosting(testConfig1)
	require.NoError(t, err)

	// Prepare a view
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type BlockTrackingView {transactionHash: String}"
	testView := view.View{
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
		Name:      "BlockTrackingView",
	}

	err = host1.PrepareView(ctx, testView)
	require.NoError(t, err)
	host1.HostedViews = append(host1.HostedViews, testView)
	host1.viewProcessedBlocks[testView.Name] = &stack.Stack[uint64]{}

	// Simulate processing some blocks by updating lastProcessedBlock
	testBlockNumber := uint64(9999)
	err = host1.updateViewLastProcessedBlock(ctx, testView.Name, testBlockNumber)
	require.NoError(t, err)
	host1.viewProcessedBlocks[testView.Name].Push(testBlockNumber)

	// Close the first host
	err = host1.Close(ctx)
	require.NoError(t, err)

	// Wait a bit to ensure defra is fully closed
	time.Sleep(200 * time.Millisecond)

	// Create a second host instance with the same store path
	testConfig2 := &config.Config{
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
		},
		ShinzoAppConfig: DefaultConfig.ShinzoAppConfig,
		HostConfig: config.HostConfig{
			LensRegistryPath: t.TempDir(),
		},
	}
	testConfig2.ShinzoAppConfig.DefraDB.Store.Path = storePath
	testConfig2.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"

	host2, err := StartHosting(testConfig2)
	require.NoError(t, err)
	defer host2.Close(ctx)

	// Wait for recovery
	time.Sleep(200 * time.Millisecond)

	// Verify the view was recovered
	require.Len(t, host2.HostedViews, 1, "Second host should have recovered the view")

	// Verify that lastProcessedBlock was restored to the stack
	require.Contains(t, host2.viewProcessedBlocks, testView.Name, "Recovered view should have a processed blocks stack")
	recoveredStack := host2.viewProcessedBlocks[testView.Name]
	require.NotNil(t, recoveredStack)
	require.False(t, recoveredStack.IsEmpty(), "Processed blocks stack should not be empty after recovery")

	// Verify the block number was restored
	recoveredBlock, err := recoveredStack.Peek()
	require.NoError(t, err)
	require.Equal(t, testBlockNumber, recoveredBlock, "Last processed block should be restored from defra")
}
