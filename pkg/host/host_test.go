package host

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shinzonetwork/host/pkg/attestation"
	"github.com/shinzonetwork/host/pkg/defra"
	"github.com/shinzonetwork/indexer/pkg/indexer"
	"github.com/shinzonetwork/indexer/pkg/logger"
	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/http"
	netConfig "github.com/sourcenetwork/defradb/net/config"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logger.Init(true)
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestStartHosting(t *testing.T) {
	err := StartHosting(false, nil)

	assert.NoError(t, err)
}

func TestStartHostingWithOwnDefraInstance(t *testing.T) {
	ctx := context.Background()

	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(true),
		node.WithStorePath(t.TempDir()),
	}

	myNode := defra.StartDefraInstance(t, ctx, options)
	assert.NotNil(t, myNode)
	defer myNode.Close(ctx)

	err := StartHosting(true, nil)

	assert.NoError(t, err)
}

func TestHostCanReplicateFromIndexer(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	ctx := context.Background()
	indexerDefra := defra.StartDefraInstance(t, ctx, options)
	defer indexerDefra.Close(ctx)
	testConfig := indexer.DefaultConfig
	testConfig.DefraDB.Url = indexerDefra.APIURL

	err := applySchema(ctx, indexerDefra)
	require.NoError(t, err)

	i := indexer.CreateIndexer(testConfig)
	go func() {
		err := i.StartIndexing(true)
		if err != nil {
			panic(fmt.Sprintf("Encountered unexpected error starting defra dependency: %v", err))
		}
	}()
	defer i.StopIndexing()

	for !i.IsStarted() || !i.HasIndexedAtLeastOneBlock() {
		time.Sleep(100 * time.Millisecond)
	}

	options = []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	hostDefra := defra.StartDefraInstance(t, ctx, options)
	defer hostDefra.Close(ctx)

	err = applySchema(ctx, hostDefra)
	require.NoError(t, err)

	indexerPort := defra.GetPort(indexerDefra)
	if indexerPort == -1 {
		t.Fatalf("Unable to retrieve indexer port")
	}
	blockNumber, err := queryBlockNumber(ctx, indexerPort)
	require.NoError(t, err)
	require.Greater(t, blockNumber, 100)

	hostPort := defra.GetPort(hostDefra)
	if hostPort == -1 {
		t.Fatalf("Unable to retrieve host port")
	}
	blockNumber, err = queryBlockNumber(ctx, hostPort)
	require.Error(t, err) // Host shouldn't know the latest block number yet as it hasn't synced with the Indexer

	err = indexerDefra.DB.SetReplicator(ctx, hostDefra.DB.PeerInfo())
	require.NoError(t, err)

	blockNumber, err = queryBlockNumber(ctx, hostPort)
	for attempts := 1; attempts < 30; attempts++ { // It may take some time to sync now that we are connected
		if err == nil {
			break
		}
		t.Logf("Attempt %d to query block number from host failed. Trying again...", attempts)
		time.Sleep(1 * time.Second)
		blockNumber, err = queryBlockNumber(ctx, hostPort)
	}
	require.NoError(t, err) // We should now have the block number on the Host
	require.Greater(t, blockNumber, 100)
}

func queryBlockNumber(ctx context.Context, port int) (int, error) {
	client, err := defra.NewQueryClientFromPort(port)
	if err != nil {
		return 0, fmt.Errorf("Error creating query client: %v", err)
	}

	query := `query GetHighestBlockNumber {
  Block(order: {number: DESC}, limit: 1) {
    number
  }
}`

	// Define the expected response structure
	type BlockResponse struct {
		Number int `json:"number"`
	}

	// Use QuerySingle to get the first block
	block, err := defra.QuerySingle[BlockResponse](client, ctx, query)
	if err != nil {
		return 0, fmt.Errorf("Error querying block number: %v", err)
	}
	if block.Number < 1 { // If we receive no blocks, we will get a block number of 0
		return 0, fmt.Errorf("Invalid block number: %d", block.Number)
	}

	return block.Number, nil
}

func TestHostCanReplicateFromIndexerViaBootstrappedConnection(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	ctx := context.Background()
	indexerDefra := defra.StartDefraInstance(t, ctx, options)
	defer indexerDefra.Close(ctx)
	testConfig := indexer.DefaultConfig
	testConfig.DefraDB.Url = indexerDefra.APIURL

	err := applySchema(ctx, indexerDefra)
	require.NoError(t, err)

	err = indexerDefra.DB.AddP2PCollections(ctx, "Block")
	require.NoError(t, err)

	i := indexer.CreateIndexer(testConfig)
	go func() {
		err := i.StartIndexing(true)
		if err != nil {
			panic(fmt.Sprintf("Encountered unexpected error starting defra dependency: %v", err))
		}
	}()
	defer i.StopIndexing()

	for !i.IsStarted() || !i.HasIndexedAtLeastOneBlock() {
		time.Sleep(100 * time.Millisecond)
	}

	bootstrapPeer, err := defra.GetBoostrapPeer(indexerDefra.DB.PeerInfo())
	require.NoError(t, err)
	options = []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
		netConfig.WithBootstrapPeers(bootstrapPeer),
	}
	hostDefra := defra.StartDefraInstance(t, ctx, options)
	defer hostDefra.Close(ctx)

	err = applySchema(ctx, hostDefra)
	require.NoError(t, err)

	indexerPort := defra.GetPort(indexerDefra)
	if indexerPort == -1 {
		t.Fatalf("Unable to retrieve indexer port")
	}
	blockNumber, err := queryBlockNumber(ctx, indexerPort)
	require.NoError(t, err)
	require.Greater(t, blockNumber, 100)

	hostPort := defra.GetPort(hostDefra)
	if hostPort == -1 {
		t.Fatalf("Unable to retrieve host port")
	}
	blockNumber, err = queryBlockNumber(ctx, hostPort)
	require.Error(t, err) // Host shouldn't know the latest block number yet as it hasn't synced with the Indexer

	err = hostDefra.DB.AddP2PCollections(ctx, "Block")
	require.NoError(t, err)

	blockNumber, err = queryBlockNumber(ctx, hostPort)
	for attempts := 1; attempts < 60; attempts++ { // It may take some time to sync now that we are connected
		if err == nil {
			break
		}
		t.Logf("Attempt %d to query block number from host failed. Trying again...", attempts)
		time.Sleep(1 * time.Second)
		blockNumber, err = queryBlockNumber(ctx, hostPort)
	}
	require.NoError(t, err) // We should now have the block number on the Host
	require.Greater(t, blockNumber, 100)
}

func TestHostReplicateFromMultipleIndexers(t *testing.T) {
	indexerDefras := []*node.Node{}
	indexers := []*indexer.ChainIndexer{}
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	for i := 0; i < 3; i++ {
		ctx := context.Background()
		nodeIdentity, err := identity.Generate(crypto.KeyTypeSecp256k1)
		require.NoError(t, err)
		options := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			netConfig.WithListenAddresses(listenAddress),
			node.WithNodeIdentity(identity.Identity(nodeIdentity)),
		}
		indexerDefra := defra.StartDefraInstance(t, ctx, options)
		defer indexerDefra.Close(ctx)
		testConfig := indexer.DefaultConfig
		testConfig.DefraDB.Url = indexerDefra.APIURL

		err = applySchema(ctx, indexerDefra)
		require.NoError(t, err)

		err = indexerDefra.DB.AddP2PCollections(ctx, "Block", "Transaction", "AccessListEntry", "Log")
		require.NoError(t, err)

		indexerDefras = append(indexerDefras, indexerDefra)

		chainIndexer := indexer.CreateIndexer(testConfig)
		go func() {
			err := chainIndexer.StartIndexing(true)
			if err != nil {
				panic(fmt.Sprintf("Encountered unexpected error starting defra dependency: %v", err))
			}
		}()
		defer chainIndexer.StopIndexing()

		indexers = append(indexers, chainIndexer)
	}

	for index, chainIndexer := range indexers {
		ctx := context.Background()
		for !chainIndexer.IsStarted() || !chainIndexer.HasIndexedAtLeastOneBlock() {
			time.Sleep(100 * time.Millisecond)
		}

		indexerPort := defra.GetPort(indexerDefras[index])
		if indexerPort == -1 {
			t.Fatalf("Unable to retrieve indexer port")
		}
		blockNumber, err := queryBlockNumber(ctx, indexerPort)
		require.NoError(t, err)
		require.Greater(t, blockNumber, 100)
	}

	boostrapPeers := []string{}
	for _, peer := range indexerDefras {
		bootstrapPeer, err := defra.GetBoostrapPeer(peer.DB.PeerInfo())
		require.NoError(t, err)
		boostrapPeers = append(boostrapPeers, bootstrapPeer)
	}
	ctx := context.Background()
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
		netConfig.WithBootstrapPeers(boostrapPeers...),
	}
	hostDefra := defra.StartDefraInstance(t, ctx, options)
	defer hostDefra.Close(ctx)

	err := applySchema(ctx, hostDefra)
	require.NoError(t, err)

	hostPort := defra.GetPort(hostDefra)
	if hostPort == -1 {
		t.Fatalf("Unable to retrieve host port")
	}
	blockNumber, err := queryBlockNumber(ctx, hostPort)
	require.Error(t, err) // Host shouldn't know the latest block number yet as it hasn't synced with the Indexer

	err = hostDefra.DB.AddP2PCollections(ctx, "Block", "Transaction", "AccessListEntry", "Log")
	require.NoError(t, err)

	blockNumber, err = queryBlockNumber(ctx, hostPort)
	for attempts := 1; attempts < 60; attempts++ { // It may take some time to sync now that we are connected
		if err == nil {
			break
		}
		t.Logf("Attempt %d to query block number from host failed. Trying again...", attempts)
		time.Sleep(1 * time.Second)
		blockNumber, err = queryBlockNumber(ctx, hostPort)
	}
	require.NoError(t, err) // We should now have the block number on the Host
	require.Greater(t, blockNumber, 100)

	queryClient, err := defra.NewQueryClient(hostDefra.APIURL)
	require.NoError(t, err)

	// time.Sleep(5 * time.Minute)

	result, err := defra.QueryArray[attestation.Block](queryClient, ctx, getAllBlocksQuery)
	require.NoError(t, err)
	t.Log(result)
}
