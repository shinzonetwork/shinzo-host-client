package host

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	appDefra "github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/host/pkg/attestation"
	"github.com/shinzonetwork/indexer/pkg/indexer"
	"github.com/shinzonetwork/indexer/pkg/logger"
	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/crypto"
	defraHTTP "github.com/sourcenetwork/defradb/http"
	netConfig "github.com/sourcenetwork/defradb/net/config"
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
	defer indexerDefra.Close(ctx)
	testConfig := indexer.DefaultConfig
	testConfig.DefraDB.Url = indexerDefra.APIURL

	indexerSchemaApplier := appDefra.SchemaApplierFromFile{DefaultPath: "schema/schema.graphql"}
	err := indexerSchemaApplier.ApplySchema(ctx, indexerDefra)
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

	testHost, err := StartHostingWithTestConfig(t)
	require.NoError(t, err)
	defer testHost.Close(ctx)
	hostDefra := testHost.DefraNode

	err = hostDefra.DB.Connect(ctx, indexerDefra.DB.PeerInfo())
	require.NoError(t, err)

	// Schema is applied automatically by the app-sdk

	blockNumber, err := queryBlockNumber(ctx, indexerDefra)
	require.NoError(t, err)
	require.Greater(t, blockNumber, uint64(100))

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
	require.Greater(t, blockNumber, uint64(100))
}

// Todo need to fix indexer, currently can only have one writing at a time on a system
func TestHostReplicateFromMultipleIndexers(t *testing.T) {
	indexerDefras := []*node.Node{}
	indexers := []*indexer.ChainIndexer{}
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	for i := 0; i < 10; i++ {
		ctx := context.Background()
		nodeIdentity, err := identity.Generate(crypto.KeyTypeSecp256k1)
		require.NoError(t, err)
		options := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			defraHTTP.WithAddress(defraUrl),
			netConfig.WithListenAddresses(listenAddress),
			node.WithNodeIdentity(identity.Identity(nodeIdentity)),
		}
		indexerDefra := startDefraInstanceForTest(t, ctx, options)
		defer indexerDefra.Close(ctx)
		testConfig := indexer.DefaultConfig
		testConfig.DefraDB.Url = indexerDefra.APIURL

		indexerSchemaApplier := appDefra.SchemaApplierFromFile{DefaultPath: "schema/schema.graphql"}
		err = indexerSchemaApplier.ApplySchema(ctx, indexerDefra)
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

		blockNumber, err := queryBlockNumber(ctx, indexerDefras[index])
		require.NoError(t, err)
		require.Greater(t, blockNumber, uint64(100))
	}

	ctx := context.Background()

	testHost, err := StartHostingWithTestConfig(t)
	require.NoError(t, err)
	defer testHost.Close(ctx)
	hostDefra := testHost.DefraNode

	for _, peer := range indexerDefras {
		err := hostDefra.DB.Connect(ctx, peer.DB.PeerInfo())
		require.NoError(t, err)
	}

	// Schema is applied automatically by the app-sdk

	blockNumber, err := queryBlockNumber(ctx, hostDefra)
	for attempts := 1; attempts < 60; attempts++ { // It may take some time to sync now that we are connected
		if err == nil {
			break
		}
		t.Logf("Attempt %d to query block number from host failed. Trying again...", attempts)
		time.Sleep(1 * time.Second)
		blockNumber, err = queryBlockNumber(ctx, hostDefra)
	}
	require.NoError(t, err) // We should now have the block number on the Host
	require.Greater(t, blockNumber, uint64(100))

	result, err := appDefra.QueryArray[attestation.Block](ctx, hostDefra, getTenBlocksQuery)
	require.NoError(t, err)
	t.Log(result)
}
