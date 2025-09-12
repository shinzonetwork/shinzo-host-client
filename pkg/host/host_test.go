package host

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/shinzonetwork/host/pkg/defra"
	indexerDefra "github.com/shinzonetwork/indexer/pkg/defra"
	"github.com/shinzonetwork/indexer/pkg/indexer"
	"github.com/shinzonetwork/indexer/pkg/logger"
	"github.com/shinzonetwork/indexer/pkg/types"
	"github.com/sourcenetwork/defradb/http"
	netConfig "github.com/sourcenetwork/defradb/net/config"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	logger.Init(true)
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

	go func() {
		err := indexer.StartIndexing(true, testConfig)
		if err != nil {
			panic(fmt.Sprintf("Encountered unexpected error starting defra dependency: %v", err))
		}
	}()
	defer indexer.StopIndexing()

	for !indexer.IsStarted || !indexer.HasIndexedAtLeastOneBlock {
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

	err = indexerDefra.Peer.SetReplicator(ctx, hostDefra.Peer.PeerInfo())
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
	handler, err := indexerDefra.NewBlockHandler(fmt.Sprintf("http://localhost:%d", port))
	if err != nil {
		return 0, fmt.Errorf("Error building block handler: %v", err)
	}
	query := `query GetHighestBlockNumber {
  Block(order: {number: DESC}, limit: 1) {
    number
  }
}`
	request := types.Request{Query: query, Type: "POST"}
	result, err := handler.SendToGraphql(ctx, request)
	if err != nil {
		return 0, fmt.Errorf("Error sending graphql query %s : %v", query, err)
	}

	var rawResponse map[string]interface{}
	if err := json.Unmarshal([]byte(result), &rawResponse); err != nil {
		return 0, fmt.Errorf("Error unmarshalling reponse: %v", err)
	}
	data, ok := rawResponse["data"].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("Data field not found in response: %s", result)
	}
	blockBlob, ok := data["Block"].([]interface{})
	if !ok {
		return 0, fmt.Errorf("Block field not found in response: %s", result)
	}
	if len(blockBlob) == 0 {
		return 0, fmt.Errorf("No blocks found in response: %s", result)
	}
	blockDataBlob, ok := blockBlob[0].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("Block field not found in response: %s", result)
	}
	blockNumberObject, ok := blockDataBlob["number"]
	if !ok {
		return 0, fmt.Errorf("Block number field not found in response: %s", result)
	}
	blockNumber, ok := blockNumberObject.(float64)
	if !ok {
		return 0, fmt.Errorf("Block number field not a number in response: %s", string(result))
	}
	return int(blockNumber), nil
}
