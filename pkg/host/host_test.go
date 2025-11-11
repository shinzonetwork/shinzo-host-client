package host

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/indexer/pkg/logger"
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

// func TestHostCanReplicateFromIndexerViaRegularConnection(t *testing.T) {
// 	logger.Init(true)
// 	ctx := t.Context()

// 	testConfig := DefaultConfig
// 	testConfig.ShinzoAppConfig.DefraDB.Store.Path = t.TempDir()
// 	testConfig.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"
// 	testConfig.ShinzoAppConfig.DefraDB.P2P.BootstrapPeers = append(testConfig.ShinzoAppConfig.DefraDB.P2P.BootstrapPeers,
// 		"/ip4/34.45.96.244/tcp/9171/p2p/12D3KooWPK7AkcUnjfqwkfkmV634TGpPg8paXoMPyFJQiGiukmSw")

// 	testHost, err := StartHostingWithTestConfig(t)
// 	require.NoError(t, err)
// 	defer testHost.Close(ctx)
// 	hostDefra := testHost.DefraNode

// 	blockNumber, err := queryBlockNumber(ctx, hostDefra)
// 	for attempts := 1; attempts < 60; attempts++ { // It may take some time to sync now that we are connected
// 		if err == nil {
// 			break
// 		}
// 		t.Logf("Attempt %d to query block number from host failed. Trying again...", attempts)
// 		time.Sleep(1 * time.Second)
// 		blockNumber, err = queryBlockNumber(ctx, hostDefra)
// 	}
// 	require.NoError(t, err) // We should now have the block number on the Host
// 	require.Greater(t, blockNumber, uint64(100))
// }
