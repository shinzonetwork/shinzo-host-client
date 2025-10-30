package defra

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/shinzonetwork/indexer/pkg/indexer"
	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func TestGetLatestCommit(t *testing.T) {
	ctx := context.Background()
	testDefra, testIndexer := startIndexer(t)
	defer testDefra.Close(ctx)
	defer testIndexer.StopIndexing()

	blockId, err := getBlockDocId(ctx, testDefra)
	require.NoError(t, err)
	require.Greater(t, len(blockId), 0)

	commit, err := GetLatestCommit(ctx, testDefra, blockId)
	require.NoError(t, err)
	require.NotNil(t, commit)
}

func startIndexer(t *testing.T) (*node.Node, *indexer.ChainIndexer) {
	logger.Init(true)
	ctx := context.Background()

	schema := &defra.SchemaApplierFromFile{DefaultPath: "schema/schema.graphql"}
	indexerDefra, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, schema)
	require.NoError(t, err)
	testConfig := indexer.DefaultConfig
	testConfig.DefraDB.Url = indexerDefra.APIURL

	err = indexerDefra.DB.AddP2PCollections(ctx, "Block")
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

func getBlockDocId(ctx context.Context, defraNode *node.Node) (string, error) {
	query := `query getLatestBlockDocId { Block(order: {number: DESC}, limit:1) { _docID number } }`
	block, err := defra.QuerySingle[attestation.Block](ctx, defraNode, query)
	if err != nil {
		return "", fmt.Errorf("Error fetching block: %v", err)
	}
	if len(block.DocId) == 0 {
		return "", fmt.Errorf("Unable to retrieve docID from block %+v", block)
	}
	if block.Number < 100 {
		return "", fmt.Errorf("Received invalid block %+v - block number must be greater than 100", block)
	}

	return block.DocId, nil
}
