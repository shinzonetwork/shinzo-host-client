package defra

import (
	"context"
	"fmt"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/shinzonetwork/host/pkg/attestation"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func TestGetLatestCommit(t *testing.T) {
	ctx := context.Background()
	testDefra := startDefra(t)
	defer testDefra.Close(ctx)

	blockId, err := getBlockDocId(ctx, testDefra)
	require.NoError(t, err)
	require.Greater(t, len(blockId), 0)

	commit, err := GetLatestCommit(ctx, testDefra, blockId)
	require.NoError(t, err)
	require.NotNil(t, commit)
}

func startDefra(t *testing.T) *node.Node {
	logger.Init(true)

	schema := &defra.SchemaApplierFromFile{DefaultPath: "schema/schema.graphql"}
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Url = "http://localhost:0"
	indexerDefra, err := defra.StartDefraInstance(defra.DefaultConfig, schema, "Block")
	require.NoError(t, err)

	return indexerDefra
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
