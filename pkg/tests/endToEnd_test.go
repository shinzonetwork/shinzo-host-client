package tests

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/attestation"
	"github.com/shinzonetwork/app-sdk/pkg/defra"
	indexerschema "github.com/shinzonetwork/indexer/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/pkg/host"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func TestReadViewsInAppAfterProcessingIndexerPrimitivesWithHost(t *testing.T) {
	// Create a bigPeer to serve as the entrypoint to the network
	bigPeer, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)

	peerInfo, err := bigPeer.DB.PeerInfo()
	require.NoError(t, err)
	indexerDefra, stopMockIndexer := startIndexer(t, peerInfo)
	defer stopMockIndexer() // stopMockIndexer now handles closing indexerDefra

	logs, err := defra.QueryArray[viewResult](t.Context(), indexerDefra, "Log { transactionHash }")
	require.NoError(t, err)
	require.Greater(t, len(logs), 0)
	testHost := host.CreateHostWithTwoViews(t, peerInfo...)
	defer testHost.Close(t.Context())
	require.Len(t, testHost.HostedViews, 2)

	appConfig := defra.DefaultConfig
	appConfig.DefraDB.P2P.BootstrapPeers = append(appConfig.DefraDB.P2P.BootstrapPeers, peerInfo...)
	appDefra, err := defra.StartDefraInstanceWithTestConfig(t, appConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer appDefra.Close(t.Context())

	for _, view := range testHost.HostedViews {
		err = view.SubscribeTo(t.Context(), appDefra)
		require.NoError(t, err)
		err = attestation.AddAttestationRecordCollection(t.Context(), appDefra, view.Name)
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

	// Retry checking for new blocks multiple times before final assertion
	maxRetries := 300
	retryDelay := 1 * time.Second
	var newUnfilteredResults []viewResult
	for i := 0; i < maxRetries; i++ {
		time.Sleep(retryDelay)
		newUnfilteredResults, err = defra.QueryArray[viewResult](t.Context(), appDefra, unfilteredQuery)
		require.NoError(t, err)
		if len(newUnfilteredResults) > len(unfilteredResults) {
			break
		}
		if i < maxRetries-1 {
			t.Logf("Retry %d/%d: No new blocks yet, waiting...", i+1, maxRetries)
		}
	}
	require.Greater(t, len(newUnfilteredResults), len(unfilteredResults), "Expected new blocks after %d retries", maxRetries)

	// Now let's also check that we have attestations for a view
	attestationRecordsQuery := fmt.Sprintf(`query {
	AttestationRecord_%s {
		attested_doc
		source_doc
		CIDs
	}
}`, testHost.HostedViews[0].Name)
	attestationRecords, err := defra.QueryArray[attestation.AttestationRecord](t.Context(), appDefra, attestationRecordsQuery)
	require.NoError(t, err)
	require.NotNil(t, attestationRecords)
	require.Greater(t, len(attestationRecords), 0)
	for _, record := range attestationRecords {
		require.NotNil(t, record.CIDs)
		require.Greater(t, len(record.CIDs), 0)
		require.Greater(t, len(record.AttestedDocId), 0)
		require.Greater(t, len(record.SourceDocId), 0)
	}
}

type viewResult struct {
	TransactionHash string `json:"transactionHash"`
}

func startIndexer(t *testing.T, bigPeer []string) (*node.Node, func()) {
	defraConfig := defra.DefaultConfig
	defraConfig.DefraDB.P2P.BootstrapPeers = append(defraConfig.DefraDB.P2P.BootstrapPeers, bigPeer...)
	indexerDefra, err := defra.StartDefraInstanceWithTestConfig(t, defraConfig, defra.NewSchemaApplierFromProvidedSchema(indexerschema.GetSchema()), "Block", "Transaction", "AccessListEntry", "Log")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	// Start mock indexer in background
	go func() {
		defer close(done)
		mockIndexer(ctx, t, indexerDefra)
	}()

	stopFunc := func() {
		// Cancel context to stop mock indexer
		cancel()
		// Wait for mock indexer goroutine to finish
		select {
		case <-done:
			// Mock indexer finished
		case <-time.After(5 * time.Second):
			// Timeout waiting for mock indexer to stop
			t.Logf("Warning: Mock indexer did not stop within 5 seconds")
		}
		// Close the defra node after mock indexer has stopped
		indexerDefra.Close(context.Background())
	}

	// Wait a bit to ensure some blocks are posted
	time.Sleep(500 * time.Millisecond)

	return indexerDefra, stopFunc
}

// mockIndexer posts mock blocks, transactions, and logs to DefraDB
func mockIndexer(ctx context.Context, t *testing.T, defraNode *node.Node) {
	baseBlockNumber := uint64(3550)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Post a few blocks around the current base block number
			numBlocks := rng.Intn(3) + 1 // 1-3 blocks
			for i := 0; i < numBlocks; i++ {
				// Random offset from base (-5 to +5)
				offset := rng.Intn(11) - 5
				blockNumber := baseBlockNumber + uint64(offset)

				// Random number of transactions (1-50)
				numTransactions := rng.Intn(50) + 1
				// Random number of logs (1-50)
				numLogs := rng.Intn(50) + 1

				postMockBlock(t, defraNode, blockNumber, numTransactions, numLogs, rng)
			}

			t.Logf("Posted blocks around %d", baseBlockNumber)

			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}

			// Increment base block number
			baseBlockNumber += uint64(100)
		}
	}
}

func postMockBlock(t *testing.T, defraNode *node.Node, blockNumber uint64, numTransactions int, numLogs int, rng *rand.Rand) {
	ctx := context.Background()

	// Create block with unique hash (include random component to ensure uniqueness)
	// Even if blockNumber is the same, the hash will be different
	blockHash := fmt.Sprintf("0x%016x%016x", blockNumber, rng.Uint64())
	blockData := map[string]any{
		"hash":             blockHash,
		"number":           int64(blockNumber),
		"timestamp":        time.Now().Format(time.RFC3339),
		"parentHash":       fmt.Sprintf("0x%016x", blockNumber-1),
		"difficulty":       "0x1",
		"totalDifficulty":  fmt.Sprintf("0x%x", blockNumber),
		"gasUsed":          "0x5208",
		"gasLimit":         "0x1c9c380",
		"baseFeePerGas":    "0x3b9aca00",
		"nonce":            fmt.Sprintf("%d", blockNumber),
		"miner":            "0x0000000000000000000000000000000000000000",
		"size":             "0x208",
		"stateRoot":        fmt.Sprintf("0x%016x", blockNumber+1000),
		"sha3Uncles":       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		"transactionsRoot": fmt.Sprintf("0x%016x", blockNumber+2000),
		"receiptsRoot":     fmt.Sprintf("0x%016x", blockNumber+3000),
		"logsBloom":        "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
		"extraData":        "0x",
		"mixHash":          fmt.Sprintf("0x%016x", blockNumber+4000),
		"uncles":           []string{},
	}

	blockCollection, err := defraNode.DB.GetCollectionByName(ctx, "Block")
	require.NoError(t, err)

	blockDoc, err := client.NewDocFromMap(blockData, blockCollection.Version())
	require.NoError(t, err)

	err = blockCollection.Save(ctx, blockDoc)
	require.NoError(t, err)

	// Create transactions
	for txIdx := 0; txIdx < numTransactions; txIdx++ {
		txHash := fmt.Sprintf("0x%032x", rng.Uint64())
		txData := map[string]any{
			"hash":                 txHash,
			"blockHash":            blockHash,
			"blockNumber":          int64(blockNumber),
			"from":                 fmt.Sprintf("0x%040x", rng.Uint64()),
			"to":                   fmt.Sprintf("0x%040x", rng.Uint64()),
			"value":                fmt.Sprintf("%d", rng.Int63n(1000000000000000000)),
			"gas":                  "21000",
			"gasPrice":             "20000000000",
			"maxFeePerGas":         "30000000000",
			"maxPriorityFeePerGas": "2000000000",
			"input":                "0x",
			"nonce":                fmt.Sprintf("%d", txIdx),
			"transactionIndex":     txIdx,
			"type":                 "0x2",
			"chainId":              "0x1",
			"v":                    "0x0",
			"r":                    fmt.Sprintf("0x%064x", rng.Uint64()),
			"s":                    fmt.Sprintf("0x%064x", rng.Uint64()),
			"status":               true,
			"cumulativeGasUsed":    "0x5208",
			"effectiveGasPrice":    "0x3b9aca00",
		}

		txCollection, err := defraNode.DB.GetCollectionByName(ctx, "Transaction")
		require.NoError(t, err)

		txDoc, err := client.NewDocFromMap(txData, txCollection.Version())
		require.NoError(t, err)

		err = txCollection.Save(ctx, txDoc)
		require.NoError(t, err)

		// Create logs for this transaction
		logsForThisTx := numLogs / numTransactions
		if txIdx < numLogs%numTransactions {
			logsForThisTx++
		}

		for logIdx := 0; logIdx < logsForThisTx; logIdx++ {
			logData := map[string]any{
				"address":          fmt.Sprintf("0x%040x", rng.Uint64()),
				"topics":           []string{fmt.Sprintf("0x%064x", rng.Uint64())},
				"data":             fmt.Sprintf("0x%064x", rng.Uint64()),
				"transactionHash":  txHash,
				"blockHash":        blockHash,
				"blockNumber":      int64(blockNumber),
				"transactionIndex": txIdx,
				"logIndex":         logIdx,
				"removed":          "false",
			}

			logCollection, err := defraNode.DB.GetCollectionByName(ctx, "Log")
			require.NoError(t, err)

			logDoc, err := client.NewDocFromMap(logData, logCollection.Version())
			require.NoError(t, err)

			err = logCollection.Save(ctx, logDoc)
			require.NoError(t, err)
		}
	}

	t.Logf("block %d - %d transactions, %d logs", blockNumber, numTransactions, numLogs)
}
