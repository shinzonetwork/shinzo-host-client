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
	"github.com/shinzonetwork/shinzo-host-client/pkg/shinzohub"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
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

// TestOutOfOrderBlockProcessing verifies that blocks posted out of order are eventually processed
func TestOutOfOrderBlockProcessing(t *testing.T) {
	// Create a bigPeer to serve as the entrypoint to the network
	bigPeer, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer bigPeer.Close(t.Context())

	peerInfo, err := bigPeer.DB.PeerInfo()
	require.NoError(t, err)

	// Create indexer Defra instance
	indexerConfig := defra.DefaultConfig
	indexerConfig.DefraDB.P2P.BootstrapPeers = append(indexerConfig.DefraDB.P2P.BootstrapPeers, peerInfo...)
	indexerDefra, err := defra.StartDefraInstanceWithTestConfig(t, indexerConfig, defra.NewSchemaApplierFromProvidedSchema(indexerschema.GetSchema()), "Block", "Transaction", "AccessListEntry", "Log")
	require.NoError(t, err)
	defer indexerDefra.Close(t.Context())

	// Create host with a simple view (no lens)
	testHostConfig := *host.DefaultConfig
	testHostConfig.ShinzoAppConfig.DefraDB.P2P.BootstrapPeers = append(testHostConfig.ShinzoAppConfig.DefraDB.P2P.BootstrapPeers, peerInfo...)
	testHostConfig.ShinzoAppConfig.DefraDB.Store.Path = t.TempDir()
	testHostConfig.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"
	testHostConfig.HostConfig.LensRegistryPath = t.TempDir()

	mockEventSub := shinzohub.NewMockEventSubscription()
	testHostConfig.Shinzo.WebSocketUrl = "ws://dummy-url" // Needed to start event handler
	testHost, err := host.StartHostingWithEventSubscription(&testHostConfig, mockEventSub)
	require.NoError(t, err)
	defer testHost.Close(t.Context())

	// Wait a bit for event handler to start
	time.Sleep(500 * time.Millisecond)

	// Create a simple view that queries logs
	query := "Log { transactionHash blockNumber }"
	sdl := "type SimpleLogView { transactionHash: String blockNumber: Int }"
	simpleView := view.View{
		Query: &query,
		Sdl:   &sdl,
		Name:  "SimpleLogView",
	}

	// Register the view
	viewEvent := &shinzohub.ViewRegisteredEvent{View: simpleView}
	mockEventSub.SendEvent(viewEvent)

	// Wait for view to be registered (with retries)
	maxViewWait := 10
	for i := 0; i < maxViewWait; i++ {
		time.Sleep(200 * time.Millisecond)
		if len(testHost.HostedViews) > 0 {
			break
		}
		if i == maxViewWait-1 {
			t.Fatalf("View was not registered after %d attempts", maxViewWait)
		}
	}
	require.Len(t, testHost.HostedViews, 1)
	require.Equal(t, "SimpleLogView", testHost.HostedViews[0].Name)
	t.Logf("View registered successfully: %s", testHost.HostedViews[0].Name)

	ctx := t.Context()

	// Step 1: Post blocks out of order
	// Post blocks 100, 102, 103, 105 (missing 101, 104)
	baseBlockNumber := uint64(1000)
	blocksToPost := []uint64{baseBlockNumber, baseBlockNumber + 2, baseBlockNumber + 3, baseBlockNumber + 5}
	missingBlocks := []uint64{baseBlockNumber + 1, baseBlockNumber + 4}

	t.Logf("Posting blocks out of order: %v (missing: %v)", blocksToPost, missingBlocks)

	for _, blockNum := range blocksToPost {
		postMockBlock(t, indexerDefra, blockNum, 5, 10, rand.New(rand.NewSource(time.Now().UnixNano())))
		time.Sleep(100 * time.Millisecond) // Small delay between posts
	}

	// Wait for blocks to propagate to host's Defra instance
	time.Sleep(2 * time.Second)

	type blockResult struct {
		TransactionHash string `json:"transactionHash"`
		BlockNumber     uint64 `json:"blockNumber"`
	}

	var results []blockResult
	// Wait for host to process available blocks
	maxWait := 30
	for i := 0; i < maxWait; i++ {
		time.Sleep(500 * time.Millisecond)
		// Check if view has processed some data
		viewQuery := fmt.Sprintf("%s { transactionHash blockNumber }", simpleView.Name)
		results, err = defra.QueryArray[blockResult](ctx, testHost.DefraNode, viewQuery)
		require.NoError(t, err)
		if len(results) > 0 {
			t.Logf("View has processed %d results after %d attempts", len(results), i+1)
			break
		}
		if i == maxWait-1 {
			t.Fatalf("View did not process any data after %d attempts", maxWait)
		}
	}
	require.NoError(t, err)
	require.Greater(t, len(results), 0)

	// Step 2: Verify view has data but chunks show missing blocks
	// Wait a bit more for processing to complete
	time.Sleep(10 * time.Second)

	chunks := testHost.ViewProcessedChunks[simpleView.Name]
	require.Greater(t, len(chunks), 0, "Should have at least one chunk")

	lastChunk := chunks[len(chunks)-1]
	t.Logf("Last chunk: %d-%d, missing blocks count: %d, missing: %v", lastChunk.StartBlock, lastChunk.EndBlock, len(lastChunk.MissingBlocks), lastChunk.MissingBlocks)

	// Verify missing blocks are tracked
	require.Greater(t, len(lastChunk.MissingBlocks), 0, "Should have missing blocks tracked")

	// Check that the missing blocks we expect are in the chunk
	missingSet := make(map[uint64]bool)
	for _, mb := range lastChunk.MissingBlocks {
		missingSet[mb] = true
	}
	for _, expectedMissing := range missingBlocks {
		require.True(t, missingSet[expectedMissing], "Expected missing block %d should be in chunk", expectedMissing)
	}

	t.Logf("Verified missing blocks are tracked correctly: %v", missingBlocks)

	// Verify view has some data but not all blocks
	viewQuery := fmt.Sprintf("%s { transactionHash blockNumber }", simpleView.Name)
	results, err = defra.QueryArray[blockResult](ctx, testHost.DefraNode, viewQuery)
	require.NoError(t, err)
	require.Greater(t, len(results), 0, "View should have processed some data")

	// Extract block numbers from results to verify we're missing some
	resultBlockNumbers := make(map[uint64]bool)
	for _, result := range results {
		resultBlockNumbers[result.BlockNumber] = true
	}

	// Step 3: Post the missing blocks
	t.Logf("Posting missing blocks: %v", missingBlocks)
	for _, blockNum := range missingBlocks {
		postMockBlock(t, indexerDefra, blockNum, 5, 10, rand.New(rand.NewSource(time.Now().UnixNano())))
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for blocks to propagate and verify they're in host's DefraDB
	t.Logf("Waiting for blocks to propagate to host's DefraDB...")
	allBlocksPresent := false
	maxPropagationWait := 20
	for i := 0; i < maxPropagationWait; i++ {
		time.Sleep(500 * time.Millisecond)
		// Check if blocks are in host's DefraDB
		allBlocksPresent = true
		for _, blockNum := range missingBlocks {
			blockQuery := fmt.Sprintf("Block(filter: { number: { _eq: %d } }) { number }", blockNum)
			blocks, err := defra.QueryArray[map[string]any](ctx, testHost.DefraNode, blockQuery)
			if err != nil || len(blocks) == 0 {
				allBlocksPresent = false
				break
			}
		}
		if allBlocksPresent {
			t.Logf("All missing blocks are now in host's DefraDB after %d attempts", i+1)
			break
		}
		if i == maxPropagationWait-1 {
			t.Logf("Warning: Some blocks may not have propagated to host's DefraDB")
		}
	}
	require.True(t, allBlocksPresent)

	// Also verify logs are present for these blocks
	var logs []map[string]any
	for _, blockNum := range missingBlocks {
		logQuery := fmt.Sprintf("Log(filter: { blockNumber: { _eq: %d } }) { blockNumber }", blockNum)
		logs, err = defra.QueryArray[map[string]any](ctx, testHost.DefraNode, logQuery)
		if err == nil && len(logs) > 0 {
			t.Logf("Block %d: Found %d logs in host's DefraDB", blockNum, len(logs))
		} else {
			t.Logf("Block %d: No logs found in host's DefraDB yet", blockNum)
		}
	}
	require.NoError(t, err)
	require.Greater(t, len(logs), 0)

	// Step 4: Wait for retry goroutine to process missing blocks
	// The retry goroutine runs every 2 seconds, so we need to wait a bit
	maxRetries := 30
	retryDelay := 1 * time.Second
	var finalChunks []*host.ProcessingChunk

	// First, manually test if we can query for the missing blocks using the _in filter
	t.Logf("Testing manual query for missing blocks...")
	testQuery := fmt.Sprintf("Log(filter: { blockNumber: { _in: [%d, %d] } }) { blockNumber transactionHash }", missingBlocks[0], missingBlocks[1])
	testResults, err := defra.QueryArray[map[string]any](ctx, testHost.DefraNode, testQuery)
	if err != nil {
		t.Logf("Error testing manual query: %v", err)
	} else {
		t.Logf("Manual query found %d results for missing blocks", len(testResults))
		if len(testResults) > 0 {
			for _, result := range testResults {
				if bn, ok := result["blockNumber"].(float64); ok {
					t.Logf("  Found block %d in query results", uint64(bn))
				}
			}
		}
	}

	for i := 0; i < maxRetries; i++ {
		time.Sleep(retryDelay)
		finalChunks = testHost.ViewProcessedChunks[simpleView.Name]
		if len(finalChunks) > 0 {
			lastChunk = finalChunks[len(finalChunks)-1]
			remainingMissing := len(lastChunk.MissingBlocks)
			if remainingMissing == 0 {
				t.Logf("All missing blocks processed after %d retries", i+1)
				break
			}
			// Log which blocks are still missing for debugging
			if remainingMissing <= 10 {
				t.Logf("Retry %d/%d: Still missing %d blocks: %v", i+1, maxRetries, remainingMissing, lastChunk.MissingBlocks)
			} else {
				t.Logf("Retry %d/%d: Still missing %d blocks (first 10: %v)", i+1, maxRetries, remainingMissing, lastChunk.MissingBlocks[:10])
			}

			// Every 5 retries, check if blocks are actually queryable
			if i%5 == 0 && remainingMissing > 0 {
				testQuery := fmt.Sprintf("Log(filter: { blockNumber: { _in: [%d] } }) { blockNumber }", lastChunk.MissingBlocks[0])
				testResults, err := defra.QueryArray[map[string]any](ctx, testHost.DefraNode, testQuery)
				if err == nil {
					t.Logf("  Direct query for missing block %d returned %d results", lastChunk.MissingBlocks[0], len(testResults))
				}
			}
		}
		if i == maxRetries-1 {
			// Log final state for debugging
			if len(finalChunks) > 0 {
				lastChunk = finalChunks[len(finalChunks)-1]
				t.Logf("Final chunk state: %d-%d, missing: %d blocks: %v", lastChunk.StartBlock, lastChunk.EndBlock, len(lastChunk.MissingBlocks), lastChunk.MissingBlocks)
			}
			t.Fatalf("Missing blocks were not processed after %d retries", maxRetries)
		}
	}

	// Step 5: Verify all blocks are now processed
	finalChunks = testHost.ViewProcessedChunks[simpleView.Name]
	require.Greater(t, len(finalChunks), 0, "Should have chunks")
	lastChunk = finalChunks[len(finalChunks)-1]
	require.Equal(t, 0, len(lastChunk.MissingBlocks), "All blocks should be processed, no missing blocks remaining")

	// Verify view now has data from all blocks (including previously missing ones)
	finalResults, err := defra.QueryArray[blockResult](ctx, testHost.DefraNode, viewQuery)
	require.NoError(t, err)
	require.Greater(t, len(finalResults), len(results), "Should have more results after processing missing blocks")

	// Verify we now have data from all expected blocks
	finalResultBlockNumbers := make(map[uint64]bool)
	for _, result := range finalResults {
		finalResultBlockNumbers[result.BlockNumber] = true
	}

	allBlocks := append(blocksToPost, missingBlocks...)
	for _, blockNum := range allBlocks {
		// Note: We check if the block number is in results, but since we're querying logs,
		// we might have multiple logs per block, so we just verify the block number appears
		hasBlock := false
		for bn := range finalResultBlockNumbers {
			if bn == blockNum {
				hasBlock = true
				break
			}
		}
		require.True(t, hasBlock, "Should have processed data from block %d", blockNum)
	}

	t.Logf("Successfully verified all blocks (including previously missing ones) were processed")
}
