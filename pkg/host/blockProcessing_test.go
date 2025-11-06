package host

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/shinzohub"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/shinzonetwork/view-creator/core/models"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func TestBlockProcessingWithLens(t *testing.T) {
	// Create a host with lens event
	testHost := CreateHostWithLensEventReceived(t)
	defer testHost.Close(t.Context())

	// Wait a bit for the host to initialize
	time.Sleep(100 * time.Millisecond)

	// Get the target address from the lens event
	targetAddress := "0x1e3aA9fE4Ef01D3cB3189c129a49E3C03126C636"
	otherAddress := "0xabcaA9fE4Ef3333cB3189c129a49E3C03126Cabc"

	// Track processing state between rounds
	var previousProcessedCount int
	var previousProcessedBlocks map[string]uint64

	// Run multiple rounds of block processing simulation
	for round := 0; round < 3; round++ {
		t.Logf("Starting round %d", round+1)

		// Simulate a random number of blocks (1-10 blocks)
		numBlocks := rand.Intn(10) + 1
		t.Logf("Round %d: Processing %d blocks", round+1, numBlocks)

		// Process each block
		for blockNum := 0; blockNum < numBlocks; blockNum++ {
			blockNumber := uint64(1000 + round*100 + blockNum)

			// Simulate a random number of logs per block (1-50 logs)
			numLogs := rand.Intn(50) + 1
			t.Logf("  Block %d: Adding %d logs", blockNumber, numLogs)

			// Add logs to this block
			for logNum := 0; logNum < numLogs; logNum++ {
				// Randomly choose target address or other address
				var address string
				if rand.Float32() < 0.3 { // 30% chance of target address
					address = targetAddress
				} else {
					address = otherAddress
				}

				postDummyLogWithBlockNumber(t, testHost.DefraNode, address, blockNumber)
			}

			// Wait a bit for processing
			time.Sleep(50 * time.Millisecond)
		}

		// Wait for processing to complete
		time.Sleep(3 * time.Second)

		// Debug: Check what block numbers we have in the database
		ctx := t.Context()
		latestBlock, err := defra.QuerySingle[attestation.Block](ctx, testHost.DefraNode, "Block(order: {number: DESC}, limit: 1) { number }")
		if err != nil {
			t.Logf("Error querying latest block: %v", err)
		} else {
			t.Logf("Latest block number in database: %d", latestBlock.Number)
		}

		// Debug: Check what views we have
		t.Logf("Number of hosted views: %d", len(testHost.HostedViews))

		// Debug: Check processed blocks
		for viewName, stack := range testHost.viewProcessedBlocks {
			blockNumber, err := stack.Peek()
			if err != nil {
				t.Logf("View %s: No processed blocks", viewName)
			} else {
				t.Logf("View %s: Last processed block %d", viewName, blockNumber)
			}
		}

		// Validate the results with improved checks
		currentProcessedCount, currentProcessedBlocks := validateProcessedDataWithProgress(t, testHost, targetAddress, round+1, previousProcessedCount, previousProcessedBlocks)
		previousProcessedCount = currentProcessedCount
		previousProcessedBlocks = currentProcessedBlocks
	}
}

func TestBlockProcessingWithoutLens(t *testing.T) {
	// Create a host without lens event
	mockEventSub := shinzohub.NewMockEventSubscription()
	testHostConfig := &config.Config{
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
			WebSocketUrl:        "ws://dummy-url",
		},
		ShinzoAppConfig: DefaultConfig.ShinzoAppConfig,
		HostConfig: config.HostConfig{
			LensRegistryPath: t.TempDir(),
		},
	}
	testHostConfig.ShinzoAppConfig.DefraDB.Store.Path = t.TempDir()
	testHostConfig.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"

	testHost, err := StartHostingWithEventSubscription(testHostConfig, mockEventSub)
	require.NoError(t, err)
	defer testHost.Close(t.Context())

	// Create a view without lens
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type FilteredAndDecodedLogs {transactionHash: String address: String}"
	appView := view.View{
		Query:     &query,
		Sdl:       &sdl,
		Name:      "FilteredAndDecodedLogs",
		Transform: models.Transform{},
	}

	// Send the view registration event
	viewEvent := &shinzohub.ViewRegisteredEvent{View: appView}
	mockEventSub.SendEvent(viewEvent)

	// Wait for view to be registered
	time.Sleep(100 * time.Millisecond)
	require.Len(t, testHost.HostedViews, 1)

	// Add some test data
	targetAddress := "0x1e3aA9fE4Ef01D3cB3189c129a49E3C03126C636"
	otherAddress := "0xabcaA9fE4Ef3333cB3189c129a49E3C03126Cabc"

	var previousProcessedCount int
	var previousProcessedBlocks map[string]uint64

	// Run multiple rounds of block processing simulation
	for round := 0; round < 3; round++ {
		t.Logf("Starting round %d", round+1)

		// Simulate a random number of blocks (1-5 blocks)
		numBlocks := rand.Intn(5) + 1
		t.Logf("Round %d: Processing %d blocks", round+1, numBlocks)

		// Process each block
		for blockNum := 0; blockNum < numBlocks; blockNum++ {
			blockNumber := uint64(2000 + round*100 + blockNum)

			// Simulate a random number of logs per block (1-20 logs)
			numLogs := rand.Intn(20) + 1
			t.Logf("  Block %d: Adding %d logs", blockNumber, numLogs)

			// Add logs to this block
			for logNum := 0; logNum < numLogs; logNum++ {
				// Randomly choose target address or other address
				var address string
				if rand.Float32() < 0.5 { // 50% chance of target address
					address = targetAddress
				} else {
					address = otherAddress
				}

				postDummyLogWithBlockNumber(t, testHost.DefraNode, address, blockNumber)
			}

			// Wait a bit for processing
			time.Sleep(50 * time.Millisecond)
		}

		// Wait for processing to complete
		time.Sleep(500 * time.Millisecond)

		// Validate the results with improved checks
		currentProcessedCount, currentProcessedBlocks := validateProcessedDataWithProgress(t, testHost, targetAddress, round+1, previousProcessedCount, previousProcessedBlocks)
		previousProcessedCount = currentProcessedCount
		previousProcessedBlocks = currentProcessedBlocks
	}
}

func TestBlockProcessingMultipleViews(t *testing.T) {
	// Create a host
	mockEventSub := shinzohub.NewMockEventSubscription()
	testHostConfig := &config.Config{
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
			WebSocketUrl:        "ws://dummy-url",
		},
		ShinzoAppConfig: DefaultConfig.ShinzoAppConfig,
		HostConfig: config.HostConfig{
			LensRegistryPath: t.TempDir(),
		},
	}
	testHostConfig.ShinzoAppConfig.DefraDB.Store.Path = t.TempDir()
	testHostConfig.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"

	testHost, err := StartHostingWithEventSubscription(testHostConfig, mockEventSub)
	require.NoError(t, err)
	defer testHost.Close(t.Context())

	// Create multiple views
	viewList := []view.View{
		{
			Query:     stringPtr("Log {address topics data transactionHash blockNumber}"),
			Sdl:       stringPtr("type View1 {transactionHash: String address: String}"),
			Name:      "View1",
			Transform: models.Transform{},
		},
		{
			Query:     stringPtr("Transaction {hash from to value blockNumber}"),
			Sdl:       stringPtr("type View2 {hash: String from: String to: String}"),
			Name:      "View2",
			Transform: models.Transform{},
		},
	}

	// Register all views
	for _, v := range viewList {
		viewEvent := &shinzohub.ViewRegisteredEvent{View: v}
		mockEventSub.SendEvent(viewEvent)
	}

	// Wait for views to be registered
	time.Sleep(200 * time.Millisecond)
	require.Len(t, testHost.HostedViews, 2)

	// Add test data for both logs and transactions in batches
	var previousView1Count, previousView2Count int
	var previousProcessedBlocks map[string]uint64

	// Run multiple rounds of block processing simulation
	for round := 0; round < 3; round++ {
		t.Logf("Starting round %d", round+1)

		// Simulate a random number of blocks (1-3 blocks)
		numBlocks := rand.Intn(3) + 1
		t.Logf("Round %d: Processing %d blocks", round+1, numBlocks)

		// Process each block
		for blockNum := 0; blockNum < numBlocks; blockNum++ {
			blockNumber := uint64(3000 + round*100 + blockNum)

			// Simulate a random number of logs and transactions per block
			numLogs := rand.Intn(10) + 1
			numTransactions := rand.Intn(5) + 1
			t.Logf("  Block %d: Adding %d logs and %d transactions", blockNumber, numLogs, numTransactions)

			// Add logs
			for i := 0; i < numLogs; i++ {
				postDummyLogWithBlockNumber(t, testHost.DefraNode, "0x123", blockNumber)
			}

			// Add transactions
			for i := 0; i < numTransactions; i++ {
				postDummyTransactionWithBlockNumber(t, testHost.DefraNode, blockNumber)
			}

			// Wait a bit for processing
			time.Sleep(50 * time.Millisecond)
		}

		// Wait for processing to complete
		time.Sleep(500 * time.Millisecond)

		// Validate both views processed data with progress tracking
		currentView1Count, currentView2Count, currentProcessedBlocks := validateMultipleViewsProcessedWithProgress(t, testHost, round+1, previousView1Count, previousView2Count, previousProcessedBlocks)
		previousView1Count = currentView1Count
		previousView2Count = currentView2Count
		previousProcessedBlocks = currentProcessedBlocks
	}
}

func TestBlockProcessingWithGaps(t *testing.T) {
	// Test processing when there are gaps in block numbers
	testHost := CreateHostWithLensEventReceived(t)
	defer testHost.Close(t.Context())

	time.Sleep(100 * time.Millisecond)

	targetAddress := "0x1e3aA9fE4Ef01D3cB3189c129a49E3C03126C636"

	var previousProcessedCount int
	var previousProcessedBlocks map[string]uint64

	// Run multiple rounds of block processing simulation with gaps
	for round := 0; round < 3; round++ {
		t.Logf("Starting round %d", round+1)

		// Define block numbers with gaps for this round
		baseBlock := uint64(1000 + round*200)
		blockNumbers := []uint64{baseBlock, baseBlock + 5, baseBlock + 15, baseBlock + 30} // Gaps in between
		t.Logf("Round %d: Processing blocks with gaps: %v", round+1, blockNumbers)

		// Process each block
		for i, blockNumber := range blockNumbers {
			numLogs := rand.Intn(10) + 1
			t.Logf("  Adding %d logs to block %d", numLogs, blockNumber)

			for j := 0; j < numLogs; j++ {
				// Mix of target and other addresses
				var address string
				if j%3 == 0 {
					address = targetAddress
				} else {
					address = "0xother"
				}
				postDummyLogWithBlockNumber(t, testHost.DefraNode, address, blockNumber)
			}

			// Wait between blocks
			if i < len(blockNumbers)-1 {
				time.Sleep(100 * time.Millisecond)
			}
		}

		// Wait for processing to complete
		time.Sleep(500 * time.Millisecond)

		// Validate the results with improved checks
		currentProcessedCount, currentProcessedBlocks := validateProcessedDataWithProgress(t, testHost, targetAddress, round+1, previousProcessedCount, previousProcessedBlocks)
		previousProcessedCount = currentProcessedCount
		previousProcessedBlocks = currentProcessedBlocks
	}
}

// Helper functions

func postDummyLogWithBlockNumber(t *testing.T, hostDefra *node.Node, address string, blockNumber uint64) {
	// First, create the block if it doesn't exist
	postDummyBlock(t, hostDefra, blockNumber)

	dummyLog := map[string]any{
		"address":         address,
		"blockNumber":     int64(blockNumber), // Convert uint64 to int64 for DefraDB
		"data":            "0x0000000000000000000000000000000000000000000000000000000000000001",
		"topics":          []string{"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"},
		"transactionHash": fmt.Sprintf("0x%x", rand.Uint64()),
	}

	ctx := t.Context()

	// Write the dummy log to the Log collection
	logCollection, err := hostDefra.DB.GetCollectionByName(ctx, "Log")
	require.NoError(t, err)

	dummyDoc, err := client.NewDocFromMap(dummyLog, logCollection.Version())
	require.NoError(t, err)

	err = logCollection.Save(ctx, dummyDoc)
	require.NoError(t, err)
}

func postDummyBlock(t *testing.T, hostDefra *node.Node, blockNumber uint64) {
	dummyBlock := map[string]any{
		"hash":             fmt.Sprintf("0x%x", blockNumber), // Simple hash based on block number
		"number":           int64(blockNumber),
		"timestamp":        "2024-01-01T00:00:00Z",
		"parentHash":       fmt.Sprintf("0x%x", blockNumber-1),
		"difficulty":       "0x1",
		"totalDifficulty":  fmt.Sprintf("0x%x", blockNumber),
		"gasUsed":          "0x5208",
		"gasLimit":         "0x1c9c380",
		"baseFeePerGas":    "0x3b9aca00",
		"nonce":            int64(blockNumber),
		"miner":            "0x0000000000000000000000000000000000000000",
		"size":             "0x208",
		"stateRoot":        fmt.Sprintf("0x%x", blockNumber+1000),
		"sha3Uncles":       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		"transactionsRoot": fmt.Sprintf("0x%x", blockNumber+2000),
		"receiptsRoot":     fmt.Sprintf("0x%x", blockNumber+3000),
		"logsBloom":        "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
		"extraData":        "0x",
		"mixHash":          fmt.Sprintf("0x%x", blockNumber+4000),
		"uncles":           []string{},
	}

	ctx := t.Context()

	// Write the dummy block to the Block collection
	blockCollection, err := hostDefra.DB.GetCollectionByName(ctx, "Block")
	require.NoError(t, err)

	dummyDoc, err := client.NewDocFromMap(dummyBlock, blockCollection.Version())
	require.NoError(t, err)

	err = blockCollection.Save(ctx, dummyDoc)
	require.NoError(t, err)
}

func postDummyTransactionWithBlockNumber(t *testing.T, hostDefra *node.Node, blockNumber uint64) {
	// First, create the block if it doesn't exist
	postDummyBlock(t, hostDefra, blockNumber)

	dummyTransaction := map[string]any{
		"hash":        fmt.Sprintf("0x%x", rand.Uint64()),
		"from":        "0xfrom123",
		"to":          "0xto456",
		"value":       "1000000000000000000",
		"blockNumber": int64(blockNumber), // Convert uint64 to int64 for DefraDB
		"gas":         "21000",
		"gasPrice":    "20000000000",
	}

	ctx := t.Context()

	// Write the dummy transaction to the Transaction collection
	transactionCollection, err := hostDefra.DB.GetCollectionByName(ctx, "Transaction")
	require.NoError(t, err)

	dummyDoc, err := client.NewDocFromMap(dummyTransaction, transactionCollection.Version())
	require.NoError(t, err)

	err = transactionCollection.Save(ctx, dummyDoc)
	require.NoError(t, err)
}

func validateProcessedData(t *testing.T, host *Host, targetAddress string, round int) {
	ctx := t.Context()

	// Check that the view processed some data
	require.Len(t, host.HostedViews, 1)
	viewName := host.HostedViews[0].Name

	// Query the processed data
	processedData, err := defra.QueryArray[map[string]any](ctx, host.DefraNode, fmt.Sprintf("%s {transactionHash}", viewName))
	require.NoError(t, err)
	require.Greater(t, len(processedData), 0, "Round %d: Should have processed some data", round)

	t.Logf("Round %d: Processed %d items", round, len(processedData))
}

func validateProcessedDataWithProgress(t *testing.T, host *Host, targetAddress string, round int, previousCount int, previousBlocks map[string]uint64) (int, map[string]uint64) {
	ctx := t.Context()

	// Check that the view processed some data
	require.Len(t, host.HostedViews, 1)
	viewName := host.HostedViews[0].Name

	// Query the processed data
	processedData, err := defra.QueryArray[map[string]any](ctx, host.DefraNode, fmt.Sprintf("%s {transactionHash}", viewName))
	require.NoError(t, err)
	require.Greater(t, len(processedData), 0, "Round %d: Should have processed some data", round)

	// Validate that processing count has increased (unless it's the first round)
	if round > 1 && previousCount > 0 {
		require.GreaterOrEqual(t, len(processedData), previousCount, "Round %d: Processed data should not decrease", round)
		t.Logf("Round %d: Processed %d items (previous: %d)", round, len(processedData), previousCount)
	} else {
		t.Logf("Round %d: Processed %d items", round, len(processedData))
	}

	// Validate that viewProcessedBlocks maps are updated correctly
	currentProcessedBlocks := make(map[string]uint64)
	for viewName, stack := range host.viewProcessedBlocks {
		if !stack.IsEmpty() {
			latestBlock, err := stack.Peek()
			if err == nil {
				currentProcessedBlocks[viewName] = latestBlock

				// Validate that block numbers are progressing (unless it's the first round)
				if round > 1 && previousBlocks != nil {
					if prevBlock, exists := previousBlocks[viewName]; exists {
						require.GreaterOrEqual(t, latestBlock, prevBlock, "Round %d: View %s block number should not decrease", round, viewName)
						t.Logf("Round %d: View %s processed block %d (previous: %d)", round, viewName, latestBlock, prevBlock)
					}
				} else {
					t.Logf("Round %d: View %s processed block %d", round, viewName, latestBlock)
				}
			}
		}
	}

	return len(processedData), currentProcessedBlocks
}

func validateAllLogsProcessed(t *testing.T, host *Host) {
	ctx := t.Context()

	require.Len(t, host.HostedViews, 1)
	viewName := host.HostedViews[0].Name

	// Get all logs
	allLogs, err := defra.QueryArray[attestation.Log](ctx, host.DefraNode, "Log {transactionHash}")
	require.NoError(t, err)

	// Get processed data
	processedData, err := defra.QueryArray[map[string]any](ctx, host.DefraNode, fmt.Sprintf("%s {transactionHash}", viewName))
	require.NoError(t, err)

	// Without lens, all logs should be processed (or at least most of them due to timing)
	t.Logf("Total logs: %d, Processed: %d", len(allLogs), len(processedData))
	require.Greater(t, len(processedData), 0, "Should have processed some data")
}

func validateMultipleViewsProcessed(t *testing.T, host *Host) {
	ctx := t.Context()

	require.Len(t, host.HostedViews, 2)

	// Check View1 (Log processing)
	view1Data, err := defra.QueryArray[map[string]any](ctx, host.DefraNode, "View1 {transactionHash}")
	require.NoError(t, err)
	require.Greater(t, len(view1Data), 0, "View1 should have processed logs")

	// Check View2 (Transaction processing)
	view2Data, err := defra.QueryArray[map[string]any](ctx, host.DefraNode, "View2 {hash}")
	require.NoError(t, err)
	require.Greater(t, len(view2Data), 0, "View2 should have processed transactions")

	t.Logf("View1 processed %d logs, View2 processed %d transactions", len(view1Data), len(view2Data))
}

func validateMultipleViewsProcessedWithProgress(t *testing.T, host *Host, round int, previousView1Count int, previousView2Count int, previousBlocks map[string]uint64) (int, int, map[string]uint64) {
	ctx := t.Context()

	require.Len(t, host.HostedViews, 2)

	// Check View1 (Log processing)
	view1Data, err := defra.QueryArray[map[string]any](ctx, host.DefraNode, "View1 {transactionHash}")
	require.NoError(t, err)
	require.Greater(t, len(view1Data), 0, "Round %d: View1 should have processed logs", round)

	// Check View2 (Transaction processing)
	view2Data, err := defra.QueryArray[map[string]any](ctx, host.DefraNode, "View2 {hash}")
	require.NoError(t, err)
	require.Greater(t, len(view2Data), 0, "Round %d: View2 should have processed transactions", round)

	// Validate that processing count has increased (unless it's the first round)
	if round > 1 && previousView1Count > 0 {
		require.GreaterOrEqual(t, len(view1Data), previousView1Count, "Round %d: View1 processed data should not decrease", round)
		require.GreaterOrEqual(t, len(view2Data), previousView2Count, "Round %d: View2 processed data should not decrease", round)
		t.Logf("Round %d: View1 processed %d logs (previous: %d), View2 processed %d transactions (previous: %d)", round, len(view1Data), previousView1Count, len(view2Data), previousView2Count)
	} else {
		t.Logf("Round %d: View1 processed %d logs, View2 processed %d transactions", round, len(view1Data), len(view2Data))
	}

	// Validate that viewProcessedBlocks maps are updated correctly
	currentProcessedBlocks := make(map[string]uint64)
	for viewName, stack := range host.viewProcessedBlocks {
		if !stack.IsEmpty() {
			latestBlock, err := stack.Peek()
			if err == nil {
				currentProcessedBlocks[viewName] = latestBlock

				// Validate that block numbers are progressing (unless it's the first round)
				if round > 1 && previousBlocks != nil {
					if prevBlock, exists := previousBlocks[viewName]; exists {
						require.GreaterOrEqual(t, latestBlock, prevBlock, "Round %d: View %s block number should not decrease", round, viewName)
						t.Logf("Round %d: View %s processed block %d (previous: %d)", round, viewName, latestBlock, prevBlock)
					}
				} else {
					t.Logf("Round %d: View %s processed block %d", round, viewName, latestBlock)
				}
			}
		}
	}

	return len(view1Data), len(view2Data), currentProcessedBlocks
}

func validateProcessedDataWithGaps(t *testing.T, host *Host, targetAddress string) {
	ctx := t.Context()

	require.Len(t, host.HostedViews, 1)
	viewName := host.HostedViews[0].Name

	// Query the processed data
	processedData, err := defra.QueryArray[map[string]any](ctx, host.DefraNode, fmt.Sprintf("%s {transactionHash}", viewName))
	require.NoError(t, err)
	require.Greater(t, len(processedData), 0, "Should have processed data despite gaps")

	t.Logf("Processed %d items with block gaps", len(processedData))
}

func stringPtr(s string) *string {
	return &s
}
