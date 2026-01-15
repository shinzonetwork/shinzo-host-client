package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	localschema "github.com/shinzonetwork/shinzo-host-client/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func init() {
	// Set DefraDB log level to error via environment variable
	os.Setenv("LOG_LEVEL", "error")
	zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	logger.Init(true, "")
}

func TestIntegration(t *testing.T) {
	ctx := context.Background()

	// Create test config with P2P enabled to replicate data from indexer
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "integration-test-secret"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0" // Use random available port
	testConfig.DefraDB.P2P.Enabled = true
	testConfig.DefraDB.P2P.BootstrapPeers = []string{
		"/ip4/35.192.219.55/tcp/9171/p2p/12D3KooWCfgCafjxcVzpJsP7DhmdKCb8dnmmfngUjywsxamzQtgB",
	}

	// Start defra with real connection using schema applier
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(localschema.GetSchemaForBuild())
	defraNode, _, err := defra.StartDefraInstance(testConfig, schemaApplier, constants.AllCollections...)
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Run all integration tests
	t.Run("ViewRegistration", func(t *testing.T) {
		testViewRegistration(t, ctx, defraNode)
	})

	t.Run("AttestationRecordRegistration", func(t *testing.T) {
		testAttestationRecordRegistration(t, ctx, defraNode)
	})

	t.Run("SchemaIndexes", func(t *testing.T) {
		testSchemaIndexes(t, ctx, defraNode)
	})

	t.Run("Queries", func(t *testing.T) {
		testQueries(t, ctx, defraNode)
	})
}

// testViewRegistration tests view registration functionality
func testViewRegistration(t *testing.T, ctx context.Context, defraNode *node.Node) {
	registryPath := t.TempDir()
	vm := view.NewViewManager(defraNode, registryPath)

	// Verify initial state
	require.Equal(t, 0, vm.GetViewCount(), "Should start with no views")
	require.Empty(t, vm.GetActiveViewNames(), "Should have no active view names")

	// Test loading with no views
	err := vm.LoadAndRegisterViews(ctx, nil)
	require.NoError(t, err, "Should handle empty view list")

	// Create a simple view without lenses
	query := "Ethereum__Mainnet__Log { address blockNumber transactionHash }"
	sdl := "type TestLogView @materialized(if: false) { address: String blockNumber: Int transactionHash: String }"
	testView := view.View{
		Name:  "TestLogView",
		Query: &query,
		Sdl:   &sdl,
	}

	// Register the view
	err = vm.RegisterView(ctx, testView)
	require.NoError(t, err, "Should register view successfully")

	// Verify view was registered
	require.Equal(t, 1, vm.GetViewCount(), "Should have one view registered")
	require.Contains(t, vm.GetActiveViewNames(), "TestLogView", "Should contain TestLogView")

	// Test duplicate registration fails
	err = vm.RegisterView(ctx, testView)
	require.Error(t, err, "Should fail on duplicate registration")
	require.Contains(t, err.Error(), "already registered")
}

// testAttestationRecordRegistration tests attestation record creation and retrieval
func testAttestationRecordRegistration(t *testing.T, ctx context.Context, defraNode *node.Node) {
	// Create a mock verifier that accepts all signatures
	verifier := &mockSignatureVerifier{}

	// Create test versions with signatures
	versions := []attestation.Version{
		{
			CID: "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
			Signature: attestation.Signature{
				Type:     "es256k",
				Identity: "test-indexer-1",
				Value:    "sig-value-1",
			},
		},
		{
			CID: "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzd2",
			Signature: attestation.Signature{
				Type:     "es256k",
				Identity: "test-indexer-2",
				Value:    "sig-value-2",
			},
		},
	}

	// Create attestation record
	docID := "test-doc-001"
	sourceDocID := "source-doc-001"
	docType := constants.CollectionBlock

	record, err := attestation.CreateAttestationRecord(ctx, verifier, docID, sourceDocID, docType, versions)
	require.NoError(t, err, "Should create attestation record")
	require.NotNil(t, record)
	require.Equal(t, docID, record.AttestedDocId)
	require.Equal(t, sourceDocID, record.SourceDocId)
	require.Len(t, record.CIDs, 2, "Should have 2 CIDs from valid signatures")

	// Post attestation record to DefraDB
	err = attestation.PostAttestationRecord(ctx, defraNode, record)
	require.NoError(t, err, "Should post attestation record")

	// Verify attestation record was stored
	existing, err := attestation.CheckExistingAttestation(ctx, defraNode, docID, docType)
	require.NoError(t, err, "Should query existing attestation")
	require.Len(t, existing, 1, "Should find one attestation record")
	require.Equal(t, docID, existing[0].AttestedDocId)

	// Test P-counter increment by posting again
	record2, err := attestation.CreateAttestationRecord(ctx, verifier, docID, sourceDocID, docType, versions)
	require.NoError(t, err)
	err = attestation.PostAttestationRecord(ctx, defraNode, record2)
	require.NoError(t, err, "Should upsert attestation record")

	// Verify vote_count incremented via P-counter
	existing2, err := attestation.CheckExistingAttestation(ctx, defraNode, docID, docType)
	require.NoError(t, err)
	require.Len(t, existing2, 1, "Should still have one record after upsert")
	require.GreaterOrEqual(t, existing2[0].VoteCount, 1, "Vote count should be at least 1")
}

// testSchemaIndexes verifies that schema indexes are properly created
func testSchemaIndexes(t *testing.T, ctx context.Context, defraNode *node.Node) {
	// Get all collections and verify they exist
	collections := []string{
		constants.CollectionBlock,
		constants.CollectionTransaction,
		constants.CollectionLog,
		constants.CollectionAccessListEntry,
		constants.CollectionAttestationRecord,
	}

	for _, collName := range collections {
		coll, err := defraNode.DB.GetCollectionByName(ctx, collName)
		require.NoError(t, err, "Collection %s should exist", collName)
		require.NotNil(t, coll, "Collection %s should not be nil", collName)

		// Get indexes for the collection
		indexes, err := coll.GetIndexes(ctx)
		require.NoError(t, err, "Should get indexes for %s", collName)

		t.Logf("Collection %s has %d indexes", collName, len(indexes))
		for _, idx := range indexes {
			t.Logf("  - Index: %s on fields: %v", idx.Name, idx.Fields)
		}
	}

	// Verify specific indexes exist on Block collection
	blockColl, _ := defraNode.DB.GetCollectionByName(ctx, constants.CollectionBlock)
	blockIndexes, _ := blockColl.GetIndexes(ctx)
	require.NotEmpty(t, blockIndexes, "Block collection should have indexes")

	// Verify AttestationRecord has indexes
	attestColl, _ := defraNode.DB.GetCollectionByName(ctx, constants.CollectionAttestationRecord)
	attestIndexes, _ := attestColl.GetIndexes(ctx)
	require.NotEmpty(t, attestIndexes, "AttestationRecord collection should have indexes")
}

// testQueries tests various query patterns
func testQueries(t *testing.T, ctx context.Context, defraNode *node.Node) {
	// wait for P2P replication from indexer
	t.Log("Waiting 10 seconds for P2P replication...")
	time.Sleep(10 * time.Second)

	// 1. Query individual collections: block, transaction, log, accesslist, attestation record
	t.Run("IndividualCollectionQueries", func(t *testing.T) {
		// Query Block
		blockQuery := fmt.Sprintf(`{ %s(limit: 1) { hash number timestamp } }`, constants.CollectionBlock)
		blocks, err := defra.QueryArray[map[string]any](ctx, defraNode, blockQuery)
		if err != nil {
			t.Logf("ERROR querying blocks: %v", err)
		}
		require.NoError(t, err, "Should query blocks")
		require.NotEmpty(t, blocks, "Should have blocks")
		t.Logf("Found %d blocks", len(blocks))

		// Query Transaction
		txQuery := fmt.Sprintf(`{ %s(limit: 10) { hash blockNumber from to } }`, constants.CollectionTransaction)
		txs, err := defra.QueryArray[map[string]any](ctx, defraNode, txQuery)
		if err != nil {
			t.Logf("ERROR querying transactions: %v", err)
		}
		require.NoError(t, err, "Should query transactions")
		require.NotEmpty(t, txs, "Should have transactions")
		t.Logf("Found %d transactions", len(txs))

		// Query Log
		logQuery := fmt.Sprintf(`{ %s(limit: 10) { address blockNumber transactionHash logIndex } }`, constants.CollectionLog)
		logs, err := defra.QueryArray[map[string]any](ctx, defraNode, logQuery)
		if err != nil {
			t.Logf("ERROR querying logs: %v", err)
		}
		require.NoError(t, err, "Should query logs")
		require.NotEmpty(t, logs, "Should have logs")
		t.Logf("Found %d logs", len(logs))

		// Query AccessListEntry
		accessQuery := fmt.Sprintf(`{ %s(limit: 3) { address storageKeys } }`, constants.CollectionAccessListEntry)
		accessList, err := defra.QueryArray[map[string]any](ctx, defraNode, accessQuery)
		if err != nil {
			t.Logf("ERROR querying access list entries: %v", err)
		}
		require.NoError(t, err, "Should query access list entries")
		t.Logf("Found %d access list entries", len(accessList))

		// Query AttestationRecord
		attestQuery := fmt.Sprintf(`{ %s(limit: 10) { attested_doc source_doc doc_type vote_count CIDs } }`, constants.CollectionAttestationRecord)
		attestations, err := defra.QueryArray[map[string]any](ctx, defraNode, attestQuery)
		if err != nil {
			t.Logf("ERROR querying attestation records: %v", err)
		}
		require.NoError(t, err, "Should query attestation records")
		t.Logf("Found %d attestation records", len(attestations))
	})

	// 2. Nested query: block{transactions{logs{}, accessList{}}}
	t.Run("NestedRelationshipQuery", func(t *testing.T) {
		nestedQuery := fmt.Sprintf(`{ 
			%s(limit: 1) { 
				hash 
				number 
				transactions { 
					hash 
					from 
					to 
					logs { 
						address 
						logIndex 
					} 
					accessList { 
						address 
						storageKeys 
					} 
				} 
			} 
		}`, constants.CollectionBlock)

		results, err := defra.QueryArray[map[string]any](ctx, defraNode, nestedQuery)
		if err != nil {
			t.Logf("ERROR executing nested query: %v", err)
		}
		require.NoError(t, err, "Should execute nested query")
		require.NotEmpty(t, results, "Should have results")

		// Verify nested structure
		block := results[0]
		require.Contains(t, block, "hash")
		require.Contains(t, block, "number")
		require.Contains(t, block, "transactions")

		if txs, ok := block["transactions"].([]interface{}); ok && len(txs) > 0 {
			tx := txs[0].(map[string]interface{})
			require.Contains(t, tx, "hash")
			require.Contains(t, tx, "logs")
			require.Contains(t, tx, "accessList")
			t.Logf("Nested query returned block with %d transactions", len(txs))
		}
	})

	// 3. Nested query on Transaction
	t.Run("NestedRelationship2", func(t *testing.T) {
		nestedQuery := fmt.Sprintf(`{ 
			%s(limit: 1) { 
				hash 
				blockNumber 
				transactionIndex
				logs { 
					address
					logIndex 
				} 
				accessList{ 
					address 
					storageKeys 
				}
			} 
		}`, constants.CollectionTransaction)

		results, err := defra.QueryArray[map[string]any](ctx, defraNode, nestedQuery)
		if err != nil {
			t.Logf("ERROR executing nested transaction query: %v", err)
		}
		require.NoError(t, err, "Should execute nested query")
		t.Logf("Nested transaction query returned %d results", len(results))
	})

	// 4. Nested query on Log
	t.Run("NestedRelationship3", func(t *testing.T) {
		nestedQuery := fmt.Sprintf(`{ 
			%s(limit: 1) { 
				address
				logIndex 
				block{
					hash
				}
			} 
		}`, constants.CollectionLog)

		results, err := defra.QueryArray[map[string]any](ctx, defraNode, nestedQuery)
		if err != nil {
			t.Logf("ERROR executing nested log query: %v", err)
		}
		require.NoError(t, err, "Should execute nested query")
		t.Logf("Nested log query returned %d results", len(results))
	})

	// 4. CID lookup (via _docID)
	t.Run("CIDLookup", func(t *testing.T) {
		// First get a document to get its _docID
		getDocQuery := fmt.Sprintf(`{ %s(limit: 1) { _docID hash number } }`, constants.CollectionBlock)
		docs, err := defra.QueryArray[map[string]any](ctx, defraNode, getDocQuery)
		if err != nil {
			t.Logf("ERROR getting document with _docID: %v", err)
		}
		require.NoError(t, err, "Should get document with _docID")
		if len(docs) == 0 {
			t.Logf("ERROR: no documents found")
		}
		require.NotEmpty(t, docs, "Should have at least one document")

		docID, ok := docs[0]["_docID"].(string)
		if !ok {
			t.Logf("ERROR: _docID is not a string, got: %T = %v", docs[0]["_docID"], docs[0]["_docID"])
		}
		require.True(t, ok, "_docID should be a string")
		if docID == "" {
			t.Logf("ERROR: _docID is empty")
		}
		require.NotEmpty(t, docID, "_docID should not be empty")
		t.Logf("Got document with _docID: %s", docID)

		// Now lookup by _docID
		cidLookupQuery := fmt.Sprintf(`{ 
			%s(docID: "%s") { 
				_docID 
				hash 
				number 
			} 
		}`, constants.CollectionBlock, docID)

		result, err := defra.QuerySingle[map[string]any](ctx, defraNode, cidLookupQuery)
		if err != nil {
			t.Logf("ERROR looking up by _docID: %v", err)
		}
		require.NoError(t, err, "Should lookup by _docID")
		if result["_docID"] != docID {
			t.Logf("ERROR: returned document has different _docID")
		}
		require.Equal(t, docID, result["_docID"], "Should return same document")
		t.Logf("CID lookup successful for docID: %s", docID)
	})
}

// insertTestData inserts test data for query tests
func insertTestData(t *testing.T, ctx context.Context, defraNode *node.Node) {
	// Insert a test block
	blockMutation := fmt.Sprintf(`mutation {
		create_%s(input: {
			hash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
			number: 12345678
			timestamp: "1234567890"
			parentHash: "0x0000000000000000000000000000000000000000000000000000000000000000"
			difficulty: "1000000"
			totalDifficulty: "10000000000"
			gasUsed: "21000"
			gasLimit: "30000000"
			baseFeePerGas: "1000000000"
			nonce: "0x0000000000000000"
			miner: "0x0000000000000000000000000000000000000001"
			size: "1000"
			stateRoot: "0x1111111111111111111111111111111111111111111111111111111111111111"
			sha3Uncles: "0x2222222222222222222222222222222222222222222222222222222222222222"
			transactionsRoot: "0x3333333333333333333333333333333333333333333333333333333333333333"
			receiptsRoot: "0x4444444444444444444444444444444444444444444444444444444444444444"
			logsBloom: "0x00000000000000000000000000000000"
			extraData: "0x"
			mixHash: "0x5555555555555555555555555555555555555555555555555555555555555555"
		}) {
			_docID
			hash
			number
		}
	}`, constants.CollectionBlock)

	blockResult, err := defra.PostMutation[map[string]any](ctx, defraNode, blockMutation)
	require.NoError(t, err, "Should create block")
	require.NotNil(t, blockResult)
	blockDocID := (*blockResult)["_docID"].(string)
	t.Logf("Created block with _docID: %s", blockDocID)

	// Insert a test transaction linked to the block
	txMutation := fmt.Sprintf(`mutation {
		create_%s(input: {
			hash: "0xabc123def456"
			blockHash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
			blockNumber: 12345678
			from: "0xsender123"
			to: "0xreceiver456"
			value: "1000000000000000000"
			gas: "21000"
			gasPrice: "20000000000"
			input: "0x"
			nonce: "1"
			transactionIndex: 0
			type: "0x2"
			chainId: "1"
			v: "0x1b"
			r: "0xaaa"
			s: "0xbbb"
			status: true
			cumulativeGasUsed: "21000"
			effectiveGasPrice: "20000000000"
			block_id: "%s"
		}) {
			_docID
			hash
			blockNumber
		}
	}`, constants.CollectionTransaction, blockDocID)

	txResult, err := defra.PostMutation[map[string]any](ctx, defraNode, txMutation)
	require.NoError(t, err, "Should create transaction")
	require.NotNil(t, txResult)
	txDocID := (*txResult)["_docID"].(string)
	t.Logf("Created transaction with _docID: %s", txDocID)

	// Insert a test log linked to the transaction
	logMutation := fmt.Sprintf(`mutation {
		create_%s(input: {
			address: "0xcontract789"
			topics: ["0xtopic1", "0xtopic2"]
			data: "0xlogdata"
			transactionHash: "0xabc123def456"
			blockHash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
			blockNumber: 12345678
			transactionIndex: 0
			logIndex: 0
			removed: "false"
			transaction_id: "%s"
		}) {
			_docID
			address
			logIndex
		}
	}`, constants.CollectionLog, txDocID)

	logResult, err := defra.PostMutation[map[string]any](ctx, defraNode, logMutation)
	require.NoError(t, err, "Should create log")
	require.NotNil(t, logResult)
	t.Logf("Created log with _docID: %s", (*logResult)["_docID"])

	// Insert a test access list entry linked to the transaction
	accessMutation := fmt.Sprintf(`mutation {
		create_%s(input: {
			address: "0xaccessaddress"
			storageKeys: ["0xkey1", "0xkey2"]
			transaction_id: "%s"
		}) {
			_docID
			address
		}
	}`, constants.CollectionAccessListEntry, txDocID)

	accessResult, err := defra.PostMutation[map[string]any](ctx, defraNode, accessMutation)
	require.NoError(t, err, "Should create access list entry")
	require.NotNil(t, accessResult)
	t.Logf("Created access list entry with _docID: %s", (*accessResult)["_docID"])
}

// mockSignatureVerifier is a mock implementation for testing
type mockSignatureVerifier struct{}

func (m *mockSignatureVerifier) Verify(ctx context.Context, cid string, signature attestation.Signature) error {
	// Accept all signatures in tests
	return nil
}
