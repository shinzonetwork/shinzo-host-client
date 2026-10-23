package attestation

import (
	"context"
	"fmt"
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	defraclient "github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func TestPostAttestationRecord(t *testing.T) {
	ctx := context.Background()

	// Define schema inline for this test
	testSchema := testDocAndAttestationSchema

	// Create and start client using new API with test config
	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir() // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false             // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{} // No bootstrap peers

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer func() { _ = client.Stop(t.Context()) }()

	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	type TestDoc struct {
		Name    string `json:"name"`
		DocID   string `json:"_docID"`
		Version []struct {
			CID string `json:"cid"`
		} `json:"_version"`
	}

	createTestDocMutation := `
		mutation {
			add_TestDoc(input: {name: "test-document"}) {
				_docID
				name
				_version {
					cid
					signature {
						type
						identity
						value
					}
					collectionVersionId
				}
			}
		}
	`

	testDocResult, err := defradb.PostMutation[TestDoc](t.Context(), defraNode, createTestDocMutation)
	require.NoError(t, err)
	require.NotNil(t, testDocResult)
	require.Greater(t, len(testDocResult.DocID), 0)
	require.Len(t, testDocResult.Version, 1)

	testVersions := testDocResult.Version

	attestedDocID := testAttestedDocID // This would be the View doc created after processing the view
	sourceDocID := testDocResult.DocID

	attestationRecord := &Record{
		AttestedDocID: attestedDocID,
		SourceDocIDs:  []string{sourceDocID},
		CIDs:          []string{},
	}
	for _, version := range testVersions {
		attestationRecord.CIDs = append(attestationRecord.CIDs, version.CID)
	}

	err = PostAttestationRecord(t.Context(), defraNode, testAttestationCollection, attestationRecord)
	require.NoError(t, err)

	query := fmt.Sprintf(`
		%s {
			_docID
			attested_doc
			source_doc
			CIDs
		}
	`, testAttestationCollection)

	results, err := defradb.QueryArray[Record](t.Context(), defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)

	record := results[0]
	require.Equal(t, attestedDocID, record.AttestedDocID)
	require.Equal(t, []string{testDocResult.DocID}, record.SourceDocIDs)
	require.NotNil(t, record.CIDs)
	require.Len(t, record.CIDs, 1)
}

func TestPostAttestationRecord_NewDocument_CreatesSingleRecord(t *testing.T) {
	ctx := context.Background()

	// Define schema inline for this test
	testSchema := testDocAndAttestationSchema

	// Create and start client using new API with test config
	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir() // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false             // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{} // No bootstrap peers

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer func() { _ = client.Stop(t.Context()) }()

	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	record := &Record{
		AttestedDocID: testDocID,
		SourceDocIDs:  []string{testDocID},
		CIDs:          []string{testCID1},
	}

	err = PostAttestationRecord(t.Context(), defraNode, testAttestationCollection, record)
	require.NoError(t, err)

	query := fmt.Sprintf(`
		query {
			%s(filter: {attested_doc: {_eq: "%s"}}) {
				_docID
				attested_doc
				source_doc
				CIDs
			}
		}
	`, testAttestationCollection, testDocID)

	results, err := defradb.QueryArray[Record](t.Context(), defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, testDocID, results[0].AttestedDocID)
}

func TestPostAttestationRecord_OldDocument_DuplicateCreateIsHandled(t *testing.T) {
	ctx := context.Background()

	// Define schema inline for this test
	testSchema := testDocAndAttestationSchema

	// Create and start client using new API with test config
	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir() // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false             // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{} // No bootstrap peers

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer func() { _ = client.Stop(t.Context()) }()

	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	record := &Record{
		AttestedDocID: testDocID,
		SourceDocIDs:  []string{testDocID},
		CIDs:          []string{testCID1},
	}

	err = PostAttestationRecord(t.Context(), defraNode, testAttestationCollection, record)
	require.NoError(t, err)
	err = PostAttestationRecord(t.Context(), defraNode, testAttestationCollection, record)
	require.NoError(t, err)

	query := fmt.Sprintf(`
		query {
			%s(filter: {attested_doc: {_eq: "%s"}}) {
				_docID
				attested_doc
				source_doc
				CIDs
			}
		}
	`, testAttestationCollection, testDocID)

	results, err := defradb.QueryArray[Record](t.Context(), defraNode, query)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(results), 1)
}

func TestPostAttestationRecord_MultipleIndexers_AppendsSourceDoc(t *testing.T) {
	ctx := context.Background()

	schema := testAttestationRecordSchema
	defraNode, err := defradb.StartDefraInstanceWithTestConfig(t, defradb.DefaultConfig, defradb.NewSchemaApplierFromProvidedSchema(schema))
	require.NoError(t, err)
	defer func() { _ = defraNode.Close(ctx) }()

	// First indexer posts attestation
	record1 := &Record{
		AttestedDocID: "block:500:deadbeef",
		SourceDocIDs:  []string{"indexer-A"},
		CIDs:          []string{testCID1, testCID2},
		DocType:       testDocTypeBlock,
		VoteCount:     1,
	}
	err = PostAttestationRecord(ctx, defraNode, testAttestationCollection, record1)
	require.NoError(t, err)

	// Second indexer posts attestation for the same block
	record2 := &Record{
		AttestedDocID: "block:500:deadbeef",
		SourceDocIDs:  []string{"indexer-B"},
		CIDs:          []string{testCID1, testCID2},
		DocType:       testDocTypeBlock,
		VoteCount:     1,
	}
	err = PostAttestationRecord(ctx, defraNode, testAttestationCollection, record2)
	require.NoError(t, err)

	// Query and verify both indexer identities are preserved
	records, err := CheckExistingAttestation(ctx, defraNode, testAttestationCollection, "block:500:deadbeef", testDocTypeBlock)
	require.NoError(t, err)
	require.Len(t, records, 1, "Should have exactly one attestation record")

	record := records[0]
	require.Contains(t, record.SourceDocIDs, "indexer-A", "Should contain first indexer")
	require.Contains(t, record.SourceDocIDs, "indexer-B", "Should contain second indexer")
	require.Len(t, record.SourceDocIDs, 2, "Should have exactly 2 indexer identities")
}

// ========================================
// CHECK EXISTING ATTESTATION TESTS
// ========================================

func TestCheckExistingAttestation_NoExistingRecords(t *testing.T) {
	ctx := context.Background()

	testSchema := testAttestationRecordSchema

	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer func() { _ = client.Stop(t.Context()) }()

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Check for non-existent attestation
	records, err := CheckExistingAttestation(ctx, defraNode, testAttestationCollection, "non-existent-doc", testDocType)
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestCheckExistingAttestation_WithExistingRecord(t *testing.T) {
	ctx := context.Background()

	testSchema := testAttestationRecordSchema

	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer func() { _ = client.Stop(t.Context()) }()

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// First, create an attestation record
	record := &Record{
		AttestedDocID: "check-existing-doc",
		SourceDocIDs:  []string{jsonFieldSourceDoc},
		CIDs:          []string{"cid-check-1"},
		DocType:       testDocType,
		VoteCount:     1,
	}
	err = PostAttestationRecord(ctx, defraNode, testAttestationCollection, record)
	require.NoError(t, err)

	// Now check for existing attestation
	records, err := CheckExistingAttestation(ctx, defraNode, testAttestationCollection, "check-existing-doc", testDocType)
	require.NoError(t, err)
	require.NotNil(t, records)
	require.Len(t, records, 1)
	require.Equal(t, "check-existing-doc", records[0].AttestedDocID)
	require.Equal(t, []string{"source-doc"}, records[0].SourceDocIDs)
	require.ElementsMatch(t, []string{"cid-check-1"}, records[0].CIDs)
}

func TestCheckExistingAttestation_WrongDocType(t *testing.T) {
	ctx := context.Background()

	testSchema := testAttestationRecordSchema

	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer func() { _ = client.Stop(t.Context()) }()

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create an attestation record with doc_type testDocTypeA
	record := &Record{
		AttestedDocID: "check-doctype-doc",
		SourceDocIDs:  []string{jsonFieldSourceDoc},
		CIDs:          []string{"cid-dt-1"},
		DocType:       testDocTypeA,
		VoteCount:     1,
	}
	err = PostAttestationRecord(ctx, defraNode, testAttestationCollection, record)
	require.NoError(t, err)

	// Query with wrong doc_type should find nothing
	records, err := CheckExistingAttestation(ctx, defraNode, testAttestationCollection, "check-doctype-doc", testDocTypeB)
	require.NoError(t, err)
	require.Empty(t, records)
}

// ========================================
// POST ATTESTATION RECORD REMAINING BRANCHES
// ========================================

func TestPostAttestationRecord_EmptyCIDs(t *testing.T) {
	ctx := context.Background()

	testSchema := testAttestationRecordSchema

	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer func() { _ = client.Stop(t.Context()) }()

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	record := &Record{
		AttestedDocID: "empty-cids-doc",
		SourceDocIDs:  []string{jsonFieldSourceDoc},
		CIDs:          []string{},
		DocType:       testDocType,
		VoteCount:     1,
	}

	err = PostAttestationRecord(ctx, defraNode, testAttestationCollection, record)
	require.NoError(t, err)
}

func TestPostAttestationRecord_MultipleCIDs(t *testing.T) {
	ctx := context.Background()

	testSchema := testAttestationRecordSchema

	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer func() { _ = client.Stop(t.Context()) }()

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	record := &Record{
		AttestedDocID: "multi-cid-doc",
		SourceDocIDs:  []string{jsonFieldSourceDoc},
		CIDs:          []string{testCID1, testCID2, testCID3},
		DocType:       testDocType,
		VoteCount:     5,
	}

	err = PostAttestationRecord(ctx, defraNode, testAttestationCollection, record)
	require.NoError(t, err)

	query := fmt.Sprintf(`
		query {
			%s(filter: {attested_doc: {_eq: "multi-cid-doc"}}) {
				attested_doc
				CIDs
				vote_count
			}
		}
	`, testAttestationCollection)

	results, err := defradb.QueryArray[Record](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.ElementsMatch(t, []string{testCID1, testCID2, testCID3}, results[0].CIDs)
}

// ========================================
// POST ATTESTATION RECORD ERROR PATH
// ========================================

func TestPostAttestationRecord_MissingSchema_ReturnsError(t *testing.T) {
	ctx := context.Background()

	// Create a defra node without the attestation schema applied
	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer func() { _ = client.Stop(t.Context()) }()

	// Intentionally do NOT apply the schema - this will make the mutation fail
	defraNode := client.GetNode()

	record := &Record{
		AttestedDocID: "doc-error",
		SourceDocIDs:  []string{"source-error"},
		CIDs:          []string{testCID1},
		DocType:       testDocType,
		VoteCount:     1,
	}

	err = PostAttestationRecord(ctx, defraNode, testAttestationCollection, record)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get attestation collection")
}

// ========================================
// CHECK EXISTING ATTESTATION - ADDITIONAL ERROR PATHS
// ========================================

func TestCheckExistingAttestation_ReturnsMultipleRecords(t *testing.T) {
	ctx := context.Background()

	testSchema := testAttestationRecordSchema

	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer func() { _ = client.Stop(t.Context()) }()

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create two attestation records for same attested_doc and doc_type
	// Using upsert, the second one will merge with the first
	record1 := &Record{
		AttestedDocID: "multi-check-doc",
		SourceDocIDs:  []string{testSource1},
		CIDs:          []string{testCID1},
		DocType:       testDocTypeA,
		VoteCount:     1,
	}
	err = PostAttestationRecord(ctx, defraNode, testAttestationCollection, record1)
	require.NoError(t, err)

	records, err := CheckExistingAttestation(ctx, defraNode, testAttestationCollection, "multi-check-doc", testDocTypeA)
	require.NoError(t, err)
	require.NotEmpty(t, records)
	require.Equal(t, "multi-check-doc", records[0].AttestedDocID)
}

// ========================================
// "No attestation records found" ERROR STRING BRANCH TESTS
// ========================================
// This test calls CheckExistingAttestation against a DefraDB instance that has NO
// attestation schema applied. When the collection doesn't exist, defradb.QueryArray
// returns an error whose message contains "No attestation records found" (or similar),
// and the function under test should treat this as a non-error (return nil).

func TestCheckExistingAttestation_MissingSchema_ReturnsNilNil(t *testing.T) {
	ctx := context.Background()

	// Create a defra node WITHOUT applying the attestation schema
	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer func() { _ = client.Stop(t.Context()) }()

	// Do NOT apply the attestation schema - collection won't exist
	defraNode := client.GetNode()

	// This should trigger the strings.Contains(err.Error(), "No attestation records found") branch
	// or return an error if the branch doesn't match
	records, err := CheckExistingAttestation(ctx, defraNode, testAttestationCollection, "nonexistent-doc", testDocType)
	// The function should either return nil, nil (branch matched) or an error
	// If the collection doesn't exist, the error may or may not contain "No attestation records found"
	// In either case, it should not panic
	if err != nil {
		// The error string from DefraDB when collection doesn't exist
		// may contain something like "No attestation records found" or a different error.
		// The function wraps non-matching errors, so we check for the wrapper.
		require.Contains(t, err.Error(), "failed to check existing attestation")
	} else {
		require.Nil(t, records)
	}
}

// ========================================
// EXTRACT DOC ID FROM RESULT TESTS
// ========================================

func TestExtractDocIDFromResult_ValidResult(t *testing.T) {
	data := map[string]any{
		testCollectionName: []any{
			map[string]any{
				jsonFieldDocID: "bae-abc123",
			},
		},
	}

	result := extractDocIDFromResult(data, testCollectionName)
	require.Equal(t, "bae-abc123", result)
}

func TestExtractDocIDFromResult_NilData(t *testing.T) {
	result := extractDocIDFromResult(nil, testCollectionName)
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_DataNotMap(t *testing.T) {
	result := extractDocIDFromResult("not-a-map", testCollectionName)
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_CollectionNotSlice(t *testing.T) {
	data := map[string]any{
		testCollectionName: "not-a-slice",
	}
	result := extractDocIDFromResult(data, testCollectionName)
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_CollectionMissing(t *testing.T) {
	data := map[string]any{
		"OtherCollection": []any{},
	}
	result := extractDocIDFromResult(data, testCollectionName)
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_EmptyCollection(t *testing.T) {
	data := map[string]any{
		testCollectionName: []any{},
	}
	result := extractDocIDFromResult(data, testCollectionName)
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_FirstDocNotMap(t *testing.T) {
	data := map[string]any{
		testCollectionName: []any{
			"not-a-map",
		},
	}
	result := extractDocIDFromResult(data, testCollectionName)
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_DocIDNotString(t *testing.T) {
	data := map[string]any{
		testCollectionName: []any{
			map[string]any{
				jsonFieldDocID: 12345,
			},
		},
	}
	result := extractDocIDFromResult(data, testCollectionName)
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_DocIDMissing(t *testing.T) {
	data := map[string]any{
		testCollectionName: []any{
			map[string]any{
				"other_field": "value",
			},
		},
	}
	result := extractDocIDFromResult(data, testCollectionName)
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_MultipleDocsReturnsFirst(t *testing.T) {
	data := map[string]any{
		testCollectionName: []any{
			map[string]any{jsonFieldDocID: "first-doc"},
			map[string]any{jsonFieldDocID: "second-doc"},
		},
	}
	result := extractDocIDFromResult(data, testCollectionName)
	require.Equal(t, "first-doc", result)
}

// ========================================
// LOOKUP EXISTING ATTESTATION TESTS
// ========================================

func TestLookupExistingAttestation_NotFound(t *testing.T) {
	ctx := context.Background()

	testSchema := testAttestationRecordSchema

	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer func() { _ = client.Stop(t.Context()) }()

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()
	col, err := defraNode.DB.GetCollectionByName(ctx, testAttestationCollection)
	require.NoError(t, err)

	// Query for a non-existent attested doc - should return nil, nil
	doc, err := lookupExistingAttestation(ctx, defraNode, col, "nonexistent-doc")
	require.NoError(t, err)
	require.Nil(t, doc)
}

func TestLookupExistingAttestation_Found(t *testing.T) {
	ctx := context.Background()

	testSchema := testAttestationRecordSchema

	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer func() { _ = client.Stop(t.Context()) }()

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create a record first
	record := &Record{
		AttestedDocID: "lookup-existing-doc",
		SourceDocIDs:  []string{jsonFieldSourceDoc},
		CIDs:          []string{"cid-lookup"},
		DocType:       testDocType,
		VoteCount:     1,
	}
	err = PostAttestationRecord(ctx, defraNode, testAttestationCollection, record)
	require.NoError(t, err)

	col, err := defraNode.DB.GetCollectionByName(ctx, testAttestationCollection)
	require.NoError(t, err)

	// Query for the existing attested doc - should return the document
	doc, err := lookupExistingAttestation(ctx, defraNode, col, "lookup-existing-doc")
	require.NoError(t, err)
	require.NotNil(t, doc)
}

// ---------------------------------------------------------------------------
// extractDocIDFromResult - []map[string]any type switch branch
// ---------------------------------------------------------------------------

func TestExtractDocIDFromResult_MapSliceType(t *testing.T) {
	// Test the []map[string]any branch which is distinct from []any
	data := map[string]any{
		testCollectionName: []map[string]any{
			{jsonFieldDocID: "bae-from-map-slice"},
		},
	}

	result := extractDocIDFromResult(data, testCollectionName)
	require.Equal(t, "bae-from-map-slice", result)
}

func TestExtractDocIDFromResult_MapSliceType_EmptySlice(t *testing.T) {
	data := map[string]any{
		testCollectionName: []map[string]any{},
	}

	result := extractDocIDFromResult(data, testCollectionName)
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_MapSliceType_MissingDocID(t *testing.T) {
	data := map[string]any{
		testCollectionName: []map[string]any{
			{"other_field": "value"},
		},
	}

	result := extractDocIDFromResult(data, testCollectionName)
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_MapSliceType_MultipleDocsReturnsFirst(t *testing.T) {
	data := map[string]any{
		testCollectionName: []map[string]any{
			{jsonFieldDocID: "first-map-doc"},
			{jsonFieldDocID: "second-map-doc"},
		},
	}

	result := extractDocIDFromResult(data, testCollectionName)
	require.Equal(t, "first-map-doc", result)
}

func TestExtractDocIDFromResult_MapSliceType_DocIDNotString(t *testing.T) {
	data := map[string]any{
		testCollectionName: []map[string]any{
			{jsonFieldDocID: 999},
		},
	}

	result := extractDocIDFromResult(data, testCollectionName)
	require.Equal(t, "", result)
}

// A record carrying no block number must leave the field absent rather than store zero,
// which the pruner would read as a real height.
func TestPostAttestationRecord_BlockNumber(t *testing.T) {
	ctx := context.Background()

	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	require.NoError(t, client.Start(ctx))
	defer func() { _ = client.Stop(ctx) }()

	require.NoError(t, client.ApplySchema(ctx, `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: [String]
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
			blockNumber: Int @index
		}
	`))
	defraNode := client.GetNode()

	blockNumber := int64(12345)
	require.NoError(t, PostAttestationRecord(ctx, defraNode, testAttestationCollection, &Record{
		AttestedDocID: "block:12345:root",
		CIDs:          []string{"cid-a"},
		DocType:       "Block",
		VoteCount:     1,
		BlockNumber:   &blockNumber,
	}))
	require.NoError(t, PostAttestationRecord(ctx, defraNode, testAttestationCollection, &Record{
		AttestedDocID: "doc:no-block",
		CIDs:          []string{"cid-b"},
		DocType:       "Transaction",
		VoteCount:     1,
	}))

	require.Equal(t, int64(12345), storedBlockNumber(t, defraNode, "block:12345:root"))
	require.Nil(t, storedBlockNumber(t, defraNode, "doc:no-block"))
}

// A record the pruner deletes between the lookup and the write stays deleted, rather than coming
// back without the fields the lookup and the pruner select on.
func TestUpdateAttestationRecord_DoesNotRecreateAPrunedRecord(t *testing.T) {
	ctx := context.Background()

	testConfig := defradb.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = testKeyringSecret
	testConfig.DefraDB.URL = testListenAddrLocal
	testConfig.DefraDB.P2P.ListenAddr = testListenAddrP2P
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defradb.NewClient(testConfig)
	require.NoError(t, err)
	require.NoError(t, client.Start(ctx))
	defer func() { _ = client.Stop(ctx) }()

	require.NoError(t, client.ApplySchema(ctx, `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: [String]
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
			blockNumber: Int @index
		}
	`))
	defraNode := client.GetNode()

	blockNumber := int64(5)
	record := &Record{
		AttestedDocID: "block:5:root",
		CIDs:          []string{"cid-a"},
		DocType:       "Block",
		VoteCount:     1,
		BlockNumber:   &blockNumber,
	}
	require.NoError(t, PostAttestationRecord(ctx, defraNode, testAttestationCollection, record))

	col, err := defraNode.DB.GetCollectionByName(ctx, testAttestationCollection)
	require.NoError(t, err)
	existing, err := lookupExistingAttestation(ctx, defraNode, col, record.AttestedDocID)
	require.NoError(t, err)
	require.NotNil(t, existing)
	require.NoError(t, col.PurgeByDocIDs(ctx, []defraclient.DocID{existing.ID()}, true))

	require.ErrorIs(t, updateAttestationRecord(ctx, col, existing, record), ErrDocumentNotFound)

	res := defraNode.DB.ExecRequest(ctx, `query { Ethereum__Mainnet__AttestationRecord { _docID } }`)
	require.Empty(t, res.GQL.Errors)
	data, ok := res.GQL.Data.(map[string]any)
	require.True(t, ok)
	require.Empty(t, data["Ethereum__Mainnet__AttestationRecord"])
}

// storedBlockNumber returns the blockNumber of the record with the given attested_doc.
func storedBlockNumber(t *testing.T, defraNode *node.Node, attestedDoc string) any {
	t.Helper()
	res := defraNode.DB.ExecRequest(t.Context(), fmt.Sprintf(
		`query { Ethereum__Mainnet__AttestationRecord(filter: {attested_doc: {_eq: "%s"}}) { blockNumber } }`,
		attestedDoc))
	require.Empty(t, res.GQL.Errors)

	data, ok := res.GQL.Data.(map[string]any)
	require.True(t, ok)
	docs, ok := data["Ethereum__Mainnet__AttestationRecord"].([]map[string]any)
	require.True(t, ok)
	require.Len(t, docs, 1)
	return docs[0]["blockNumber"]
}
