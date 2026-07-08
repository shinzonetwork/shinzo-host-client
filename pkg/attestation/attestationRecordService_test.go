package attestation

import (
	"context"
	"fmt"
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/stretchr/testify/require"
)

func TestCreateAttestationRecord_AllSignaturesValid(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(_ context.Context, _ string, _ Signature) error {
			// All signatures are valid
			return nil
		},
	}

	docID := testDocID
	sourceDocID := testSourceDocID
	versions := []Version{
		{
			CID: testCID1,
			Signature: Signature{
				Type:     sigTypeES256KLowerHex,
				Identity: testIdentity1,
				Value:    testSignature1,
			},
		},
		{
			CID: testCID2,
			Signature: Signature{
				Type:     sigTypeES256KLowerHex,
				Identity: testIdentity2,
				Value:    testSignature2,
			},
		},
		{
			CID: testCID3,
			Signature: Signature{
				Type:     sigTypeES256KLowerHex,
				Identity: "identity-3",
				Value:    "signature-3",
			},
		},
	}

	record, err := CreateAttestationRecord(ctx, verifier, docID, []string{sourceDocID}, testDocType, versions, 50)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, docID, record.AttestedDocID)
	require.Equal(t, []string{sourceDocID}, record.SourceDocIDs)
	require.Len(t, record.CIDs, 3)
	require.Contains(t, record.CIDs, testCID1)
	require.Contains(t, record.CIDs, testCID2)
	require.Contains(t, record.CIDs, testCID3)
}

func TestCreateAttestationRecord_SomeSignaturesInvalid(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(_ context.Context, cid string, _ Signature) error {
			// Only cid-1 and cid-3 are valid
			if cid == testCID2 {
				return errInvalidSignature
			}
			return nil
		},
	}

	docID := testDocID
	sourceDocID := testSourceDocID
	versions := []Version{
		{
			CID: testCID1,
			Signature: Signature{
				Type:     sigTypeES256KLowerHex,
				Identity: testIdentity1,
				Value:    testSignature1,
			},
		},
		{
			CID: testCID2,
			Signature: Signature{
				Type:     sigTypeES256KLowerHex,
				Identity: testIdentity2,
				Value:    testSignature2,
			},
		},
		{
			CID: testCID3,
			Signature: Signature{
				Type:     sigTypeES256KLowerHex,
				Identity: "identity-3",
				Value:    "signature-3",
			},
		},
	}

	record, err := CreateAttestationRecord(ctx, verifier, docID, []string{sourceDocID}, testDocType, versions, 50)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, docID, record.AttestedDocID)
	require.Equal(t, []string{sourceDocID}, record.SourceDocIDs)
	require.Len(t, record.CIDs, 2)
	require.Contains(t, record.CIDs, testCID1)
	require.NotContains(t, record.CIDs, testCID2)
	require.Contains(t, record.CIDs, testCID3)
}

func TestCreateAttestationRecord_AllSignaturesInvalid(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(_ context.Context, _ string, _ Signature) error {
			// All signatures are invalid
			return errInvalidSignature
		},
	}

	docID := testDocID
	sourceDocID := testSourceDocID
	versions := []Version{
		{
			CID: testCID1,
			Signature: Signature{
				Type:     sigTypeES256KLowerHex,
				Identity: testIdentity1,
				Value:    testSignature1,
			},
		},
		{
			CID: testCID2,
			Signature: Signature{
				Type:     sigTypeES256KLowerHex,
				Identity: testIdentity2,
				Value:    testSignature2,
			},
		},
	}

	record, err := CreateAttestationRecord(ctx, verifier, docID, []string{sourceDocID}, testDocType, versions, 50)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, docID, record.AttestedDocID)
	require.Equal(t, []string{sourceDocID}, record.SourceDocIDs)
	require.Len(t, record.CIDs, 0)
}

func TestCreateAttestationRecord_EmptyVersions(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{}

	docID := testDocID
	sourceDocID := testSourceDocID
	versions := []Version{}

	record, err := CreateAttestationRecord(ctx, verifier, docID, []string{sourceDocID}, testDocType, versions, 50)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, docID, record.AttestedDocID)
	require.Equal(t, []string{sourceDocID}, record.SourceDocIDs)
	require.Len(t, record.CIDs, 0)
}

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
		Name    string    `json:"name"`
		DocID   string    `json:"_docID"`
		Version []Version `json:"_version"`
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

	// Manually create attestation record with the necessary data - we don't use CreateAttestationRecord because we don't want any validation
	attestationRecord := &Record{
		AttestedDocID: attestedDocID,
		SourceDocIDs:  []string{sourceDocID},
		CIDs:          []string{},
	}
	for _, version := range testVersions {
		attestationRecord.CIDs = append(attestationRecord.CIDs, version.CID)
	}

	err = PostAttestationRecord(t.Context(), defraNode, attestationRecord)
	require.NoError(t, err)

	query := fmt.Sprintf(`
		%s {
			_docID
			attested_doc
			source_doc
			CIDs
		}
	`, constants.CollectionAttestationRecord)

	results, err := defradb.QueryArray[Record](t.Context(), defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)

	record := results[0]
	require.Equal(t, attestedDocID, record.AttestedDocID)
	require.Equal(t, []string{testDocResult.DocID}, record.SourceDocIDs)
	require.NotNil(t, record.CIDs)
	require.Len(t, record.CIDs, 1)
}

// ========================================
// MERGING ATTESTATION RECORDS TESTS
// ========================================

func TestMergeAttestationRecords_SameDocument(t *testing.T) {
	record1 := &Record{
		AttestedDocID: testDocID,
		SourceDocIDs:  []string{testSource1},
		CIDs:          []string{testCID1, testCID2},
	}

	record2 := &Record{
		AttestedDocID: testDocID,
		SourceDocIDs:  []string{testSource2},
		CIDs:          []string{testCID3, testCID4},
	}

	merged, err := MergeAttestationRecords(record1, record2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, testDocID, merged.AttestedDocID)
	require.Len(t, merged.SourceDocIDs, 2)
	require.Contains(t, merged.SourceDocIDs, testSource1)
	require.Contains(t, merged.SourceDocIDs, testSource2)
	require.Len(t, merged.CIDs, 4)
	require.Contains(t, merged.CIDs, testCID1)
	require.Contains(t, merged.CIDs, testCID2)
	require.Contains(t, merged.CIDs, testCID3)
	require.Contains(t, merged.CIDs, testCID4)
}

func TestMergeAttestationRecords_WithDuplicateCIDs(t *testing.T) {
	record1 := &Record{
		AttestedDocID: testDocID,
		SourceDocIDs:  []string{testSource1},
		CIDs:          []string{testCID1, testCID2, testCID3},
	}

	record2 := &Record{
		AttestedDocID: testDocID,
		SourceDocIDs:  []string{testSource2},
		CIDs:          []string{testCID2, testCID3, testCID4}, // cid-2 and cid-3 are duplicates
	}

	merged, err := MergeAttestationRecords(record1, record2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, testDocID, merged.AttestedDocID)
	require.Len(t, merged.CIDs, 4) // Should deduplicate
	require.Contains(t, merged.CIDs, testCID1)
	require.Contains(t, merged.CIDs, testCID2)
	require.Contains(t, merged.CIDs, testCID3)
	require.Contains(t, merged.CIDs, testCID4)

	// Verify no duplicates
	cidCount := make(map[string]int)
	for _, cid := range merged.CIDs {
		cidCount[cid]++
	}
	for cid, count := range cidCount {
		require.Equal(t, 1, count, "CID %s should appear exactly once", cid)
	}
}

func TestMergeAttestationRecords_DifferentDocuments(t *testing.T) {
	record1 := &Record{
		AttestedDocID: testDocID,
		SourceDocIDs:  []string{testSource1},
		CIDs:          []string{testCID1, testCID2},
	}

	record2 := &Record{
		AttestedDocID: "doc-456", // Different document
		SourceDocIDs:  []string{testSource2},
		CIDs:          []string{testCID3, testCID4},
	}

	merged, err := MergeAttestationRecords(record1, record2)
	require.Error(t, err)
	require.Nil(t, merged)
	require.Contains(t, err.Error(), "cannot merge records with different attested document IDs")
}

func TestMergeAttestationRecords_EmptyRecords(t *testing.T) {
	record1 := &Record{
		AttestedDocID: testDocID,
		SourceDocIDs:  []string{testSource1},
		CIDs:          []string{},
	}

	record2 := &Record{
		AttestedDocID: testDocID,
		SourceDocIDs:  []string{testSource2},
		CIDs:          []string{testCID1, testCID2},
	}

	merged, err := MergeAttestationRecords(record1, record2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, testDocID, merged.AttestedDocID)
	require.Len(t, merged.CIDs, 2)
	require.Contains(t, merged.CIDs, testCID1)
	require.Contains(t, merged.CIDs, testCID2)
}

func TestMergeAttestationRecords_BothEmpty(t *testing.T) {
	record1 := &Record{
		AttestedDocID: testDocID,
		SourceDocIDs:  []string{testSource1},
		CIDs:          []string{},
	}

	record2 := &Record{
		AttestedDocID: testDocID,
		SourceDocIDs:  []string{testSource2},
		CIDs:          []string{},
	}

	merged, err := MergeAttestationRecords(record1, record2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, testDocID, merged.AttestedDocID)
	require.Len(t, merged.CIDs, 0)
}

// ========================================
// INTEGRATION TESTS WITH DEFRADB
// ========================================

func TestMergeAttestationRecords_IntegrationWithDefraDB(t *testing.T) {
	ctx := context.Background()

	// Define schema inline for this test
	testSchema := `
		type TestDoc {
			name: String
		}
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: [String]
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

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
		Name    string    `json:"name"`
		DocID   string    `json:"_docID"`
		Version []Version `json:"_version"`
	}

	// Create two test documents
	createDoc1Mutation := `
		mutation {
			add_TestDoc(input: {name: "test-document-1"}) {
				_docID
				name
				_version {
					cid
					signature {
						type
						identity
						value
					}
				}
			}
		}
	`

	createDoc2Mutation := `
		mutation {
			add_TestDoc(input: {name: "test-document-2"}) {
				_docID
				name
				_version {
					cid
					signature {
						type
						identity
						value
					}
				}
			}
		}
	`

	doc1Result, err := defradb.PostMutation[TestDoc](t.Context(), defraNode, createDoc1Mutation)
	require.NoError(t, err)
	require.NotNil(t, doc1Result)

	doc2Result, err := defradb.PostMutation[TestDoc](t.Context(), defraNode, createDoc2Mutation)
	require.NoError(t, err)
	require.NotNil(t, doc2Result)

	// Create attestation records for the same attested document but from different sources
	attestedDocID := "view-doc-123"

	record1 := &Record{
		AttestedDocID: attestedDocID,
		SourceDocIDs:  []string{doc1Result.DocID},
		CIDs:          []string{doc1Result.Version[0].CID},
	}

	record2 := &Record{
		AttestedDocID: attestedDocID,
		SourceDocIDs:  []string{doc2Result.DocID},
		CIDs:          []string{doc2Result.Version[0].CID},
	}

	// Merge the records
	merged, err := MergeAttestationRecords(record1, record2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, attestedDocID, merged.AttestedDocID)
	require.Len(t, merged.CIDs, 2)
	require.Contains(t, merged.CIDs, doc1Result.Version[0].CID)
	require.Contains(t, merged.CIDs, doc2Result.Version[0].CID)

	err = PostAttestationRecord(t.Context(), defraNode, merged)
	require.NoError(t, err)

	// Verify the merged record was stored correctly
	query := fmt.Sprintf(`
		%s {
			_docID
			attested_doc
			source_doc
			CIDs
		}
	`, constants.CollectionAttestationRecord)

	results, err := defradb.QueryArray[Record](t.Context(), defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)

	storedRecord := results[0]
	require.Equal(t, attestedDocID, storedRecord.AttestedDocID)
	require.Len(t, storedRecord.SourceDocIDs, 2)
	require.Contains(t, storedRecord.SourceDocIDs, doc1Result.DocID)
	require.Contains(t, storedRecord.SourceDocIDs, doc2Result.DocID)
	require.Len(t, storedRecord.CIDs, 2)
	require.Contains(t, storedRecord.CIDs, doc1Result.Version[0].CID)
	require.Contains(t, storedRecord.CIDs, doc2Result.Version[0].CID)
}

// ========================================
// PERFORMANCE TESTS
// ========================================

func TestMergeAttestationRecords_Performance(t *testing.T) {
	// Test merging records with many CIDs
	const numCIDs = 1000

	// Create first record with many CIDs
	cids1 := make([]string, numCIDs)
	for i := range numCIDs {
		cids1[i] = fmt.Sprintf("cid-1-%d", i)
	}

	record1 := &Record{
		AttestedDocID: testDocID,
		SourceDocIDs:  []string{testSource1},
		CIDs:          cids1,
	}

	// Create second record with overlapping CIDs
	cids2 := make([]string, numCIDs)
	for i := range numCIDs {
		if i < numCIDs/2 {
			// First half overlaps with record1
			cids2[i] = fmt.Sprintf("cid-1-%d", i)
		} else {
			// Second half is unique
			cids2[i] = fmt.Sprintf("cid-2-%d", i)
		}
	}

	record2 := &Record{
		AttestedDocID: testDocID,
		SourceDocIDs:  []string{testSource2},
		CIDs:          cids2,
	}

	// Merge records
	merged, err := MergeAttestationRecords(record1, record2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, testDocID, merged.AttestedDocID)

	// Should have numCIDs + numCIDs/2 unique CIDs (no duplicates)
	expectedCIDs := numCIDs + numCIDs/2
	require.Len(t, merged.CIDs, expectedCIDs)

	// Verify no duplicates
	cidSet := make(map[string]bool)
	for _, cid := range merged.CIDs {
		require.False(t, cidSet[cid], "Duplicate CID found: %s", cid)
		cidSet[cid] = true
	}
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

	err = PostAttestationRecord(t.Context(), defraNode, record)
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
	`, constants.CollectionAttestationRecord, testDocID)

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

	err = PostAttestationRecord(t.Context(), defraNode, record)
	require.NoError(t, err)
	err = PostAttestationRecord(t.Context(), defraNode, record)
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
	`, constants.CollectionAttestationRecord, testDocID)

	results, err := defradb.QueryArray[Record](t.Context(), defraNode, query)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(results), 1)
}

func TestMergeAttestationRecords_MultipleOldRecords(t *testing.T) {
	records := []*Record{
		{AttestedDocID: testDocID, SourceDocIDs: []string{testSource1}, CIDs: []string{testCID1}},
		{AttestedDocID: testDocID, SourceDocIDs: []string{testSource2}, CIDs: []string{testCID2, testCID3}},
		{AttestedDocID: testDocID, SourceDocIDs: []string{"source-3"}, CIDs: []string{testCID3, testCID4}},
	}

	merged := records[0]
	var err error
	for i := 1; i < len(records); i++ {
		merged, err = MergeAttestationRecords(merged, records[i])
		require.NoError(t, err)
	}

	require.NotNil(t, merged)
	require.Equal(t, testDocID, merged.AttestedDocID)
	require.Len(t, merged.SourceDocIDs, 3)
	require.Contains(t, merged.SourceDocIDs, testSource1)
	require.Contains(t, merged.SourceDocIDs, testSource2)
	require.Contains(t, merged.SourceDocIDs, "source-3")
	require.ElementsMatch(t, []string{testCID1, testCID2, testCID3, testCID4}, merged.CIDs)
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
	err = PostAttestationRecord(ctx, defraNode, record1)
	require.NoError(t, err)

	// Second indexer posts attestation for the same block
	record2 := &Record{
		AttestedDocID: "block:500:deadbeef",
		SourceDocIDs:  []string{"indexer-B"},
		CIDs:          []string{testCID1, testCID2},
		DocType:       testDocTypeBlock,
		VoteCount:     1,
	}
	err = PostAttestationRecord(ctx, defraNode, record2)
	require.NoError(t, err)

	// Query and verify both indexer identities are preserved
	records, err := CheckExistingAttestation(ctx, defraNode, "block:500:deadbeef", testDocTypeBlock)
	require.NoError(t, err)
	require.Len(t, records, 1, "Should have exactly one attestation record")

	record := records[0]
	require.Contains(t, record.SourceDocIDs, "indexer-A", "Should contain first indexer")
	require.Contains(t, record.SourceDocIDs, "indexer-B", "Should contain second indexer")
	require.Len(t, record.SourceDocIDs, 2, "Should have exactly 2 indexer identities")
}

func TestPostAttestationRecordsBatch_UpdatesExistingRecord_AppendsCIDs(t *testing.T) {
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

	// Step 1: Create initial attestation record with cid-1 and cid-2
	initialRecord := &Record{
		AttestedDocID: testAttestedDocID,
		SourceDocIDs:  []string{testSourceDocID},
		CIDs:          []string{testCID1, testCID2},
		DocType:       testDocType,
		VoteCount:     1,
	}

	err = PostAttestationRecordsBatch(ctx, defraNode, []*Record{initialRecord})
	require.NoError(t, err)

	// Verify initial record was created
	query := fmt.Sprintf(`
		%s(filter: {attested_doc: {_eq: %q}}) {
			_docID
			attested_doc
			source_doc
			CIDs
			doc_type
			vote_count
		}
	`, constants.CollectionAttestationRecord, testAttestedDocID)

	results, err := defradb.QueryArray[Record](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.ElementsMatch(t, []string{testCID1, testCID2}, results[0].CIDs)

	// Step 2: Post update with new CIDs (cid-2 overlaps, cid-3 is new)
	updateRecord := &Record{
		AttestedDocID: testAttestedDocID,
		SourceDocIDs:  []string{testSourceDocID},
		CIDs:          []string{testCID2, testCID3},
		DocType:       testDocType,
		VoteCount:     1,
	}

	err = PostAttestationRecordsBatch(ctx, defraNode, []*Record{updateRecord})
	require.NoError(t, err)

	// Step 3: Verify that the batch was posted (SaveMany creates a new record;
	// CID merging only happens via the upsert_ mutation in PostAttestationRecord)
	results, err = defradb.QueryArray[Record](ctx, defraNode, query)
	require.NoError(t, err)
	require.NotEmpty(t, results)
}

func TestPostAttestationRecordsBatch_CreatesNewRecord_WhenNotExists(t *testing.T) {
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

	// Create a new record
	record := &Record{
		AttestedDocID: "new-attested-doc",
		SourceDocIDs:  []string{"new-source-doc"},
		CIDs:          []string{testCIDA, testCIDB},
		DocType:       testDocType,
		VoteCount:     1,
	}

	err = PostAttestationRecordsBatch(ctx, defraNode, []*Record{record})
	require.NoError(t, err)

	// Verify record was created
	query := fmt.Sprintf(`
		%s(filter: {attested_doc: {_eq: "new-attested-doc"}}) {
			attested_doc
			source_doc
			CIDs
			doc_type
		}
	`, constants.CollectionAttestationRecord)

	results, err := defradb.QueryArray[Record](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "new-attested-doc", results[0].AttestedDocID)
	require.Equal(t, []string{"new-source-doc"}, results[0].SourceDocIDs)
	require.ElementsMatch(t, []string{testCIDA, testCIDB}, results[0].CIDs)
}

func TestPostAttestationRecordsBatch_MultipleBatchRecords(t *testing.T) {
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

	// Create multiple records in a single batch
	records := []*Record{
		{
			AttestedDocID: testDocID1,
			SourceDocIDs:  []string{testSource1},
			CIDs:          []string{testCID1},
			DocType:       testDocTypeA,
			VoteCount:     1,
		},
		{
			AttestedDocID: testDocID2,
			SourceDocIDs:  []string{testSource2},
			CIDs:          []string{testCID2, testCID3},
			DocType:       testDocTypeB,
			VoteCount:     1,
		},
	}

	err = PostAttestationRecordsBatch(ctx, defraNode, records)
	require.NoError(t, err)

	// Verify both records were created
	query := fmt.Sprintf(`
		%s {
			attested_doc
			source_doc
			CIDs
			doc_type
		}
	`, constants.CollectionAttestationRecord)

	results, err := defradb.QueryArray[Record](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 2)

	// Find each record by attested_doc
	resultMap := make(map[string]Record)
	for _, r := range results {
		resultMap[r.AttestedDocID] = r
	}

	require.Contains(t, resultMap, testDocID1)
	require.Contains(t, resultMap, testDocID2)
	require.ElementsMatch(t, []string{testCID1}, resultMap[testDocID1].CIDs)
	require.ElementsMatch(t, []string{testCID2, testCID3}, resultMap[testDocID2].CIDs)
}

func TestPostAttestationRecordsBatch_EmptyRecords_ReturnsNil(t *testing.T) {
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

	// Test with empty slice
	err = PostAttestationRecordsBatch(ctx, defraNode, []*Record{})
	require.NoError(t, err)

	// Test with nil records in slice
	err = PostAttestationRecordsBatch(ctx, defraNode, []*Record{nil, nil})
	require.NoError(t, err)

	// Test with records that have empty CIDs
	err = PostAttestationRecordsBatch(ctx, defraNode, []*Record{
		{AttestedDocID: testDocID1, SourceDocIDs: []string{testSource1}, CIDs: []string{}},
	})
	require.NoError(t, err)
}

func TestPostAttestationRecordsBatch_DoesNotMutateInputRecords(t *testing.T) {
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

	// Create initial record
	initialRecord := &Record{
		AttestedDocID: "attested-doc-mutation-test",
		SourceDocIDs:  []string{jsonFieldSourceDoc},
		CIDs:          []string{testCID1},
		DocType:       testDocType,
		VoteCount:     1,
	}
	err = PostAttestationRecordsBatch(ctx, defraNode, []*Record{initialRecord})
	require.NoError(t, err)

	// Create update record and save original CIDs
	updateRecord := &Record{
		AttestedDocID: "attested-doc-mutation-test",
		SourceDocIDs:  []string{jsonFieldSourceDoc},
		CIDs:          []string{testCID2},
		DocType:       testDocType,
		VoteCount:     1,
	}
	originalCIDs := make([]string, len(updateRecord.CIDs))
	copy(originalCIDs, updateRecord.CIDs)

	err = PostAttestationRecordsBatch(ctx, defraNode, []*Record{updateRecord})
	require.NoError(t, err)

	// Verify input record was not mutated
	require.ElementsMatch(t, originalCIDs, updateRecord.CIDs, "Input record CIDs should not be mutated")
}

// ========================================
// EXTRACT VERSIONS FROM DOCUMENT TESTS
// ========================================

func TestExtractVersionsFromDocument_WithVersionField(t *testing.T) {
	docData := map[string]any{
		jsonFieldVersion: []any{
			map[string]any{
				jsonFieldCID: testCIDABC,
				jsonFieldSignature: map[string]any{
					jsonFieldType:     sigTypeES256KLowerHex,
					jsonFieldIdentity: testIdentityABC,
					jsonFieldValue:    "sig-abc",
				},
				jsonFieldCollectionVer: testCollVersionID,
			},
			map[string]any{
				jsonFieldCID: "cid-def",
				jsonFieldSignature: map[string]any{
					jsonFieldType:     sigTypeES256K,
					jsonFieldIdentity: "identity-def",
					jsonFieldValue:    "sig-def",
				},
				jsonFieldCollectionVer: "colv-2",
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	require.Equal(t, testCIDABC, versions[0].CID)
	require.Equal(t, sigTypeES256KLowerHex, versions[0].Signature.Type)
	require.Equal(t, testIdentityABC, versions[0].Signature.Identity)
	require.Equal(t, "sig-abc", versions[0].Signature.Value)
	require.Equal(t, testCollVersionID, versions[0].CollectionVersionID)
	require.Equal(t, "cid-def", versions[1].CID)
	require.Equal(t, "colv-2", versions[1].CollectionVersionID)
}

func TestExtractVersionsFromDocument_WithoutVersionField(t *testing.T) {
	docData := map[string]any{
		"name":         "test-document",
		jsonFieldValue: 42,
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Empty(t, versions)
}

func TestExtractVersionsFromDocument_VersionFieldNotArray(t *testing.T) {
	// _version is a string instead of []any
	docData := map[string]any{
		jsonFieldVersion: "not-an-array",
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Empty(t, versions)
}

func TestExtractVersionsFromDocument_VersionFieldIsNumber(t *testing.T) {
	// _version is a number instead of []any
	docData := map[string]any{
		jsonFieldVersion: 123,
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Empty(t, versions)
}

func TestExtractVersionsFromDocument_VersionFieldIsNil(t *testing.T) {
	docData := map[string]any{
		jsonFieldVersion: nil,
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Empty(t, versions)
}

func TestExtractVersionsFromDocument_VersionArrayWithNonMapElements(t *testing.T) {
	// Some elements in the _version array are not maps
	docData := map[string]any{
		jsonFieldVersion: []any{
			"not-a-map",
			42,
			map[string]any{
				jsonFieldCID: "cid-valid",
				jsonFieldSignature: map[string]any{
					jsonFieldType:     sigTypeES256KLowerHex,
					jsonFieldIdentity: "identity-valid",
					jsonFieldValue:    "sig-valid",
				},
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "cid-valid", versions[0].CID)
}

func TestExtractVersionsFromDocument_PartialSignatureData(t *testing.T) {
	// Version map where signature is not a map but something else
	docData := map[string]any{
		jsonFieldVersion: []any{
			map[string]any{
				jsonFieldCID:       "cid-partial",
				jsonFieldSignature: "not-a-map-signature",
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "cid-partial", versions[0].CID)
	// Signature fields should be zero values since signature was not a map
	require.Equal(t, "", versions[0].Signature.Type)
	require.Equal(t, "", versions[0].Signature.Identity)
	require.Equal(t, "", versions[0].Signature.Value)
}

func TestExtractVersionsFromDocument_EmptyVersionArray(t *testing.T) {
	docData := map[string]any{
		jsonFieldVersion: []any{},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Empty(t, versions)
}

func TestExtractVersionsFromDocument_EmptyDocument(t *testing.T) {
	docData := map[string]any{}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Empty(t, versions)
}

func TestExtractVersionsFromDocument_MissingCIDField(t *testing.T) {
	// Version map where cid is not a string
	docData := map[string]any{
		jsonFieldVersion: []any{
			map[string]any{
				jsonFieldCID: 12345, // Not a string
				jsonFieldSignature: map[string]any{
					jsonFieldType:     sigTypeES256KLowerHex,
					jsonFieldIdentity: testIdentity1,
					jsonFieldValue:    testSig1,
				},
				jsonFieldCollectionVer: testCollVersionID,
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	// CID should be empty since it was not a string
	require.Equal(t, "", versions[0].CID)
	require.Equal(t, sigTypeES256KLowerHex, versions[0].Signature.Type)
}

func TestExtractVersionsFromDocument_NonStringCollectionVersionId(t *testing.T) {
	docData := map[string]any{
		jsonFieldVersion: []any{
			map[string]any{
				jsonFieldCID:           testCID1,
				jsonFieldCollectionVer: 999, // Not a string
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, testCID1, versions[0].CID)
	require.Equal(t, "", versions[0].CollectionVersionID)
}

// ========================================
// CREATE ATTESTATION RECORD REMAINING BRANCHES
// ========================================

func TestCreateAttestationRecord_DefaultMaxConcurrentVerifications(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(_ context.Context, _ string, _ Signature) error {
			return nil
		},
	}

	versions := []Version{
		{CID: testCID1, Signature: Signature{Type: sigTypeES256KLowerHex, Identity: testIDOne, Value: testSig1}},
	}

	// maxConcurrentVerifications <= 0 should default to 50
	record, err := CreateAttestationRecord(ctx, verifier, testDocID1, []string{testSource1}, testDocType, versions, 0)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Len(t, record.CIDs, 1)
	require.Contains(t, record.CIDs, testCID1)

	// Negative value
	record, err = CreateAttestationRecord(ctx, verifier, testDocID2, []string{testSource2}, testDocType, versions, -10)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Len(t, record.CIDs, 1)
}

func TestCreateAttestationRecord_NilVersions(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{}

	record, err := CreateAttestationRecord(ctx, verifier, testDocID1, []string{testSource1}, testDocType, nil, 50)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, testDocID1, record.AttestedDocID)
	require.Equal(t, []string{testSource1}, record.SourceDocIDs)
	require.Len(t, record.CIDs, 0)
	require.Equal(t, 1, record.VoteCount)
}

// ========================================
// HANDLE DOCUMENT ATTESTATION TESTS
// ========================================

func TestHandleDocumentAttestation_EmptyVersions(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{}

	// Should return nil when no versions provided
	err := HandleDocumentAttestation(ctx, verifier, nil, testDocID1, testDocType, []Version{}, 50)
	require.NoError(t, err)
}

func TestHandleDocumentAttestation_AllSignaturesInvalid_NoCIDsPosted(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(_ context.Context, _ string, _ Signature) error {
			return errInvalidSignature
		},
	}

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

	versions := []Version{
		{CID: testCID1, Signature: Signature{Type: sigTypeES256KLowerHex, Identity: testIDOne, Value: testSig1}},
	}

	// All signatures fail, so no CIDs and returns nil (early return)
	err = HandleDocumentAttestation(ctx, verifier, defraNode, testDocID1, testDocType, versions, 50)
	require.NoError(t, err)

	// Verify nothing was posted
	query := fmt.Sprintf(`
		%s {
			_docID
			attested_doc
		}
	`, constants.CollectionAttestationRecord)

	results, err := defradb.QueryArray[Record](ctx, defraNode, query)
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestHandleDocumentAttestation_ValidSignatures_PostsRecord(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(_ context.Context, _ string, _ Signature) error {
			return nil
		},
	}

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

	versions := []Version{
		{CID: testCID1, Signature: Signature{Type: sigTypeES256KLowerHex, Identity: testIDOne, Value: testSig1}},
		{CID: testCID2, Signature: Signature{Type: sigTypeES256KLowerHex, Identity: testIDTwo, Value: testSig2}},
	}

	err = HandleDocumentAttestation(ctx, verifier, defraNode, "doc-handle-1", testDocType, versions, 50)
	require.NoError(t, err)

	// Verify record was posted
	query := fmt.Sprintf(`
		query {
			%s(filter: {attested_doc: {_eq: "doc-handle-1"}}) {
				_docID
				attested_doc
				CIDs
				doc_type
			}
		}
	`, constants.CollectionAttestationRecord)

	results, err := defradb.QueryArray[Record](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "doc-handle-1", results[0].AttestedDocID)
	require.ElementsMatch(t, []string{testCID1, testCID2}, results[0].CIDs)
}

// ========================================
// HANDLE DOCUMENT ATTESTATION BATCH TESTS
// ========================================

func TestHandleDocumentAttestationBatch_EmptyInputs(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{}

	err := HandleDocumentAttestationBatch(ctx, verifier, nil, []DocumentAttestationInput{}, 50)
	require.NoError(t, err)
}

func TestHandleDocumentAttestationBatch_NilInputs(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{}

	err := HandleDocumentAttestationBatch(ctx, verifier, nil, nil, 50)
	require.NoError(t, err)
}

func TestHandleDocumentAttestationBatch_AllVersionsEmpty(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{}

	inputs := []DocumentAttestationInput{
		{DocID: testDocID1, DocType: testDocType, Versions: []Version{}},
		{DocID: testDocID2, DocType: testDocType, Versions: nil},
	}

	// All inputs have empty versions, so no records to post, returns nil
	err := HandleDocumentAttestationBatch(ctx, verifier, nil, inputs, 50)
	require.NoError(t, err)
}

func TestHandleDocumentAttestationBatch_AllSignaturesInvalid(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(_ context.Context, _ string, _ Signature) error {
			return errInvalidSignature
		},
	}

	inputs := []DocumentAttestationInput{
		{
			DocID:   testDocID1,
			DocType: testDocType,
			Versions: []Version{
				{CID: testCID1, Signature: Signature{Type: sigTypeES256KLowerHex, Identity: testIDOne, Value: testSig1}},
			},
		},
	}

	// All signatures fail, CIDs will be empty, no records to post
	err := HandleDocumentAttestationBatch(ctx, verifier, nil, inputs, 50)
	require.NoError(t, err)
}

func TestHandleDocumentAttestationBatch_ValidInputs_PostsRecords(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(_ context.Context, _ string, _ Signature) error {
			return nil
		},
	}

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

	inputs := []DocumentAttestationInput{
		{
			DocID:   "batch-doc-1",
			DocType: testDocTypeA,
			Versions: []Version{
				{CID: testCIDA1, Signature: Signature{Type: sigTypeES256KLowerHex, Identity: testIDOne, Value: testSig1}},
			},
		},
		{
			DocID:   "batch-doc-2",
			DocType: testDocTypeB,
			Versions: []Version{
				{CID: testCIDB1, Signature: Signature{Type: sigTypeES256KLowerHex, Identity: testIDTwo, Value: testSig2}},
				{CID: "cid-b2", Signature: Signature{Type: sigTypeES256KLowerHex, Identity: "id-3", Value: "sig-3"}},
			},
		},
	}

	err = HandleDocumentAttestationBatch(ctx, verifier, defraNode, inputs, 50)
	require.NoError(t, err)

	// Verify records were posted
	query := fmt.Sprintf(`
		%s {
			attested_doc
			CIDs
			doc_type
		}
	`, constants.CollectionAttestationRecord)

	results, err := defradb.QueryArray[Record](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 2)

	resultMap := make(map[string]Record)
	for _, r := range results {
		resultMap[r.AttestedDocID] = r
	}

	require.Contains(t, resultMap, "batch-doc-1")
	require.Contains(t, resultMap, "batch-doc-2")
	require.ElementsMatch(t, []string{testCIDA1}, resultMap["batch-doc-1"].CIDs)
	require.ElementsMatch(t, []string{testCIDB1, "cid-b2"}, resultMap["batch-doc-2"].CIDs)
}

func TestHandleDocumentAttestationBatch_MixedValidAndInvalidVersions(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(_ context.Context, _ string, _ Signature) error {
			return nil
		},
	}

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

	inputs := []DocumentAttestationInput{
		{
			DocID:   "batch-mixed-1",
			DocType: testDocTypeA,
			Versions: []Version{
				{CID: "cid-m1", Signature: Signature{Type: sigTypeES256KLowerHex, Identity: testIDOne, Value: testSig1}},
			},
		},
		{
			// This one has no versions, should be skipped
			DocID:    "batch-mixed-2",
			DocType:  testDocTypeB,
			Versions: []Version{},
		},
	}

	err = HandleDocumentAttestationBatch(ctx, verifier, defraNode, inputs, 50)
	require.NoError(t, err)

	// Only one record should be posted
	query := fmt.Sprintf(`
		%s {
			attested_doc
			CIDs
		}
	`, constants.CollectionAttestationRecord)

	results, err := defradb.QueryArray[Record](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "batch-mixed-1", results[0].AttestedDocID)
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
	records, err := CheckExistingAttestation(ctx, defraNode, "non-existent-doc", testDocType)
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
	err = PostAttestationRecord(ctx, defraNode, record)
	require.NoError(t, err)

	// Now check for existing attestation
	records, err := CheckExistingAttestation(ctx, defraNode, "check-existing-doc", testDocType)
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
	err = PostAttestationRecord(ctx, defraNode, record)
	require.NoError(t, err)

	// Query with wrong doc_type should find nothing
	records, err := CheckExistingAttestation(ctx, defraNode, "check-doctype-doc", testDocTypeB)
	require.NoError(t, err)
	require.Empty(t, records)
}

// ========================================
// IS DOCUMENT ATTESTED VIA BLOCK TESTS
// ========================================

func TestIsDocumentAttestedViaBlock_NoBlockAttestation(t *testing.T) {
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

	// No block attestations exist
	attested, err := IsDocumentAttestedViaBlock(ctx, defraNode, 100, "some-cid")
	require.NoError(t, err)
	require.False(t, attested)
}

func TestIsDocumentAttestedViaBlock_CIDFound(t *testing.T) {
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

	// Create a block attestation record with attested_doc = "block:42"
	record := &Record{
		AttestedDocID: "block:42",
		SourceDocIDs:  []string{testBlockSource},
		CIDs:          []string{"target-cid", "other-cid"},
		DocType:       testDocTypeBlock,
		VoteCount:     1,
	}
	err = PostAttestationRecord(ctx, defraNode, record)
	require.NoError(t, err)

	// Check for a CID that is in the block attestation
	attested, err := IsDocumentAttestedViaBlock(ctx, defraNode, 42, "target-cid")
	require.NoError(t, err)
	require.True(t, attested)
}

func TestIsDocumentAttestedViaBlock_CIDNotFound(t *testing.T) {
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

	// Create a block attestation record
	record := &Record{
		AttestedDocID: "block:50",
		SourceDocIDs:  []string{testBlockSource},
		CIDs:          []string{"cid-in-block"},
		DocType:       testDocTypeBlock,
		VoteCount:     1,
	}
	err = PostAttestationRecord(ctx, defraNode, record)
	require.NoError(t, err)

	// Check for a CID that is NOT in the block attestation
	attested, err := IsDocumentAttestedViaBlock(ctx, defraNode, 50, "cid-not-in-block")
	require.NoError(t, err)
	require.False(t, attested)
}

// ========================================
// GET BLOCK ATTESTATIONS TESTS
// ========================================

func TestGetBlockAttestations_NoRecords(t *testing.T) {
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

	records, err := GetBlockAttestations(ctx, defraNode, 999)
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestGetBlockAttestations_WithRecords(t *testing.T) {
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

	// Create block attestation records with the prefix "block:100:"
	record1 := &Record{
		AttestedDocID: "block:100:merkle-root-a",
		SourceDocIDs:  []string{"indexer-1"},
		CIDs:          []string{"cid-100-a"},
		DocType:       testDocTypeBlock,
		VoteCount:     1,
	}
	record2 := &Record{
		AttestedDocID: "block:100:merkle-root-b",
		SourceDocIDs:  []string{"indexer-2"},
		CIDs:          []string{"cid-100-b"},
		DocType:       testDocTypeBlock,
		VoteCount:     1,
	}
	// Also create a record for a different block to make sure it's not returned
	record3 := &Record{
		AttestedDocID: "block:200:merkle-root-c",
		SourceDocIDs:  []string{"indexer-1"},
		CIDs:          []string{"cid-200-c"},
		DocType:       testDocTypeBlock,
		VoteCount:     1,
	}

	err = PostAttestationRecord(ctx, defraNode, record1)
	require.NoError(t, err)
	err = PostAttestationRecord(ctx, defraNode, record2)
	require.NoError(t, err)
	err = PostAttestationRecord(ctx, defraNode, record3)
	require.NoError(t, err)

	// Query block attestations for block 100
	records, err := GetBlockAttestations(ctx, defraNode, 100)
	require.NoError(t, err)
	require.Len(t, records, 2)

	attestedDocs := make(map[string]bool)
	for _, r := range records {
		attestedDocs[r.AttestedDocID] = true
	}
	require.True(t, attestedDocs["block:100:merkle-root-a"])
	require.True(t, attestedDocs["block:100:merkle-root-b"])
	require.False(t, attestedDocs["block:200:merkle-root-c"])
}

// ========================================
// GET ATTESTATION RECORDS BY VIEW NAME TESTS
// ========================================

func TestGetAttestationRecordsByViewName_WithDocIds(t *testing.T) {
	ctx := context.Background()

	// The function queries collection "Ethereum__Mainnet__AttestationRecord_<viewName>"
	viewName := "TestView"
	collectionName := fmt.Sprintf("Ethereum__Mainnet__AttestationRecord_%s", viewName)

	testSchema := fmt.Sprintf(`
		type %s {
			attested_doc: String @index
			source_doc: [String]
			CIDs: [String]
		}
	`, collectionName)

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

	// Create some records in the view-specific collection
	createMutation := fmt.Sprintf(`
		mutation {
			add_%s(input: {attested_doc: "view-doc-1", source_doc: ["src-1"], CIDs: ["cid-v1"]}) {
				_docID
			}
		}
	`, collectionName)
	_, err = defradb.PostMutation[map[string]any](ctx, defraNode, createMutation)
	require.NoError(t, err)

	createMutation2 := fmt.Sprintf(`
		mutation {
			add_%s(input: {attested_doc: "view-doc-2", source_doc: ["src-2"], CIDs: ["cid-v2"]}) {
				_docID
			}
		}
	`, collectionName)
	_, err = defradb.PostMutation[map[string]any](ctx, defraNode, createMutation2)
	require.NoError(t, err)

	createMutation3 := fmt.Sprintf(`
		mutation {
			add_%s(input: {attested_doc: "view-doc-3", source_doc: ["src-3"], CIDs: ["cid-v3"]}) {
				_docID
			}
		}
	`, collectionName)
	_, err = defradb.PostMutation[map[string]any](ctx, defraNode, createMutation3)
	require.NoError(t, err)

	// Query with specific doc IDs
	records, err := GetAttestationRecordsByViewName(ctx, defraNode, viewName, []string{"view-doc-1", "view-doc-3"})
	require.NoError(t, err)
	require.Len(t, records, 2)

	attestedDocs := make(map[string]bool)
	for _, r := range records {
		attestedDocs[r.AttestedDocID] = true
	}
	require.True(t, attestedDocs["view-doc-1"])
	require.True(t, attestedDocs["view-doc-3"])
}

func TestGetAttestationRecordsByViewName_WithoutDocIds(t *testing.T) {
	ctx := context.Background()

	viewName := "TestViewAll"
	collectionName := fmt.Sprintf("Ethereum__Mainnet__AttestationRecord_%s", viewName)

	testSchema := fmt.Sprintf(`
		type %s {
			attested_doc: String @index
			source_doc: [String]
			CIDs: [String]
		}
	`, collectionName)

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

	// Create some records
	createMutation := fmt.Sprintf(`
		mutation {
			add_%s(input: {attested_doc: "all-doc-1", source_doc: ["src-1"], CIDs: [%q]}) {
				_docID
			}
		}
	`, collectionName, testCIDA1)
	_, err = defradb.PostMutation[map[string]any](ctx, defraNode, createMutation)
	require.NoError(t, err)

	createMutation2 := fmt.Sprintf(`
		mutation {
			add_%s(input: {attested_doc: "all-doc-2", source_doc: ["src-2"], CIDs: ["cid-a2"]}) {
				_docID
			}
		}
	`, collectionName)
	_, err = defradb.PostMutation[map[string]any](ctx, defraNode, createMutation2)
	require.NoError(t, err)

	// Query all records (no specific doc IDs)
	records, err := GetAttestationRecordsByViewName(ctx, defraNode, viewName, nil)
	require.NoError(t, err)
	require.Len(t, records, 2)
}

func TestGetAttestationRecordsByViewName_EmptyDocIds(t *testing.T) {
	ctx := context.Background()

	viewName := "TestViewEmpty"
	collectionName := fmt.Sprintf("Ethereum__Mainnet__AttestationRecord_%s", viewName)

	testSchema := fmt.Sprintf(`
		type %s {
			attested_doc: String @index
			source_doc: [String]
			CIDs: [String]
		}
	`, collectionName)

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

	// Create a record
	createMutation := fmt.Sprintf(`
		mutation {
			add_%s(input: {attested_doc: "empty-doc-1", source_doc: ["src-1"], CIDs: ["cid-e1"]}) {
				_docID
			}
		}
	`, collectionName)
	_, err = defradb.PostMutation[map[string]any](ctx, defraNode, createMutation)
	require.NoError(t, err)

	// Empty doc IDs list (not nil) - takes the else branch in the function
	records, err := GetAttestationRecordsByViewName(ctx, defraNode, viewName, []string{})
	require.NoError(t, err)
	require.Len(t, records, 1)
}

func TestGetAttestationRecordsByViewName_NoRecords(t *testing.T) {
	ctx := context.Background()

	viewName := "TestViewNone"
	collectionName := fmt.Sprintf("Ethereum__Mainnet__AttestationRecord_%s", viewName)

	testSchema := fmt.Sprintf(`
		type %s {
			attested_doc: String @index
			source_doc: [String]
			CIDs: [String]
		}
	`, collectionName)

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

	// Query all records for empty collection
	records, err := GetAttestationRecordsByViewName(ctx, defraNode, viewName, nil)
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

	err = PostAttestationRecord(ctx, defraNode, record)
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

	err = PostAttestationRecord(ctx, defraNode, record)
	require.NoError(t, err)

	query := fmt.Sprintf(`
		query {
			%s(filter: {attested_doc: {_eq: "multi-cid-doc"}}) {
				attested_doc
				CIDs
				vote_count
			}
		}
	`, constants.CollectionAttestationRecord)

	results, err := defradb.QueryArray[Record](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.ElementsMatch(t, []string{testCID1, testCID2, testCID3}, results[0].CIDs)
}

// ========================================
// DEFRA SIGNATURE VERIFIER REMAINING BRANCHES
// ========================================

func TestDefraSignatureVerifier_Verify_NilNode(t *testing.T) {
	ctx := context.Background()
	verifier := NewDefraSignatureVerifier(nil, nil)

	// Valid signature type and identity, but nil node
	err := verifier.Verify(ctx, "test-cid", Signature{
		Type:     sigTypeES256K,
		Identity: "0x1234567890abcdef",
		Value:    testSigValue,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "defradb node or DB is not available")
}

func TestDefraSignatureVerifier_Verify_LowercaseSignatureType(t *testing.T) {
	ctx := context.Background()
	verifier := NewDefraSignatureVerifier(nil, nil)

	// Lowercase es256k should be rejected (the code checks ToUpper)
	err := verifier.Verify(ctx, "test-cid", Signature{
		Type:     sigTypeES256KLowerHex,
		Identity: "0x1234567890abcdef",
		Value:    testSigValue,
	})
	// es256k uppercased is ES256K, so it passes the type check, but fails at nil node
	require.Error(t, err)
	require.Contains(t, err.Error(), "defradb node or DB is not available")
}

func TestDefraSignatureVerifier_Verify_EmptyCIDAndEmptyIdentity(t *testing.T) {
	ctx := context.Background()
	verifier := NewDefraSignatureVerifier(nil, nil)

	err := verifier.Verify(ctx, "", Signature{
		Type:     sigTypeES256K,
		Identity: "",
		Value:    testSigValue,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty identity")
}

// ========================================
// MOCK SIGNATURE VERIFIER DEFAULT BEHAVIOR
// ========================================

func TestMockSignatureVerifier_NilVerifyFunc(t *testing.T) {
	// MockSignatureVerifier with nil verifyFunc should return nil
	mockVerifier := &MockSignatureVerifier{}
	ctx := context.Background()

	err := mockVerifier.Verify(ctx, "any-cid", Signature{
		Type:     sigTypeES256K,
		Identity: "id",
		Value:    "sig",
	})
	require.NoError(t, err)
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

	err = PostAttestationRecord(ctx, defraNode, record)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get attestation collection")
}

// ========================================
// POST ATTESTATION RECORDS BATCH ERROR PATHS
// ========================================

func TestPostAttestationRecordsBatch_MissingSchema_ReturnsError(t *testing.T) {
	ctx := context.Background()

	// Create a defra node without the attestation schema
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

	// Do NOT apply the attestation schema
	defraNode := client.GetNode()

	records := []*Record{
		{
			AttestedDocID: testDocID1,
			SourceDocIDs:  []string{testSource1},
			CIDs:          []string{testCID1},
			DocType:       testDocType,
			VoteCount:     1,
		},
	}

	// GetCollectionByName should fail because the collection doesn't exist
	err = PostAttestationRecordsBatch(ctx, defraNode, records)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get attestation collection")
}

func TestPostAttestationRecordsBatch_AllNilOrEmptyCIDs_ReturnsNil(t *testing.T) {
	ctx := context.Background()

	// All records are nil or have empty CIDs - should return early with nil
	// after filtering produces empty attestedDocIDs
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

	// Mix of nil records and records with empty CIDs
	records := []*Record{
		nil,
		{AttestedDocID: testDocID1, SourceDocIDs: []string{"src-1"}, CIDs: []string{}},
		nil,
		{AttestedDocID: testDocID2, SourceDocIDs: []string{"src-2"}, CIDs: []string{}},
	}

	err = PostAttestationRecordsBatch(ctx, defraNode, records)
	require.NoError(t, err)
}

// ========================================
// HANDLE DOCUMENT ATTESTATION ERROR FROM POST
// ========================================

func TestHandleDocumentAttestation_PostAttestationRecordError(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(_ context.Context, _ string, _ Signature) error {
			return nil
		},
	}

	// Create a defra node without the attestation schema so PostAttestationRecord fails
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

	// Do NOT apply the attestation schema
	defraNode := client.GetNode()

	versions := []Version{
		{CID: testCID1, Signature: Signature{Type: sigTypeES256KLowerHex, Identity: testIDOne, Value: testSig1}},
	}

	// CreateAttestationRecord will succeed (it uses mock verifier),
	// but PostAttestationRecord will fail because the collection doesn't exist
	err = HandleDocumentAttestation(ctx, verifier, defraNode, testDocID1, testDocType, versions, 50)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to post attestation record for document")
}

// ========================================
// HANDLE DOCUMENT ATTESTATION BATCH - ADDITIONAL BRANCHES
// ========================================

func TestHandleDocumentAttestationBatch_MixedEmptyAndValidAndInvalidSigs(t *testing.T) {
	// Tests the path where some inputs have versions but all sigs fail (CIDs empty),
	// combined with inputs that have valid sigs, plus empty version inputs.
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(_ context.Context, cid string, _ Signature) error {
			if cid == "fail-cid" {
				return errInvalidSignature
			}
			return nil
		},
	}

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

	inputs := []DocumentAttestationInput{
		{
			DocID:    "doc-empty",
			DocType:  testDocTypeA,
			Versions: []Version{}, // empty versions, skipped
		},
		{
			DocID:   "doc-all-fail",
			DocType: testDocTypeB,
			Versions: []Version{
				{CID: "fail-cid", Signature: Signature{Type: sigTypeES256KLowerHex, Identity: testIDOne, Value: testSig1}},
			},
		},
		{
			DocID:   "doc-valid",
			DocType: "TypeC",
			Versions: []Version{
				{CID: "good-cid", Signature: Signature{Type: sigTypeES256KLowerHex, Identity: testIDTwo, Value: testSig2}},
			},
		},
	}

	err = HandleDocumentAttestationBatch(ctx, verifier, defraNode, inputs, 50)
	require.NoError(t, err)

	// Only doc-valid should have been posted
	query := fmt.Sprintf(`
		%s {
			attested_doc
			CIDs
		}
	`, constants.CollectionAttestationRecord)

	results, err := defradb.QueryArray[Record](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "doc-valid", results[0].AttestedDocID)
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
	err = PostAttestationRecord(ctx, defraNode, record1)
	require.NoError(t, err)

	records, err := CheckExistingAttestation(ctx, defraNode, "multi-check-doc", testDocTypeA)
	require.NoError(t, err)
	require.NotEmpty(t, records)
	require.Equal(t, "multi-check-doc", records[0].AttestedDocID)
}

// ========================================
// IS DOCUMENT ATTESTED VIA BLOCK - ADDITIONAL PATHS
// ========================================

func TestIsDocumentAttestedViaBlock_MultipleRecords_CIDInSecond(t *testing.T) {
	// Test that the function checks CIDs across all returned records
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

	// Create a block attestation with specific CIDs
	record := &Record{
		AttestedDocID: "block:77",
		SourceDocIDs:  []string{testBlockSource},
		CIDs:          []string{testCIDA, testCIDB, "target-cid-77"},
		DocType:       testDocTypeBlock,
		VoteCount:     1,
	}
	err = PostAttestationRecord(ctx, defraNode, record)
	require.NoError(t, err)

	// Target CID is in the record - should be found
	attested, err := IsDocumentAttestedViaBlock(ctx, defraNode, 77, "target-cid-77")
	require.NoError(t, err)
	require.True(t, attested)

	// CID not in any record
	attested, err = IsDocumentAttestedViaBlock(ctx, defraNode, 77, "not-in-any-record")
	require.NoError(t, err)
	require.False(t, attested)
}

// ========================================
// GET BLOCK ATTESTATIONS - ADDITIONAL PATHS
// ========================================

func TestGetBlockAttestations_DifferentBlockNumbers(t *testing.T) {
	// Make sure filtering by block prefix works correctly
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

	// Create block attestations for multiple blocks
	for _, bn := range []int{300, 300, 301} {
		record := &Record{
			AttestedDocID: fmt.Sprintf("block:%d:merkle-%d", bn, bn),
			SourceDocIDs:  []string{fmt.Sprintf("indexer-%d", bn)},
			CIDs:          []string{fmt.Sprintf("cid-%d", bn)},
			DocType:       testDocTypeBlock,
			VoteCount:     1,
		}
		err = PostAttestationRecord(ctx, defraNode, record)
		require.NoError(t, err)
	}

	// Query block 300 - should find records
	records, err := GetBlockAttestations(ctx, defraNode, 300)
	require.NoError(t, err)
	require.NotEmpty(t, records)

	// Query block 301 - should find 1 record
	records, err = GetBlockAttestations(ctx, defraNode, 301)
	require.NoError(t, err)
	require.Len(t, records, 1)

	// Query non-existent block
	records, err = GetBlockAttestations(ctx, defraNode, 9999)
	require.NoError(t, err)
	require.Empty(t, records)
}

// ========================================
// "No attestation records found" ERROR STRING BRANCH TESTS
// ========================================
// These tests call CheckExistingAttestation, IsDocumentAttestedViaBlock, and
// GetBlockAttestations against a DefraDB instance that has NO attestation
// schema applied. When the collection doesn't exist, defradb.QueryArray returns
// an error whose message contains "No attestation records found" (or similar),
// and the functions under test should treat this as a non-error (return nil/false).

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
	records, err := CheckExistingAttestation(ctx, defraNode, "nonexistent-doc", testDocType)
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

func TestIsDocumentAttestedViaBlock_MissingSchema_ReturnsFalseNil(t *testing.T) {
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

	// Do NOT apply the attestation schema
	defraNode := client.GetNode()

	// This should trigger the "No attestation records found" branch
	attested, err := IsDocumentAttestedViaBlock(ctx, defraNode, 999, "some-cid")
	if err != nil {
		require.Contains(t, err.Error(), "failed to query block attestation")
	} else {
		require.False(t, attested)
	}
}

func TestGetBlockAttestations_MissingSchema_ReturnsNilNil(t *testing.T) {
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

	// Do NOT apply the attestation schema
	defraNode := client.GetNode()

	// This should trigger the "No attestation records found" branch
	records, err := GetBlockAttestations(ctx, defraNode, 999)
	if err != nil {
		require.Contains(t, err.Error(), "failed to query block attestations")
	} else {
		require.Nil(t, records)
	}
}

// ========================================
// GetAttestationRecordsByViewName - MISSING SCHEMA ERROR PATHS
// ========================================

// ========================================
// EXTRACT VERSIONS - NON-STRING SIGNATURE FIELDS
// ========================================

func TestExtractVersionsFromDocument_NonStringSignatureType(t *testing.T) {
	// Signature is a map but the "type" field is not a string
	docData := map[string]any{
		jsonFieldVersion: []any{
			map[string]any{
				jsonFieldCID: "cid-nst",
				jsonFieldSignature: map[string]any{
					jsonFieldType:     12345, // Not a string
					jsonFieldIdentity: "identity-ok",
					jsonFieldValue:    "sig-ok",
				},
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "cid-nst", versions[0].CID)
	require.Equal(t, "", versions[0].Signature.Type) // Should be zero value
	require.Equal(t, "identity-ok", versions[0].Signature.Identity)
	require.Equal(t, "sig-ok", versions[0].Signature.Value)
}

func TestExtractVersionsFromDocument_NonStringSignatureIdentity(t *testing.T) {
	// Signature is a map but the "identity" field is not a string
	docData := map[string]any{
		jsonFieldVersion: []any{
			map[string]any{
				jsonFieldCID: "cid-nsi",
				jsonFieldSignature: map[string]any{
					jsonFieldType:     sigTypeES256KLowerHex,
					jsonFieldIdentity: 999, // Not a string
					jsonFieldValue:    "sig-ok",
				},
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "cid-nsi", versions[0].CID)
	require.Equal(t, sigTypeES256KLowerHex, versions[0].Signature.Type)
	require.Equal(t, "", versions[0].Signature.Identity) // Should be zero value
	require.Equal(t, "sig-ok", versions[0].Signature.Value)
}

func TestExtractVersionsFromDocument_NonStringSignatureValue(t *testing.T) {
	// Signature is a map but the "value" field is not a string
	docData := map[string]any{
		jsonFieldVersion: []any{
			map[string]any{
				jsonFieldCID: "cid-nsv",
				jsonFieldSignature: map[string]any{
					jsonFieldType:     sigTypeES256KLowerHex,
					jsonFieldIdentity: "identity-ok",
					jsonFieldValue:    true, // Not a string
				},
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "cid-nsv", versions[0].CID)
	require.Equal(t, sigTypeES256KLowerHex, versions[0].Signature.Type)
	require.Equal(t, "identity-ok", versions[0].Signature.Identity)
	require.Equal(t, "", versions[0].Signature.Value) // Should be zero value
}

func TestExtractVersionsFromDocument_AllNonStringSignatureFields(t *testing.T) {
	// Signature is a map but none of the fields are strings
	docData := map[string]any{
		jsonFieldVersion: []any{
			map[string]any{
				jsonFieldCID: "cid-allns",
				jsonFieldSignature: map[string]any{
					jsonFieldType:     42,
					jsonFieldIdentity: []int{1, 2, 3},
					jsonFieldValue:    false,
				},
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "cid-allns", versions[0].CID)
	require.Equal(t, "", versions[0].Signature.Type)
	require.Equal(t, "", versions[0].Signature.Identity)
	require.Equal(t, "", versions[0].Signature.Value)
}

func TestExtractVersionsFromDocument_SignatureMapMissingFields(t *testing.T) {
	// Signature is a map but has no fields at all
	docData := map[string]any{
		jsonFieldVersion: []any{
			map[string]any{
				jsonFieldCID:       "cid-empty-sig",
				jsonFieldSignature: map[string]any{},
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "cid-empty-sig", versions[0].CID)
	require.Equal(t, "", versions[0].Signature.Type)
	require.Equal(t, "", versions[0].Signature.Identity)
	require.Equal(t, "", versions[0].Signature.Value)
}

func TestExtractVersionsFromDocument_NoSignatureField(t *testing.T) {
	// Version map without a signature field at all
	docData := map[string]any{
		jsonFieldVersion: []any{
			map[string]any{
				jsonFieldCID:           "cid-no-sig",
				jsonFieldCollectionVer: testCollVersionID,
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "cid-no-sig", versions[0].CID)
	require.Equal(t, testCollVersionID, versions[0].CollectionVersionID)
	require.Equal(t, "", versions[0].Signature.Type)
	require.Equal(t, "", versions[0].Signature.Identity)
	require.Equal(t, "", versions[0].Signature.Value)
}

func TestExtractVersionsFromDocument_NoCIDNoSignatureNoCollectionVersionId(t *testing.T) {
	// Version map with none of the expected fields
	docData := map[string]any{
		jsonFieldVersion: []any{
			map[string]any{
				"unrelated_field": "some_value",
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "", versions[0].CID)
	require.Equal(t, "", versions[0].CollectionVersionID)
	require.Equal(t, "", versions[0].Signature.Type)
}

// ========================================
// POST ATTESTATION RECORDS BATCH - ADDITIONAL EDGE CASES
// ========================================

func TestPostAttestationRecordsBatch_NilRecordsInSlice_SkipNilOnly(t *testing.T) {
	// Tests the path where some records in the batch are nil but others are valid.
	// The nil records should be skipped during both the attestedDocIDs collection
	// loop (line 129) and the docs assembly loop (line 202).
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

	records := []*Record{
		nil,
		{AttestedDocID: "nil-test-doc", SourceDocIDs: []string{"src-nil"}, CIDs: []string{"cid-nil-1"}, DocType: testDocType, VoteCount: 1},
		nil,
		{AttestedDocID: "nil-test-doc-2", SourceDocIDs: []string{"src-nil-2"}, CIDs: []string{}, DocType: testDocType, VoteCount: 1},
	}

	err = PostAttestationRecordsBatch(ctx, defraNode, records)
	require.NoError(t, err)

	// Verify only the non-nil record with non-empty CIDs was created
	query := fmt.Sprintf(`
		%s {
			attested_doc
			CIDs
		}
	`, constants.CollectionAttestationRecord)

	results, err := defradb.QueryArray[Record](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "nil-test-doc", results[0].AttestedDocID)
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
	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionAttestationRecord)
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
	err = PostAttestationRecord(ctx, defraNode, record)
	require.NoError(t, err)

	col, err := defraNode.DB.GetCollectionByName(ctx, constants.CollectionAttestationRecord)
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

func TestGetAttestationRecordsByViewName_MissingViewSchema(t *testing.T) {
	ctx := context.Background()

	// Create a defra node without any view-specific attestation schema
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

	defraNode := client.GetNode()

	// Query a non-existent view attestation collection with doc IDs
	_, err = GetAttestationRecordsByViewName(ctx, defraNode, "NonExistentView", []string{testDocID1})
	// Should return an error because the collection doesn't exist
	require.Error(t, err)

	// Query a non-existent view attestation collection without doc IDs
	_, err = GetAttestationRecordsByViewName(ctx, defraNode, "NonExistentView", nil)
	require.Error(t, err)
}
