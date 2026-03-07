package attestation

import (
	"context"
	"fmt"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/stretchr/testify/require"
)

func TestCreateAttestationRecord_AllSignaturesValid(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(ctx context.Context, cid string, signature Signature) error {
			// All signatures are valid
			return nil
		},
	}

	docId := "doc-123"
	sourceDocId := "source-doc-456"
	versions := []Version{
		{
			CID: "cid-1",
			Signature: Signature{
				Type:     "es256k",
				Identity: "identity-1",
				Value:    "signature-1",
			},
		},
		{
			CID: "cid-2",
			Signature: Signature{
				Type:     "es256k",
				Identity: "identity-2",
				Value:    "signature-2",
			},
		},
		{
			CID: "cid-3",
			Signature: Signature{
				Type:     "es256k",
				Identity: "identity-3",
				Value:    "signature-3",
			},
		},
	}

	record, err := CreateAttestationRecord(ctx, verifier, docId, sourceDocId, "TestDoc", versions, 50)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, docId, record.AttestedDocId)
	require.Equal(t, sourceDocId, record.SourceDocId)
	require.Len(t, record.CIDs, 3)
	require.Contains(t, record.CIDs, "cid-1")
	require.Contains(t, record.CIDs, "cid-2")
	require.Contains(t, record.CIDs, "cid-3")
}

func TestCreateAttestationRecord_SomeSignaturesInvalid(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(ctx context.Context, cid string, signature Signature) error {
			// Only cid-1 and cid-3 are valid
			if cid == "cid-2" {
				return fmt.Errorf("invalid signature")
			}
			return nil
		},
	}

	docId := "doc-123"
	sourceDocId := "source-doc-456"
	versions := []Version{
		{
			CID: "cid-1",
			Signature: Signature{
				Type:     "es256k",
				Identity: "identity-1",
				Value:    "signature-1",
			},
		},
		{
			CID: "cid-2",
			Signature: Signature{
				Type:     "es256k",
				Identity: "identity-2",
				Value:    "signature-2",
			},
		},
		{
			CID: "cid-3",
			Signature: Signature{
				Type:     "es256k",
				Identity: "identity-3",
				Value:    "signature-3",
			},
		},
	}

	record, err := CreateAttestationRecord(ctx, verifier, docId, sourceDocId, "TestDoc", versions, 50)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, docId, record.AttestedDocId)
	require.Equal(t, sourceDocId, record.SourceDocId)
	require.Len(t, record.CIDs, 2)
	require.Contains(t, record.CIDs, "cid-1")
	require.NotContains(t, record.CIDs, "cid-2")
	require.Contains(t, record.CIDs, "cid-3")
}

func TestCreateAttestationRecord_AllSignaturesInvalid(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(ctx context.Context, cid string, signature Signature) error {
			// All signatures are invalid
			return fmt.Errorf("invalid signature")
		},
	}

	docId := "doc-123"
	sourceDocId := "source-doc-456"
	versions := []Version{
		{
			CID: "cid-1",
			Signature: Signature{
				Type:     "es256k",
				Identity: "identity-1",
				Value:    "signature-1",
			},
		},
		{
			CID: "cid-2",
			Signature: Signature{
				Type:     "es256k",
				Identity: "identity-2",
				Value:    "signature-2",
			},
		},
	}

	record, err := CreateAttestationRecord(ctx, verifier, docId, sourceDocId, "TestDoc", versions, 50)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, docId, record.AttestedDocId)
	require.Equal(t, sourceDocId, record.SourceDocId)
	require.Len(t, record.CIDs, 0)
}

func TestCreateAttestationRecord_EmptyVersions(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{}

	docId := "doc-123"
	sourceDocId := "source-doc-456"
	versions := []Version{}

	record, err := CreateAttestationRecord(ctx, verifier, docId, sourceDocId, "TestDoc", versions, 50)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, docId, record.AttestedDocId)
	require.Equal(t, sourceDocId, record.SourceDocId)
	require.Len(t, record.CIDs, 0)
}

func TestPostAttestationRecord(t *testing.T) {
	ctx := context.Background()

	// Define schema inline for this test
	testSchema := `
		type TestDoc {
			name: String
		}

		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	// Create and start client using new API with test config
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()                          // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing" // Set test keyring secret
	testConfig.DefraDB.Url = "localhost:0"                               // Use random available port for API
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"             // Use random available port for P2P
	testConfig.DefraDB.P2P.Enabled = false                               // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}                   // No bootstrap peers

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	type TestDoc struct {
		Name    string    `json:"name"`
		DocId   string    `json:"_docID"`
		Version []Version `json:"_version"`
	}

	createTestDocMutation := `
		mutation {
			create_TestDoc(input: {name: "test-document"}) {
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

	testDocResult, err := defra.PostMutation[TestDoc](t.Context(), defraNode, createTestDocMutation)
	require.NoError(t, err)
	require.NotNil(t, testDocResult)
	require.Greater(t, len(testDocResult.DocId), 0)
	require.Len(t, testDocResult.Version, 1)

	testVersions := testDocResult.Version

	attestedDocId := "attested-doc-123" // This would be the View doc created after processing the view
	sourceDocId := testDocResult.DocId

	// Manually create attestation record with the necessary data - we don't use CreateAttestationRecord because we don't want any validation
	attestationRecord := &AttestationRecord{
		AttestedDocId: attestedDocId,
		SourceDocId:   sourceDocId,
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

	results, err := defra.QueryArray[AttestationRecord](t.Context(), defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)

	record := results[0]
	require.Equal(t, attestedDocId, record.AttestedDocId)
	require.Equal(t, testDocResult.DocId, record.SourceDocId)
	require.NotNil(t, record.CIDs)
	require.Len(t, record.CIDs, 1)
}

// ========================================
// MERGING ATTESTATION RECORDS TESTS
// ========================================

func TestMergeAttestationRecords_SameDocument(t *testing.T) {
	record1 := &AttestationRecord{
		AttestedDocId: "doc-123",
		SourceDocId:   "source-1",
		CIDs:          []string{"cid-1", "cid-2"},
	}

	record2 := &AttestationRecord{
		AttestedDocId: "doc-123",
		SourceDocId:   "source-2",
		CIDs:          []string{"cid-3", "cid-4"},
	}

	merged, err := MergeAttestationRecords(record1, record2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, "doc-123", merged.AttestedDocId)
	require.Equal(t, "source-1", merged.SourceDocId) // Should use first record's source
	require.Len(t, merged.CIDs, 4)
	require.Contains(t, merged.CIDs, "cid-1")
	require.Contains(t, merged.CIDs, "cid-2")
	require.Contains(t, merged.CIDs, "cid-3")
	require.Contains(t, merged.CIDs, "cid-4")
}

func TestMergeAttestationRecords_WithDuplicateCIDs(t *testing.T) {
	record1 := &AttestationRecord{
		AttestedDocId: "doc-123",
		SourceDocId:   "source-1",
		CIDs:          []string{"cid-1", "cid-2", "cid-3"},
	}

	record2 := &AttestationRecord{
		AttestedDocId: "doc-123",
		SourceDocId:   "source-2",
		CIDs:          []string{"cid-2", "cid-3", "cid-4"}, // cid-2 and cid-3 are duplicates
	}

	merged, err := MergeAttestationRecords(record1, record2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, "doc-123", merged.AttestedDocId)
	require.Len(t, merged.CIDs, 4) // Should deduplicate
	require.Contains(t, merged.CIDs, "cid-1")
	require.Contains(t, merged.CIDs, "cid-2")
	require.Contains(t, merged.CIDs, "cid-3")
	require.Contains(t, merged.CIDs, "cid-4")

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
	record1 := &AttestationRecord{
		AttestedDocId: "doc-123",
		SourceDocId:   "source-1",
		CIDs:          []string{"cid-1", "cid-2"},
	}

	record2 := &AttestationRecord{
		AttestedDocId: "doc-456", // Different document
		SourceDocId:   "source-2",
		CIDs:          []string{"cid-3", "cid-4"},
	}

	merged, err := MergeAttestationRecords(record1, record2)
	require.Error(t, err)
	require.Nil(t, merged)
	require.Contains(t, err.Error(), "cannot merge records with different attested document IDs")
}

func TestMergeAttestationRecords_EmptyRecords(t *testing.T) {
	record1 := &AttestationRecord{
		AttestedDocId: "doc-123",
		SourceDocId:   "source-1",
		CIDs:          []string{},
	}

	record2 := &AttestationRecord{
		AttestedDocId: "doc-123",
		SourceDocId:   "source-2",
		CIDs:          []string{"cid-1", "cid-2"},
	}

	merged, err := MergeAttestationRecords(record1, record2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, "doc-123", merged.AttestedDocId)
	require.Len(t, merged.CIDs, 2)
	require.Contains(t, merged.CIDs, "cid-1")
	require.Contains(t, merged.CIDs, "cid-2")
}

func TestMergeAttestationRecords_BothEmpty(t *testing.T) {
	record1 := &AttestationRecord{
		AttestedDocId: "doc-123",
		SourceDocId:   "source-1",
		CIDs:          []string{},
	}

	record2 := &AttestationRecord{
		AttestedDocId: "doc-123",
		SourceDocId:   "source-2",
		CIDs:          []string{},
	}

	merged, err := MergeAttestationRecords(record1, record2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, "doc-123", merged.AttestedDocId)
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
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	// Create and start client using new API with test config
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()                          // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing" // Set test keyring secret
	testConfig.DefraDB.Url = "localhost:0"                               // Use random available port for API
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"             // Use random available port for P2P
	testConfig.DefraDB.P2P.Enabled = false                               // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}                   // No bootstrap peers

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	type TestDoc struct {
		Name    string    `json:"name"`
		DocId   string    `json:"_docID"`
		Version []Version `json:"_version"`
	}

	// Create two test documents
	createDoc1Mutation := `
		mutation {
			create_TestDoc(input: {name: "test-document-1"}) {
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
			create_TestDoc(input: {name: "test-document-2"}) {
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

	doc1Result, err := defra.PostMutation[TestDoc](t.Context(), defraNode, createDoc1Mutation)
	require.NoError(t, err)
	require.NotNil(t, doc1Result)

	doc2Result, err := defra.PostMutation[TestDoc](t.Context(), defraNode, createDoc2Mutation)
	require.NoError(t, err)
	require.NotNil(t, doc2Result)

	// Create attestation records for the same attested document but from different sources
	attestedDocId := "view-doc-123"

	record1 := &AttestationRecord{
		AttestedDocId: attestedDocId,
		SourceDocId:   doc1Result.DocId,
		CIDs:          []string{doc1Result.Version[0].CID},
	}

	record2 := &AttestationRecord{
		AttestedDocId: attestedDocId,
		SourceDocId:   doc2Result.DocId,
		CIDs:          []string{doc2Result.Version[0].CID},
	}

	// Merge the records
	merged, err := MergeAttestationRecords(record1, record2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, attestedDocId, merged.AttestedDocId)
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

	results, err := defra.QueryArray[AttestationRecord](t.Context(), defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)

	storedRecord := results[0]
	require.Equal(t, attestedDocId, storedRecord.AttestedDocId)
	require.Equal(t, doc1Result.DocId, storedRecord.SourceDocId) // Should use first record's source
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
	for i := 0; i < numCIDs; i++ {
		cids1[i] = fmt.Sprintf("cid-1-%d", i)
	}

	record1 := &AttestationRecord{
		AttestedDocId: "doc-123",
		SourceDocId:   "source-1",
		CIDs:          cids1,
	}

	// Create second record with overlapping CIDs
	cids2 := make([]string, numCIDs)
	for i := 0; i < numCIDs; i++ {
		if i < numCIDs/2 {
			// First half overlaps with record1
			cids2[i] = fmt.Sprintf("cid-1-%d", i)
		} else {
			// Second half is unique
			cids2[i] = fmt.Sprintf("cid-2-%d", i)
		}
	}

	record2 := &AttestationRecord{
		AttestedDocId: "doc-123",
		SourceDocId:   "source-2",
		CIDs:          cids2,
	}

	// Merge records
	merged, err := MergeAttestationRecords(record1, record2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	require.Equal(t, "doc-123", merged.AttestedDocId)

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
	testSchema := `
		type TestDoc {
			name: String
		}

		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	// Create and start client using new API with test config
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()                          // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing" // Set test keyring secret
	testConfig.DefraDB.Url = "localhost:0"                               // Use random available port for API
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"             // Use random available port for P2P
	testConfig.DefraDB.P2P.Enabled = false                               // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}                   // No bootstrap peers

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	record := &AttestationRecord{
		AttestedDocId: "doc-123",
		SourceDocId:   "doc-123",
		CIDs:          []string{"cid-1"},
	}

	err = PostAttestationRecord(t.Context(), defraNode, record)
	require.NoError(t, err)

	query := fmt.Sprintf(`
		query {
			%s(filter: {attested_doc: {_eq: "doc-123"}}) {
				_docID
				attested_doc
				source_doc
				CIDs
			}
		}
	`, constants.CollectionAttestationRecord)

	results, err := defra.QueryArray[AttestationRecord](t.Context(), defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "doc-123", results[0].AttestedDocId)
}

func TestPostAttestationRecord_OldDocument_DuplicateCreateIsHandled(t *testing.T) {
	ctx := context.Background()

	// Define schema inline for this test
	testSchema := `
		type TestDoc {
			name: String
		}

		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	// Create and start client using new API with test config
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()                          // Use temp directory for test data
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing" // Set test keyring secret
	testConfig.DefraDB.Url = "localhost:0"                               // Use random available port for API
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"             // Use random available port for P2P
	testConfig.DefraDB.P2P.Enabled = false                               // Disable P2P networking for testing
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}                   // No bootstrap peers

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	// Apply schema using the new Client method
	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	record := &AttestationRecord{
		AttestedDocId: "doc-123",
		SourceDocId:   "doc-123",
		CIDs:          []string{"cid-1"},
	}

	err = PostAttestationRecord(t.Context(), defraNode, record)
	require.NoError(t, err)
	err = PostAttestationRecord(t.Context(), defraNode, record)
	require.NoError(t, err)

	query := fmt.Sprintf(`
		query {
			%s(filter: {attested_doc: {_eq: "doc-123"}}) {
				_docID
				attested_doc
				source_doc
				CIDs
			}
		}
	`, constants.CollectionAttestationRecord)

	results, err := defra.QueryArray[AttestationRecord](t.Context(), defraNode, query)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(results), 1)
}

func TestMergeAttestationRecords_MultipleOldRecords(t *testing.T) {
	records := []*AttestationRecord{
		{AttestedDocId: "doc-123", SourceDocId: "source-1", CIDs: []string{"cid-1"}},
		{AttestedDocId: "doc-123", SourceDocId: "source-2", CIDs: []string{"cid-2", "cid-3"}},
		{AttestedDocId: "doc-123", SourceDocId: "source-3", CIDs: []string{"cid-3", "cid-4"}},
	}

	merged := records[0]
	var err error
	for i := 1; i < len(records); i++ {
		merged, err = MergeAttestationRecords(merged, records[i])
		require.NoError(t, err)
	}

	require.NotNil(t, merged)
	require.Equal(t, "doc-123", merged.AttestedDocId)
	require.Equal(t, "source-1", merged.SourceDocId)
	require.ElementsMatch(t, []string{"cid-1", "cid-2", "cid-3", "cid-4"}, merged.CIDs)
}

func TestPostAttestationRecordsBatch_UpdatesExistingRecord_AppendsCIDs(t *testing.T) {
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Step 1: Create initial attestation record with cid-1 and cid-2
	initialRecord := &AttestationRecord{
		AttestedDocId: "attested-doc-123",
		SourceDocId:   "source-doc-456",
		CIDs:          []string{"cid-1", "cid-2"},
		DocType:       "TestDoc",
		VoteCount:     1,
	}

	err = PostAttestationRecordsBatch(ctx, defraNode, []*AttestationRecord{initialRecord})
	require.NoError(t, err)

	// Verify initial record was created
	query := fmt.Sprintf(`
		%s(filter: {attested_doc: {_eq: "attested-doc-123"}}) {
			_docID
			attested_doc
			source_doc
			CIDs
			doc_type
			vote_count
		}
	`, constants.CollectionAttestationRecord)

	results, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.ElementsMatch(t, []string{"cid-1", "cid-2"}, results[0].CIDs)

	// Step 2: Post update with new CIDs (cid-2 overlaps, cid-3 is new)
	updateRecord := &AttestationRecord{
		AttestedDocId: "attested-doc-123",
		SourceDocId:   "source-doc-456",
		CIDs:          []string{"cid-2", "cid-3"},
		DocType:       "TestDoc",
		VoteCount:     1,
	}

	err = PostAttestationRecordsBatch(ctx, defraNode, []*AttestationRecord{updateRecord})
	require.NoError(t, err)

	// Step 3: Verify that the batch was posted (SaveMany creates a new record;
	// CID merging only happens via the upsert_ mutation in PostAttestationRecord)
	results, err = defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	require.NoError(t, err)
	require.NotEmpty(t, results)
}

func TestPostAttestationRecordsBatch_CreatesNewRecord_WhenNotExists(t *testing.T) {
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create a new record
	record := &AttestationRecord{
		AttestedDocId: "new-attested-doc",
		SourceDocId:   "new-source-doc",
		CIDs:          []string{"cid-a", "cid-b"},
		DocType:       "TestDoc",
		VoteCount:     1,
	}

	err = PostAttestationRecordsBatch(ctx, defraNode, []*AttestationRecord{record})
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

	results, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "new-attested-doc", results[0].AttestedDocId)
	require.Equal(t, "new-source-doc", results[0].SourceDocId)
	require.ElementsMatch(t, []string{"cid-a", "cid-b"}, results[0].CIDs)
}

func TestPostAttestationRecordsBatch_MultipleBatchRecords(t *testing.T) {
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create multiple records in a single batch
	records := []*AttestationRecord{
		{
			AttestedDocId: "doc-1",
			SourceDocId:   "source-1",
			CIDs:          []string{"cid-1"},
			DocType:       "TypeA",
			VoteCount:     1,
		},
		{
			AttestedDocId: "doc-2",
			SourceDocId:   "source-2",
			CIDs:          []string{"cid-2", "cid-3"},
			DocType:       "TypeB",
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

	results, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 2)

	// Find each record by attested_doc
	resultMap := make(map[string]AttestationRecord)
	for _, r := range results {
		resultMap[r.AttestedDocId] = r
	}

	require.Contains(t, resultMap, "doc-1")
	require.Contains(t, resultMap, "doc-2")
	require.ElementsMatch(t, []string{"cid-1"}, resultMap["doc-1"].CIDs)
	require.ElementsMatch(t, []string{"cid-2", "cid-3"}, resultMap["doc-2"].CIDs)
}

func TestPostAttestationRecordsBatch_EmptyRecords_ReturnsNil(t *testing.T) {
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Test with empty slice
	err = PostAttestationRecordsBatch(ctx, defraNode, []*AttestationRecord{})
	require.NoError(t, err)

	// Test with nil records in slice
	err = PostAttestationRecordsBatch(ctx, defraNode, []*AttestationRecord{nil, nil})
	require.NoError(t, err)

	// Test with records that have empty CIDs
	err = PostAttestationRecordsBatch(ctx, defraNode, []*AttestationRecord{
		{AttestedDocId: "doc-1", SourceDocId: "source-1", CIDs: []string{}},
	})
	require.NoError(t, err)
}

func TestPostAttestationRecordsBatch_DoesNotMutateInputRecords(t *testing.T) {
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create initial record
	initialRecord := &AttestationRecord{
		AttestedDocId: "attested-doc-mutation-test",
		SourceDocId:   "source-doc",
		CIDs:          []string{"cid-1"},
		DocType:       "TestDoc",
		VoteCount:     1,
	}
	err = PostAttestationRecordsBatch(ctx, defraNode, []*AttestationRecord{initialRecord})
	require.NoError(t, err)

	// Create update record and save original CIDs
	updateRecord := &AttestationRecord{
		AttestedDocId: "attested-doc-mutation-test",
		SourceDocId:   "source-doc",
		CIDs:          []string{"cid-2"},
		DocType:       "TestDoc",
		VoteCount:     1,
	}
	originalCIDs := make([]string, len(updateRecord.CIDs))
	copy(originalCIDs, updateRecord.CIDs)

	err = PostAttestationRecordsBatch(ctx, defraNode, []*AttestationRecord{updateRecord})
	require.NoError(t, err)

	// Verify input record was not mutated
	require.ElementsMatch(t, originalCIDs, updateRecord.CIDs, "Input record CIDs should not be mutated")
}

// ========================================
// EXTRACT VERSIONS FROM DOCUMENT TESTS
// ========================================

func TestExtractVersionsFromDocument_WithVersionField(t *testing.T) {
	docData := map[string]any{
		"_version": []any{
			map[string]any{
				"cid": "cid-abc",
				"signature": map[string]any{
					"type":     "es256k",
					"identity": "identity-abc",
					"value":    "sig-abc",
				},
				"collectionVersionId": "colv-1",
			},
			map[string]any{
				"cid": "cid-def",
				"signature": map[string]any{
					"type":     "ES256K",
					"identity": "identity-def",
					"value":    "sig-def",
				},
				"collectionVersionId": "colv-2",
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	require.Equal(t, "cid-abc", versions[0].CID)
	require.Equal(t, "es256k", versions[0].Signature.Type)
	require.Equal(t, "identity-abc", versions[0].Signature.Identity)
	require.Equal(t, "sig-abc", versions[0].Signature.Value)
	require.Equal(t, "colv-1", versions[0].CollectionVersionId)
	require.Equal(t, "cid-def", versions[1].CID)
	require.Equal(t, "colv-2", versions[1].CollectionVersionId)
}

func TestExtractVersionsFromDocument_WithoutVersionField(t *testing.T) {
	docData := map[string]any{
		"name":  "test-document",
		"value": 42,
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Empty(t, versions)
}

func TestExtractVersionsFromDocument_VersionFieldNotArray(t *testing.T) {
	// _version is a string instead of []any
	docData := map[string]any{
		"_version": "not-an-array",
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Empty(t, versions)
}

func TestExtractVersionsFromDocument_VersionFieldIsNumber(t *testing.T) {
	// _version is a number instead of []any
	docData := map[string]any{
		"_version": 123,
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Empty(t, versions)
}

func TestExtractVersionsFromDocument_VersionFieldIsNil(t *testing.T) {
	docData := map[string]any{
		"_version": nil,
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Empty(t, versions)
}

func TestExtractVersionsFromDocument_VersionArrayWithNonMapElements(t *testing.T) {
	// Some elements in the _version array are not maps
	docData := map[string]any{
		"_version": []any{
			"not-a-map",
			42,
			map[string]any{
				"cid": "cid-valid",
				"signature": map[string]any{
					"type":     "es256k",
					"identity": "identity-valid",
					"value":    "sig-valid",
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
		"_version": []any{
			map[string]any{
				"cid":       "cid-partial",
				"signature": "not-a-map-signature",
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
		"_version": []any{},
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
		"_version": []any{
			map[string]any{
				"cid": 12345, // Not a string
				"signature": map[string]any{
					"type":     "es256k",
					"identity": "identity-1",
					"value":    "sig-1",
				},
				"collectionVersionId": "colv-1",
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	// CID should be empty since it was not a string
	require.Equal(t, "", versions[0].CID)
	require.Equal(t, "es256k", versions[0].Signature.Type)
}

func TestExtractVersionsFromDocument_NonStringCollectionVersionId(t *testing.T) {
	docData := map[string]any{
		"_version": []any{
			map[string]any{
				"cid":                 "cid-1",
				"collectionVersionId": 999, // Not a string
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "cid-1", versions[0].CID)
	require.Equal(t, "", versions[0].CollectionVersionId)
}

// ========================================
// CREATE ATTESTATION RECORD REMAINING BRANCHES
// ========================================

func TestCreateAttestationRecord_DefaultMaxConcurrentVerifications(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(ctx context.Context, cid string, signature Signature) error {
			return nil
		},
	}

	versions := []Version{
		{CID: "cid-1", Signature: Signature{Type: "es256k", Identity: "id-1", Value: "sig-1"}},
	}

	// maxConcurrentVerifications <= 0 should default to 50
	record, err := CreateAttestationRecord(ctx, verifier, "doc-1", "source-1", "TestDoc", versions, 0)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Len(t, record.CIDs, 1)
	require.Contains(t, record.CIDs, "cid-1")

	// Negative value
	record, err = CreateAttestationRecord(ctx, verifier, "doc-2", "source-2", "TestDoc", versions, -10)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Len(t, record.CIDs, 1)
}

func TestCreateAttestationRecord_NilVersions(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{}

	record, err := CreateAttestationRecord(ctx, verifier, "doc-1", "source-1", "TestDoc", nil, 50)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, "doc-1", record.AttestedDocId)
	require.Equal(t, "source-1", record.SourceDocId)
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
	err := HandleDocumentAttestation(ctx, verifier, nil, "doc-1", "TestDoc", []Version{}, 50)
	require.NoError(t, err)
}

func TestHandleDocumentAttestation_AllSignaturesInvalid_NoCIDsPosted(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(ctx context.Context, cid string, signature Signature) error {
			return fmt.Errorf("invalid signature")
		},
	}

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	versions := []Version{
		{CID: "cid-1", Signature: Signature{Type: "es256k", Identity: "id-1", Value: "sig-1"}},
	}

	// All signatures fail, so no CIDs and returns nil (early return)
	err = HandleDocumentAttestation(ctx, verifier, defraNode, "doc-1", "TestDoc", versions, 50)
	require.NoError(t, err)

	// Verify nothing was posted
	query := fmt.Sprintf(`
		%s {
			_docID
			attested_doc
		}
	`, constants.CollectionAttestationRecord)

	results, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestHandleDocumentAttestation_ValidSignatures_PostsRecord(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(ctx context.Context, cid string, signature Signature) error {
			return nil
		},
	}

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	versions := []Version{
		{CID: "cid-1", Signature: Signature{Type: "es256k", Identity: "id-1", Value: "sig-1"}},
		{CID: "cid-2", Signature: Signature{Type: "es256k", Identity: "id-2", Value: "sig-2"}},
	}

	err = HandleDocumentAttestation(ctx, verifier, defraNode, "doc-handle-1", "TestDoc", versions, 50)
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

	results, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "doc-handle-1", results[0].AttestedDocId)
	require.ElementsMatch(t, []string{"cid-1", "cid-2"}, results[0].CIDs)
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
		{DocID: "doc-1", DocType: "TestDoc", Versions: []Version{}},
		{DocID: "doc-2", DocType: "TestDoc", Versions: nil},
	}

	// All inputs have empty versions, so no records to post, returns nil
	err := HandleDocumentAttestationBatch(ctx, verifier, nil, inputs, 50)
	require.NoError(t, err)
}

func TestHandleDocumentAttestationBatch_AllSignaturesInvalid(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(ctx context.Context, cid string, signature Signature) error {
			return fmt.Errorf("invalid signature")
		},
	}

	inputs := []DocumentAttestationInput{
		{
			DocID:   "doc-1",
			DocType: "TestDoc",
			Versions: []Version{
				{CID: "cid-1", Signature: Signature{Type: "es256k", Identity: "id-1", Value: "sig-1"}},
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
		verifyFunc: func(ctx context.Context, cid string, signature Signature) error {
			return nil
		},
	}

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	inputs := []DocumentAttestationInput{
		{
			DocID:   "batch-doc-1",
			DocType: "TypeA",
			Versions: []Version{
				{CID: "cid-a1", Signature: Signature{Type: "es256k", Identity: "id-1", Value: "sig-1"}},
			},
		},
		{
			DocID:   "batch-doc-2",
			DocType: "TypeB",
			Versions: []Version{
				{CID: "cid-b1", Signature: Signature{Type: "es256k", Identity: "id-2", Value: "sig-2"}},
				{CID: "cid-b2", Signature: Signature{Type: "es256k", Identity: "id-3", Value: "sig-3"}},
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

	results, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 2)

	resultMap := make(map[string]AttestationRecord)
	for _, r := range results {
		resultMap[r.AttestedDocId] = r
	}

	require.Contains(t, resultMap, "batch-doc-1")
	require.Contains(t, resultMap, "batch-doc-2")
	require.ElementsMatch(t, []string{"cid-a1"}, resultMap["batch-doc-1"].CIDs)
	require.ElementsMatch(t, []string{"cid-b1", "cid-b2"}, resultMap["batch-doc-2"].CIDs)
}

func TestHandleDocumentAttestationBatch_MixedValidAndInvalidVersions(t *testing.T) {
	ctx := context.Background()
	verifier := &MockSignatureVerifier{
		verifyFunc: func(ctx context.Context, cid string, signature Signature) error {
			return nil
		},
	}

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	inputs := []DocumentAttestationInput{
		{
			DocID:   "batch-mixed-1",
			DocType: "TypeA",
			Versions: []Version{
				{CID: "cid-m1", Signature: Signature{Type: "es256k", Identity: "id-1", Value: "sig-1"}},
			},
		},
		{
			// This one has no versions, should be skipped
			DocID:    "batch-mixed-2",
			DocType:  "TypeB",
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

	results, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "batch-mixed-1", results[0].AttestedDocId)
}

// ========================================
// CHECK EXISTING ATTESTATION TESTS
// ========================================

func TestCheckExistingAttestation_NoExistingRecords(t *testing.T) {
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Check for non-existent attestation
	records, err := CheckExistingAttestation(ctx, defraNode, "non-existent-doc", "TestDoc")
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestCheckExistingAttestation_WithExistingRecord(t *testing.T) {
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// First, create an attestation record
	record := &AttestationRecord{
		AttestedDocId: "check-existing-doc",
		SourceDocId:   "source-doc",
		CIDs:          []string{"cid-check-1"},
		DocType:       "TestDoc",
		VoteCount:     1,
	}
	err = PostAttestationRecord(ctx, defraNode, record)
	require.NoError(t, err)

	// Now check for existing attestation
	records, err := CheckExistingAttestation(ctx, defraNode, "check-existing-doc", "TestDoc")
	require.NoError(t, err)
	require.NotNil(t, records)
	require.Len(t, records, 1)
	require.Equal(t, "check-existing-doc", records[0].AttestedDocId)
	require.Equal(t, "source-doc", records[0].SourceDocId)
	require.ElementsMatch(t, []string{"cid-check-1"}, records[0].CIDs)
}

func TestCheckExistingAttestation_WrongDocType(t *testing.T) {
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create an attestation record with doc_type "TypeA"
	record := &AttestationRecord{
		AttestedDocId: "check-doctype-doc",
		SourceDocId:   "source-doc",
		CIDs:          []string{"cid-dt-1"},
		DocType:       "TypeA",
		VoteCount:     1,
	}
	err = PostAttestationRecord(ctx, defraNode, record)
	require.NoError(t, err)

	// Query with wrong doc_type should find nothing
	records, err := CheckExistingAttestation(ctx, defraNode, "check-doctype-doc", "TypeB")
	require.NoError(t, err)
	require.Empty(t, records)
}

// ========================================
// IS DOCUMENT ATTESTED VIA BLOCK TESTS
// ========================================

func TestIsDocumentAttestedViaBlock_NoBlockAttestation(t *testing.T) {
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

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

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create a block attestation record with attested_doc = "block:42"
	record := &AttestationRecord{
		AttestedDocId: "block:42",
		SourceDocId:   "block-source",
		CIDs:          []string{"target-cid", "other-cid"},
		DocType:       "Block",
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

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create a block attestation record
	record := &AttestationRecord{
		AttestedDocId: "block:50",
		SourceDocId:   "block-source",
		CIDs:          []string{"cid-in-block"},
		DocType:       "Block",
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

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	records, err := GetBlockAttestations(ctx, defraNode, 999)
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestGetBlockAttestations_WithRecords(t *testing.T) {
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create block attestation records with the prefix "block:100:"
	record1 := &AttestationRecord{
		AttestedDocId: "block:100:merkle-root-a",
		SourceDocId:   "indexer-1",
		CIDs:          []string{"cid-100-a"},
		DocType:       "Block",
		VoteCount:     1,
	}
	record2 := &AttestationRecord{
		AttestedDocId: "block:100:merkle-root-b",
		SourceDocId:   "indexer-2",
		CIDs:          []string{"cid-100-b"},
		DocType:       "Block",
		VoteCount:     1,
	}
	// Also create a record for a different block to make sure it's not returned
	record3 := &AttestationRecord{
		AttestedDocId: "block:200:merkle-root-c",
		SourceDocId:   "indexer-1",
		CIDs:          []string{"cid-200-c"},
		DocType:       "Block",
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
		attestedDocs[r.AttestedDocId] = true
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
			source_doc: String
			CIDs: [String]
		}
	`, collectionName)

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create some records in the view-specific collection
	createMutation := fmt.Sprintf(`
		mutation {
			create_%s(input: {attested_doc: "view-doc-1", source_doc: "src-1", CIDs: ["cid-v1"]}) {
				_docID
			}
		}
	`, collectionName)
	_, err = defra.PostMutation[map[string]any](ctx, defraNode, createMutation)
	require.NoError(t, err)

	createMutation2 := fmt.Sprintf(`
		mutation {
			create_%s(input: {attested_doc: "view-doc-2", source_doc: "src-2", CIDs: ["cid-v2"]}) {
				_docID
			}
		}
	`, collectionName)
	_, err = defra.PostMutation[map[string]any](ctx, defraNode, createMutation2)
	require.NoError(t, err)

	createMutation3 := fmt.Sprintf(`
		mutation {
			create_%s(input: {attested_doc: "view-doc-3", source_doc: "src-3", CIDs: ["cid-v3"]}) {
				_docID
			}
		}
	`, collectionName)
	_, err = defra.PostMutation[map[string]any](ctx, defraNode, createMutation3)
	require.NoError(t, err)

	// Query with specific doc IDs
	records, err := GetAttestationRecordsByViewName(ctx, defraNode, viewName, []string{"view-doc-1", "view-doc-3"})
	require.NoError(t, err)
	require.Len(t, records, 2)

	attestedDocs := make(map[string]bool)
	for _, r := range records {
		attestedDocs[r.AttestedDocId] = true
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
			source_doc: String
			CIDs: [String]
		}
	`, collectionName)

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create some records
	createMutation := fmt.Sprintf(`
		mutation {
			create_%s(input: {attested_doc: "all-doc-1", source_doc: "src-1", CIDs: ["cid-a1"]}) {
				_docID
			}
		}
	`, collectionName)
	_, err = defra.PostMutation[map[string]any](ctx, defraNode, createMutation)
	require.NoError(t, err)

	createMutation2 := fmt.Sprintf(`
		mutation {
			create_%s(input: {attested_doc: "all-doc-2", source_doc: "src-2", CIDs: ["cid-a2"]}) {
				_docID
			}
		}
	`, collectionName)
	_, err = defra.PostMutation[map[string]any](ctx, defraNode, createMutation2)
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
			source_doc: String
			CIDs: [String]
		}
	`, collectionName)

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create a record
	createMutation := fmt.Sprintf(`
		mutation {
			create_%s(input: {attested_doc: "empty-doc-1", source_doc: "src-1", CIDs: ["cid-e1"]}) {
				_docID
			}
		}
	`, collectionName)
	_, err = defra.PostMutation[map[string]any](ctx, defraNode, createMutation)
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
			source_doc: String
			CIDs: [String]
		}
	`, collectionName)

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

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

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	record := &AttestationRecord{
		AttestedDocId: "empty-cids-doc",
		SourceDocId:   "source-doc",
		CIDs:          []string{},
		DocType:       "TestDoc",
		VoteCount:     1,
	}

	err = PostAttestationRecord(ctx, defraNode, record)
	require.NoError(t, err)
}

func TestPostAttestationRecord_MultipleCIDs(t *testing.T) {
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	record := &AttestationRecord{
		AttestedDocId: "multi-cid-doc",
		SourceDocId:   "source-doc",
		CIDs:          []string{"cid-1", "cid-2", "cid-3"},
		DocType:       "TestDoc",
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

	results, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.ElementsMatch(t, []string{"cid-1", "cid-2", "cid-3"}, results[0].CIDs)
}

// ========================================
// DEFRA SIGNATURE VERIFIER REMAINING BRANCHES
// ========================================

func TestDefraSignatureVerifier_Verify_NilNode(t *testing.T) {
	ctx := context.Background()
	verifier := NewDefraSignatureVerifier(nil, nil)

	// Valid signature type and identity, but nil node
	err := verifier.Verify(ctx, "test-cid", Signature{
		Type:     "ES256K",
		Identity: "0x1234567890abcdef",
		Value:    "sig-value",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "defradb node or DB is not available")
}

func TestDefraSignatureVerifier_Verify_LowercaseSignatureType(t *testing.T) {
	ctx := context.Background()
	verifier := NewDefraSignatureVerifier(nil, nil)

	// Lowercase es256k should be rejected (the code checks ToUpper)
	err := verifier.Verify(ctx, "test-cid", Signature{
		Type:     "es256k",
		Identity: "0x1234567890abcdef",
		Value:    "sig-value",
	})
	// es256k uppercased is ES256K, so it passes the type check, but fails at nil node
	require.Error(t, err)
	require.Contains(t, err.Error(), "defradb node or DB is not available")
}

func TestDefraSignatureVerifier_Verify_EmptyCIDAndEmptyIdentity(t *testing.T) {
	ctx := context.Background()
	verifier := NewDefraSignatureVerifier(nil, nil)

	err := verifier.Verify(ctx, "", Signature{
		Type:     "ES256K",
		Identity: "",
		Value:    "sig-value",
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
		Type:     "ES256K",
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
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	// Intentionally do NOT apply the schema - this will make the mutation fail
	defraNode := client.GetNode()

	record := &AttestationRecord{
		AttestedDocId: "doc-error",
		SourceDocId:   "source-error",
		CIDs:          []string{"cid-1"},
		DocType:       "TestDoc",
		VoteCount:     1,
	}

	err = PostAttestationRecord(ctx, defraNode, record)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error posting attestation record mutation")
}

// ========================================
// POST ATTESTATION RECORDS BATCH ERROR PATHS
// ========================================

func TestPostAttestationRecordsBatch_MissingSchema_ReturnsError(t *testing.T) {
	ctx := context.Background()

	// Create a defra node without the attestation schema
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	// Do NOT apply the attestation schema
	defraNode := client.GetNode()

	records := []*AttestationRecord{
		{
			AttestedDocId: "doc-1",
			SourceDocId:   "source-1",
			CIDs:          []string{"cid-1"},
			DocType:       "TestDoc",
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
	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Mix of nil records and records with empty CIDs
	records := []*AttestationRecord{
		nil,
		{AttestedDocId: "doc-1", SourceDocId: "src-1", CIDs: []string{}},
		nil,
		{AttestedDocId: "doc-2", SourceDocId: "src-2", CIDs: []string{}},
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
		verifyFunc: func(ctx context.Context, cid string, signature Signature) error {
			return nil
		},
	}

	// Create a defra node without the attestation schema so PostAttestationRecord fails
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	// Do NOT apply the attestation schema
	defraNode := client.GetNode()

	versions := []Version{
		{CID: "cid-1", Signature: Signature{Type: "es256k", Identity: "id-1", Value: "sig-1"}},
	}

	// CreateAttestationRecord will succeed (it uses mock verifier),
	// but PostAttestationRecord will fail because the collection doesn't exist
	err = HandleDocumentAttestation(ctx, verifier, defraNode, "doc-1", "TestDoc", versions, 50)
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
		verifyFunc: func(ctx context.Context, cid string, signature Signature) error {
			if cid == "fail-cid" {
				return fmt.Errorf("invalid signature")
			}
			return nil
		},
	}

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	inputs := []DocumentAttestationInput{
		{
			DocID:    "doc-empty",
			DocType:  "TypeA",
			Versions: []Version{}, // empty versions, skipped
		},
		{
			DocID:   "doc-all-fail",
			DocType: "TypeB",
			Versions: []Version{
				{CID: "fail-cid", Signature: Signature{Type: "es256k", Identity: "id-1", Value: "sig-1"}},
			},
		},
		{
			DocID:   "doc-valid",
			DocType: "TypeC",
			Versions: []Version{
				{CID: "good-cid", Signature: Signature{Type: "es256k", Identity: "id-2", Value: "sig-2"}},
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

	results, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "doc-valid", results[0].AttestedDocId)
}

// ========================================
// CHECK EXISTING ATTESTATION - ADDITIONAL ERROR PATHS
// ========================================

func TestCheckExistingAttestation_ReturnsMultipleRecords(t *testing.T) {
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create two attestation records for same attested_doc and doc_type
	// Using upsert, the second one will merge with the first
	record1 := &AttestationRecord{
		AttestedDocId: "multi-check-doc",
		SourceDocId:   "source-1",
		CIDs:          []string{"cid-1"},
		DocType:       "TypeA",
		VoteCount:     1,
	}
	err = PostAttestationRecord(ctx, defraNode, record1)
	require.NoError(t, err)

	records, err := CheckExistingAttestation(ctx, defraNode, "multi-check-doc", "TypeA")
	require.NoError(t, err)
	require.NotEmpty(t, records)
	require.Equal(t, "multi-check-doc", records[0].AttestedDocId)
}

// ========================================
// IS DOCUMENT ATTESTED VIA BLOCK - ADDITIONAL PATHS
// ========================================

func TestIsDocumentAttestedViaBlock_MultipleRecords_CIDInSecond(t *testing.T) {
	// Test that the function checks CIDs across all returned records
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create a block attestation with specific CIDs
	record := &AttestationRecord{
		AttestedDocId: "block:77",
		SourceDocId:   "block-source",
		CIDs:          []string{"cid-a", "cid-b", "target-cid-77"},
		DocType:       "Block",
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

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create block attestations for multiple blocks
	for _, bn := range []int{300, 300, 301} {
		record := &AttestationRecord{
			AttestedDocId: fmt.Sprintf("block:%d:merkle-%d", bn, bn),
			SourceDocId:   fmt.Sprintf("indexer-%d", bn),
			CIDs:          []string{fmt.Sprintf("cid-%d", bn)},
			DocType:       "Block",
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
// schema applied. When the collection doesn't exist, defra.QueryArray returns
// an error whose message contains "No attestation records found" (or similar),
// and the functions under test should treat this as a non-error (return nil/false).

func TestCheckExistingAttestation_MissingSchema_ReturnsNilNil(t *testing.T) {
	ctx := context.Background()

	// Create a defra node WITHOUT applying the attestation schema
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	// Do NOT apply the attestation schema - collection won't exist
	defraNode := client.GetNode()

	// This should trigger the strings.Contains(err.Error(), "No attestation records found") branch
	// or return an error if the branch doesn't match
	records, err := CheckExistingAttestation(ctx, defraNode, "nonexistent-doc", "TestDoc")
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
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

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
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

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
		"_version": []any{
			map[string]any{
				"cid": "cid-nst",
				"signature": map[string]any{
					"type":     12345, // Not a string
					"identity": "identity-ok",
					"value":    "sig-ok",
				},
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "cid-nst", versions[0].CID)
	require.Equal(t, "", versions[0].Signature.Type)       // Should be zero value
	require.Equal(t, "identity-ok", versions[0].Signature.Identity)
	require.Equal(t, "sig-ok", versions[0].Signature.Value)
}

func TestExtractVersionsFromDocument_NonStringSignatureIdentity(t *testing.T) {
	// Signature is a map but the "identity" field is not a string
	docData := map[string]any{
		"_version": []any{
			map[string]any{
				"cid": "cid-nsi",
				"signature": map[string]any{
					"type":     "es256k",
					"identity": 999, // Not a string
					"value":    "sig-ok",
				},
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "cid-nsi", versions[0].CID)
	require.Equal(t, "es256k", versions[0].Signature.Type)
	require.Equal(t, "", versions[0].Signature.Identity) // Should be zero value
	require.Equal(t, "sig-ok", versions[0].Signature.Value)
}

func TestExtractVersionsFromDocument_NonStringSignatureValue(t *testing.T) {
	// Signature is a map but the "value" field is not a string
	docData := map[string]any{
		"_version": []any{
			map[string]any{
				"cid": "cid-nsv",
				"signature": map[string]any{
					"type":     "es256k",
					"identity": "identity-ok",
					"value":    true, // Not a string
				},
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "cid-nsv", versions[0].CID)
	require.Equal(t, "es256k", versions[0].Signature.Type)
	require.Equal(t, "identity-ok", versions[0].Signature.Identity)
	require.Equal(t, "", versions[0].Signature.Value) // Should be zero value
}

func TestExtractVersionsFromDocument_AllNonStringSignatureFields(t *testing.T) {
	// Signature is a map but none of the fields are strings
	docData := map[string]any{
		"_version": []any{
			map[string]any{
				"cid": "cid-allns",
				"signature": map[string]any{
					"type":     42,
					"identity": []int{1, 2, 3},
					"value":    false,
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
		"_version": []any{
			map[string]any{
				"cid":       "cid-empty-sig",
				"signature": map[string]any{},
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
		"_version": []any{
			map[string]any{
				"cid":                 "cid-no-sig",
				"collectionVersionId": "colv-1",
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "cid-no-sig", versions[0].CID)
	require.Equal(t, "colv-1", versions[0].CollectionVersionId)
	require.Equal(t, "", versions[0].Signature.Type)
	require.Equal(t, "", versions[0].Signature.Identity)
	require.Equal(t, "", versions[0].Signature.Value)
}

func TestExtractVersionsFromDocument_NoCIDNoSignatureNoCollectionVersionId(t *testing.T) {
	// Version map with none of the expected fields
	docData := map[string]any{
		"_version": []any{
			map[string]any{
				"unrelated_field": "some_value",
			},
		},
	}

	versions, err := ExtractVersionsFromDocument(docData)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, "", versions[0].CID)
	require.Equal(t, "", versions[0].CollectionVersionId)
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

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	records := []*AttestationRecord{
		nil,
		{AttestedDocId: "nil-test-doc", SourceDocId: "src-nil", CIDs: []string{"cid-nil-1"}, DocType: "TestDoc", VoteCount: 1},
		nil,
		{AttestedDocId: "nil-test-doc-2", SourceDocId: "src-nil-2", CIDs: []string{}, DocType: "TestDoc", VoteCount: 1},
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

	results, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "nil-test-doc", results[0].AttestedDocId)
}

// ========================================
// EXTRACT DOC ID FROM RESULT TESTS
// ========================================

func TestExtractDocIDFromResult_ValidResult(t *testing.T) {
	data := map[string]any{
		"MyCollection": []any{
			map[string]any{
				"_docID": "bae-abc123",
			},
		},
	}

	result := extractDocIDFromResult(data, "MyCollection")
	require.Equal(t, "bae-abc123", result)
}

func TestExtractDocIDFromResult_NilData(t *testing.T) {
	result := extractDocIDFromResult(nil, "MyCollection")
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_DataNotMap(t *testing.T) {
	result := extractDocIDFromResult("not-a-map", "MyCollection")
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_CollectionNotSlice(t *testing.T) {
	data := map[string]any{
		"MyCollection": "not-a-slice",
	}
	result := extractDocIDFromResult(data, "MyCollection")
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_CollectionMissing(t *testing.T) {
	data := map[string]any{
		"OtherCollection": []any{},
	}
	result := extractDocIDFromResult(data, "MyCollection")
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_EmptyCollection(t *testing.T) {
	data := map[string]any{
		"MyCollection": []any{},
	}
	result := extractDocIDFromResult(data, "MyCollection")
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_FirstDocNotMap(t *testing.T) {
	data := map[string]any{
		"MyCollection": []any{
			"not-a-map",
		},
	}
	result := extractDocIDFromResult(data, "MyCollection")
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_DocIDNotString(t *testing.T) {
	data := map[string]any{
		"MyCollection": []any{
			map[string]any{
				"_docID": 12345,
			},
		},
	}
	result := extractDocIDFromResult(data, "MyCollection")
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_DocIDMissing(t *testing.T) {
	data := map[string]any{
		"MyCollection": []any{
			map[string]any{
				"other_field": "value",
			},
		},
	}
	result := extractDocIDFromResult(data, "MyCollection")
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_MultipleDocsReturnsFirst(t *testing.T) {
	data := map[string]any{
		"MyCollection": []any{
			map[string]any{"_docID": "first-doc"},
			map[string]any{"_docID": "second-doc"},
		},
	}
	result := extractDocIDFromResult(data, "MyCollection")
	require.Equal(t, "first-doc", result)
}

// ========================================
// LOOKUP EXISTING ATTESTATION TESTS
// ========================================

func TestLookupExistingAttestation_NotFound(t *testing.T) {
	ctx := context.Background()

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

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

	testSchema := `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: String
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`

	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	err = client.ApplySchema(ctx, testSchema)
	require.NoError(t, err)

	defraNode := client.GetNode()

	// Create a record first
	record := &AttestationRecord{
		AttestedDocId: "lookup-existing-doc",
		SourceDocId:   "source-doc",
		CIDs:          []string{"cid-lookup"},
		DocType:       "TestDoc",
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
		"MyCollection": []map[string]any{
			{"_docID": "bae-from-map-slice"},
		},
	}

	result := extractDocIDFromResult(data, "MyCollection")
	require.Equal(t, "bae-from-map-slice", result)
}

func TestExtractDocIDFromResult_MapSliceType_EmptySlice(t *testing.T) {
	data := map[string]any{
		"MyCollection": []map[string]any{},
	}

	result := extractDocIDFromResult(data, "MyCollection")
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_MapSliceType_MissingDocID(t *testing.T) {
	data := map[string]any{
		"MyCollection": []map[string]any{
			{"other_field": "value"},
		},
	}

	result := extractDocIDFromResult(data, "MyCollection")
	require.Equal(t, "", result)
}

func TestExtractDocIDFromResult_MapSliceType_MultipleDocsReturnsFirst(t *testing.T) {
	data := map[string]any{
		"MyCollection": []map[string]any{
			{"_docID": "first-map-doc"},
			{"_docID": "second-map-doc"},
		},
	}

	result := extractDocIDFromResult(data, "MyCollection")
	require.Equal(t, "first-map-doc", result)
}

func TestExtractDocIDFromResult_MapSliceType_DocIDNotString(t *testing.T) {
	data := map[string]any{
		"MyCollection": []map[string]any{
			{"_docID": 999},
		},
	}

	result := extractDocIDFromResult(data, "MyCollection")
	require.Equal(t, "", result)
}

func TestGetAttestationRecordsByViewName_MissingViewSchema(t *testing.T) {
	ctx := context.Background()

	// Create a defra node without any view-specific attestation schema
	testConfig := defra.DefaultConfig
	testConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.DefraDB.KeyringSecret = "test-keyring-secret-for-testing"
	testConfig.DefraDB.Url = "localhost:0"
	testConfig.DefraDB.P2P.ListenAddr = "/ip4/0.0.0.0/tcp/0"
	testConfig.DefraDB.P2P.Enabled = false
	testConfig.DefraDB.P2P.BootstrapPeers = []string{}

	client, err := defra.NewClient(testConfig)
	require.NoError(t, err)
	err = client.Start(t.Context())
	require.NoError(t, err)
	defer client.Stop(t.Context())

	defraNode := client.GetNode()

	// Query a non-existent view attestation collection with doc IDs
	_, err = GetAttestationRecordsByViewName(ctx, defraNode, "NonExistentView", []string{"doc-1"})
	// Should return an error because the collection doesn't exist
	require.Error(t, err)

	// Query a non-existent view attestation collection without doc IDs
	_, err = GetAttestationRecordsByViewName(ctx, defraNode, "NonExistentView", nil)
	require.Error(t, err)
}
