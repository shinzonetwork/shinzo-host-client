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
