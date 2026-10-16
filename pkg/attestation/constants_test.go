package attestation

const (
	testDocID = "doc-123"

	testKeyringSecret   = "test-keyring-secret-for-testing"
	testListenAddrLocal = "localhost:0"
	testListenAddrP2P   = "/ip4/0.0.0.0/tcp/0"

	// CIDs used across attestation-record test fixtures.
	testCID1  = "cid-1"
	testCID2  = "cid-2"
	testCID3  = "cid-3"
	testCID4  = "cid-4"
	testCIDA  = "cid-a"
	testCIDB  = "cid-b"
	testCIDA1 = "cid-a1"
	testCIDB1 = "cid-b1"

	// Source doc IDs used across attestation-record test fixtures.
	testSource1     = "source-1"
	testSource2     = "source-2"
	testBlockSource = "block-source"

	// Signature identity / value fixtures.
	testSigAabb = "aabb"
	testSigCcdd = "ccdd"

	// DefraDB doc-type and collection fixture names.
	testDocType        = "TestDoc"
	testDocTypeBlock   = "Block"
	testDocTypeA       = "TypeA"
	testDocTypeB       = "TypeB"
	testCollectionName = "MyCollection"
	testAttestedDocID  = "attested-doc-123"
	testParsePubKeyErr = "failed to parse public key"

	// DefraDB JSON metadata field names.
	jsonFieldDocID     = "_docID"
	jsonFieldSourceDoc = "source-doc"

	testDocAndAttestationSchema = `
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

	testAttestationRecordSchema = `
		type Ethereum__Mainnet__AttestationRecord {
			attested_doc: String @index
			source_doc: [String]
			CIDs: [String]
			doc_type: String @index
			vote_count: Int @crdt(type: pcounter)
		}
	`
)
