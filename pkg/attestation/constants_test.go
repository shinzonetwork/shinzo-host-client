package attestation

const (
	testDocID       = "doc-123"
	testSourceDocID = "source-doc-456"

	testKeyringSecret   = "test-keyring-secret-for-testing"
	testListenAddrLocal = "localhost:0"
	testListenAddrP2P   = "/ip4/0.0.0.0/tcp/0"

	// CIDs used across attestation-record test fixtures.
	testCID1   = "cid-1"
	testCID2   = "cid-2"
	testCID3   = "cid-3"
	testCID4   = "cid-4"
	testCIDA   = "cid-a"
	testCIDB   = "cid-b"
	testCIDABC = "cid-abc"
	testCIDA1  = "cid-a1"
	testCIDB1  = "cid-b1"

	// Source doc IDs used across attestation-record test fixtures.
	testSource1     = "source-1"
	testSource2     = "source-2"
	testBlockSource = "block-source"

	// Signature identity / value fixtures.
	testIdentity1    = "identity-1"
	testIdentity2    = "identity-2"
	testIdentityABC  = "identity-abc"
	testIDOne        = "id-1"
	testIDTwo        = "id-2"
	testSig1         = "sig-1"
	testSig2         = "sig-2"
	testSigValue     = "sig-value"
	testSignature1   = "signature-1"
	testSignature2   = "signature-2"
	testSignature123 = "signature123"
	testSigAabb      = "aabb"
	testSigCcdd      = "ccdd"

	// DefraDB doc-type and collection fixture names.
	testDocType        = "TestDoc"
	testDocTypeBlock   = "Block"
	testDocTypeA       = "TypeA"
	testDocTypeB       = "TypeB"
	testCollectionName = "MyCollection"
	testAttestedDocID  = "attested-doc-123"
	testDocID1         = "doc-1"
	testDocID2         = "doc-2"
	testCollVersionID  = "colv-1"
	testParsePubKeyErr = "failed to parse public key"

	// DefraDB JSON metadata field names.
	jsonFieldVersion       = "_version"
	jsonFieldDocID         = "_docID"
	jsonFieldCID           = "cid"
	jsonFieldSignature     = "signature"
	jsonFieldIdentity      = "identity"
	jsonFieldType          = "type"
	jsonFieldValue         = "value"
	jsonFieldCollectionVer = "collectionVersionId"
	jsonFieldSourceDoc     = "source-doc"

	// Lowercase signature-type ID used in test signatures.
	sigTypeES256KLowerHex = "es256k"

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
