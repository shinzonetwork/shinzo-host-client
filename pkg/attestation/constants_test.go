package attestation

const (
	testDocID       = "doc-123"
	testSourceDocID = "source-doc-456"

	testKeyringSecret   = "test-keyring-secret-for-testing"
	testListenAddrLocal = "localhost:0"
	testListenAddrP2P   = "/ip4/0.0.0.0/tcp/0"

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
