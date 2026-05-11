package host

const (
	testQueryBlocks   = "SELECT * FROM blocks"
	testQueryTest     = "SELECT * FROM test"
	testViewSDL       = "type TestView { field: String }"
	testSnapshotsPath = "/snapshots"

	// View / doc / signer test fixtures shared across handler/pipeline/peer tests.
	testViewName     = "TestView"
	testWASMViewID   = "WASMView_myview"
	testViewA        = "view-A"
	testViewB        = "view-B"
	testViewAB       = "view-A,view-B"
	testTestView     = "testview"
	testContractKey1 = "0xkey1"
	testCreator1     = "creator1"
	testDoc1         = "doc1"
	testDoc2         = "doc2"
	testDoc3         = "doc3"
	testSignerAddr   = "0xsigner"
	testSignerName   = "test-signer"
	testSigValue     = "testsig"

	// Hex test addresses for replication-filter tests (case-sensitive pairs).
	testHexLogUpper = "0xLOG"
	testHexLogLower = "0xlog"
	testHexSigUpper = "0xSIG"
	testHexSigLower = "0xsig"
	testHexAleUpper = "0xALE"
	testHexAleLower = "0xale"
	testHexWrong    = "0xWRONG"
	testHexTransfer = "0xtransfer"

	// Snapshot file-name fixtures used by snapshot_bootstrap_test.go.
	testSnapshotFirst   = "snap-1-100.tar"
	testSnapshotSecond  = "snap-100-200.tar"
	testSnapshotsPathQS = "snapshots"

	// Test signature crypto type (matches the ES256K identifier the
	// signature verifier accepts).
	testSigTypeES256K = "ES256K"

	// Test JSON map keys reused across pipeline tests.
	testMapKeyAddr = "addr"

	// GraphQL query-fragment prefixes asserted by query-builder tests.
	testQueryEthBlock       = "Ethereum__Mainnet__Block {"
	testQueryEthTransaction = "Ethereum__Mainnet__Transaction {"
	testQueryEthLog         = "Ethereum__Mainnet__Log {"

	// Multi-peer libp2p multiaddr used by peer_discovery_test.go.
	testPeerMultiaddr = "/ip4/10.0.0.1/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r"

	// CID fixture observed in snapshot_bootstrap_test.go.
	testSnapshotCID = "bafyreie7qr6d2gw5mvg7lrliqhk7opnbcpjfqkxvkm5pj5mzhtxhsb3q4"

	// Document docID prefix used in batch-write tests.
	testBaeOrig = "bae-orig"
	testBae1    = "bae-1"
	testBae2    = "bae-2"
	testBae3    = "bae-3"
	testBae4    = "bae-4"
	testBae5    = "bae-5"
	testBaeSrc1 = "bae-src1"

	// JSON map key for round-trip test fixtures in provenance tests.
	testJSONFieldResult = "result"

	// Compact CID fixtures used in attestation batch tests.
	testCID1 = "cid1"
	testCID2 = "cid2"

	// Misc generic test fixtures.
	testAbc  = "abc"
	testDone = "done"
	testNum  = "num"

	// Generic view-name placeholder reused across host_test and handler tests.
	testNameTest = "test"

	// JSON map keys observed in Defra _version test fixtures and attestation
	// signature blobs. They mirror the protocol-level field names that
	// pkg/attestation parses in production.
	testJSONFieldCID       = "cid"
	testJSONFieldSignature = "signature"
	testJSONFieldType      = "type"
	testJSONFieldIdentity  = "identity"

	// "value" reused as both the signature-blob JSON field name and a generic
	// round-trip test value.
	testValue = "value"

	// Pubkey fixture stored in the signature blob `identity` field.
	testIdentityPubkey = "testpubkey"

	// GraphQL filter-syntax prefix asserted in query-builder negative checks.
	testQueryFilterPrefix = "filter:"

	// Snapshot file-name fixtures used in snapshot_bootstrap_test.go.
	testSnapName200_300 = "snap-200-300.tar"
	testSnapName300_400 = "snap-300-400.tar"

	// Block-signature merkle-root fixtures.
	testMerkleRoot1 = "root1"
	testMerkleRoot2 = "root2"

	// CID-name fixtures used in attestation batch tests.
	testCIDName1 = "cid-1"
	testCIDName2 = "cid-2"

	// Subtest names reused across multiple replication-filter test functions.
	testNameMatchingAddressAllowed = "matching address allowed"
	testNameMissingKey             = "missing key"
)
