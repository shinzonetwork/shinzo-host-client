package host

import (
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	localschema "github.com/shinzonetwork/shinzo-host-client/pkg/schema"
)

const (
	testQueryBlocks   = "SELECT * FROM blocks"
	testQueryTest     = "SELECT * FROM test"
	testViewSDL       = "type TestView { field: String }"
	testSnapshotsPath = "/snapshots"

	// View / doc / signer test fixtures.
	testViewName     = "TestView"
	testTestView     = "testview"
	testContractKey1 = "0xkey1"
	testCreator1     = "creator1"
	testDoc1         = "doc1"
	testDoc2         = "doc2"
	testDoc3         = "doc3"
	testSignerAddr   = "0xsigner"
	testSignerName   = "test-signer"

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

	// JSON map key used by replication-filter tests.
	testMapKeyAddr = "addr"

	// Loopback address on a port the OS picks.
	testLoopbackAddr = "127.0.0.1:0"

	// Multi-peer libp2p multiaddr used by peer_discovery_test.go.
	testPeerMultiaddr = "/ip4/10.0.0.1/tcp/9171/p2p/12D3KooWNgSiQsYTdRon2r7439zSockGQxqwNSGFrwmdqTknhN6r"

	// Compact CID fixtures used in attestation batch tests.
	testCID1 = "cid1"
	testCID2 = "cid2"

	// Misc generic test fixtures.
	testAbc = "abc"
	testNum = "num"

	// Generic view-name placeholder reused across host_test and handler tests.
	testNameTest = "test"

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

	// DefraDB document metadata field names.
	defraFieldDocID = "_docID"

	// Document field names used as keys in test documents.
	gqlFieldAddress     = "address"
	gqlFieldTo          = "to"
	gqlFieldBlockNumber = "blockNumber"
	gqlFieldNumber      = "number"
	gqlFieldTopics      = "topics"
)

// testCollections are the Ethereum mainnet collections the tests use.
var testCollections = chain.EVM(chain.EthereumMainnet)

// testSchemaApplier creates the test chain's collections the way a host does.
var testSchemaApplier = localschema.ChainApplier{Tables: localschema.GetSchema(), Collections: testCollections}
