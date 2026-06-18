package attestation

import (
	"context"
	"fmt"
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	localschema "github.com/shinzonetwork/shinzo-host-client/pkg/schema"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

// Test fixtures, prefixed "ba-" to avoid colliding with literals in other attestation
// test files (goconst counts string occurrences per package).
const (
	baHostID  = "ba-host"
	baSignerA = "ba-signer-a"
	baSignerB = "ba-signer-b"
	baCID1    = "ba-cid-1"
	baCID2    = "ba-cid-2"
)

// stubSign is a deterministic SignFunc for tests. Returning the payload prefixed
// lets a test assert that hostSignature tracks the exact bytes that were signed.
func stubSign(payload string) (string, error) { return "sig:" + payload, nil }

// newBlockAttestationNode spins up an in-memory DefraDB with only the
// BlockAttestation collection registered.
func newBlockAttestationNode(t *testing.T) *node.Node {
	t.Helper()
	defraNode, err := defradb.StartDefraInstanceWithTestConfig(
		t, defradb.DefaultConfig,
		defradb.NewSchemaApplierFromProvidedSchema(localschema.BlockAttestationSchema),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = defraNode.Close(context.Background()) })
	return defraNode
}

func queryBlockAttestations(ctx context.Context, defraNode *node.Node, blockNumber int64) ([]constants.BlockAttestation, error) {
	query := fmt.Sprintf(`query {
		%s(filter: {blockNumber: {_eq: %d}}) {
			hostId
			blockNumber
			blockHash
			merkleRoot
			signerIdentities
			cids
			voteCount
			hostSignature
			shinzoHeight
			createdAt
		}
	}`, constants.CollectionBlockAttestation, blockNumber)
	return defradb.QueryArray[constants.BlockAttestation](ctx, defraNode, query)
}

// TestBlockAttestationSigningPayload_DeterministicAndSorted asserts the canonical
// payload is independent of the order signer identities were observed, which is
// what lets the accounting service reconstruct and verify hostSignature.
func TestBlockAttestationSigningPayload_DeterministicAndSorted(t *testing.T) {
	r1 := &constants.BlockAttestation{
		HostID: baHostID, BlockNumber: 42, BlockHash: "ba-hash", MerkleRoot: "ba-root",
		SignerIdentities: []string{baSignerB, baSignerA},
	}
	r2 := &constants.BlockAttestation{
		HostID: baHostID, BlockNumber: 42, BlockHash: "ba-hash", MerkleRoot: "ba-root",
		SignerIdentities: []string{baSignerA, baSignerB},
	}
	require.Equal(t, BlockAttestationSigningPayload(r1), BlockAttestationSigningPayload(r2))
	require.Equal(t, baHostID+"|42|ba-hash|ba-root|"+baSignerA+","+baSignerB, BlockAttestationSigningPayload(r1))
}

// TestPostBlockAttestation_VoteCountIsDistinctSigners posts the same
// (block, merkleRoot) for two distinct indexers, then re-posts one, and expects one
// record with voteCount == 2 (distinct signers), not 3 (write count).
func TestPostBlockAttestation_VoteCountIsDistinctSigners(t *testing.T) {
	ctx := context.Background()
	defraNode := newBlockAttestationNode(t)

	record := func(signer string) *constants.BlockAttestation {
		return &constants.BlockAttestation{
			HostID: baHostID, BlockNumber: 500, BlockHash: "ba-hash-500", MerkleRoot: "ba-root-500",
			SignerIdentities: []string{signer}, CIDs: []string{baCID1, baCID2},
			ShinzoHeight: 123, CreatedAt: 1000,
		}
	}

	require.NoError(t, PostBlockAttestation(ctx, defraNode, record(baSignerA), stubSign))
	require.NoError(t, PostBlockAttestation(ctx, defraNode, record(baSignerB), stubSign))
	require.NoError(t, PostBlockAttestation(ctx, defraNode, record(baSignerA), stubSign)) // duplicate write

	recs, err := queryBlockAttestations(ctx, defraNode, 500)
	require.NoError(t, err)
	require.Len(t, recs, 1, "same (block, merkleRoot) must merge into one record")

	rec := recs[0]
	require.ElementsMatch(t, []string{baSignerA, baSignerB}, rec.SignerIdentities)
	require.Equal(t, 2, rec.VoteCount, "voteCount must be distinct signers, not write count")
	require.Equal(t, baHostID, rec.HostID)
	require.Equal(t, "ba-hash-500", rec.BlockHash)
	require.Equal(t, int64(123), rec.ShinzoHeight)

	// hostSignature is re-signed over the merged, sorted signer set.
	want, _ := stubSign(BlockAttestationSigningPayload(&constants.BlockAttestation{
		HostID: baHostID, BlockNumber: 500, BlockHash: "ba-hash-500", MerkleRoot: "ba-root-500",
		SignerIdentities: []string{baSignerA, baSignerB},
	}))
	require.Equal(t, want, rec.HostSignature)
}

// TestPostBlockAttestation_DivergentMerkleRoots_SeparateRecords verifies two
// indexers reporting different merkle roots for the same block produce two
// separate records.
func TestPostBlockAttestation_DivergentMerkleRoots_SeparateRecords(t *testing.T) {
	ctx := context.Background()
	defraNode := newBlockAttestationNode(t)

	recX := &constants.BlockAttestation{HostID: baHostID, BlockNumber: 600, BlockHash: "ba-hash-x", MerkleRoot: "ba-root-x", SignerIdentities: []string{baSignerA}, CIDs: []string{"ba-cid-x1"}}
	recY := &constants.BlockAttestation{HostID: baHostID, BlockNumber: 600, BlockHash: "ba-hash-y", MerkleRoot: "ba-root-y", SignerIdentities: []string{baSignerB}, CIDs: []string{"ba-cid-y1"}}
	require.NoError(t, PostBlockAttestation(ctx, defraNode, recX, stubSign))
	require.NoError(t, PostBlockAttestation(ctx, defraNode, recY, stubSign))

	recs, err := queryBlockAttestations(ctx, defraNode, 600)
	require.NoError(t, err)
	require.Len(t, recs, 2, "different merkleRoots for the same block must be separate records")
}

// TestPostBlockAttestation_PreservesFields confirms the indexer identity, CID list,
// and signature are persisted, and voteCount starts at the single signer.
func TestPostBlockAttestation_PreservesFields(t *testing.T) {
	ctx := context.Background()
	defraNode := newBlockAttestationNode(t)

	rec := &constants.BlockAttestation{
		HostID: baHostID, BlockNumber: 700, BlockHash: "ba-hash-700", MerkleRoot: "ba-root-700",
		SignerIdentities: []string{baSignerA}, CIDs: []string{"ba-cid-p1", "ba-cid-p2", "ba-cid-p3"},
		ShinzoHeight: 9, CreatedAt: 42,
	}
	require.NoError(t, PostBlockAttestation(ctx, defraNode, rec, stubSign))

	recs, err := queryBlockAttestations(ctx, defraNode, 700)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	got := recs[0]
	require.Equal(t, []string{baSignerA}, got.SignerIdentities)
	require.Equal(t, []string{"ba-cid-p1", "ba-cid-p2", "ba-cid-p3"}, got.CIDs)
	require.Equal(t, 1, got.VoteCount)
	require.Equal(t, "ba-root-700", got.MerkleRoot)
	require.NotEmpty(t, got.HostSignature)
	require.Equal(t, int64(42), got.CreatedAt)
}

// TestPostBlockAttestation_DifferentHosts_SeparateRecords guards the host-scoped
// lookup: two hosts writing the same (blockNumber, merkleRoot) must keep separate
// records, so one host's write never merges into or re-signs another's. Without the
// hostId filter the second write would update the first host's record instead.
func TestPostBlockAttestation_DifferentHosts_SeparateRecords(t *testing.T) {
	ctx := context.Background()
	defraNode := newBlockAttestationNode(t)

	recA := &constants.BlockAttestation{
		HostID: "ba-host-a", BlockNumber: 800, BlockHash: "ba-hash-800", MerkleRoot: "ba-root-800",
		SignerIdentities: []string{baSignerA}, CIDs: []string{baCID1},
	}
	recB := &constants.BlockAttestation{
		HostID: "ba-host-b", BlockNumber: 800, BlockHash: "ba-hash-800", MerkleRoot: "ba-root-800",
		SignerIdentities: []string{baSignerB}, CIDs: []string{baCID1},
	}
	require.NoError(t, PostBlockAttestation(ctx, defraNode, recA, stubSign))
	require.NoError(t, PostBlockAttestation(ctx, defraNode, recB, stubSign))

	recs, err := queryBlockAttestations(ctx, defraNode, 800)
	require.NoError(t, err)
	require.Len(t, recs, 2, "same (block, merkleRoot) from different hosts must not merge")

	byHost := map[string][]string{}
	for _, r := range recs {
		byHost[r.HostID] = r.SignerIdentities
	}
	require.Equal(t, []string{baSignerA}, byHost["ba-host-a"], "host A record must keep only its own signer")
	require.Equal(t, []string{baSignerB}, byHost["ba-host-b"], "host B record must keep only its own signer")
}
